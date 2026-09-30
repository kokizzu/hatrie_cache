package hatSql

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

const (
	MaxSQLSinkFrontierCheckpointEntries = 65536
	MaxSQLSinkFrontierMessageNameBytes  = 256
)

var (
	ErrSQLSinkFrontierCheckpointNil       = errors.New("SQL sink frontier checkpoint coordinator is nil")
	ErrSQLSinkFrontierCheckpointDuplicate = errors.New("SQL sink frontier checkpoint is duplicated")
	ErrSQLSinkFrontierMessageInvalid      = errors.New("SQL sink frontier message is invalid")
	ErrSQLSinkFrontierMessageNotEmitted   = errors.New("SQL sink frontier message was not emitted")
	ErrSQLSinkFrontierMessageConflict     = errors.New("SQL sink frontier message conflicts with emitted state")
	ErrSQLSinkFrontierMessageStale        = errors.New("SQL sink frontier message is stale")
)

// SQLSinkFrontierMessage identifies one emitted progress-only message. The
// subscription ID, revision, frontier, and completion bit are part of the
// message identity so a checkpoint cannot acknowledge an unrelated frame that
// merely carries the same numeric frontier.
type SQLSinkFrontierMessage struct {
	Sink           string `json:"sink"`
	Partition      string `json:"partition"`
	SubscriptionID uint64 `json:"subscription_id"`
	Revision       uint64 `json:"revision"`
	Frontier       uint64 `json:"frontier"`
	Complete       bool   `json:"complete"`
}

// SQLSinkFrontierCheckpoint is the latest emitted message for one sink
// partition and whether that exact message has been durably acknowledged.
// An unacknowledged entry is intentionally retained for replay after restart.
type SQLSinkFrontierCheckpoint struct {
	Message      SQLSinkFrontierMessage `json:"message"`
	Acknowledged bool                   `json:"acknowledged"`
}

// SQLSinkFrontierCheckpointCoordinator couples progress acknowledgement to an
// exact emitted frontier message. It retains only one bounded checkpoint per
// sink partition and supports caller-owned durable Snapshot/Restore.
type SQLSinkFrontierCheckpointCoordinator struct {
	mu          sync.RWMutex
	checkpoints map[sqlSinkProgressKey]SQLSinkFrontierCheckpoint
}

// NewSQLSinkFrontierCheckpointCoordinator creates an empty coordinator.
func NewSQLSinkFrontierCheckpointCoordinator() *SQLSinkFrontierCheckpointCoordinator {
	return &SQLSinkFrontierCheckpointCoordinator{
		checkpoints: make(map[sqlSinkProgressKey]SQLSinkFrontierCheckpoint),
	}
}

// Emit records that a progress-only frontier message was emitted. Re-emitting
// the exact same message is an idempotent no-op. A different message at the
// same frontier is rejected because it could acknowledge a different stream
// revision.
func (coordinator *SQLSinkFrontierCheckpointCoordinator) Emit(message SQLSinkFrontierMessage) (bool, error) {
	if coordinator == nil {
		return false, ErrSQLSinkFrontierCheckpointNil
	}
	key, normalized, err := normalizeSQLSinkFrontierMessage(message)
	if err != nil {
		return false, err
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	coordinator.ensureMapLocked()
	current, found := coordinator.checkpoints[key]
	if !found {
		coordinator.checkpoints[key] = SQLSinkFrontierCheckpoint{Message: normalized}
		return true, nil
	}
	if normalized.Frontier < current.Message.Frontier {
		return false, ErrSQLSinkFrontierMessageStale
	}
	if normalized.Frontier == current.Message.Frontier {
		if normalized != current.Message {
			return false, ErrSQLSinkFrontierMessageConflict
		}
		return false, nil
	}
	coordinator.checkpoints[key] = SQLSinkFrontierCheckpoint{Message: normalized}
	return true, nil
}

// Acknowledge records durable acknowledgement of the exact message previously
// passed to Emit. A message that was not emitted, or was superseded by a
// newer frontier message, cannot advance the checkpoint.
func (coordinator *SQLSinkFrontierCheckpointCoordinator) Acknowledge(message SQLSinkFrontierMessage) (bool, error) {
	if coordinator == nil {
		return false, ErrSQLSinkFrontierCheckpointNil
	}
	key, normalized, err := normalizeSQLSinkFrontierMessage(message)
	if err != nil {
		return false, err
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	current, found := coordinator.checkpoints[key]
	if !found {
		return false, ErrSQLSinkFrontierMessageNotEmitted
	}
	if normalized != current.Message {
		if normalized.Frontier == current.Message.Frontier {
			return false, ErrSQLSinkFrontierMessageConflict
		}
		return false, ErrSQLSinkFrontierMessageNotEmitted
	}
	if current.Acknowledged {
		return false, nil
	}
	current.Acknowledged = true
	coordinator.checkpoints[key] = current
	return true, nil
}

// Checkpoint returns one independently owned checkpoint for a sink partition.
func (coordinator *SQLSinkFrontierCheckpointCoordinator) Checkpoint(sink, partition string) (SQLSinkFrontierCheckpoint, bool) {
	if coordinator == nil {
		return SQLSinkFrontierCheckpoint{}, false
	}
	sink = strings.TrimSpace(sink)
	partition = strings.TrimSpace(partition)
	if sink == "" || partition == "" {
		return SQLSinkFrontierCheckpoint{}, false
	}
	coordinator.mu.RLock()
	defer coordinator.mu.RUnlock()
	checkpoint, found := coordinator.checkpoints[sqlSinkProgressKey{sink: sink, partition: partition}]
	return checkpoint, found
}

// Snapshot returns deterministic, independently owned checkpoints for durable
// storage. Unacknowledged entries are included so recovery can replay them.
func (coordinator *SQLSinkFrontierCheckpointCoordinator) Snapshot() []SQLSinkFrontierCheckpoint {
	if coordinator == nil {
		return nil
	}
	coordinator.mu.RLock()
	defer coordinator.mu.RUnlock()
	snapshot := make([]SQLSinkFrontierCheckpoint, 0, len(coordinator.checkpoints))
	for _, checkpoint := range coordinator.checkpoints {
		snapshot = append(snapshot, checkpoint)
	}
	sort.Slice(snapshot, func(left, right int) bool {
		if snapshot[left].Message.Sink != snapshot[right].Message.Sink {
			return snapshot[left].Message.Sink < snapshot[right].Message.Sink
		}
		if snapshot[left].Message.Partition != snapshot[right].Message.Partition {
			return snapshot[left].Message.Partition < snapshot[right].Message.Partition
		}
		return snapshot[left].Message.Frontier < snapshot[right].Message.Frontier
	})
	return snapshot
}

// Restore atomically replaces all checkpoints. Invalid or duplicate input
// leaves the existing coordinator unchanged.
func (coordinator *SQLSinkFrontierCheckpointCoordinator) Restore(snapshot []SQLSinkFrontierCheckpoint) error {
	if coordinator == nil {
		return ErrSQLSinkFrontierCheckpointNil
	}
	if len(snapshot) > MaxSQLSinkFrontierCheckpointEntries {
		return fmt.Errorf("%w: %d entries exceeds %d", ErrSQLSinkFrontierMessageInvalid, len(snapshot), MaxSQLSinkFrontierCheckpointEntries)
	}
	replacement := make(map[sqlSinkProgressKey]SQLSinkFrontierCheckpoint, len(snapshot))
	for _, checkpoint := range snapshot {
		key, normalized, err := normalizeSQLSinkFrontierMessage(checkpoint.Message)
		if err != nil {
			return err
		}
		if _, found := replacement[key]; found {
			return ErrSQLSinkFrontierCheckpointDuplicate
		}
		replacement[key] = SQLSinkFrontierCheckpoint{Message: normalized, Acknowledged: checkpoint.Acknowledged}
	}
	coordinator.mu.Lock()
	coordinator.checkpoints = replacement
	coordinator.mu.Unlock()
	return nil
}

// NewSQLSinkFrontierMessageFromBatch converts a progress-only differential
// subscription batch into the exact message identity tracked by this
// coordinator. Row-bearing batches must be acknowledged only after a later
// progress frame is emitted.
func NewSQLSinkFrontierMessageFromBatch(sink, partition string, batch QuerySubscriptionDeltaBatch) (SQLSinkFrontierMessage, error) {
	message := SQLSinkFrontierMessage{
		Sink:           sink,
		Partition:      partition,
		SubscriptionID: batch.ID,
		Revision:       batch.Revision,
		Frontier:       batch.Frontier,
		Complete:       batch.Complete,
	}
	if !batch.Progress || batch.Reset || len(batch.Columns) != 0 || len(batch.Deltas) != 0 {
		return SQLSinkFrontierMessage{}, ErrSQLSinkFrontierMessageInvalid
	}
	_, normalized, err := normalizeSQLSinkFrontierMessage(message)
	if err != nil {
		return SQLSinkFrontierMessage{}, err
	}
	return normalized, nil
}

// Progress converts an emitted message to the existing sink progress shape.
func (message SQLSinkFrontierMessage) Progress() SQLSinkProgress {
	return SQLSinkProgress{Sink: message.Sink, Partition: message.Partition, Frontier: message.Frontier}
}

func (coordinator *SQLSinkFrontierCheckpointCoordinator) ensureMapLocked() {
	if coordinator.checkpoints == nil {
		coordinator.checkpoints = make(map[sqlSinkProgressKey]SQLSinkFrontierCheckpoint)
	}
}

func normalizeSQLSinkFrontierMessage(message SQLSinkFrontierMessage) (sqlSinkProgressKey, SQLSinkFrontierMessage, error) {
	message.Sink = strings.TrimSpace(message.Sink)
	message.Partition = strings.TrimSpace(message.Partition)
	if message.Sink == "" || message.Partition == "" || message.SubscriptionID == 0 || message.Revision == 0 || strings.IndexByte(message.Sink, 0) >= 0 || strings.IndexByte(message.Partition, 0) >= 0 || len(message.Sink) > MaxSQLSinkFrontierMessageNameBytes || len(message.Partition) > MaxSQLSinkFrontierMessageNameBytes {
		return sqlSinkProgressKey{}, SQLSinkFrontierMessage{}, ErrSQLSinkFrontierMessageInvalid
	}
	return sqlSinkProgressKey{sink: message.Sink, partition: message.Partition}, message, nil
}
