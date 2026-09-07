package hatSql

import (
	"errors"
	"sort"
	"strings"
	"sync"
)

var (
	// ErrSQLSinkProgressNil reports a nil sink progress tracker.
	ErrSQLSinkProgressNil = errors.New("SQL sink progress tracker is nil")
	// ErrSQLSinkProgressInvalid reports an empty sink or partition name.
	ErrSQLSinkProgressInvalid = errors.New("SQL sink progress is invalid")
	// ErrSQLSinkProgressDuplicate reports duplicate sink/partition entries in
	// one atomic batch or restore snapshot.
	ErrSQLSinkProgressDuplicate = errors.New("SQL sink progress is duplicated")
)

// SQLSinkProgress identifies an acknowledged frontier for one sink
// partition. Frontier zero is valid and has the same source-defined meaning
// as the first acknowledged position.
type SQLSinkProgress struct {
	Sink      string `json:"sink"`
	Partition string `json:"partition"`
	Frontier  uint64 `json:"frontier"`
}

type sqlSinkProgressKey struct {
	sink      string
	partition string
}

// SQLSinkProgressTracker stores one monotone acknowledged frontier per sink
// partition. It does not retain payloads or delivery records.
type SQLSinkProgressTracker struct {
	mu        sync.RWMutex
	frontiers map[sqlSinkProgressKey]uint64
}

// NewSQLSinkProgressTracker creates an empty sink progress tracker.
func NewSQLSinkProgressTracker() *SQLSinkProgressTracker {
	return &SQLSinkProgressTracker{frontiers: make(map[sqlSinkProgressKey]uint64)}
}

// Acknowledge records frontier when it is newer than the current sink
// frontier. It returns false for a replayed or older acknowledgement.
func (tracker *SQLSinkProgressTracker) Acknowledge(progress SQLSinkProgress) (bool, error) {
	if tracker == nil {
		return false, ErrSQLSinkProgressNil
	}
	key, normalized, err := normalizeSQLSinkProgress(progress)
	if err != nil {
		return false, err
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if current, found := tracker.frontiers[key]; found && normalized.Frontier <= current {
		return false, nil
	}
	tracker.ensureMapLocked()
	tracker.frontiers[key] = normalized.Frontier
	return true, nil
}

// AcknowledgeBatch validates distinct sink partitions before applying the
// batch. Stale entries are valid no-ops and do not prevent newer entries from
// being acknowledged.
func (tracker *SQLSinkProgressTracker) AcknowledgeBatch(progress []SQLSinkProgress) (int, error) {
	if tracker == nil {
		return 0, ErrSQLSinkProgressNil
	}
	normalized := make([]SQLSinkProgress, len(progress))
	keys := make(map[sqlSinkProgressKey]struct{}, len(progress))
	for index, value := range progress {
		key, normalizedValue, err := normalizeSQLSinkProgress(value)
		if err != nil {
			return 0, err
		}
		if _, found := keys[key]; found {
			return 0, ErrSQLSinkProgressDuplicate
		}
		keys[key] = struct{}{}
		normalized[index] = normalizedValue
	}

	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	acknowledged := 0
	for _, value := range normalized {
		key := sqlSinkProgressKey{sink: value.Sink, partition: value.Partition}
		if current, found := tracker.frontiers[key]; found && value.Frontier <= current {
			continue
		}
		tracker.ensureMapLocked()
		tracker.frontiers[key] = value.Frontier
		acknowledged++
	}
	return acknowledged, nil
}

// Frontier returns the current acknowledged frontier for sink and partition.
func (tracker *SQLSinkProgressTracker) Frontier(sink, partition string) (uint64, bool) {
	if tracker == nil {
		return 0, false
	}
	sink = strings.TrimSpace(sink)
	partition = strings.TrimSpace(partition)
	if sink == "" || partition == "" {
		return 0, false
	}
	tracker.mu.RLock()
	defer tracker.mu.RUnlock()
	frontier, found := tracker.frontiers[sqlSinkProgressKey{sink: sink, partition: partition}]
	return frontier, found
}

// Snapshot returns an independently owned, deterministic list of current
// sink acknowledgements.
func (tracker *SQLSinkProgressTracker) Snapshot() []SQLSinkProgress {
	if tracker == nil {
		return nil
	}
	tracker.mu.RLock()
	defer tracker.mu.RUnlock()
	snapshot := make([]SQLSinkProgress, 0, len(tracker.frontiers))
	for key, frontier := range tracker.frontiers {
		snapshot = append(snapshot, SQLSinkProgress{Sink: key.sink, Partition: key.partition, Frontier: frontier})
	}
	sort.Slice(snapshot, func(left, right int) bool {
		if snapshot[left].Sink != snapshot[right].Sink {
			return snapshot[left].Sink < snapshot[right].Sink
		}
		return snapshot[left].Partition < snapshot[right].Partition
	})
	return snapshot
}

// Restore atomically replaces all tracked sink acknowledgements with
// snapshot. Invalid or duplicate entries leave the current state unchanged.
func (tracker *SQLSinkProgressTracker) Restore(snapshot []SQLSinkProgress) error {
	if tracker == nil {
		return ErrSQLSinkProgressNil
	}
	replacement := make(map[sqlSinkProgressKey]uint64, len(snapshot))
	for _, value := range snapshot {
		key, normalized, err := normalizeSQLSinkProgress(value)
		if err != nil {
			return err
		}
		if _, found := replacement[key]; found {
			return ErrSQLSinkProgressDuplicate
		}
		replacement[key] = normalized.Frontier
	}
	tracker.mu.Lock()
	tracker.frontiers = replacement
	tracker.mu.Unlock()
	return nil
}

func (tracker *SQLSinkProgressTracker) ensureMapLocked() {
	if tracker.frontiers == nil {
		tracker.frontiers = make(map[sqlSinkProgressKey]uint64)
	}
}

func normalizeSQLSinkProgress(progress SQLSinkProgress) (sqlSinkProgressKey, SQLSinkProgress, error) {
	progress.Sink = strings.TrimSpace(progress.Sink)
	progress.Partition = strings.TrimSpace(progress.Partition)
	if progress.Sink == "" || progress.Partition == "" {
		return sqlSinkProgressKey{}, SQLSinkProgress{}, ErrSQLSinkProgressInvalid
	}
	return sqlSinkProgressKey{sink: progress.Sink, partition: progress.Partition}, progress, nil
}
