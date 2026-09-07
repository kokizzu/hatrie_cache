package hatSql

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

var (
	// ErrSQLSinkCommitNil reports a nil commit coordinator.
	ErrSQLSinkCommitNil = errors.New("SQL sink commit coordinator is nil")
	// ErrSQLSinkCommitInvalid reports a missing or malformed commit.
	ErrSQLSinkCommitInvalid = errors.New("SQL sink commit is invalid")
	// ErrSQLSinkCommitConflict reports reuse of a transaction ID with a
	// different sink progress definition.
	ErrSQLSinkCommitConflict = errors.New("SQL sink commit conflicts with an existing transaction")
	// ErrSQLSinkCommitApplyRequired reports a nil sink commit callback.
	ErrSQLSinkCommitApplyRequired = errors.New("SQL sink commit callback is required")
	// ErrSQLSinkCommitInFlight reports restore while a commit callback is
	// still running.
	ErrSQLSinkCommitInFlight = errors.New("SQL sink commit is in flight")
	// ErrSQLSinkCommitApplyPanic reports a callback panic to callers that were
	// waiting for the panicking callback. The callback owner receives the
	// original panic after the coordinator state has been cleaned up.
	ErrSQLSinkCommitApplyPanic = errors.New("SQL sink commit callback panicked")
)

// SQLSinkCommit identifies one sink transaction and the frontiers it
// acknowledges. Progress entries must all belong to Sink and use distinct
// sink partitions.
type SQLSinkCommit struct {
	Sink          string            `json:"sink"`
	TransactionID string            `json:"transaction_id"`
	Progress      []SQLSinkProgress `json:"progress"`
}

type sqlSinkCommitKey struct {
	sink          string
	transactionID string
}

type sqlSinkCommitState struct {
	progress  []SQLSinkProgress
	done      chan struct{}
	committed bool
	err       error
}

// SQLSinkCommitCoordinator provides a single-flight, idempotent commit gate.
// It retains only committed transaction metadata and does not store sink
// payloads. Snapshot and Restore let callers persist the gate with sink
// transaction state.
type SQLSinkCommitCoordinator struct {
	mu      sync.Mutex
	commits map[sqlSinkCommitKey]*sqlSinkCommitState
}

// NewSQLSinkCommitCoordinator creates an empty sink commit coordinator.
func NewSQLSinkCommitCoordinator() *SQLSinkCommitCoordinator {
	return &SQLSinkCommitCoordinator{commits: make(map[sqlSinkCommitKey]*sqlSinkCommitState)}
}

// Commit invokes apply once for a new transaction. Concurrent calls for the
// same transaction wait for the first callback. A successful duplicate
// returns false without invoking its callback; a failed callback is removed
// so a later call can retry it.
func (coordinator *SQLSinkCommitCoordinator) Commit(commit SQLSinkCommit, apply func() error) (bool, error) {
	if coordinator == nil {
		return false, ErrSQLSinkCommitNil
	}
	if apply == nil {
		return false, ErrSQLSinkCommitApplyRequired
	}
	key, normalized, err := normalizeSQLSinkCommit(commit)
	if err != nil {
		return false, err
	}

	coordinator.mu.Lock()
	coordinator.ensureMapLocked()
	if existing, found := coordinator.commits[key]; found {
		if !equalSQLSinkProgress(existing.progress, normalized.Progress) {
			coordinator.mu.Unlock()
			return false, ErrSQLSinkCommitConflict
		}
		if existing.committed {
			coordinator.mu.Unlock()
			return false, nil
		}
		done := existing.done
		coordinator.mu.Unlock()
		<-done
		return false, existing.err
	}
	state := &sqlSinkCommitState{
		progress: normalized.Progress,
		done:     make(chan struct{}),
	}
	coordinator.commits[key] = state
	coordinator.mu.Unlock()

	panicked := true
	var panicValue any
	func() {
		defer func() {
			if panicked {
				panicValue = recover()
			}
		}()
		err = apply()
		panicked = false
	}()
	if panicked {
		coordinator.mu.Lock()
		state.err = fmt.Errorf("%w: %v", ErrSQLSinkCommitApplyPanic, panicValue)
		delete(coordinator.commits, key)
		close(state.done)
		coordinator.mu.Unlock()
		panic(panicValue)
	}
	coordinator.mu.Lock()
	if err == nil {
		state.committed = true
		close(state.done)
		coordinator.mu.Unlock()
		return true, nil
	}
	state.err = err
	delete(coordinator.commits, key)
	close(state.done)
	coordinator.mu.Unlock()
	return false, err
}

// Committed reports whether transactionID has already committed for sink.
func (coordinator *SQLSinkCommitCoordinator) Committed(sink, transactionID string) bool {
	if coordinator == nil {
		return false
	}
	sink = strings.TrimSpace(sink)
	transactionID = strings.TrimSpace(transactionID)
	if sink == "" || transactionID == "" {
		return false
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	state, found := coordinator.commits[sqlSinkCommitKey{sink: sink, transactionID: transactionID}]
	return found && state.committed
}

// Snapshot returns committed transactions in deterministic order with
// independently owned progress slices. In-flight transactions are omitted.
func (coordinator *SQLSinkCommitCoordinator) Snapshot() []SQLSinkCommit {
	if coordinator == nil {
		return nil
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	snapshot := make([]SQLSinkCommit, 0, len(coordinator.commits))
	for key, state := range coordinator.commits {
		if !state.committed {
			continue
		}
		snapshot = append(snapshot, SQLSinkCommit{
			Sink:          key.sink,
			TransactionID: key.transactionID,
			Progress:      cloneSQLSinkProgress(state.progress),
		})
	}
	sort.Slice(snapshot, func(left, right int) bool {
		if snapshot[left].Sink != snapshot[right].Sink {
			return snapshot[left].Sink < snapshot[right].Sink
		}
		return snapshot[left].TransactionID < snapshot[right].TransactionID
	})
	return snapshot
}

// Restore atomically replaces committed transaction metadata. Invalid or
// duplicate snapshots leave the current state unchanged.
func (coordinator *SQLSinkCommitCoordinator) Restore(snapshot []SQLSinkCommit) error {
	if coordinator == nil {
		return ErrSQLSinkCommitNil
	}
	replacement := make(map[sqlSinkCommitKey]*sqlSinkCommitState, len(snapshot))
	for _, commit := range snapshot {
		key, normalized, err := normalizeSQLSinkCommit(commit)
		if err != nil {
			return err
		}
		if _, found := replacement[key]; found {
			return fmt.Errorf("transaction %q for sink %q: %w", key.transactionID, key.sink, ErrSQLSinkCommitInvalid)
		}
		replacement[key] = &sqlSinkCommitState{
			progress:  normalized.Progress,
			done:      closedSQLSinkCommitChannel(),
			committed: true,
		}
	}

	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	for _, state := range coordinator.commits {
		if !state.committed {
			return ErrSQLSinkCommitInFlight
		}
	}
	coordinator.commits = replacement
	return nil
}

func (coordinator *SQLSinkCommitCoordinator) ensureMapLocked() {
	if coordinator.commits == nil {
		coordinator.commits = make(map[sqlSinkCommitKey]*sqlSinkCommitState)
	}
}

func normalizeSQLSinkCommit(commit SQLSinkCommit) (sqlSinkCommitKey, SQLSinkCommit, error) {
	commit.Sink = strings.TrimSpace(commit.Sink)
	commit.TransactionID = strings.TrimSpace(commit.TransactionID)
	if commit.Sink == "" || commit.TransactionID == "" || len(commit.Progress) == 0 {
		return sqlSinkCommitKey{}, SQLSinkCommit{}, ErrSQLSinkCommitInvalid
	}
	normalized := make([]SQLSinkProgress, len(commit.Progress))
	seen := make(map[sqlSinkProgressKey]struct{}, len(commit.Progress))
	for index, progress := range commit.Progress {
		key, value, err := normalizeSQLSinkProgress(progress)
		if err != nil || value.Sink != commit.Sink {
			return sqlSinkCommitKey{}, SQLSinkCommit{}, fmt.Errorf("transaction %q: %w", commit.TransactionID, ErrSQLSinkCommitInvalid)
		}
		if _, found := seen[key]; found {
			return sqlSinkCommitKey{}, SQLSinkCommit{}, fmt.Errorf("transaction %q: %w", commit.TransactionID, ErrSQLSinkCommitInvalid)
		}
		seen[key] = struct{}{}
		normalized[index] = value
	}
	sort.Slice(normalized, func(left, right int) bool {
		return normalized[left].Partition < normalized[right].Partition
	})
	commit.Progress = normalized
	return sqlSinkCommitKey{sink: commit.Sink, transactionID: commit.TransactionID}, commit, nil
}

func equalSQLSinkProgress(left, right []SQLSinkProgress) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func cloneSQLSinkProgress(progress []SQLSinkProgress) []SQLSinkProgress {
	if len(progress) == 0 {
		return nil
	}
	clone := make([]SQLSinkProgress, len(progress))
	copy(clone, progress)
	return clone
}

func closedSQLSinkCommitChannel() chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}
