package hatSql

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

var (
	// ErrSQLSourceIngestionNil reports a nil ingestion coordinator.
	ErrSQLSourceIngestionNil = errors.New("SQL source ingestion coordinator is nil")
	// ErrSQLSourceIngestionInvalid reports a missing or malformed ingestion.
	ErrSQLSourceIngestionInvalid = errors.New("SQL source ingestion is invalid")
	// ErrSQLSourceIngestionConflict reports reuse of a transaction ID with a
	// different source offset definition.
	ErrSQLSourceIngestionConflict = errors.New("SQL source ingestion conflicts with an existing transaction")
	// ErrSQLSourceIngestionApplyRequired reports a nil ingestion callback.
	ErrSQLSourceIngestionApplyRequired = errors.New("SQL source ingestion callback is required")
	// ErrSQLSourceIngestionInFlight reports restore while an ingestion callback
	// is still running.
	ErrSQLSourceIngestionInFlight = errors.New("SQL source ingestion is in flight")
	// ErrSQLSourceIngestionApplyPanic reports a callback panic to callers that
	// were waiting for the panicking callback.
	ErrSQLSourceIngestionApplyPanic = errors.New("SQL source ingestion callback panicked")
)

// SQLSourceIngestion identifies one source transaction and its complete
// offset group. All offsets must belong to Source.
type SQLSourceIngestion struct {
	Source      string               `json:"source"`
	Transaction SQLSourceTransaction `json:"transaction"`
}

type sqlSourceIngestionKey struct {
	source        string
	transactionID string
}

type sqlSourceIngestionState struct {
	offsets   []SQLSourceOffset
	done      chan struct{}
	committed bool
	err       error
}

// SQLSourceIngestionCoordinator provides a single-flight, idempotent source
// transaction gate. It stores only transaction metadata and does not retain
// ingested rows or source payloads.
type SQLSourceIngestionCoordinator struct {
	mu         sync.Mutex
	ingestions map[sqlSourceIngestionKey]*sqlSourceIngestionState
}

// NewSQLSourceIngestionCoordinator creates an empty source ingestion
// coordinator.
func NewSQLSourceIngestionCoordinator() *SQLSourceIngestionCoordinator {
	return &SQLSourceIngestionCoordinator{ingestions: make(map[sqlSourceIngestionKey]*sqlSourceIngestionState)}
}

// Ingest invokes apply once for a new source transaction. Concurrent calls for
// the same transaction wait for the first callback. A successful duplicate
// returns false without invoking its callback; a failed callback is removed so
// a later call can retry it.
func (coordinator *SQLSourceIngestionCoordinator) Ingest(ingestion SQLSourceIngestion, apply func() error) (bool, error) {
	if coordinator == nil {
		return false, ErrSQLSourceIngestionNil
	}
	if apply == nil {
		return false, ErrSQLSourceIngestionApplyRequired
	}
	key, normalized, err := normalizeSQLSourceIngestion(ingestion)
	if err != nil {
		return false, err
	}

	coordinator.mu.Lock()
	coordinator.ensureMapLocked()
	if existing, found := coordinator.ingestions[key]; found {
		if !equalSQLSourceOffsets(existing.offsets, normalized.Transaction.Offsets) {
			coordinator.mu.Unlock()
			return false, ErrSQLSourceIngestionConflict
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
	state := &sqlSourceIngestionState{
		offsets: normalized.Transaction.Offsets,
		done:    make(chan struct{}),
	}
	coordinator.ingestions[key] = state
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
		state.err = fmt.Errorf("%w: %v", ErrSQLSourceIngestionApplyPanic, panicValue)
		delete(coordinator.ingestions, key)
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
	delete(coordinator.ingestions, key)
	close(state.done)
	coordinator.mu.Unlock()
	return false, err
}

// Committed reports whether transactionID has already committed for source.
func (coordinator *SQLSourceIngestionCoordinator) Committed(source, transactionID string) bool {
	if coordinator == nil {
		return false
	}
	source = strings.TrimSpace(source)
	transactionID = strings.TrimSpace(transactionID)
	if source == "" || transactionID == "" {
		return false
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	state, found := coordinator.ingestions[sqlSourceIngestionKey{source: source, transactionID: transactionID}]
	return found && state.committed
}

// Snapshot returns committed transactions in deterministic order with
// independently owned offset slices. In-flight transactions are omitted.
func (coordinator *SQLSourceIngestionCoordinator) Snapshot() []SQLSourceIngestion {
	if coordinator == nil {
		return nil
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	snapshot := make([]SQLSourceIngestion, 0, len(coordinator.ingestions))
	for key, state := range coordinator.ingestions {
		if !state.committed {
			continue
		}
		snapshot = append(snapshot, SQLSourceIngestion{
			Source: key.source,
			Transaction: SQLSourceTransaction{
				ID:      key.transactionID,
				Offsets: cloneSQLSourceOffsets(state.offsets),
			},
		})
	}
	sort.Slice(snapshot, func(left, right int) bool {
		if snapshot[left].Source != snapshot[right].Source {
			return snapshot[left].Source < snapshot[right].Source
		}
		return snapshot[left].Transaction.ID < snapshot[right].Transaction.ID
	})
	return snapshot
}

// Restore atomically replaces committed transaction metadata. Invalid or
// duplicate snapshots leave the current state unchanged.
func (coordinator *SQLSourceIngestionCoordinator) Restore(snapshot []SQLSourceIngestion) error {
	if coordinator == nil {
		return ErrSQLSourceIngestionNil
	}
	replacement := make(map[sqlSourceIngestionKey]*sqlSourceIngestionState, len(snapshot))
	for _, ingestion := range snapshot {
		key, normalized, err := normalizeSQLSourceIngestion(ingestion)
		if err != nil {
			return err
		}
		if _, found := replacement[key]; found {
			return fmt.Errorf("transaction %q for source %q: %w", key.transactionID, key.source, ErrSQLSourceIngestionInvalid)
		}
		replacement[key] = &sqlSourceIngestionState{
			offsets:   normalized.Transaction.Offsets,
			done:      closedSQLSourceIngestionChannel(),
			committed: true,
		}
	}

	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	for _, state := range coordinator.ingestions {
		if !state.committed {
			return ErrSQLSourceIngestionInFlight
		}
	}
	coordinator.ingestions = replacement
	return nil
}

func (coordinator *SQLSourceIngestionCoordinator) ensureMapLocked() {
	if coordinator.ingestions == nil {
		coordinator.ingestions = make(map[sqlSourceIngestionKey]*sqlSourceIngestionState)
	}
}

func normalizeSQLSourceIngestion(ingestion SQLSourceIngestion) (sqlSourceIngestionKey, SQLSourceIngestion, error) {
	ingestion.Source = strings.TrimSpace(ingestion.Source)
	ingestion.Transaction.ID = strings.TrimSpace(ingestion.Transaction.ID)
	if ingestion.Source == "" || ingestion.Transaction.ID == "" || len(ingestion.Transaction.Offsets) == 0 {
		return sqlSourceIngestionKey{}, SQLSourceIngestion{}, ErrSQLSourceIngestionInvalid
	}
	normalized := make([]SQLSourceOffset, len(ingestion.Transaction.Offsets))
	seen := make(map[sqlSourceOffsetKey]struct{}, len(ingestion.Transaction.Offsets))
	for index, offset := range ingestion.Transaction.Offsets {
		key, value, err := normalizeSQLSourceOffset(offset)
		if err != nil || value.Source != ingestion.Source {
			return sqlSourceIngestionKey{}, SQLSourceIngestion{}, fmt.Errorf("transaction %q: %w", ingestion.Transaction.ID, ErrSQLSourceIngestionInvalid)
		}
		if _, found := seen[key]; found {
			return sqlSourceIngestionKey{}, SQLSourceIngestion{}, fmt.Errorf("transaction %q: %w", ingestion.Transaction.ID, ErrSQLSourceIngestionInvalid)
		}
		seen[key] = struct{}{}
		normalized[index] = value
	}
	sort.Slice(normalized, func(left, right int) bool {
		return normalized[left].Partition < normalized[right].Partition
	})
	ingestion.Transaction.Offsets = normalized
	return sqlSourceIngestionKey{source: ingestion.Source, transactionID: ingestion.Transaction.ID}, ingestion, nil
}

func equalSQLSourceOffsets(left, right []SQLSourceOffset) bool {
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

func cloneSQLSourceOffsets(offsets []SQLSourceOffset) []SQLSourceOffset {
	if len(offsets) == 0 {
		return nil
	}
	clone := make([]SQLSourceOffset, len(offsets))
	copy(clone, offsets)
	return clone
}

func closedSQLSourceIngestionChannel() chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}
