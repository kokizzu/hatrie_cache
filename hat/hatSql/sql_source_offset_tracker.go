package hatSql

import (
	"errors"
	"sort"
	"strings"
	"sync"
)

var (
	// ErrSQLSourceOffsetTrackerNil reports a nil tracker receiver.
	ErrSQLSourceOffsetTrackerNil = errors.New("SQL source offset tracker is nil")
	// ErrSQLSourceOffsetInvalid reports an empty source or partition name.
	ErrSQLSourceOffsetInvalid = errors.New("SQL source offset is invalid")
	// ErrSQLSourceOffsetDuplicate reports duplicate source/partition entries in
	// one atomic batch or restore snapshot.
	ErrSQLSourceOffsetDuplicate = errors.New("SQL source offset is duplicated")
)

// SQLSourceOffset identifies a monotonically advancing source partition
// position. Offset zero is valid; Kafka-style consumers normally persist the
// next position to read or the last position successfully processed according
// to their own convention.
type SQLSourceOffset struct {
	Source    string `json:"source"`
	Partition string `json:"partition"`
	Offset    uint64 `json:"offset"`
}

type sqlSourceOffsetKey struct {
	source    string
	partition string
}

// SQLSourceOffsetTracker stores one high-watermark per source partition. It
// is safe for concurrent callers and keeps no source records or payloads.
type SQLSourceOffsetTracker struct {
	mu      sync.RWMutex
	offsets map[sqlSourceOffsetKey]uint64
}

// NewSQLSourceOffsetTracker creates an empty source offset tracker.
func NewSQLSourceOffsetTracker() *SQLSourceOffsetTracker {
	return &SQLSourceOffsetTracker{offsets: make(map[sqlSourceOffsetKey]uint64)}
}

// Advance records offset when it is newer than the current partition
// high-watermark. It returns false for a replayed or older offset.
func (tracker *SQLSourceOffsetTracker) Advance(offset SQLSourceOffset) (bool, error) {
	if tracker == nil {
		return false, ErrSQLSourceOffsetTrackerNil
	}
	key, normalized, err := normalizeSQLSourceOffset(offset)
	if err != nil {
		return false, err
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if current, found := tracker.offsets[key]; found && normalized.Offset <= current {
		return false, nil
	}
	tracker.ensureMapLocked()
	tracker.offsets[key] = normalized.Offset
	return true, nil
}

// AdvanceBatch atomically validates and advances distinct source partitions.
// The returned count is the number of entries newer than their current
// high-watermarks; stale entries are valid no-ops.
func (tracker *SQLSourceOffsetTracker) AdvanceBatch(offsets []SQLSourceOffset) (int, error) {
	if tracker == nil {
		return 0, ErrSQLSourceOffsetTrackerNil
	}
	normalized := make([]SQLSourceOffset, len(offsets))
	keys := make(map[sqlSourceOffsetKey]struct{}, len(offsets))
	for index, offset := range offsets {
		key, value, err := normalizeSQLSourceOffset(offset)
		if err != nil {
			return 0, err
		}
		if _, found := keys[key]; found {
			return 0, ErrSQLSourceOffsetDuplicate
		}
		keys[key] = struct{}{}
		normalized[index] = value
	}

	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	advanced := 0
	for _, offset := range normalized {
		key := sqlSourceOffsetKey{source: offset.Source, partition: offset.Partition}
		if current, found := tracker.offsets[key]; found && offset.Offset <= current {
			continue
		}
		tracker.ensureMapLocked()
		tracker.offsets[key] = offset.Offset
		advanced++
	}
	return advanced, nil
}

// Offset returns the current high-watermark for source and partition.
func (tracker *SQLSourceOffsetTracker) Offset(source, partition string) (uint64, bool) {
	if tracker == nil {
		return 0, false
	}
	source = strings.TrimSpace(source)
	partition = strings.TrimSpace(partition)
	if source == "" || partition == "" {
		return 0, false
	}
	tracker.mu.RLock()
	defer tracker.mu.RUnlock()
	offset, found := tracker.offsets[sqlSourceOffsetKey{source: source, partition: partition}]
	return offset, found
}

// Snapshot returns an independently owned, deterministic list of current
// source offsets.
func (tracker *SQLSourceOffsetTracker) Snapshot() []SQLSourceOffset {
	if tracker == nil {
		return nil
	}
	tracker.mu.RLock()
	defer tracker.mu.RUnlock()
	snapshot := make([]SQLSourceOffset, 0, len(tracker.offsets))
	for key, offset := range tracker.offsets {
		snapshot = append(snapshot, SQLSourceOffset{Source: key.source, Partition: key.partition, Offset: offset})
	}
	sort.Slice(snapshot, func(left, right int) bool {
		if snapshot[left].Source != snapshot[right].Source {
			return snapshot[left].Source < snapshot[right].Source
		}
		return snapshot[left].Partition < snapshot[right].Partition
	})
	return snapshot
}

// Restore atomically replaces all tracked offsets with snapshot. It rejects
// invalid or duplicate entries without changing the current state.
func (tracker *SQLSourceOffsetTracker) Restore(snapshot []SQLSourceOffset) error {
	if tracker == nil {
		return ErrSQLSourceOffsetTrackerNil
	}
	replacement := make(map[sqlSourceOffsetKey]uint64, len(snapshot))
	for _, offset := range snapshot {
		key, normalized, err := normalizeSQLSourceOffset(offset)
		if err != nil {
			return err
		}
		if _, found := replacement[key]; found {
			return ErrSQLSourceOffsetDuplicate
		}
		replacement[key] = normalized.Offset
	}
	tracker.mu.Lock()
	tracker.offsets = replacement
	tracker.mu.Unlock()
	return nil
}

func (tracker *SQLSourceOffsetTracker) ensureMapLocked() {
	if tracker.offsets == nil {
		tracker.offsets = make(map[sqlSourceOffsetKey]uint64)
	}
}

func normalizeSQLSourceOffset(offset SQLSourceOffset) (sqlSourceOffsetKey, SQLSourceOffset, error) {
	offset.Source = strings.TrimSpace(offset.Source)
	offset.Partition = strings.TrimSpace(offset.Partition)
	if offset.Source == "" || offset.Partition == "" {
		return sqlSourceOffsetKey{}, SQLSourceOffset{}, ErrSQLSourceOffsetInvalid
	}
	return sqlSourceOffsetKey{source: offset.Source, partition: offset.Partition}, offset, nil
}
