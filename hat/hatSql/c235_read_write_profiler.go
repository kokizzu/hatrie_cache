package hatSql

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultSQLReadWriteProfilerMaxEntries bounds retained table/part/column
	// aggregates for a default profiler.
	DefaultSQLReadWriteProfilerMaxEntries = 1024
	maxSQLReadWriteProfilerEntries        = 65536
	maxSQLReadWriteProfilerLabelBytes     = 256
)

var (
	ErrSQLReadWriteProfilerClosed          = errors.New("hatSql: SQL read/write profiler is closed")
	ErrSQLReadWriteProfilerLimitInvalid    = errors.New("hatSql: SQL read/write profiler limit is invalid")
	ErrSQLReadWriteProfilerTableRequired   = errors.New("hatSql: SQL read/write profiler table is required")
	ErrSQLReadWriteProfilerPartRequired    = errors.New("hatSql: SQL read/write profiler part is required")
	ErrSQLReadWriteProfilerColumnRequired  = errors.New("hatSql: SQL read/write profiler column is required")
	ErrSQLReadWriteProfilerLabelTooLong    = errors.New("hatSql: SQL read/write profiler label is too long")
	ErrSQLReadWriteProfilerDurationInvalid = errors.New("hatSql: SQL read/write profiler duration is invalid")
)

// SQLReadWriteOperation identifies the direction of one physical task.
type SQLReadWriteOperation string

const (
	SQLReadWriteOperationRead  SQLReadWriteOperation = "read"
	SQLReadWriteOperationWrite SQLReadWriteOperation = "write"
)

// SQLReadWriteTaskSample contains caller-supplied counters for one physical
// table-part-column task. Storage adapters should report the bytes actually
// transferred, not an estimate based on logical row size.
type SQLReadWriteTaskSample struct {
	Rows      uint64        `json:"rows"`
	Bytes     uint64        `json:"bytes"`
	Duration  time.Duration `json:"duration"`
	Timestamp time.Time     `json:"timestamp"`
}

// SQLReadWriteTaskAggregate is the bounded sum for one direction and physical
// table-part-column identity. Numeric sums saturate instead of wrapping.
type SQLReadWriteTaskAggregate struct {
	Table         string                `json:"table"`
	Part          string                `json:"part"`
	Column        string                `json:"column"`
	Operation     SQLReadWriteOperation `json:"operation"`
	Tasks         uint64                `json:"tasks"`
	Rows          uint64                `json:"rows"`
	Bytes         uint64                `json:"bytes"`
	Duration      time.Duration         `json:"duration"`
	LastTimestamp time.Time             `json:"last_timestamp"`
}

// SQLReadWriteProfilerOptions configures a bounded, opt-in task aggregator.
// Zero selects the documented default.
type SQLReadWriteProfilerOptions struct {
	MaxEntries int
}

// SQLReadWriteProfilerStats exposes aggregate counters without retaining
// individual labels in the monitoring response.
type SQLReadWriteProfilerStats struct {
	EntryCount        int           `json:"entry_count"`
	ReadTaskCount     uint64        `json:"read_task_count"`
	WriteTaskCount    uint64        `json:"write_task_count"`
	ReadRows          uint64        `json:"read_rows"`
	WriteRows         uint64        `json:"write_rows"`
	ReadBytes         uint64        `json:"read_bytes"`
	WriteBytes        uint64        `json:"write_bytes"`
	ReadDuration      time.Duration `json:"read_duration"`
	WriteDuration     time.Duration `json:"write_duration"`
	EvictedEntryCount uint64        `json:"evicted_entry_count"`
}

type sqlReadWriteProfilerKey struct {
	table     string
	part      string
	column    string
	operation SQLReadWriteOperation
}

type sqlReadWriteProfilerEntry struct {
	aggregate SQLReadWriteTaskAggregate
	lastSeen  uint64
}

// SQLReadWriteProfiler retains bounded per-table-part-column I/O aggregates.
// It has no goroutines and does nothing unless a storage adapter explicitly
// records a task.
type SQLReadWriteProfiler struct {
	mu                sync.RWMutex
	maxEntries        int
	sequence          uint64
	closed            bool
	entries           map[sqlReadWriteProfilerKey]*sqlReadWriteProfilerEntry
	readTaskCount     uint64
	writeTaskCount    uint64
	readRows          uint64
	writeRows         uint64
	readBytes         uint64
	writeBytes        uint64
	readDuration      time.Duration
	writeDuration     time.Duration
	evictedEntryCount uint64
}

// NewSQLReadWriteProfiler creates a bounded read/write task profiler.
func NewSQLReadWriteProfiler(options SQLReadWriteProfilerOptions) (*SQLReadWriteProfiler, error) {
	maxEntries := options.MaxEntries
	if maxEntries == 0 {
		maxEntries = DefaultSQLReadWriteProfilerMaxEntries
	}
	if maxEntries < 0 || maxEntries > maxSQLReadWriteProfilerEntries {
		return nil, fmt.Errorf("%w: max entries", ErrSQLReadWriteProfilerLimitInvalid)
	}
	return &SQLReadWriteProfiler{maxEntries: maxEntries}, nil
}

// RecordRead aggregates one physical read task.
func (profiler *SQLReadWriteProfiler) RecordRead(table, part, column string, sample SQLReadWriteTaskSample) (bool, error) {
	return profiler.record(SQLReadWriteOperationRead, table, part, column, sample)
}

// RecordWrite aggregates one physical write task.
func (profiler *SQLReadWriteProfiler) RecordWrite(table, part, column string, sample SQLReadWriteTaskSample) (bool, error) {
	return profiler.record(SQLReadWriteOperationWrite, table, part, column, sample)
}

func (profiler *SQLReadWriteProfiler) record(operation SQLReadWriteOperation, table, part, column string, sample SQLReadWriteTaskSample) (bool, error) {
	if profiler == nil {
		return false, ErrSQLReadWriteProfilerClosed
	}
	table, err := normalizeSQLReadWriteProfilerLabel(table, ErrSQLReadWriteProfilerTableRequired)
	if err != nil {
		return false, err
	}
	part, err = normalizeSQLReadWriteProfilerLabel(part, ErrSQLReadWriteProfilerPartRequired)
	if err != nil {
		return false, err
	}
	column, err = normalizeSQLReadWriteProfilerLabel(column, ErrSQLReadWriteProfilerColumnRequired)
	if err != nil {
		return false, err
	}
	if sample.Duration < 0 {
		return false, ErrSQLReadWriteProfilerDurationInvalid
	}
	if sample.Timestamp.IsZero() {
		sample.Timestamp = time.Now().UTC()
	}

	profiler.mu.Lock()
	defer profiler.mu.Unlock()
	if profiler.closed {
		return false, ErrSQLReadWriteProfilerClosed
	}
	profiler.sequence++
	sequence := profiler.sequence
	if profiler.maxEntries == 0 {
		return false, nil
	}
	if profiler.entries == nil {
		capacity := profiler.maxEntries
		if capacity > 64 {
			capacity = 64
		}
		profiler.entries = make(map[sqlReadWriteProfilerKey]*sqlReadWriteProfilerEntry, capacity)
	}
	key := sqlReadWriteProfilerKey{table: table, part: part, column: column, operation: operation}
	entry := profiler.entries[key]
	if entry == nil {
		if len(profiler.entries) >= profiler.maxEntries {
			profiler.evictOldestLocked()
		}
		entry = &sqlReadWriteProfilerEntry{aggregate: SQLReadWriteTaskAggregate{
			Table: table, Part: part, Column: column, Operation: operation,
		}}
		profiler.entries[key] = entry
	}
	entry.lastSeen = sequence
	entry.aggregate.Tasks = saturatingAddUint64(entry.aggregate.Tasks, 1)
	entry.aggregate.Rows = saturatingAddUint64(entry.aggregate.Rows, sample.Rows)
	entry.aggregate.Bytes = saturatingAddUint64(entry.aggregate.Bytes, sample.Bytes)
	entry.aggregate.Duration = saturatingDurationAdd(entry.aggregate.Duration, sample.Duration)
	if sample.Timestamp.After(entry.aggregate.LastTimestamp) {
		entry.aggregate.LastTimestamp = sample.Timestamp
	}
	if operation == SQLReadWriteOperationRead {
		profiler.readTaskCount = saturatingAddUint64(profiler.readTaskCount, 1)
		profiler.readRows = saturatingAddUint64(profiler.readRows, sample.Rows)
		profiler.readBytes = saturatingAddUint64(profiler.readBytes, sample.Bytes)
		profiler.readDuration = saturatingDurationAdd(profiler.readDuration, sample.Duration)
	} else {
		profiler.writeTaskCount = saturatingAddUint64(profiler.writeTaskCount, 1)
		profiler.writeRows = saturatingAddUint64(profiler.writeRows, sample.Rows)
		profiler.writeBytes = saturatingAddUint64(profiler.writeBytes, sample.Bytes)
		profiler.writeDuration = saturatingDurationAdd(profiler.writeDuration, sample.Duration)
	}
	return true, nil
}

// Snapshot returns aggregates sorted by table, part, column, and direction.
func (profiler *SQLReadWriteProfiler) Snapshot() []SQLReadWriteTaskAggregate {
	if profiler == nil {
		return nil
	}
	profiler.mu.RLock()
	snapshot := make([]SQLReadWriteTaskAggregate, 0, len(profiler.entries))
	for _, entry := range profiler.entries {
		snapshot = append(snapshot, entry.aggregate)
	}
	profiler.mu.RUnlock()
	sort.Slice(snapshot, func(left, right int) bool {
		if snapshot[left].Table != snapshot[right].Table {
			return snapshot[left].Table < snapshot[right].Table
		}
		if snapshot[left].Part != snapshot[right].Part {
			return snapshot[left].Part < snapshot[right].Part
		}
		if snapshot[left].Column != snapshot[right].Column {
			return snapshot[left].Column < snapshot[right].Column
		}
		return snapshot[left].Operation < snapshot[right].Operation
	})
	return snapshot
}

// Stats returns bounded aggregate counters for operational monitoring.
func (profiler *SQLReadWriteProfiler) Stats() SQLReadWriteProfilerStats {
	if profiler == nil {
		return SQLReadWriteProfilerStats{}
	}
	profiler.mu.RLock()
	defer profiler.mu.RUnlock()
	return SQLReadWriteProfilerStats{
		EntryCount:        len(profiler.entries),
		ReadTaskCount:     profiler.readTaskCount,
		WriteTaskCount:    profiler.writeTaskCount,
		ReadRows:          profiler.readRows,
		WriteRows:         profiler.writeRows,
		ReadBytes:         profiler.readBytes,
		WriteBytes:        profiler.writeBytes,
		ReadDuration:      profiler.readDuration,
		WriteDuration:     profiler.writeDuration,
		EvictedEntryCount: profiler.evictedEntryCount,
	}
}

// Close stops future recording while preserving the last bounded snapshot.
func (profiler *SQLReadWriteProfiler) Close() {
	if profiler == nil {
		return
	}
	profiler.mu.Lock()
	profiler.closed = true
	profiler.mu.Unlock()
}

func (profiler *SQLReadWriteProfiler) evictOldestLocked() {
	var oldestKey sqlReadWriteProfilerKey
	var oldestSequence uint64
	found := false
	for key, entry := range profiler.entries {
		if !found || entry.lastSeen < oldestSequence {
			oldestKey = key
			oldestSequence = entry.lastSeen
			found = true
		}
	}
	if found {
		delete(profiler.entries, oldestKey)
		profiler.evictedEntryCount = saturatingAddUint64(profiler.evictedEntryCount, 1)
	}
}

func normalizeSQLReadWriteProfilerLabel(value string, required error) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", required
	}
	if len(value) > maxSQLReadWriteProfilerLabelBytes {
		return "", fmt.Errorf("%w: exceeds %d bytes", ErrSQLReadWriteProfilerLabelTooLong, maxSQLReadWriteProfilerLabelBytes)
	}
	return value, nil
}

func saturatingDurationAdd(left, right time.Duration) time.Duration {
	if right <= 0 || left >= time.Duration(1<<63-1)-right {
		if right > 0 {
			return time.Duration(1<<63 - 1)
		}
		return left
	}
	return left + right
}
