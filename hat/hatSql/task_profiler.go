package hatSql

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

const (
	// DefaultSQLTaskProfilerMaxEntries bounds the number of table-part-column
	// operation profiles retained by a default task profiler.
	DefaultSQLTaskProfilerMaxEntries = 4096
	maxSQLTaskProfilerMaxEntries     = 65536
	maxSQLTaskProfilerFieldBytes     = 256
	maxSQLTaskProfilerDuration       = time.Duration(1<<63 - 1)
)

var (
	// ErrSQLTaskProfilerClosed reports recording after Close.
	ErrSQLTaskProfilerClosed = errors.New("hatSql: task profiler is closed")
	// ErrSQLTaskProfilerOptionsInvalid reports an invalid profiler bound.
	ErrSQLTaskProfilerOptionsInvalid = errors.New("hatSql: task profiler options are invalid")
	// ErrSQLTaskProfilerFieldRequired reports an empty or oversized identity
	// field. Identity fields are bounded to keep label cardinality controlled.
	ErrSQLTaskProfilerFieldRequired = errors.New("hatSql: task profiler identity field is invalid")
	// ErrSQLTaskProfilerOperationInvalid reports an operation other than read or
	// write.
	ErrSQLTaskProfilerOperationInvalid = errors.New("hatSql: task profiler operation is invalid")
	// ErrSQLTaskProfilerDurationInvalid reports a negative task duration.
	ErrSQLTaskProfilerDurationInvalid = errors.New("hatSql: task profiler duration is invalid")
)

// SQLTaskOperation identifies the kind of storage task being aggregated.
type SQLTaskOperation string

const (
	// SQLTaskRead records a task that reads one column from one immutable part.
	SQLTaskRead SQLTaskOperation = "read"
	// SQLTaskWrite records a task that writes one column to one immutable part.
	SQLTaskWrite SQLTaskOperation = "write"
)

// SQLTaskProfilerOptions configures a bounded, opt-in task profiler. A zero
// MaxEntries selects DefaultSQLTaskProfilerMaxEntries.
type SQLTaskProfilerOptions struct {
	MaxEntries int
}

// SQLTaskProfileRecord is one caller-supplied read or write observation. The
// profiler stores only bounded identity labels and aggregate counters; it does
// not retain row values, query text, or payload bytes.
type SQLTaskProfileRecord struct {
	Table     string
	Part      string
	Column    string
	Operation SQLTaskOperation
	Rows      uint64
	Bytes     uint64
	Duration  time.Duration
}

// SQLTaskProfile is a deterministic aggregate for one table, part, column,
// and operation identity. Rows, Bytes, and Duration describe that operation;
// ReadTasks and WriteTasks provide explicit operation counters for exporters.
type SQLTaskProfile struct {
	Table       string           `json:"table"`
	Part        string           `json:"part"`
	Column      string           `json:"column"`
	Operation   SQLTaskOperation `json:"operation"`
	Tasks       uint64           `json:"tasks"`
	ReadTasks   uint64           `json:"read_tasks"`
	WriteTasks  uint64           `json:"write_tasks"`
	Rows        uint64           `json:"rows"`
	Bytes       uint64           `json:"bytes"`
	Duration    time.Duration    `json:"duration"`
	LastUpdated uint64           `json:"last_updated_sequence"`
}

// SQLTaskProfilerStats is a bounded point-in-time summary of the collector.
type SQLTaskProfilerStats struct {
	EntryCount        int    `json:"entry_count"`
	RecordCount       uint64 `json:"record_count"`
	ReadTaskCount     uint64 `json:"read_task_count"`
	WriteTaskCount    uint64 `json:"write_task_count"`
	EvictedEntryCount uint64 `json:"evicted_entry_count"`
}

type sqlTaskProfileKey struct {
	table     string
	part      string
	column    string
	operation SQLTaskOperation
}

type sqlTaskProfileState struct {
	profile SQLTaskProfile
	seen    uint64
}

// SQLTaskProfiler aggregates caller-instrumented storage work by table part
// and column. It is concurrency-safe, retains at most MaxEntries identities,
// and has no goroutine. Recording an existing identity performs one map lookup
// and updates a preallocated state; snapshots are the allocation boundary.
type SQLTaskProfiler struct {
	mu                sync.RWMutex
	maxEntries        int
	sequence          uint64
	recordCount       uint64
	readTaskCount     uint64
	writeTaskCount    uint64
	evictedEntryCount uint64
	closed            bool
	profiles          map[sqlTaskProfileKey]*sqlTaskProfileState
}

// NewSQLTaskProfiler creates a bounded task profiler. The profiler is opt-in;
// callers that do not construct one pay no recording or retention cost.
func NewSQLTaskProfiler(options SQLTaskProfilerOptions) (*SQLTaskProfiler, error) {
	maxEntries := options.MaxEntries
	if maxEntries == 0 {
		maxEntries = DefaultSQLTaskProfilerMaxEntries
	}
	if maxEntries < 0 || maxEntries > maxSQLTaskProfilerMaxEntries {
		return nil, fmt.Errorf("%w: max entries", ErrSQLTaskProfilerOptionsInvalid)
	}
	return &SQLTaskProfiler{
		maxEntries: maxEntries,
		profiles:   make(map[sqlTaskProfileKey]*sqlTaskProfileState, maxEntries),
	}, nil
}

// Record aggregates one storage task. It returns false only when the profiler
// is configured with no entries; a full profiler evicts its least-recently
// observed identity so a new identity can be retained without unbounded map
// growth.
func (profiler *SQLTaskProfiler) Record(record SQLTaskProfileRecord) (bool, error) {
	if profiler == nil {
		return false, ErrSQLTaskProfilerClosed
	}
	key, err := normalizeSQLTaskProfileRecord(record)
	if err != nil {
		return false, err
	}

	profiler.mu.Lock()
	defer profiler.mu.Unlock()
	if profiler.closed {
		return false, ErrSQLTaskProfilerClosed
	}
	if profiler.maxEntries == 0 {
		return false, nil
	}
	profiler.sequence++
	sequence := profiler.sequence
	state := profiler.profiles[key]
	if state == nil {
		if len(profiler.profiles) >= profiler.maxEntries {
			profiler.evictOldestLocked()
		}
		state = &sqlTaskProfileState{profile: SQLTaskProfile{
			Table:     record.Table,
			Part:      record.Part,
			Column:    record.Column,
			Operation: record.Operation,
		}}
		profiler.profiles[key] = state
	}
	state.seen = sequence
	state.profile.LastUpdated = sequence
	state.profile.Tasks = saturatingAddSQLTaskUint64(state.profile.Tasks, 1)
	if record.Operation == SQLTaskRead {
		state.profile.ReadTasks = saturatingAddSQLTaskUint64(state.profile.ReadTasks, 1)
		profiler.readTaskCount = saturatingAddSQLTaskUint64(profiler.readTaskCount, 1)
	} else {
		state.profile.WriteTasks = saturatingAddSQLTaskUint64(state.profile.WriteTasks, 1)
		profiler.writeTaskCount = saturatingAddSQLTaskUint64(profiler.writeTaskCount, 1)
	}
	state.profile.Rows = saturatingAddSQLTaskUint64(state.profile.Rows, record.Rows)
	state.profile.Bytes = saturatingAddSQLTaskUint64(state.profile.Bytes, record.Bytes)
	state.profile.Duration = saturatingAddSQLTaskDuration(state.profile.Duration, record.Duration)
	profiler.recordCount = saturatingAddSQLTaskUint64(profiler.recordCount, 1)
	return true, nil
}

// Profiles returns independent profiles sorted by table, part, column, and
// operation. LastUpdated is a sequence number rather than a wall-clock time,
// which keeps snapshots deterministic and avoids allocating time values on the
// record path.
func (profiler *SQLTaskProfiler) Profiles() []SQLTaskProfile {
	if profiler == nil {
		return nil
	}
	profiler.mu.RLock()
	profiles := make([]SQLTaskProfile, 0, len(profiler.profiles))
	for _, state := range profiler.profiles {
		profiles = append(profiles, state.profile)
	}
	profiler.mu.RUnlock()
	sort.Slice(profiles, func(left, right int) bool {
		if profiles[left].Table != profiles[right].Table {
			return profiles[left].Table < profiles[right].Table
		}
		if profiles[left].Part != profiles[right].Part {
			return profiles[left].Part < profiles[right].Part
		}
		if profiles[left].Column != profiles[right].Column {
			return profiles[left].Column < profiles[right].Column
		}
		return profiles[left].Operation < profiles[right].Operation
	})
	return profiles
}

// Stats returns bounded aggregate counters without exposing labels.
func (profiler *SQLTaskProfiler) Stats() SQLTaskProfilerStats {
	if profiler == nil {
		return SQLTaskProfilerStats{}
	}
	profiler.mu.RLock()
	defer profiler.mu.RUnlock()
	return SQLTaskProfilerStats{
		EntryCount:        len(profiler.profiles),
		RecordCount:       profiler.recordCount,
		ReadTaskCount:     profiler.readTaskCount,
		WriteTaskCount:    profiler.writeTaskCount,
		EvictedEntryCount: profiler.evictedEntryCount,
	}
}

// Close stops recording and leaves the last bounded snapshot available.
func (profiler *SQLTaskProfiler) Close() {
	if profiler == nil {
		return
	}
	profiler.mu.Lock()
	profiler.closed = true
	profiler.mu.Unlock()
}

func (profiler *SQLTaskProfiler) evictOldestLocked() {
	var oldestKey sqlTaskProfileKey
	var oldest *sqlTaskProfileState
	for key, state := range profiler.profiles {
		if oldest == nil || state.seen < oldest.seen {
			oldestKey = key
			oldest = state
		}
	}
	if oldest == nil {
		return
	}
	delete(profiler.profiles, oldestKey)
	profiler.evictedEntryCount = saturatingAddSQLTaskUint64(profiler.evictedEntryCount, 1)
}

func normalizeSQLTaskProfileRecord(record SQLTaskProfileRecord) (sqlTaskProfileKey, error) {
	if err := validateSQLTaskProfilerField(record.Table, "table"); err != nil {
		return sqlTaskProfileKey{}, err
	}
	if err := validateSQLTaskProfilerField(record.Part, "part"); err != nil {
		return sqlTaskProfileKey{}, err
	}
	if err := validateSQLTaskProfilerField(record.Column, "column"); err != nil {
		return sqlTaskProfileKey{}, err
	}
	if record.Operation != SQLTaskRead && record.Operation != SQLTaskWrite {
		return sqlTaskProfileKey{}, fmt.Errorf("%w: %q", ErrSQLTaskProfilerOperationInvalid, record.Operation)
	}
	if record.Duration < 0 {
		return sqlTaskProfileKey{}, ErrSQLTaskProfilerDurationInvalid
	}
	return sqlTaskProfileKey{
		table:     record.Table,
		part:      record.Part,
		column:    record.Column,
		operation: record.Operation,
	}, nil
}

func validateSQLTaskProfilerField(value, name string) error {
	if value == "" || len(value) > maxSQLTaskProfilerFieldBytes {
		return fmt.Errorf("%w: %s", ErrSQLTaskProfilerFieldRequired, name)
	}
	return nil
}

func saturatingAddSQLTaskUint64(current, added uint64) uint64 {
	if ^uint64(0)-current < added {
		return ^uint64(0)
	}
	return current + added
}

func saturatingAddSQLTaskDuration(current, added time.Duration) time.Duration {
	if maxSQLTaskProfilerDuration-current < added {
		return maxSQLTaskProfilerDuration
	}
	return current + added
}
