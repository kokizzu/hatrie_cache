package hatSql

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	// DefaultSQLTaskProfilerMaxEntries bounds retained table/part/column keys.
	DefaultSQLTaskProfilerMaxEntries = 1024
	// DefaultSQLTaskProfilerMaxTableBytes bounds one table identifier.
	DefaultSQLTaskProfilerMaxTableBytes = 256
	// DefaultSQLTaskProfilerMaxPartBytes bounds one physical-part identifier.
	DefaultSQLTaskProfilerMaxPartBytes = 256
	// DefaultSQLTaskProfilerMaxColumnBytes bounds one column identifier.
	DefaultSQLTaskProfilerMaxColumnBytes = 256

	maxSQLTaskProfilerEntries   = 1 << 20
	maxSQLTaskProfilerTextBytes = 1 << 20
)

var (
	// ErrSQLTaskProfilerClosed indicates that Record was called after Close.
	ErrSQLTaskProfilerClosed = errors.New("hatSql: SQL task profiler is closed")
	// ErrSQLTaskProfilerLimitInvalid indicates an invalid or unsafe bound.
	ErrSQLTaskProfilerLimitInvalid = errors.New("hatSql: SQL task profiler limit is invalid")
	// ErrSQLTaskProfilerInputInvalid indicates malformed task identity or data.
	ErrSQLTaskProfilerInputInvalid = errors.New("hatSql: invalid SQL task profiler input")
)

// SQLTaskOperation identifies the direction of one physical task.
type SQLTaskOperation string

const (
	SQLTaskRead  SQLTaskOperation = "read"
	SQLTaskWrite SQLTaskOperation = "write"
)

// SQLTaskProfileSample is one caller-supplied read or write observation. The
// profiler aggregates each sample into a table/part/column key. A blank
// Column records a table-part-level operation.
type SQLTaskProfileSample struct {
	Operation        SQLTaskOperation `json:"operation"`
	Table            string           `json:"table"`
	Part             string           `json:"part"`
	Column           string           `json:"column,omitempty"`
	Rows             uint64           `json:"rows"`
	Bytes            uint64           `json:"bytes"`
	ElapsedNanos     time.Duration    `json:"elapsed_nanos"`
	AllocatedBytes   uint64           `json:"allocated_bytes,omitempty"`
	AllocatedObjects uint64           `json:"allocated_objects,omitempty"`
	Failed           bool             `json:"failed,omitempty"`
}

// SQLTaskProfile is the deterministic aggregate for one operation, table,
// physical part, and optional column.
type SQLTaskProfile struct {
	Operation        SQLTaskOperation `json:"operation"`
	Table            string           `json:"table"`
	Part             string           `json:"part"`
	Column           string           `json:"column,omitempty"`
	Operations       uint64           `json:"operations"`
	Rows             uint64           `json:"rows"`
	Bytes            uint64           `json:"bytes"`
	ElapsedNanos     time.Duration    `json:"elapsed_nanos"`
	AllocatedBytes   uint64           `json:"allocated_bytes"`
	AllocatedObjects uint64           `json:"allocated_objects"`
	Failures         uint64           `json:"failures"`
}

// SQLTaskProfilerOptions bounds retained task keys and identifier sizes.
// Zero values select the documented defaults.
type SQLTaskProfilerOptions struct {
	MaxEntries     int
	MaxTableBytes  int
	MaxPartBytes   int
	MaxColumnBytes int
}

// SQLTaskProfilerStats summarizes bounded profiler state.
type SQLTaskProfilerStats struct {
	EntryCount         int    `json:"entry_count"`
	RecordCount        uint64 `json:"record_count"`
	DroppedRecordCount uint64 `json:"dropped_record_count"`
}

type sqlTaskProfileKey struct {
	operation SQLTaskOperation
	table     string
	part      string
	column    string
}

// SQLTaskProfiler is a concurrency-safe bounded aggregate for storage-layer
// read/write work. It performs no sampling goroutine and retains no task
// payload beyond bounded identifiers and counters.
type SQLTaskProfiler struct {
	mu                 sync.RWMutex
	maxEntries         int
	maxTableBytes      int
	maxPartBytes       int
	maxColumnBytes     int
	profiles           map[sqlTaskProfileKey]SQLTaskProfile
	recordCount        uint64
	droppedRecordCount uint64
	closed             bool
}

// NewSQLTaskProfiler creates an empty bounded task profiler.
func NewSQLTaskProfiler(options SQLTaskProfilerOptions) (*SQLTaskProfiler, error) {
	maxEntries, err := normalizeSQLTaskProfilerLimit(options.MaxEntries, DefaultSQLTaskProfilerMaxEntries, maxSQLTaskProfilerEntries, "entries")
	if err != nil {
		return nil, err
	}
	maxTableBytes, err := normalizeSQLTaskProfilerLimit(options.MaxTableBytes, DefaultSQLTaskProfilerMaxTableBytes, maxSQLTaskProfilerTextBytes, "table bytes")
	if err != nil {
		return nil, err
	}
	maxPartBytes, err := normalizeSQLTaskProfilerLimit(options.MaxPartBytes, DefaultSQLTaskProfilerMaxPartBytes, maxSQLTaskProfilerTextBytes, "part bytes")
	if err != nil {
		return nil, err
	}
	maxColumnBytes, err := normalizeSQLTaskProfilerLimit(options.MaxColumnBytes, DefaultSQLTaskProfilerMaxColumnBytes, maxSQLTaskProfilerTextBytes, "column bytes")
	if err != nil {
		return nil, err
	}
	return &SQLTaskProfiler{
		maxEntries:     maxEntries,
		maxTableBytes:  maxTableBytes,
		maxPartBytes:   maxPartBytes,
		maxColumnBytes: maxColumnBytes,
		profiles:       make(map[sqlTaskProfileKey]SQLTaskProfile, maxEntries),
	}, nil
}

// Record aggregates one valid task sample. It returns false with a nil error
// when a new key is rejected because MaxEntries is already full.
func (profiler *SQLTaskProfiler) Record(sample SQLTaskProfileSample) (bool, error) {
	if profiler == nil {
		return false, ErrSQLTaskProfilerClosed
	}
	if err := profiler.validateSample(sample); err != nil {
		return false, err
	}
	key := sqlTaskProfileKey{
		operation: sample.Operation,
		table:     sample.Table,
		part:      sample.Part,
		column:    sample.Column,
	}

	profiler.mu.Lock()
	defer profiler.mu.Unlock()
	if profiler.closed {
		return false, ErrSQLTaskProfilerClosed
	}
	profile, exists := profiler.profiles[key]
	if !exists {
		if err := profiler.validateSampleIdentity(sample); err != nil {
			return false, err
		}
		if len(profiler.profiles) >= profiler.maxEntries {
			profiler.droppedRecordCount = saturatingAddSQLTaskUint64(profiler.droppedRecordCount, 1)
			return false, nil
		}
		profile = SQLTaskProfile{
			Operation: sample.Operation,
			Table:     sample.Table,
			Part:      sample.Part,
			Column:    sample.Column,
		}
	}
	profile.Operations = saturatingAddSQLTaskUint64(profile.Operations, 1)
	profile.Rows = saturatingAddSQLTaskUint64(profile.Rows, sample.Rows)
	profile.Bytes = saturatingAddSQLTaskUint64(profile.Bytes, sample.Bytes)
	profile.ElapsedNanos = saturatingAddSQLTaskDuration(profile.ElapsedNanos, sample.ElapsedNanos)
	profile.AllocatedBytes = saturatingAddSQLTaskUint64(profile.AllocatedBytes, sample.AllocatedBytes)
	profile.AllocatedObjects = saturatingAddSQLTaskUint64(profile.AllocatedObjects, sample.AllocatedObjects)
	if sample.Failed {
		profile.Failures = saturatingAddSQLTaskUint64(profile.Failures, 1)
	}
	profiler.profiles[key] = profile
	profiler.recordCount = saturatingAddSQLTaskUint64(profiler.recordCount, 1)
	return true, nil
}

// Profiles returns an independent snapshot ordered by operation, table, part,
// and column. Mutating the returned slice does not affect the profiler.
func (profiler *SQLTaskProfiler) Profiles() []SQLTaskProfile {
	if profiler == nil {
		return nil
	}
	profiler.mu.RLock()
	profiles := make([]SQLTaskProfile, 0, len(profiler.profiles))
	for _, profile := range profiler.profiles {
		profiles = append(profiles, profile)
	}
	profiler.mu.RUnlock()
	sort.Slice(profiles, func(left, right int) bool {
		if profiles[left].Operation != profiles[right].Operation {
			return profiles[left].Operation < profiles[right].Operation
		}
		if profiles[left].Table != profiles[right].Table {
			return profiles[left].Table < profiles[right].Table
		}
		if profiles[left].Part != profiles[right].Part {
			return profiles[left].Part < profiles[right].Part
		}
		return profiles[left].Column < profiles[right].Column
	})
	return profiles
}

// Stats returns bounded profiler counters without exposing mutable state.
func (profiler *SQLTaskProfiler) Stats() SQLTaskProfilerStats {
	if profiler == nil {
		return SQLTaskProfilerStats{}
	}
	profiler.mu.RLock()
	defer profiler.mu.RUnlock()
	return SQLTaskProfilerStats{
		EntryCount:         len(profiler.profiles),
		RecordCount:        profiler.recordCount,
		DroppedRecordCount: profiler.droppedRecordCount,
	}
}

// Close prevents future records while preserving the final snapshot.
func (profiler *SQLTaskProfiler) Close() {
	if profiler == nil {
		return
	}
	profiler.mu.Lock()
	profiler.closed = true
	profiler.mu.Unlock()
}

func (profiler *SQLTaskProfiler) validateSample(sample SQLTaskProfileSample) error {
	if sample.Operation != SQLTaskRead && sample.Operation != SQLTaskWrite {
		return fmt.Errorf("%w: operation %q", ErrSQLTaskProfilerInputInvalid, sample.Operation)
	}
	if err := validateSQLTaskProfilerText(sample.Table, profiler.maxTableBytes, "table", true, false); err != nil {
		return err
	}
	if err := validateSQLTaskProfilerText(sample.Part, profiler.maxPartBytes, "part", true, false); err != nil {
		return err
	}
	if err := validateSQLTaskProfilerText(sample.Column, profiler.maxColumnBytes, "column", false, false); err != nil {
		return err
	}
	if sample.ElapsedNanos < 0 {
		return fmt.Errorf("%w: elapsed nanos is negative", ErrSQLTaskProfilerInputInvalid)
	}
	return nil
}

func (profiler *SQLTaskProfiler) validateSampleIdentity(sample SQLTaskProfileSample) error {
	if err := validateSQLTaskProfilerText(sample.Table, profiler.maxTableBytes, "table", true, true); err != nil {
		return err
	}
	if err := validateSQLTaskProfilerText(sample.Part, profiler.maxPartBytes, "part", true, true); err != nil {
		return err
	}
	return validateSQLTaskProfilerText(sample.Column, profiler.maxColumnBytes, "column", false, true)
}

func normalizeSQLTaskProfilerLimit(value, defaultValue, maximum int, name string) (int, error) {
	if value == 0 {
		return defaultValue, nil
	}
	if value < 0 || value > maximum {
		return 0, fmt.Errorf("%w: %s", ErrSQLTaskProfilerLimitInvalid, name)
	}
	return value, nil
}

func validateSQLTaskProfilerText(value string, limit int, name string, required, validateEncoding bool) error {
	if required && value == "" {
		return fmt.Errorf("%w: %s is required", ErrSQLTaskProfilerInputInvalid, name)
	}
	if len(value) > limit {
		return fmt.Errorf("%w: %s exceeds %d bytes", ErrSQLTaskProfilerInputInvalid, name, limit)
	}
	if validateEncoding && required && strings.TrimSpace(value) == "" {
		return fmt.Errorf("%w: %s is required", ErrSQLTaskProfilerInputInvalid, name)
	}
	if validateEncoding && !utf8.ValidString(value) {
		return fmt.Errorf("%w: %s is not valid UTF-8", ErrSQLTaskProfilerInputInvalid, name)
	}
	return nil
}

func saturatingAddSQLTaskUint64(current, value uint64) uint64 {
	maximum := ^uint64(0)
	if maximum-current < value {
		return maximum
	}
	return current + value
}

func saturatingAddSQLTaskDuration(current, value time.Duration) time.Duration {
	maximum := time.Duration(^uint64(0) >> 1)
	if maximum-current < value {
		return maximum
	}
	return current + value
}
