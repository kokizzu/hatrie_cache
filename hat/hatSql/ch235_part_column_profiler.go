package hatSql

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	// DefaultSQLQueryProfilerMaxPartColumnsPerQuery bounds opt-in part/column
	// aggregation without allowing an unbounded cardinality dimension.
	DefaultSQLQueryProfilerMaxPartColumnsPerQuery = 64
	maxSQLQueryProfilerPartColumnsPerQuery        = 4096
)

var (
	ErrSQLQueryProfilerPartRequired     = errors.New("SQL profiler part is required")
	ErrSQLQueryProfilerColumnRequired   = errors.New("SQL profiler column is required")
	ErrSQLQueryProfilerOperationInvalid = errors.New("SQL profiler operation must be read or write")
)

// SQLQueryPartColumnSample is one caller-supplied read or write observation
// for a physical part and column. The profiler does not inspect storage state.
type SQLQueryPartColumnSample struct {
	Part      string        `json:"part"`
	Column    string        `json:"column"`
	Operation string        `json:"operation"`
	CPUTime   time.Duration `json:"cpu_time"`
	Rows      uint64        `json:"rows"`
	Bytes     uint64        `json:"bytes"`
}

// SQLQueryPartColumnProfileEntry is a bounded aggregate for one physical
// part, column, and read/write operation.
type SQLQueryPartColumnProfileEntry struct {
	Part         string        `json:"part"`
	Column       string        `json:"column"`
	Operation    string        `json:"operation"`
	Observations uint64        `json:"observations"`
	CPUTime      time.Duration `json:"cpu_time"`
	Rows         uint64        `json:"rows"`
	Bytes        uint64        `json:"bytes"`
}

// SQLQueryPartColumnProfile is an independent deterministic snapshot for one
// query. Entries are sorted by part, column, and operation.
type SQLQueryPartColumnProfile struct {
	QueryID             string                           `json:"query_id"`
	Entries             []SQLQueryPartColumnProfileEntry `json:"entries"`
	DroppedObservations uint64                           `json:"dropped_observations"`
}

type sqlQueryPartColumnKey struct {
	part      string
	column    string
	operation string
}

type sqlQueryPartColumnProfileState struct {
	profile  SQLQueryPartColumnProfile
	entries  map[sqlQueryPartColumnKey]SQLQueryPartColumnProfileEntry
	lastSeen uint64
}

// RecordPartColumn records one bounded read/write part-column observation. It
// is lazy and independent of the ordinary sample and memory profiles.
func (profiler *SQLQueryProfiler) RecordPartColumn(queryID string, sample SQLQueryPartColumnSample) (bool, error) {
	if profiler == nil {
		return false, ErrSQLQueryProfilerClosed
	}
	queryID, err := normalizeSQLQueryProfilerQueryID(queryID)
	if err != nil {
		return false, err
	}
	part, err := normalizeSQLQueryProfilerPartColumnName(sample.Part, ErrSQLQueryProfilerPartRequired)
	if err != nil {
		return false, err
	}
	column, err := normalizeSQLQueryProfilerPartColumnName(sample.Column, ErrSQLQueryProfilerColumnRequired)
	if err != nil {
		return false, err
	}
	operation := strings.ToLower(strings.TrimSpace(sample.Operation))
	if operation != "read" && operation != "write" {
		return false, ErrSQLQueryProfilerOperationInvalid
	}
	if sample.CPUTime < 0 {
		return false, ErrSQLQueryProfilerDurationInvalid
	}

	profiler.mu.Lock()
	defer profiler.mu.Unlock()
	if profiler.closed {
		return false, ErrSQLQueryProfilerClosed
	}
	profiler.partColumnSequence++
	if profiler.partColumnProfiles == nil {
		profiler.partColumnProfiles = make(map[string]*sqlQueryPartColumnProfileState, profiler.maxQueries)
	}
	state := profiler.partColumnProfiles[queryID]
	if state == nil {
		if len(profiler.partColumnProfiles) >= profiler.maxQueries {
			profiler.evictOldestPartColumnProfileLocked()
		}
		state = &sqlQueryPartColumnProfileState{
			profile: SQLQueryPartColumnProfile{
				QueryID: queryID,
			},
			entries: make(map[sqlQueryPartColumnKey]SQLQueryPartColumnProfileEntry, profiler.maxPartColumnsPerQuery),
		}
		profiler.partColumnProfiles[queryID] = state
	}
	state.lastSeen = profiler.partColumnSequence
	key := sqlQueryPartColumnKey{part: part, column: column, operation: operation}
	entry, found := state.entries[key]
	if !found {
		if len(state.entries) >= profiler.maxPartColumnsPerQuery {
			state.profile.DroppedObservations++
			profiler.droppedPartColumnObservationCount++
			return false, nil
		}
		entry.Part = part
		entry.Column = column
		entry.Operation = operation
	}
	entry.Observations++
	entry.CPUTime = saturatingSQLQueryProfilerDuration(entry.CPUTime, sample.CPUTime)
	entry.Rows = saturatingAddUint64(entry.Rows, sample.Rows)
	entry.Bytes = saturatingAddUint64(entry.Bytes, sample.Bytes)
	state.entries[key] = entry
	profiler.partColumnObservationCount++
	return true, nil
}

// PartColumnProfile returns an independent deterministic snapshot for one
// query's part/column observations.
func (profiler *SQLQueryProfiler) PartColumnProfile(queryID string) (SQLQueryPartColumnProfile, bool) {
	if profiler == nil {
		return SQLQueryPartColumnProfile{}, false
	}
	queryID, err := normalizeSQLQueryProfilerQueryID(queryID)
	if err != nil {
		return SQLQueryPartColumnProfile{}, false
	}
	profiler.mu.RLock()
	defer profiler.mu.RUnlock()
	state, ok := profiler.partColumnProfiles[queryID]
	if !ok {
		return SQLQueryPartColumnProfile{}, false
	}
	return snapshotSQLQueryPartColumnProfile(state), true
}

// PartColumnProfiles returns all retained part/column profiles in deterministic
// query-ID order.
func (profiler *SQLQueryProfiler) PartColumnProfiles() []SQLQueryPartColumnProfile {
	if profiler == nil {
		return nil
	}
	profiler.mu.RLock()
	profiles := make([]SQLQueryPartColumnProfile, 0, len(profiler.partColumnProfiles))
	for _, state := range profiler.partColumnProfiles {
		profiles = append(profiles, snapshotSQLQueryPartColumnProfile(state))
	}
	profiler.mu.RUnlock()
	sort.Slice(profiles, func(left, right int) bool {
		return profiles[left].QueryID < profiles[right].QueryID
	})
	return profiles
}

func normalizeSQLQueryProfilerPartColumnName(value string, required error) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", required
	}
	if len(value) > maxSQLQueryProfilerOperatorBytes {
		return "", fmt.Errorf("%w: value exceeds %d bytes", required, maxSQLQueryProfilerOperatorBytes)
	}
	return value, nil
}

func saturatingSQLQueryProfilerDuration(value, add time.Duration) time.Duration {
	const maxDuration = time.Duration(1<<63 - 1)
	if add > maxDuration-value {
		return maxDuration
	}
	return value + add
}

func (profiler *SQLQueryProfiler) evictOldestPartColumnProfileLocked() {
	var oldestID string
	var oldestSequence uint64
	for queryID, state := range profiler.partColumnProfiles {
		if oldestID == "" || state.lastSeen < oldestSequence {
			oldestID = queryID
			oldestSequence = state.lastSeen
		}
	}
	if oldestID != "" {
		delete(profiler.partColumnProfiles, oldestID)
		profiler.evictedPartColumnQueryCount++
	}
}

func snapshotSQLQueryPartColumnProfile(state *sqlQueryPartColumnProfileState) SQLQueryPartColumnProfile {
	profile := SQLQueryPartColumnProfile{
		QueryID:             state.profile.QueryID,
		DroppedObservations: state.profile.DroppedObservations,
		Entries:             make([]SQLQueryPartColumnProfileEntry, 0, len(state.entries)),
	}
	for _, entry := range state.entries {
		profile.Entries = append(profile.Entries, entry)
	}
	sort.Slice(profile.Entries, func(left, right int) bool {
		if profile.Entries[left].Part != profile.Entries[right].Part {
			return profile.Entries[left].Part < profile.Entries[right].Part
		}
		if profile.Entries[left].Column != profile.Entries[right].Column {
			return profile.Entries[left].Column < profile.Entries[right].Column
		}
		return profile.Entries[left].Operation < profile.Entries[right].Operation
	})
	return profile
}
