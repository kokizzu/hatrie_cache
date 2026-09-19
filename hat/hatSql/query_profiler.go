package hatSql

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

const (
	// DefaultSQLQueryProfilerMaxQueries bounds the number of query IDs retained
	// by a default profiler.
	DefaultSQLQueryProfilerMaxQueries = 256
	// DefaultSQLQueryProfilerMaxSamplesPerQuery bounds samples retained for one
	// query by a default profiler.
	DefaultSQLQueryProfilerMaxSamplesPerQuery = 64
	// DefaultSQLQueryProfilerMaxMemoryOperatorsPerQuery bounds distinct
	// operators retained by a default memory profile.
	DefaultSQLQueryProfilerMaxMemoryOperatorsPerQuery = 64
	defaultSQLQueryProfilerSampleEvery                = 1
	maxSQLQueryProfilerQueries                        = 4096
	maxSQLQueryProfilerSamplesPerQuery                = 1024
	maxSQLQueryProfilerMemoryOperatorsPerQuery        = 1024
	maxSQLQueryProfilerTotalSamples                   = 1 << 20
	maxSQLQueryProfilerQueryIDBytes                   = 256
	maxSQLQueryProfilerOperatorBytes                  = 256
)

var (
	ErrSQLQueryProfilerClosed           = errors.New("hatSql: SQL query profiler is closed")
	ErrSQLQueryProfilerLimitInvalid     = errors.New("hatSql: SQL query profiler limit is invalid")
	ErrSQLQueryProfilerQueryIDRequired  = errors.New("hatSql: SQL query profiler query ID is required")
	ErrSQLQueryProfilerOperatorRequired = errors.New("hatSql: SQL query profiler operator is required")
	ErrSQLQueryProfilerDurationInvalid  = errors.New("hatSql: SQL query profiler duration is invalid")
)

// SQLQueryProfilerOptions configures a bounded, opt-in query sample store.
// Zero limits select the documented defaults. SampleEvery keeps the first
// sample and then retains every Nth submitted sample globally.
type SQLQueryProfilerOptions struct {
	MaxQueries                 int
	MaxSamplesPerQuery         int
	MaxMemoryOperatorsPerQuery int
	SampleEvery                uint64
}

// SQLQueryProfileSample is one privacy-safe operator observation. CPUTime and
// BlockedTime are supplied by the caller's instrumentation; the profiler does
// not inspect query text or collect process-wide stack traces.
type SQLQueryProfileSample struct {
	Operator    string        `json:"operator"`
	CPUTime     time.Duration `json:"cpu_time"`
	BlockedTime time.Duration `json:"blocked_time"`
	Rows        uint64        `json:"rows"`
	Bytes       uint64        `json:"bytes"`
	Timestamp   time.Time     `json:"timestamp"`
}

// SQLQueryMemorySample is one caller-supplied operator memory observation.
// AllocatedBytes is cumulative allocation work for the observation, while
// PeakBytes and RetainedBytes describe the operator's peak and retained
// resident bytes. The profiler does not read process-wide runtime statistics;
// callers provide measurements from their operator instrumentation.
type SQLQueryMemorySample struct {
	AllocatedBytes uint64 `json:"allocated_bytes"`
	PeakBytes      uint64 `json:"peak_bytes"`
	RetainedBytes  uint64 `json:"retained_bytes"`
}

// SQLQueryOperatorMemoryProfile is a bounded aggregate for one operator.
// AllocatedBytes is saturating, and the other byte fields retain maxima across
// accepted observations.
type SQLQueryOperatorMemoryProfile struct {
	Operator         string `json:"operator"`
	Observations     uint64 `json:"observations"`
	AllocatedBytes   uint64 `json:"allocated_bytes"`
	PeakBytes        uint64 `json:"peak_bytes"`
	MaxRetainedBytes uint64 `json:"max_retained_bytes"`
}

// SQLQueryMemoryProfile is a deterministic snapshot for one query. Operators
// are sorted by name and DroppedObservations counts samples rejected after the
// per-query operator bound was reached.
type SQLQueryMemoryProfile struct {
	QueryID             string                          `json:"query_id"`
	Operators           []SQLQueryOperatorMemoryProfile `json:"operators"`
	DroppedObservations uint64                          `json:"dropped_observations"`
}

// SQLQueryProfile is an oldest-first snapshot for one query ID. SamplesSeen
// counts captured samples before the per-query ring bound; SamplesDropped
// counts samples overwritten after that bound was reached.
type SQLQueryProfile struct {
	QueryID        string                  `json:"query_id"`
	Samples        []SQLQueryProfileSample `json:"samples"`
	SamplesSeen    uint64                  `json:"samples_seen"`
	SamplesDropped uint64                  `json:"samples_dropped"`
}

// SQLQueryProfilerStats describes bounded profiler state without exposing
// query IDs or sample contents.
type SQLQueryProfilerStats struct {
	QueryCount                    int    `json:"query_count"`
	SampleCount                   uint64 `json:"sample_count"`
	UnsampledCount                uint64 `json:"unsampled_count"`
	DroppedSampleCount            uint64 `json:"dropped_sample_count"`
	EvictedQueryCount             uint64 `json:"evicted_query_count"`
	MemoryQueryCount              int    `json:"memory_query_count"`
	MemoryObservationCount        uint64 `json:"memory_observation_count"`
	DroppedMemoryObservationCount uint64 `json:"dropped_memory_observation_count"`
	EvictedMemoryQueryCount       uint64 `json:"evicted_memory_query_count"`
}

type sqlQueryProfileState struct {
	profile  SQLQueryProfile
	next     int
	lastSeen uint64
}

type sqlQueryMemoryProfileState struct {
	profile   SQLQueryMemoryProfile
	operators map[string]SQLQueryOperatorMemoryProfile
	lastSeen  uint64
}

// SQLQueryProfiler retains bounded sampled operator observations keyed by
// query ID. It has no goroutines and adds no cost unless callers construct it
// and call Record.
type SQLQueryProfiler struct {
	mu                            sync.RWMutex
	maxQueries                    int
	maxSamplesPerQuery            int
	maxMemoryOperatorsPerQuery    int
	sampleEvery                   uint64
	sequence                      uint64
	sampleCount                   uint64
	unsampledCount                uint64
	droppedSampleCount            uint64
	evictedQueryCount             uint64
	closed                        bool
	queries                       map[string]*sqlQueryProfileState
	memorySequence                uint64
	memoryProfiles                map[string]*sqlQueryMemoryProfileState
	memoryObservationCount        uint64
	droppedMemoryObservationCount uint64
	evictedMemoryQueryCount       uint64
}

// NewSQLQueryProfiler creates a bounded profiler. Limits above the safety
// bounds are rejected so an operator setting cannot request unbounded memory.
func NewSQLQueryProfiler(options SQLQueryProfilerOptions) (*SQLQueryProfiler, error) {
	maxQueries := options.MaxQueries
	if maxQueries == 0 {
		maxQueries = DefaultSQLQueryProfilerMaxQueries
	}
	if maxQueries < 0 || maxQueries > maxSQLQueryProfilerQueries {
		return nil, fmt.Errorf("%w: max queries", ErrSQLQueryProfilerLimitInvalid)
	}
	maxSamples := options.MaxSamplesPerQuery
	if maxSamples == 0 {
		maxSamples = DefaultSQLQueryProfilerMaxSamplesPerQuery
	}
	if maxSamples < 0 || maxSamples > maxSQLQueryProfilerSamplesPerQuery {
		return nil, fmt.Errorf("%w: max samples per query", ErrSQLQueryProfilerLimitInvalid)
	}
	if maxQueries > maxSQLQueryProfilerTotalSamples/maxSamples {
		return nil, fmt.Errorf("%w: total samples", ErrSQLQueryProfilerLimitInvalid)
	}
	maxMemoryOperators := options.MaxMemoryOperatorsPerQuery
	if maxMemoryOperators == 0 {
		maxMemoryOperators = DefaultSQLQueryProfilerMaxMemoryOperatorsPerQuery
	}
	if maxMemoryOperators < 0 || maxMemoryOperators > maxSQLQueryProfilerMemoryOperatorsPerQuery {
		return nil, fmt.Errorf("%w: max memory operators per query", ErrSQLQueryProfilerLimitInvalid)
	}
	if maxQueries > maxSQLQueryProfilerTotalSamples/maxMemoryOperators {
		return nil, fmt.Errorf("%w: total memory operators", ErrSQLQueryProfilerLimitInvalid)
	}
	sampleEvery := options.SampleEvery
	if sampleEvery == 0 {
		sampleEvery = defaultSQLQueryProfilerSampleEvery
	}
	return &SQLQueryProfiler{
		maxQueries:                 maxQueries,
		maxSamplesPerQuery:         maxSamples,
		maxMemoryOperatorsPerQuery: maxMemoryOperators,
		sampleEvery:                sampleEvery,
		queries:                    make(map[string]*sqlQueryProfileState, maxQueries),
	}, nil
}

// Record submits one operator observation. It returns false when the global
// SampleEvery policy intentionally skips the observation. A successful record
// copies the sample into bounded profiler-owned state.
func (profiler *SQLQueryProfiler) Record(queryID string, sample SQLQueryProfileSample) (bool, error) {
	if profiler == nil {
		return false, ErrSQLQueryProfilerClosed
	}
	queryID, err := normalizeSQLQueryProfilerQueryID(queryID)
	if err != nil {
		return false, err
	}
	if sample.Operator == "" {
		return false, ErrSQLQueryProfilerOperatorRequired
	}
	if len(sample.Operator) > maxSQLQueryProfilerOperatorBytes {
		return false, fmt.Errorf("%w: operator exceeds %d bytes", ErrSQLQueryProfilerOperatorRequired, maxSQLQueryProfilerOperatorBytes)
	}
	if sample.CPUTime < 0 || sample.BlockedTime < 0 {
		return false, ErrSQLQueryProfilerDurationInvalid
	}
	if sample.Timestamp.IsZero() {
		sample.Timestamp = time.Now()
	}

	profiler.mu.Lock()
	defer profiler.mu.Unlock()
	if profiler.closed {
		return false, ErrSQLQueryProfilerClosed
	}
	profiler.sequence++
	sequence := profiler.sequence
	if (sequence-1)%profiler.sampleEvery != 0 {
		profiler.unsampledCount++
		return false, nil
	}

	state := profiler.queries[queryID]
	if state == nil {
		if len(profiler.queries) >= profiler.maxQueries {
			profiler.evictOldestLocked()
		}
		state = &sqlQueryProfileState{
			profile: SQLQueryProfile{
				QueryID: queryID,
				Samples: make([]SQLQueryProfileSample, 0, profiler.maxSamplesPerQuery),
			},
		}
		profiler.queries[queryID] = state
	}
	state.lastSeen = sequence
	state.profile.SamplesSeen++
	profiler.sampleCount++
	if len(state.profile.Samples) < profiler.maxSamplesPerQuery {
		state.profile.Samples = append(state.profile.Samples, sample)
		if len(state.profile.Samples) == profiler.maxSamplesPerQuery {
			state.next = 0
		}
		return true, nil
	}
	state.profile.Samples[state.next] = sample
	state.next++
	if state.next == profiler.maxSamplesPerQuery {
		state.next = 0
	}
	state.profile.SamplesDropped++
	profiler.droppedSampleCount++
	return true, nil
}

// RecordMemory records one bounded operator memory observation. It is
// independent of the ordinary sample ring and is lazy: callers pay its map
// and lock cost only when they explicitly use this method.
func (profiler *SQLQueryProfiler) RecordMemory(queryID, operator string, sample SQLQueryMemorySample) (bool, error) {
	if profiler == nil {
		return false, ErrSQLQueryProfilerClosed
	}
	queryID, err := normalizeSQLQueryProfilerQueryID(queryID)
	if err != nil {
		return false, err
	}
	if err := validateSQLQueryProfilerOperator(operator); err != nil {
		return false, err
	}

	profiler.mu.Lock()
	defer profiler.mu.Unlock()
	if profiler.closed {
		return false, ErrSQLQueryProfilerClosed
	}
	profiler.memorySequence++
	if profiler.memoryProfiles == nil {
		profiler.memoryProfiles = make(map[string]*sqlQueryMemoryProfileState, profiler.maxQueries)
	}
	state := profiler.memoryProfiles[queryID]
	if state == nil {
		if len(profiler.memoryProfiles) >= profiler.maxQueries {
			profiler.evictOldestMemoryProfileLocked()
		}
		state = &sqlQueryMemoryProfileState{
			profile:   SQLQueryMemoryProfile{QueryID: queryID},
			operators: make(map[string]SQLQueryOperatorMemoryProfile, profiler.maxMemoryOperatorsPerQuery),
		}
		profiler.memoryProfiles[queryID] = state
	}
	state.lastSeen = profiler.memorySequence
	operatorProfile, found := state.operators[operator]
	if !found {
		if len(state.operators) >= profiler.maxMemoryOperatorsPerQuery {
			state.profile.DroppedObservations++
			profiler.droppedMemoryObservationCount++
			return false, nil
		}
		operatorProfile.Operator = operator
	}
	operatorProfile.Observations++
	operatorProfile.AllocatedBytes = saturatingAddUint64(operatorProfile.AllocatedBytes, sample.AllocatedBytes)
	if sample.PeakBytes > operatorProfile.PeakBytes {
		operatorProfile.PeakBytes = sample.PeakBytes
	}
	if sample.RetainedBytes > operatorProfile.MaxRetainedBytes {
		operatorProfile.MaxRetainedBytes = sample.RetainedBytes
	}
	state.operators[operator] = operatorProfile
	profiler.memoryObservationCount++
	return true, nil
}

// Profile returns an oldest-first copy of one query profile.
func (profiler *SQLQueryProfiler) Profile(queryID string) (SQLQueryProfile, bool) {
	if profiler == nil {
		return SQLQueryProfile{}, false
	}
	queryID, err := normalizeSQLQueryProfilerQueryID(queryID)
	if err != nil {
		return SQLQueryProfile{}, false
	}
	profiler.mu.RLock()
	defer profiler.mu.RUnlock()
	state, ok := profiler.queries[queryID]
	if !ok {
		return SQLQueryProfile{}, false
	}
	return snapshotSQLQueryProfile(state), true
}

// Profiles returns all retained profiles in deterministic query-ID order.
func (profiler *SQLQueryProfiler) Profiles() []SQLQueryProfile {
	if profiler == nil {
		return nil
	}
	profiler.mu.RLock()
	profiles := make([]SQLQueryProfile, 0, len(profiler.queries))
	for _, state := range profiler.queries {
		profiles = append(profiles, snapshotSQLQueryProfile(state))
	}
	profiler.mu.RUnlock()
	sort.Slice(profiles, func(left, right int) bool {
		return profiles[left].QueryID < profiles[right].QueryID
	})
	return profiles
}

// MemoryProfile returns an independent deterministic snapshot for one query's
// operator memory observations.
func (profiler *SQLQueryProfiler) MemoryProfile(queryID string) (SQLQueryMemoryProfile, bool) {
	if profiler == nil {
		return SQLQueryMemoryProfile{}, false
	}
	queryID, err := normalizeSQLQueryProfilerQueryID(queryID)
	if err != nil {
		return SQLQueryMemoryProfile{}, false
	}
	profiler.mu.RLock()
	defer profiler.mu.RUnlock()
	state, ok := profiler.memoryProfiles[queryID]
	if !ok {
		return SQLQueryMemoryProfile{}, false
	}
	return snapshotSQLQueryMemoryProfile(state), true
}

// MemoryProfiles returns all retained memory profiles in deterministic
// query-ID and operator-name order.
func (profiler *SQLQueryProfiler) MemoryProfiles() []SQLQueryMemoryProfile {
	if profiler == nil {
		return nil
	}
	profiler.mu.RLock()
	profiles := make([]SQLQueryMemoryProfile, 0, len(profiler.memoryProfiles))
	for _, state := range profiler.memoryProfiles {
		profiles = append(profiles, snapshotSQLQueryMemoryProfile(state))
	}
	profiler.mu.RUnlock()
	sort.Slice(profiles, func(left, right int) bool {
		return profiles[left].QueryID < profiles[right].QueryID
	})
	return profiles
}

// Stats returns bounded aggregate counters for operational monitoring.
func (profiler *SQLQueryProfiler) Stats() SQLQueryProfilerStats {
	if profiler == nil {
		return SQLQueryProfilerStats{}
	}
	profiler.mu.RLock()
	defer profiler.mu.RUnlock()
	return SQLQueryProfilerStats{
		QueryCount:                    len(profiler.queries),
		SampleCount:                   profiler.sampleCount,
		UnsampledCount:                profiler.unsampledCount,
		DroppedSampleCount:            profiler.droppedSampleCount,
		EvictedQueryCount:             profiler.evictedQueryCount,
		MemoryQueryCount:              len(profiler.memoryProfiles),
		MemoryObservationCount:        profiler.memoryObservationCount,
		DroppedMemoryObservationCount: profiler.droppedMemoryObservationCount,
		EvictedMemoryQueryCount:       profiler.evictedMemoryQueryCount,
	}
}

// Close stops future recording while leaving the last bounded snapshot
// available for inspection.
func (profiler *SQLQueryProfiler) Close() {
	if profiler == nil {
		return
	}
	profiler.mu.Lock()
	profiler.closed = true
	profiler.mu.Unlock()
}

func (profiler *SQLQueryProfiler) evictOldestLocked() {
	var oldestID string
	var oldestSequence uint64
	for queryID, state := range profiler.queries {
		if oldestID == "" || state.lastSeen < oldestSequence {
			oldestID = queryID
			oldestSequence = state.lastSeen
		}
	}
	if oldestID != "" {
		delete(profiler.queries, oldestID)
		profiler.evictedQueryCount++
	}
}

func (profiler *SQLQueryProfiler) evictOldestMemoryProfileLocked() {
	var oldestID string
	var oldestSequence uint64
	for queryID, state := range profiler.memoryProfiles {
		if oldestID == "" || state.lastSeen < oldestSequence {
			oldestID = queryID
			oldestSequence = state.lastSeen
		}
	}
	if oldestID != "" {
		delete(profiler.memoryProfiles, oldestID)
		profiler.evictedMemoryQueryCount++
	}
}

func snapshotSQLQueryProfile(state *sqlQueryProfileState) SQLQueryProfile {
	profile := state.profile
	profile.Samples = make([]SQLQueryProfileSample, len(state.profile.Samples))
	if len(state.profile.Samples) == 0 || state.next == 0 {
		copy(profile.Samples, state.profile.Samples)
		return profile
	}
	for index := range state.profile.Samples {
		profile.Samples[index] = state.profile.Samples[(state.next+index)%len(state.profile.Samples)]
	}
	return profile
}

func snapshotSQLQueryMemoryProfile(state *sqlQueryMemoryProfileState) SQLQueryMemoryProfile {
	profile := SQLQueryMemoryProfile{
		QueryID:             state.profile.QueryID,
		DroppedObservations: state.profile.DroppedObservations,
		Operators:           make([]SQLQueryOperatorMemoryProfile, 0, len(state.operators)),
	}
	for _, operator := range state.operators {
		profile.Operators = append(profile.Operators, operator)
	}
	sort.Slice(profile.Operators, func(left, right int) bool {
		return profile.Operators[left].Operator < profile.Operators[right].Operator
	})
	return profile
}

func validateSQLQueryProfilerOperator(operator string) error {
	if operator == "" {
		return ErrSQLQueryProfilerOperatorRequired
	}
	if len(operator) > maxSQLQueryProfilerOperatorBytes {
		return fmt.Errorf("%w: operator exceeds %d bytes", ErrSQLQueryProfilerOperatorRequired, maxSQLQueryProfilerOperatorBytes)
	}
	return nil
}

func saturatingAddUint64(left, right uint64) uint64 {
	if ^uint64(0)-left < right {
		return ^uint64(0)
	}
	return left + right
}

func normalizeSQLQueryProfilerQueryID(queryID string) (string, error) {
	if queryID == "" {
		return "", ErrSQLQueryProfilerQueryIDRequired
	}
	if len(queryID) > maxSQLQueryProfilerQueryIDBytes {
		return "", fmt.Errorf("%w: query ID exceeds %d bytes", ErrSQLQueryProfilerQueryIDRequired, maxSQLQueryProfilerQueryIDBytes)
	}
	return queryID, nil
}
