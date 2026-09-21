package hatSql

import (
	"fmt"
	"sort"
	"time"
)

// SQLQueryStageSample is one caller-supplied stage observation. The profiler
// aggregates it immediately, retaining no per-observation slice.
type SQLQueryStageSample struct {
	Stage          string
	CPUTime        time.Duration
	BlockedTime    time.Duration
	Rows           uint64
	Bytes          uint64
	AllocatedBytes uint64
	PeakBytes      uint64
	RetainedBytes  uint64
}

// SQLQueryStageMetrics is a bounded aggregate for one query stage. CPUTime,
// BlockedTime, Bytes, and AllocatedBytes are saturating totals; PeakBytes and
// MaxRetainedBytes retain maxima.
type SQLQueryStageMetrics struct {
	Stage            string        `json:"stage"`
	Observations     uint64        `json:"observations"`
	CPUTime          time.Duration `json:"cpu_time"`
	BlockedTime      time.Duration `json:"blocked_time"`
	Rows             uint64        `json:"rows"`
	Bytes            uint64        `json:"bytes"`
	AllocatedBytes   uint64        `json:"allocated_bytes"`
	PeakBytes        uint64        `json:"peak_bytes"`
	MaxRetainedBytes uint64        `json:"max_retained_bytes"`
}

// SQLQueryStageProfile is a deterministic, bounded snapshot for one query.
// DroppedObservations counts records rejected after the stage bound was
// reached.
type SQLQueryStageProfile struct {
	QueryID             string                 `json:"query_id"`
	Stages              []SQLQueryStageMetrics `json:"stages"`
	DroppedObservations uint64                 `json:"dropped_observations"`
}

type sqlQueryStageProfileState struct {
	profile  SQLQueryStageProfile
	stages   map[string]SQLQueryStageMetrics
	lastSeen uint64
}

// RecordStage aggregates one stage observation. Stage profiling is opt-in and
// independent of ordinary sample sampling; ordinary Record callers pay no
// stage-map cost.
func (profiler *SQLQueryProfiler) RecordStage(queryID string, sample SQLQueryStageSample) (bool, error) {
	if profiler == nil {
		return false, ErrSQLQueryProfilerClosed
	}
	queryID, err := normalizeSQLQueryProfilerQueryID(queryID)
	if err != nil {
		return false, err
	}
	if err := validateSQLQueryProfilerStage(sample.Stage); err != nil {
		return false, err
	}
	if sample.CPUTime < 0 || sample.BlockedTime < 0 {
		return false, ErrSQLQueryProfilerDurationInvalid
	}

	profiler.mu.Lock()
	defer profiler.mu.Unlock()
	if profiler.closed {
		return false, ErrSQLQueryProfilerClosed
	}
	profiler.stageSequence++
	state := profiler.stageProfiles[queryID]
	if state == nil {
		if profiler.stageProfiles == nil {
			profiler.stageProfiles = make(map[string]*sqlQueryStageProfileState, profiler.maxQueries)
		}
		if len(profiler.stageProfiles) >= profiler.maxQueries {
			profiler.evictOldestStageProfileLocked()
		}
		state = &sqlQueryStageProfileState{
			profile: SQLQueryStageProfile{QueryID: queryID},
			stages:  make(map[string]SQLQueryStageMetrics, profiler.maxStagesPerQuery),
		}
		profiler.stageProfiles[queryID] = state
	}
	state.lastSeen = profiler.stageSequence
	metrics, found := state.stages[sample.Stage]
	if !found {
		if len(state.stages) >= profiler.maxStagesPerQuery {
			state.profile.DroppedObservations = saturatingAddUint64(state.profile.DroppedObservations, 1)
			profiler.droppedStageObservationCount = saturatingAddUint64(profiler.droppedStageObservationCount, 1)
			return false, nil
		}
		metrics.Stage = sample.Stage
	}
	metrics.Observations = saturatingAddUint64(metrics.Observations, 1)
	metrics.CPUTime = saturatingAddDuration(metrics.CPUTime, sample.CPUTime)
	metrics.BlockedTime = saturatingAddDuration(metrics.BlockedTime, sample.BlockedTime)
	metrics.Rows = saturatingAddUint64(metrics.Rows, sample.Rows)
	metrics.Bytes = saturatingAddUint64(metrics.Bytes, sample.Bytes)
	metrics.AllocatedBytes = saturatingAddUint64(metrics.AllocatedBytes, sample.AllocatedBytes)
	if sample.PeakBytes > metrics.PeakBytes {
		metrics.PeakBytes = sample.PeakBytes
	}
	if sample.RetainedBytes > metrics.MaxRetainedBytes {
		metrics.MaxRetainedBytes = sample.RetainedBytes
	}
	state.stages[sample.Stage] = metrics
	profiler.stageObservationCount = saturatingAddUint64(profiler.stageObservationCount, 1)
	return true, nil
}

// StageProfile returns an independent deterministic snapshot for one query.
func (profiler *SQLQueryProfiler) StageProfile(queryID string) (SQLQueryStageProfile, bool) {
	if profiler == nil {
		return SQLQueryStageProfile{}, false
	}
	queryID, err := normalizeSQLQueryProfilerQueryID(queryID)
	if err != nil {
		return SQLQueryStageProfile{}, false
	}
	profiler.mu.RLock()
	defer profiler.mu.RUnlock()
	state, ok := profiler.stageProfiles[queryID]
	if !ok {
		return SQLQueryStageProfile{}, false
	}
	return snapshotSQLQueryStageProfile(state), true
}

// StageProfiles returns all retained stage profiles in query-ID order.
func (profiler *SQLQueryProfiler) StageProfiles() []SQLQueryStageProfile {
	if profiler == nil {
		return nil
	}
	profiler.mu.RLock()
	profiles := make([]SQLQueryStageProfile, 0, len(profiler.stageProfiles))
	for _, state := range profiler.stageProfiles {
		profiles = append(profiles, snapshotSQLQueryStageProfile(state))
	}
	profiler.mu.RUnlock()
	sort.Slice(profiles, func(left, right int) bool {
		return profiles[left].QueryID < profiles[right].QueryID
	})
	return profiles
}

func (profiler *SQLQueryProfiler) evictOldestStageProfileLocked() {
	var oldestID string
	var oldestSequence uint64
	for queryID, state := range profiler.stageProfiles {
		if oldestID == "" || state.lastSeen < oldestSequence {
			oldestID = queryID
			oldestSequence = state.lastSeen
		}
	}
	if oldestID != "" {
		delete(profiler.stageProfiles, oldestID)
		profiler.evictedStageQueryCount = saturatingAddUint64(profiler.evictedStageQueryCount, 1)
	}
}

func snapshotSQLQueryStageProfile(state *sqlQueryStageProfileState) SQLQueryStageProfile {
	profile := SQLQueryStageProfile{
		QueryID:             state.profile.QueryID,
		DroppedObservations: state.profile.DroppedObservations,
		Stages:              make([]SQLQueryStageMetrics, 0, len(state.stages)),
	}
	for _, metrics := range state.stages {
		profile.Stages = append(profile.Stages, metrics)
	}
	sort.Slice(profile.Stages, func(left, right int) bool {
		return profile.Stages[left].Stage < profile.Stages[right].Stage
	})
	return profile
}

func validateSQLQueryProfilerStage(stage string) error {
	if stage == "" {
		return ErrSQLQueryProfilerStageRequired
	}
	if len(stage) > maxSQLQueryProfilerOperatorBytes {
		return fmt.Errorf("%w: stage exceeds %d bytes", ErrSQLQueryProfilerStageRequired, maxSQLQueryProfilerOperatorBytes)
	}
	return nil
}

func saturatingAddDuration(left, right time.Duration) time.Duration {
	const maxDuration = time.Duration(1<<63 - 1)
	if maxDuration-left < right {
		return maxDuration
	}
	return left + right
}
