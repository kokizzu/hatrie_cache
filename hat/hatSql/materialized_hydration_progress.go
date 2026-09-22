package hatSql

import (
	"context"
	"sync/atomic"
	"time"
)

// MaterializedViewHydrationProgress reports source-row work observed while a
// materialized view is hydrating. EstimatedWork is the sum of optional source
// cardinality estimates; it is not an estimate of the final result size.
type MaterializedViewHydrationProgress struct {
	CompletedWork          int64     `json:"completed_work"`
	EstimatedWork          int64     `json:"estimated_work"`
	EstimatedRemainingWork int64     `json:"estimated_remaining_work"`
	Progress               float64   `json:"progress"`
	ProgressKnown          bool      `json:"progress_known"`
	EstimateAvailable      bool      `json:"estimate_available"`
	EstimateExact          bool      `json:"estimate_exact"`
	StartedAt              time.Time `json:"started_at,omitempty"`
}

type materializedViewHydrationProgressState struct {
	estimatedWork     int64
	estimateAvailable bool
	estimateExact     bool
	startedAt         time.Time
	completedWork     int64
	finished          uint32
}

func newMaterializedViewHydrationProgressState(estimatedWork int64, estimateAvailable, estimateExact bool) *materializedViewHydrationProgressState {
	if estimatedWork < 0 {
		estimatedWork = 0
		estimateAvailable = false
		estimateExact = false
	}
	return &materializedViewHydrationProgressState{
		estimatedWork:     estimatedWork,
		estimateAvailable: estimateAvailable,
		estimateExact:     estimateExact,
		startedAt:         time.Now().UTC(),
	}
}

func (state *materializedViewHydrationProgressState) recordWork() {
	if state == nil || atomic.LoadUint32(&state.finished) != 0 {
		return
	}
	atomic.AddInt64(&state.completedWork, 1)
}

func (state *materializedViewHydrationProgressState) complete() {
	if state == nil {
		return
	}
	if state.estimateAvailable {
		atomic.StoreInt64(&state.completedWork, state.estimatedWork)
	}
	atomic.StoreUint32(&state.finished, 1)
}

func (state *materializedViewHydrationProgressState) snapshot() MaterializedViewHydrationProgress {
	if state == nil {
		return MaterializedViewHydrationProgress{}
	}
	completed := atomic.LoadInt64(&state.completedWork)
	if completed < 0 {
		completed = 0
	}
	if state.estimateAvailable && completed > state.estimatedWork {
		completed = state.estimatedWork
	}
	finished := atomic.LoadUint32(&state.finished) != 0
	progress := 0.0
	if finished {
		progress = 1
	} else if state.estimateAvailable && state.estimatedWork > 0 {
		progress = float64(completed) / float64(state.estimatedWork)
	}
	remaining := int64(0)
	if state.estimateAvailable && state.estimatedWork > completed {
		remaining = state.estimatedWork - completed
	}
	return MaterializedViewHydrationProgress{
		CompletedWork:          completed,
		EstimatedWork:          state.estimatedWork,
		EstimatedRemainingWork: remaining,
		Progress:               progress,
		ProgressKnown:          state.estimateAvailable,
		EstimateAvailable:      state.estimateAvailable,
		EstimateExact:          state.estimateExact,
		StartedAt:              state.startedAt,
	}
}

type materializedViewHydrationProgressContextKey struct{}

func withMaterializedViewHydrationProgress(ctx context.Context, state *materializedViewHydrationProgressState) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, materializedViewHydrationProgressContextKey{}, state)
}

func reportMaterializedViewHydrationWork(ctx context.Context) {
	if ctx == nil {
		return
	}
	state, ok := ctx.Value(materializedViewHydrationProgressContextKey{}).(*materializedViewHydrationProgressState)
	if ok {
		state.recordWork()
	}
}

func estimateMaterializedViewHydrationWork(resolver SourceResolver, dependencies []string) (int64, bool, bool) {
	if len(dependencies) == 0 {
		return 0, true, true
	}
	cardinality, ok := resolver.(SourceCardinalityResolver)
	if !ok {
		return 0, false, false
	}
	var total int64
	exact := true
	for _, dependency := range dependencies {
		rows, rowExact, available, err := cardinality.SQLSourceCardinality("CACHE", dependency)
		if err != nil || !available || rows < 0 {
			return 0, false, false
		}
		value := int64(rows)
		if total > int64(^uint64(0)>>1)-value {
			return 0, false, false
		}
		total += value
		exact = exact && rowExact
	}
	return total, true, exact
}
