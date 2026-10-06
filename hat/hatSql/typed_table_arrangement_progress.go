package hatSql

import (
	"sync"
	"time"
)

// TypedTableArrangementHydrationProgressSnapshot is a point-in-time view of
// one caller-owned hydration operation. Total and Completed count changes from
// the checkpoint observed by the first update. ETA is zero until a rate can
// be estimated or the operation is complete.
type TypedTableArrangementHydrationProgressSnapshot struct {
	StartedAt      time.Time     `json:"started_at"`
	UpdatedAt      time.Time     `json:"updated_at"`
	SourceSequence uint64        `json:"source_sequence"`
	Completed      uint64        `json:"completed"`
	Total          uint64        `json:"total"`
	Pending        uint64        `json:"pending"`
	Elapsed        time.Duration `json:"elapsed"`
	ETA            time.Duration `json:"eta"`
	Complete       bool          `json:"complete"`
}

// TypedTableArrangementHydrationProgress is a concurrently pollable,
// caller-owned progress tracker. It has no allocation or clock cost unless a
// caller opts into HydrateWithProgress.
type TypedTableArrangementHydrationProgress struct {
	mu        sync.RWMutex
	now       func() time.Time
	startedAt time.Time
	updatedAt time.Time
	base      uint64
	source    uint64
	completed uint64
	total     uint64
	complete  bool
}

// NewTypedTableArrangementHydrationProgress creates a progress tracker. A nil
// clock uses time.Now; a clock can be supplied for deterministic tests.
func NewTypedTableArrangementHydrationProgress(now func() time.Time) *TypedTableArrangementHydrationProgress {
	if now == nil {
		now = time.Now
	}
	return &TypedTableArrangementHydrationProgress{now: now}
}

// Snapshot returns a race-safe view. It is valid to call it before the first
// hydration update or after the arrangement has completed.
func (progress *TypedTableArrangementHydrationProgress) Snapshot() TypedTableArrangementHydrationProgressSnapshot {
	if progress == nil {
		return TypedTableArrangementHydrationProgressSnapshot{}
	}
	now := progress.clock()
	progress.mu.RLock()
	defer progress.mu.RUnlock()
	return progress.snapshotLocked(now)
}

// HydrateWithProgress preserves Hydrate's behavior and updates progress after
// each successful bounded replay. Passing nil is equivalent to Hydrate.
func (arrangement *TypedTableAggregateArrangement) HydrateWithProgress(limit int, progress *TypedTableArrangementHydrationProgress) (TypedTableAggregateArrangementHydration, error) {
	report, err := arrangement.Hydrate(limit)
	if err != nil {
		return report, err
	}
	if progress != nil {
		progress.observe(report.Before, report.After, report.SourceSequence, report.Complete)
	}
	return report, nil
}

// HydrateWithProgress preserves Hydrate's behavior for both join inputs and
// reports their combined progress through one caller-owned tracker.
func (arrangement *TypedTableJoinArrangement) HydrateWithProgress(limit int, progress *TypedTableArrangementHydrationProgress) (TypedTableJoinArrangementHydration, error) {
	report, err := arrangement.Hydrate(limit)
	if err != nil {
		return report, err
	}
	if progress != nil {
		progress.observe(
			hydrationProgressSum(report.LeftBefore, report.RightBefore),
			hydrationProgressSum(report.LeftAfter, report.RightAfter),
			hydrationProgressSum(report.LeftSourceSequence, report.RightSourceSequence),
			report.Complete,
		)
	}
	return report, nil
}

func (progress *TypedTableArrangementHydrationProgress) observe(before, after, source uint64, complete bool) {
	now := progress.clock()
	progress.mu.Lock()
	defer progress.mu.Unlock()
	if progress.startedAt.IsZero() {
		progress.startedAt = now
		progress.base = before
	}
	if source < progress.base {
		source = progress.base
	}
	if after < progress.base {
		after = progress.base
	}
	progress.source = source
	progress.total = source - progress.base
	progress.completed = after - progress.base
	if progress.completed > progress.total {
		progress.total = progress.completed
	}
	progress.complete = complete || progress.completed >= progress.total
	progress.updatedAt = now
}

func (progress *TypedTableArrangementHydrationProgress) snapshotLocked(now time.Time) TypedTableArrangementHydrationProgressSnapshot {
	snapshot := TypedTableArrangementHydrationProgressSnapshot{
		StartedAt:      progress.startedAt,
		UpdatedAt:      progress.updatedAt,
		SourceSequence: progress.source,
		Completed:      progress.completed,
		Total:          progress.total,
		Complete:       progress.complete,
	}
	if snapshot.Total >= snapshot.Completed {
		snapshot.Pending = snapshot.Total - snapshot.Completed
	}
	if !snapshot.StartedAt.IsZero() && now.After(snapshot.StartedAt) {
		snapshot.Elapsed = now.Sub(snapshot.StartedAt)
	}
	if !snapshot.Complete && snapshot.Pending > 0 && snapshot.Completed > 0 && snapshot.Elapsed > 0 {
		eta := float64(snapshot.Pending) / float64(snapshot.Completed) * float64(snapshot.Elapsed)
		maxDuration := float64(time.Duration(1<<63 - 1))
		if eta > 0 && eta < maxDuration {
			snapshot.ETA = time.Duration(eta)
		}
	}
	return snapshot
}

func (progress *TypedTableArrangementHydrationProgress) clock() time.Time {
	progress.mu.RLock()
	now := progress.now
	progress.mu.RUnlock()
	if now == nil {
		return time.Now()
	}
	return now()
}

func hydrationProgressSum(left, right uint64) uint64 {
	if ^uint64(0)-left < right {
		return ^uint64(0)
	}
	return left + right
}
