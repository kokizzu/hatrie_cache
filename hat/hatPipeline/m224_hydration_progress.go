package hatPipeline

import (
	"math"
	"time"
)

// HydrationEstimate reports exact progress plus an optional caller-supplied
// rate-based remaining-time estimate. The estimate is zero when no positive
// rate is configured or the generation is not actively hydrating.
type HydrationEstimate struct {
	State              HydrationState `json:"state"`
	Generation         uint64         `json:"generation"`
	Completed          uint64         `json:"completed"`
	Total              uint64         `json:"total"`
	Remaining          uint64         `json:"remaining"`
	UnitsPerSecond     float64        `json:"units_per_second"`
	EstimatedRemaining time.Duration  `json:"estimated_remaining_ns"`
}

// SetRate records a caller-owned smoothed work rate for the active generation.
// It does not sample the clock or wake waiters. A rate of zero disables the
// estimate. Begin resets the rate so a new generation cannot inherit stale
// timing data.
func (machine *HydrationStateMachine) SetRate(unitsPerSecond float64) error {
	if machine == nil {
		return ErrHydrationInvalid
	}
	if unitsPerSecond < 0 || math.IsNaN(unitsPerSecond) || math.IsInf(unitsPerSecond, 0) {
		return ErrHydrationRateInvalid
	}
	machine.mu.Lock()
	machine.rate = unitsPerSecond
	machine.mu.Unlock()
	return nil
}

// Estimate returns a detached exact-progress view and, when a positive rate
// was supplied with SetRate, a rounded-up remaining duration. Estimation is
// intentionally explicit so ordinary Snapshot and Advance calls retain their
// existing cost.
func (machine *HydrationStateMachine) Estimate() HydrationEstimate {
	if machine == nil {
		return HydrationEstimate{}
	}
	machine.mu.Lock()
	defer machine.mu.Unlock()
	snapshot := machine.snapshotLocked()
	estimate := HydrationEstimate{
		State:          snapshot.State,
		Generation:     snapshot.Generation,
		Completed:      snapshot.Completed,
		Total:          snapshot.Total,
		Remaining:      snapshot.Remaining,
		UnitsPerSecond: machine.rate,
	}
	if snapshot.State == HydrationStateHydrating && snapshot.Remaining > 0 && machine.rate > 0 {
		estimate.EstimatedRemaining = hydrationRemainingDuration(snapshot.Remaining, machine.rate)
	}
	return estimate
}

func hydrationRemainingDuration(remaining uint64, unitsPerSecond float64) time.Duration {
	const maxDurationNanos = float64(1<<63 - 1)
	nanos := (float64(remaining) / unitsPerSecond) * float64(time.Second)
	if math.IsInf(nanos, 0) || nanos >= maxDurationNanos {
		return time.Duration(1<<63 - 1)
	}
	if nanos <= 0 {
		return 0
	}
	return time.Duration(math.Ceil(nanos))
}
