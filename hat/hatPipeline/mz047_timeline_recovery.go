package hatPipeline

import (
	"errors"
	"sort"
)

const (
	// DefaultTimelineRecoveryMaxComponents bounds the number of frontiers in a
	// recovery plan when the caller does not provide a limit.
	DefaultTimelineRecoveryMaxComponents = 1024
	maxTimelineRecoveryComponents        = 1 << 20
	maxTimelineRecoveryIDBytes           = 1 << 20
)

var (
	// ErrTimelineRecoveryOptionsInvalid indicates an invalid recovery limit.
	ErrTimelineRecoveryOptionsInvalid = errors.New("hatPipeline: timeline recovery options are invalid")
	// ErrTimelineRecoveryCheckpointInvalid indicates an empty, oversized, or
	// descending checkpoint ID/bound pair.
	ErrTimelineRecoveryCheckpointInvalid = errors.New("hatPipeline: timeline recovery checkpoint is invalid")
	// ErrTimelineRecoveryDuplicateID indicates that one input contains an ID
	// more than once.
	ErrTimelineRecoveryDuplicateID = errors.New("hatPipeline: timeline recovery checkpoint ID is duplicated")
	// ErrTimelineRecoveryComponentMismatch indicates that persisted and
	// observed inputs do not contain the same component IDs.
	ErrTimelineRecoveryComponentMismatch = errors.New("hatPipeline: timeline recovery component sets do not match")
	// ErrTimelineRecoveryComponentLimit indicates that an input exceeds its
	// configured component bound.
	ErrTimelineRecoveryComponentLimit = errors.New("hatPipeline: timeline recovery component limit reached")
)

// TimelineRecoveryOptions bounds a reconciliation request. A zero
// MaxComponents selects DefaultTimelineRecoveryMaxComponents.
type TimelineRecoveryOptions struct {
	MaxComponents int
}

// TimelineRecoveryCheckpoint is the durable or observed progress of one
// named timeline component. Lower and Upper use the same frontier semantics
// as FrontierSnapshot; Generation fences progress from an older recovery.
type TimelineRecoveryCheckpoint struct {
	ID         string
	Lower      uint64
	Upper      uint64
	Generation uint64
}

// TimelineRecoveryAction tells the caller how to handle one component after
// a crash or partially persisted cutover.
type TimelineRecoveryAction uint8

const (
	// TimelineRecoveryAdopt means observed progress exactly matches the
	// persisted checkpoint and can be used as-is.
	TimelineRecoveryAdopt TimelineRecoveryAction = iota + 1
	// TimelineRecoveryReplay means observed progress is older and must replay
	// toward the persisted checkpoint before publication.
	TimelineRecoveryReplay
	// TimelineRecoveryQuarantine means observed progress is newer or internally
	// mixed with the persisted checkpoint and must not be published silently.
	TimelineRecoveryQuarantine
)

// String returns the stable action name used by diagnostics.
func (action TimelineRecoveryAction) String() string {
	switch action {
	case TimelineRecoveryAdopt:
		return "adopt"
	case TimelineRecoveryReplay:
		return "replay"
	case TimelineRecoveryQuarantine:
		return "quarantine"
	default:
		return "unknown"
	}
}

// TimelineRecoveryDecision contains the deterministic decision for one
// component. ReplayFrom and ReplayThrough are zero for adopt/quarantine and
// describe the observed-to-persisted lower-bound interval for replay.
type TimelineRecoveryDecision struct {
	ID            string
	Persisted     TimelineRecoveryCheckpoint
	Observed      TimelineRecoveryCheckpoint
	Action        TimelineRecoveryAction
	ReplayFrom    uint64
	ReplayThrough uint64
}

// TimelineRecoveryPlan is a detached, ID-sorted recovery plan. SafeLower and
// SafeUpper are the conservative common bounds across both checkpoints; a
// caller must not publish beyond them while applying the plan.
type TimelineRecoveryPlan struct {
	Decisions []TimelineRecoveryDecision
	SafeLower uint64
	SafeUpper uint64
}

// HasQuarantine reports whether any component requires operator or caller
// intervention before progress can be published.
func (plan TimelineRecoveryPlan) HasQuarantine() bool {
	for _, decision := range plan.Decisions {
		if decision.Action == TimelineRecoveryQuarantine {
			return true
		}
	}
	return false
}

// ReplayCount reports how many components need replay.
func (plan TimelineRecoveryPlan) ReplayCount() int {
	count := 0
	for _, decision := range plan.Decisions {
		if decision.Action == TimelineRecoveryReplay {
			count++
		}
	}
	return count
}

// ReconcileTimelineRecovery compares a persisted multi-component timeline
// checkpoint with observed progress after restart. It performs no I/O and
// mutates neither input slice. Exact progress is adoptable, older progress is
// replayable, and newer or mixed progress is quarantined to prevent a partial
// restore from silently skipping data.
func ReconcileTimelineRecovery(persisted, observed []TimelineRecoveryCheckpoint, options TimelineRecoveryOptions) (TimelineRecoveryPlan, error) {
	maxComponents := options.MaxComponents
	if maxComponents == 0 {
		maxComponents = DefaultTimelineRecoveryMaxComponents
	}
	if maxComponents < 1 || maxComponents > maxTimelineRecoveryComponents {
		return TimelineRecoveryPlan{}, ErrTimelineRecoveryOptionsInvalid
	}
	if len(persisted) > maxComponents || len(observed) > maxComponents {
		return TimelineRecoveryPlan{}, ErrTimelineRecoveryComponentLimit
	}

	persistedSorted, err := sortedTimelineRecoveryCheckpoints(persisted)
	if err != nil {
		return TimelineRecoveryPlan{}, err
	}
	observedSorted, err := sortedTimelineRecoveryCheckpoints(observed)
	if err != nil {
		return TimelineRecoveryPlan{}, err
	}
	if len(persistedSorted) != len(observedSorted) {
		return TimelineRecoveryPlan{}, ErrTimelineRecoveryComponentMismatch
	}

	plan := TimelineRecoveryPlan{
		Decisions: make([]TimelineRecoveryDecision, 0, len(persistedSorted)),
	}
	for index, persistedCheckpoint := range persistedSorted {
		observedCheckpoint := observedSorted[index]
		if persistedCheckpoint.ID != observedCheckpoint.ID {
			return TimelineRecoveryPlan{}, ErrTimelineRecoveryComponentMismatch
		}
		decision := TimelineRecoveryDecision{
			ID:        persistedCheckpoint.ID,
			Persisted: persistedCheckpoint,
			Observed:  observedCheckpoint,
			Action:    timelineRecoveryAction(persistedCheckpoint, observedCheckpoint),
		}
		if decision.Action == TimelineRecoveryReplay {
			decision.ReplayFrom = observedCheckpoint.Lower
			decision.ReplayThrough = persistedCheckpoint.Lower
		}
		plan.Decisions = append(plan.Decisions, decision)

		if index == 0 {
			plan.SafeLower = persistedCheckpoint.Lower
			plan.SafeUpper = persistedCheckpoint.Upper
		} else {
			plan.SafeLower = minUint64(plan.SafeLower, persistedCheckpoint.Lower)
			plan.SafeUpper = minUint64(plan.SafeUpper, persistedCheckpoint.Upper)
		}
		plan.SafeLower = minUint64(plan.SafeLower, observedCheckpoint.Lower)
		plan.SafeUpper = minUint64(plan.SafeUpper, observedCheckpoint.Upper)
	}
	return plan, nil
}

func sortedTimelineRecoveryCheckpoints(checkpoints []TimelineRecoveryCheckpoint) ([]TimelineRecoveryCheckpoint, error) {
	sorted := append([]TimelineRecoveryCheckpoint(nil), checkpoints...)
	sort.Sort(timelineRecoveryCheckpointsByID(sorted))
	for index, checkpoint := range sorted {
		if checkpoint.ID == "" || len(checkpoint.ID) > maxTimelineRecoveryIDBytes || checkpoint.Lower > checkpoint.Upper {
			return nil, ErrTimelineRecoveryCheckpointInvalid
		}
		if index > 0 && sorted[index-1].ID == checkpoint.ID {
			return nil, ErrTimelineRecoveryDuplicateID
		}
	}
	return sorted, nil
}

type timelineRecoveryCheckpointsByID []TimelineRecoveryCheckpoint

func (checkpoints timelineRecoveryCheckpointsByID) Len() int {
	return len(checkpoints)
}

func (checkpoints timelineRecoveryCheckpointsByID) Less(left, right int) bool {
	return checkpoints[left].ID < checkpoints[right].ID
}

func (checkpoints timelineRecoveryCheckpointsByID) Swap(left, right int) {
	checkpoints[left], checkpoints[right] = checkpoints[right], checkpoints[left]
}

func timelineRecoveryAction(persisted, observed TimelineRecoveryCheckpoint) TimelineRecoveryAction {
	if observed.Lower > persisted.Lower || observed.Upper > persisted.Upper || observed.Generation > persisted.Generation {
		return TimelineRecoveryQuarantine
	}
	if observed.Lower < persisted.Lower || observed.Upper < persisted.Upper || observed.Generation < persisted.Generation {
		return TimelineRecoveryReplay
	}
	return TimelineRecoveryAdopt
}

func minUint64(left, right uint64) uint64 {
	if left < right {
		return left
	}
	return right
}
