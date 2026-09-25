package hatStorage

import (
	"errors"
	"time"
)

var (
	// ErrCompactionPriorityPolicyDisabled reports use of signal scheduling
	// without an explicitly configured policy.
	ErrCompactionPriorityPolicyDisabled = errors.New("hatriecache: compaction priority policy is disabled")
	// ErrCompactionPriorityPolicyInvalid reports invalid policy units or weights.
	ErrCompactionPriorityPolicyInvalid = errors.New("hatriecache: compaction priority policy is invalid")
	// ErrCompactionPriorityMetricsInvalid reports a negative freshness lag.
	ErrCompactionPriorityMetricsInvalid = errors.New("hatriecache: compaction priority metrics are invalid")
)

// CompactionPriorityMetrics describes the two maintenance pressures that can
// raise a merge task's priority. ReclaimableBytes is an estimate of space
// recovered by the task, not its input size.
type CompactionPriorityMetrics struct {
	FreshnessLag     time.Duration
	ReclaimableBytes uint64
}

// CompactionPriorityPolicy converts freshness and reclaimable-space metrics
// into the scheduler's integer priority. Values are normalized by their units
// before weights are applied and the result saturates at max int.
type CompactionPriorityPolicy struct {
	FreshnessWeight uint64
	SpaceWeight     uint64
	FreshnessUnit   time.Duration
	SpaceUnit       uint64
}

// DefaultCompactionPriorityPolicy gives freshness and space equal normalized
// influence. The scheduler remains disabled unless this policy is supplied in
// CompactionSchedulerOptions.
func DefaultCompactionPriorityPolicy() CompactionPriorityPolicy {
	return CompactionPriorityPolicy{
		FreshnessWeight: 1,
		SpaceWeight:     1,
		FreshnessUnit:   time.Minute,
		SpaceUnit:       1 << 20,
	}
}

// Priority returns the saturating score for metrics. Invalid policy values or
// negative freshness are represented as zero; scheduler entry points validate
// both and return the corresponding error.
func (policy CompactionPriorityPolicy) Priority(metrics CompactionPriorityMetrics) int {
	if policy.validate() != nil || metrics.FreshnessLag < 0 {
		return 0
	}
	freshnessUnits := uint64(metrics.FreshnessLag / policy.FreshnessUnit)
	if metrics.FreshnessLag > 0 && freshnessUnits == 0 {
		freshnessUnits = 1
	}
	spaceUnits := metrics.ReclaimableBytes / policy.SpaceUnit
	if metrics.ReclaimableBytes > 0 && spaceUnits == 0 {
		spaceUnits = 1
	}
	score := saturatingCompactionPriority(freshnessUnits, policy.FreshnessWeight)
	score = saturatingCompactionPriorityAdd(score, saturatingCompactionPriority(spaceUnits, policy.SpaceWeight))
	maxInt := uint64(^uint(0) >> 1)
	if score > maxInt {
		return int(maxInt)
	}
	return int(score)
}

func (policy CompactionPriorityPolicy) validate() error {
	if policy.FreshnessWeight == 0 && policy.SpaceWeight == 0 || policy.FreshnessUnit <= 0 || policy.SpaceUnit == 0 {
		return ErrCompactionPriorityPolicyInvalid
	}
	return nil
}

func saturatingCompactionPriority(left, right uint64) uint64 {
	if left == 0 || right == 0 {
		return 0
	}
	maxUint := ^uint64(0)
	if left > maxUint/right {
		return maxUint
	}
	return left * right
}

func saturatingCompactionPriorityAdd(left, right uint64) uint64 {
	if ^uint64(0)-left < right {
		return ^uint64(0)
	}
	return left + right
}
