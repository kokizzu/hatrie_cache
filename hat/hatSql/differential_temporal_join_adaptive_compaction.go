package hatSql

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var ErrDifferentialTemporalJoinCompactionPolicyInvalid = errors.New("hatSql: differential temporal join compaction policy is invalid")

const (
	DefaultDifferentialTemporalJoinCompactionMinUpdates           uint64 = 1024
	DefaultDifferentialTemporalJoinCompactionMaxStateBytes        uint64 = 64 << 20
	DefaultDifferentialTemporalJoinCompactionMaxFrontierAge              = time.Minute
	DefaultDifferentialTemporalJoinCompactionEstimatedBytesPerRow uint64 = 128
)

// DifferentialTemporalJoinCompactionPolicy enables bounded, caller-driven
// compaction recommendations. A nil policy leaves the existing join path
// unchanged. The policy never starts a goroutine or compacts state from an
// Apply call.
type DifferentialTemporalJoinCompactionPolicy struct {
	// MinUpdates recommends compaction after this many non-zero input updates.
	// Zero uses DefaultDifferentialTemporalJoinCompactionMinUpdates.
	MinUpdates uint64
	// MaxStateBytes estimates retained state as rows multiplied by
	// EstimatedBytesPerRow. Zero uses the 64 MiB default.
	MaxStateBytes uint64
	// MaxFrontierAge recommends compaction after this duration since the last
	// successful frontier compaction. Zero uses a one-minute default.
	MaxFrontierAge time.Duration
	// EstimatedBytesPerRow is deliberately explicit because row shapes vary.
	// Zero uses DefaultDifferentialTemporalJoinCompactionEstimatedBytesPerRow.
	EstimatedBytesPerRow uint64
}

// DifferentialTemporalJoinCompactionReason identifies the first adaptive
// threshold that requested a compaction.
type DifferentialTemporalJoinCompactionReason uint8

const (
	DifferentialTemporalJoinCompactionReasonNone DifferentialTemporalJoinCompactionReason = iota
	DifferentialTemporalJoinCompactionReasonUpdates
	DifferentialTemporalJoinCompactionReasonBytes
	DifferentialTemporalJoinCompactionReasonAge
)

// DifferentialTemporalJoinCompactionRecommendation reports why the caller
// should consider invoking CompactIfNeeded. It is a hint only; frontier safety
// remains enforced by the exact compaction operation.
type DifferentialTemporalJoinCompactionRecommendation struct {
	ShouldCompact          bool
	Reason                 DifferentialTemporalJoinCompactionReason
	UpdatesSinceCompaction uint64
	EstimatedStateBytes    uint64
	RetainedRows           int
	SinceLastCompaction    time.Duration
}

// CompactionRecommendation evaluates the configured policy without changing
// state. Passing a zero time uses the current UTC time.
func (join *DifferentialTemporalJoin) CompactionRecommendation(now time.Time) DifferentialTemporalJoinCompactionRecommendation {
	if join == nil {
		return DifferentialTemporalJoinCompactionRecommendation{}
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	join.mu.Lock()
	defer join.mu.Unlock()
	return join.differentialTemporalJoinCompactionRecommendationLocked(now)
}

// CompactIfNeeded performs one exact, caller-triggered compaction when the
// policy recommends it. The candidate keys are collected before any delete;
// cancellation therefore leaves both input maps, indexes, and frontiers
// unchanged.
func (join *DifferentialTemporalJoin) CompactIfNeeded(ctx context.Context, leftFrontier, rightFrontier uint64, now time.Time) (DifferentialTemporalJoinCompactionStats, DifferentialTemporalJoinCompactionRecommendation, error) {
	if join == nil {
		return DifferentialTemporalJoinCompactionStats{}, DifferentialTemporalJoinCompactionRecommendation{}, ErrDifferentialTemporalJoinNil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	recommendation := join.CompactionRecommendation(now)
	if !recommendation.ShouldCompact {
		return DifferentialTemporalJoinCompactionStats{}, recommendation, nil
	}
	if ctx == nil || ctx.Done() == nil {
		stats, err := join.Compact(leftFrontier, rightFrontier)
		return stats, recommendation, err
	}
	if err := ctx.Err(); err != nil {
		return DifferentialTemporalJoinCompactionStats{}, recommendation, err
	}
	stats, err := join.compactDifferentialTemporalJoinContext(ctx, leftFrontier, rightFrontier, now)
	return stats, recommendation, err
}

func normalizeDifferentialTemporalJoinCompactionPolicy(policy *DifferentialTemporalJoinCompactionPolicy) (*DifferentialTemporalJoinCompactionPolicy, error) {
	if policy == nil {
		return nil, nil
	}
	normalized := *policy
	if normalized.MinUpdates == 0 {
		normalized.MinUpdates = DefaultDifferentialTemporalJoinCompactionMinUpdates
	}
	if normalized.MaxStateBytes == 0 {
		normalized.MaxStateBytes = DefaultDifferentialTemporalJoinCompactionMaxStateBytes
	}
	if normalized.MaxFrontierAge == 0 {
		normalized.MaxFrontierAge = DefaultDifferentialTemporalJoinCompactionMaxFrontierAge
	}
	if normalized.EstimatedBytesPerRow == 0 {
		normalized.EstimatedBytesPerRow = DefaultDifferentialTemporalJoinCompactionEstimatedBytesPerRow
	}
	if normalized.MaxFrontierAge < 0 || normalized.MaxStateBytes == 0 || normalized.EstimatedBytesPerRow == 0 {
		return nil, ErrDifferentialTemporalJoinCompactionPolicyInvalid
	}
	return &normalized, nil
}

func (join *DifferentialTemporalJoin) differentialTemporalJoinCompactionRecommendationLocked(now time.Time) DifferentialTemporalJoinCompactionRecommendation {
	policy := join.compactionPolicy
	if policy == nil {
		return DifferentialTemporalJoinCompactionRecommendation{}
	}
	retainedRows := len(join.left) + len(join.right)
	recommendation := DifferentialTemporalJoinCompactionRecommendation{
		UpdatesSinceCompaction: join.updatesSinceCompaction,
		EstimatedStateBytes:    differentialTemporalJoinEstimatedStateBytes(retainedRows, policy.EstimatedBytesPerRow),
		RetainedRows:           retainedRows,
	}
	if !join.lastCompactionAt.IsZero() && now.After(join.lastCompactionAt) {
		recommendation.SinceLastCompaction = now.Sub(join.lastCompactionAt)
	}
	if retainedRows == 0 {
		return recommendation
	}
	if recommendation.EstimatedStateBytes >= policy.MaxStateBytes {
		recommendation.ShouldCompact = true
		recommendation.Reason = DifferentialTemporalJoinCompactionReasonBytes
		return recommendation
	}
	if recommendation.UpdatesSinceCompaction >= policy.MinUpdates {
		recommendation.ShouldCompact = true
		recommendation.Reason = DifferentialTemporalJoinCompactionReasonUpdates
		return recommendation
	}
	if recommendation.SinceLastCompaction >= policy.MaxFrontierAge {
		recommendation.ShouldCompact = true
		recommendation.Reason = DifferentialTemporalJoinCompactionReasonAge
	}
	return recommendation
}

func (join *DifferentialTemporalJoin) recordDifferentialTemporalJoinUpdates(changes []DifferentialRow) {
	if join.compactionPolicy == nil {
		return
	}
	for _, change := range changes {
		if change.Diff == 0 {
			continue
		}
		if join.updatesSinceCompaction == ^uint64(0) {
			return
		}
		join.updatesSinceCompaction++
	}
}

func (join *DifferentialTemporalJoin) markDifferentialTemporalJoinCompacted(at time.Time) {
	if join.compactionPolicy == nil {
		return
	}
	join.updatesSinceCompaction = 0
	join.lastCompactionAt = at
}

func (join *DifferentialTemporalJoin) compactDifferentialTemporalJoinContext(ctx context.Context, leftFrontier, rightFrontier uint64, completedAt time.Time) (DifferentialTemporalJoinCompactionStats, error) {
	join.mu.Lock()
	defer join.mu.Unlock()
	if leftFrontier < join.leftFrontier || rightFrontier < join.rightFrontier {
		return DifferentialTemporalJoinCompactionStats{}, fmt.Errorf("frontiers %d/%d follow %d/%d: %w", leftFrontier, rightFrontier, join.leftFrontier, join.rightFrontier, ErrDifferentialTemporalJoinFrontierRegression)
	}
	leftExpired := make([]string, 0)
	index := 0
	for key, entry := range join.left {
		if index&255 == 0 {
			if err := ctx.Err(); err != nil {
				return DifferentialTemporalJoinCompactionStats{}, err
			}
		}
		index++
		if differentialTemporalJoinExpired(entry.time, leftFrontier, rightFrontier, join.maxTimeDistance) {
			leftExpired = append(leftExpired, key)
		}
	}
	rightExpired := make([]string, 0)
	index = 0
	for key, entry := range join.right {
		if index&255 == 0 {
			if err := ctx.Err(); err != nil {
				return DifferentialTemporalJoinCompactionStats{}, err
			}
		}
		index++
		if differentialTemporalJoinExpired(entry.time, rightFrontier, leftFrontier, join.maxTimeDistance) {
			rightExpired = append(rightExpired, key)
		}
	}
	if err := ctx.Err(); err != nil {
		return DifferentialTemporalJoinCompactionStats{}, err
	}
	for _, key := range leftExpired {
		delete(join.left, key)
	}
	for _, key := range rightExpired {
		delete(join.right, key)
	}
	differentialTemporalJoinRebuildGroups(join.left, join.leftGroups, join.leftGroupKnown)
	differentialTemporalJoinRebuildGroups(join.right, join.rightGroups, join.rightGroupKnown)
	join.leftFrontier = leftFrontier
	join.rightFrontier = rightFrontier
	stats := DifferentialTemporalJoinCompactionStats{
		LeftFrontier:  leftFrontier,
		RightFrontier: rightFrontier,
		RemovedLeft:   len(leftExpired),
		RemovedRight:  len(rightExpired),
		RetainedLeft:  len(join.left),
		RetainedRight: len(join.right),
	}
	join.markDifferentialTemporalJoinCompacted(completedAt)
	return stats, nil
}

func differentialTemporalJoinEstimatedStateBytes(retainedRows int, bytesPerRow uint64) uint64 {
	if retainedRows <= 0 {
		return 0
	}
	rows := uint64(retainedRows)
	if rows > ^uint64(0)/bytesPerRow {
		return ^uint64(0)
	}
	return rows * bytesPerRow
}
