package hatPartition

import (
	"fmt"

	"github.com/cespare/xxhash/v2"
)

// ResizeOperation identifies the direction of one adjacent partition resize.
type ResizeOperation string

const (
	// ResizeSplit doubles the partition count.
	ResizeSplit ResizeOperation = "split"
	// ResizeMerge halves the partition count.
	ResizeMerge ResizeOperation = "merge"
)

// ResizeMove maps one source partition to one target partition. A split emits
// two moves for every source partition; a merge emits two source moves for
// every target partition.
type ResizeMove struct {
	Source int
	Target int
}

// ResizePlan is an immutable description of one adjacent power-of-two
// partition resize. Create plans with PlanSplit, PlanMerge, or PlanResize.
// Plans only describe routing; they do not move data or change configuration.
type ResizePlan struct {
	sourceCount int
	targetCount int
	operation   ResizeOperation
	sourceMask  uint64
	targetMask  uint64
	initialized bool
}

// PlanSplit creates a plan that doubles an enabled partition count.
func PlanSplit(sourceCount int) (ResizePlan, error) {
	if err := validateResizeCount(sourceCount); err != nil {
		return ResizePlan{}, err
	}
	if sourceCount > MaxCount/2 {
		return ResizePlan{}, fmt.Errorf("hatriecache: cannot split %d partitions beyond the maximum of %d", sourceCount, MaxCount)
	}
	return ResizePlan{
		sourceCount: sourceCount,
		targetCount: sourceCount * 2,
		operation:   ResizeSplit,
		sourceMask:  uint64(sourceCount - 1),
		targetMask:  uint64(sourceCount*2 - 1),
		initialized: true,
	}, nil
}

// PlanMerge creates a plan that halves an enabled partition count. The
// minimum supported target count remains two because one partition is the
// single-trie mode rather than a local partition layout.
func PlanMerge(sourceCount int) (ResizePlan, error) {
	if err := validateResizeCount(sourceCount); err != nil {
		return ResizePlan{}, err
	}
	if sourceCount <= 2 {
		return ResizePlan{}, fmt.Errorf("hatriecache: cannot merge %d partitions below the minimum of 2", sourceCount)
	}
	return ResizePlan{
		sourceCount: sourceCount,
		targetCount: sourceCount / 2,
		operation:   ResizeMerge,
		sourceMask:  uint64(sourceCount - 1),
		targetMask:  uint64(sourceCount/2 - 1),
		initialized: true,
	}, nil
}

// PlanResize creates a split or merge plan when targetCount is exactly twice
// or half sourceCount. Arbitrary count changes are rejected because they do
// not have a stable one-step mapping for the existing power-of-two router.
func PlanResize(sourceCount, targetCount int) (ResizePlan, error) {
	if err := validateResizeCount(sourceCount); err != nil {
		return ResizePlan{}, err
	}
	if err := validateResizeCount(targetCount); err != nil {
		return ResizePlan{}, err
	}
	if targetCount == sourceCount*2 {
		return PlanSplit(sourceCount)
	}
	if sourceCount == targetCount*2 {
		return PlanMerge(sourceCount)
	}
	return ResizePlan{}, fmt.Errorf("hatriecache: partition resize must double or halve the count: %d to %d", sourceCount, targetCount)
}

func validateResizeCount(count int) error {
	if count == 0 {
		return fmt.Errorf("hatriecache: partition resize requires enabled partitions")
	}
	return Validate(count)
}

func (plan ResizePlan) valid() bool {
	return plan.initialized
}

// SourceCount returns the layout being resized.
func (plan ResizePlan) SourceCount() int { return plan.sourceCount }

// TargetCount returns the layout produced by the plan.
func (plan ResizePlan) TargetCount() int { return plan.targetCount }

// Operation returns ResizeSplit or ResizeMerge.
func (plan ResizePlan) Operation() ResizeOperation { return plan.operation }

// SourcePartition returns the current partition for key, or -1 for an invalid
// zero-value plan. It performs the same allocation-free hash routing as Index.
func (plan ResizePlan) SourcePartition(key string) int {
	if !plan.valid() {
		return -1
	}
	return int(xxhash.Sum64String(key) & plan.sourceMask)
}

// TargetPartition returns the destination partition for key, or -1 for an
// invalid zero-value plan. It performs no allocation.
func (plan ResizePlan) TargetPartition(key string) int {
	if !plan.valid() {
		return -1
	}
	return int(xxhash.Sum64String(key) & plan.targetMask)
}

// RouteKey returns both the current and destination partitions for key using
// one hash. The boolean is false for an invalid zero-value plan.
func (plan ResizePlan) RouteKey(key string) (source, target int, ok bool) {
	if !plan.valid() {
		return -1, -1, false
	}
	hash := xxhash.Sum64String(key)
	return int(hash & plan.sourceMask), int(hash & plan.targetMask), true
}

// MovesKey reports whether key changes partition under the plan.
func (plan ResizePlan) MovesKey(key string) bool {
	source, target, ok := plan.RouteKey(key)
	return ok && source != target
}

// Moves returns the deterministic partition mapping represented by the plan.
// It allocates only when an operator asks for the complete mapping; per-key
// migration code should use SourcePartition and TargetPartition instead.
func (plan ResizePlan) Moves() []ResizeMove {
	if !plan.valid() {
		return nil
	}
	moves := make([]ResizeMove, 0, plan.sourceCount+plan.targetCount)
	if plan.operation == ResizeSplit {
		for source := 0; source < plan.sourceCount; source++ {
			moves = append(moves,
				ResizeMove{Source: source, Target: source},
				ResizeMove{Source: source, Target: source + plan.sourceCount},
			)
		}
		return moves
	}
	for target := 0; target < plan.targetCount; target++ {
		moves = append(moves,
			ResizeMove{Source: target, Target: target},
			ResizeMove{Source: target + plan.targetCount, Target: target},
		)
	}
	return moves
}
