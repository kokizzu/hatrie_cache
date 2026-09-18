package hatPipeline

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	// DefaultConsumerGroupFenceMaxPartitions bounds one consumer-group
	// assignment snapshot when no explicit bound is supplied.
	DefaultConsumerGroupFenceMaxPartitions = 256
	maxConsumerGroupFencePartitions        = 1 << 20
)

var (
	// ErrConsumerGroupFenceNil indicates a method call on a nil fence.
	ErrConsumerGroupFenceNil = errors.New("hatPipeline: consumer group fence is nil")
	// ErrConsumerGroupFenceInvalidGroup indicates an empty consumer-group ID.
	ErrConsumerGroupFenceInvalidGroup = errors.New("hatPipeline: consumer group fence group is invalid")
	// ErrConsumerGroupFenceInvalidMember indicates an empty member ID.
	ErrConsumerGroupFenceInvalidMember = errors.New("hatPipeline: consumer group fence member is invalid")
	// ErrConsumerGroupFenceInvalidPartition indicates a negative partition ID.
	ErrConsumerGroupFenceInvalidPartition = errors.New("hatPipeline: consumer group fence partition is invalid")
	// ErrConsumerGroupFenceDuplicatePartition indicates two owners for one partition.
	ErrConsumerGroupFenceDuplicatePartition = errors.New("hatPipeline: consumer group fence partition is duplicated")
	// ErrConsumerGroupFenceOptionsInvalid indicates a capacity outside the supported bounds.
	ErrConsumerGroupFenceOptionsInvalid = errors.New("hatPipeline: consumer group fence options are invalid")
	// ErrConsumerGroupFencePartitionUnassigned indicates that a partition has no owner.
	ErrConsumerGroupFencePartitionUnassigned = errors.New("hatPipeline: consumer group fence partition is unassigned")
	// ErrConsumerGroupFenceWrongOwner indicates that the lease member is not the owner.
	ErrConsumerGroupFenceWrongOwner = errors.New("hatPipeline: consumer group fence member is not the owner")
	// ErrConsumerGroupFenceWrongGroup indicates a lease for another group.
	ErrConsumerGroupFenceWrongGroup = errors.New("hatPipeline: consumer group fence group does not match")
	// ErrConsumerGroupFenceStaleGeneration indicates a lease from before the latest rebalance.
	ErrConsumerGroupFenceStaleGeneration = errors.New("hatPipeline: consumer group fence generation is stale")
	// ErrConsumerGroupFenceGenerationExhausted indicates that the generation cannot advance.
	ErrConsumerGroupFenceGenerationExhausted = errors.New("hatPipeline: consumer group fence generation is exhausted")
)

// ConsumerGroupFenceOptions bounds one consumer group's assignment size.
type ConsumerGroupFenceOptions struct {
	// MaxPartitions is the maximum number of assigned partitions. Zero uses
	// DefaultConsumerGroupFenceMaxPartitions.
	MaxPartitions int
}

// ConsumerGroupPartitionOwner describes the member that owns one partition.
// A partition may appear at most once in a rebalance.
type ConsumerGroupPartitionOwner struct {
	Partition int32
	Member    string
}

// ConsumerGroupLease is an epoch-fenced permission for one member and
// partition. Pass it to Validate immediately before a side effect.
type ConsumerGroupLease struct {
	Group      string
	Member     string
	Partition  int32
	Generation uint64
}

// ConsumerGroupFenceSnapshot is a detached assignment view. Assignments are
// sorted by partition and can be retained or modified by the caller.
type ConsumerGroupFenceSnapshot struct {
	Group       string
	Generation  uint64
	Assignments []ConsumerGroupPartitionOwner
}

// ConsumerGroupFence rejects work from consumers that lost ownership during a
// rebalance. Rebalance serializes generation changes and publishes immutable
// state atomically; Lease and Validate are lock-free and allocation-free.
//
// The fence is a local correctness primitive. A caller must persist or
// distribute the generation and use a shared coordinator when membership spans
// processes or machines. Do not copy a fence after first use.
type ConsumerGroupFence struct {
	group          string
	maxPartitions  int
	rebalanceMutex sync.Mutex
	state          atomic.Pointer[consumerGroupFenceState]
}

type consumerGroupFenceState struct {
	generation        uint64
	denseOwners       []string
	sparseOwners      map[int32]string
	sparseAssignments []ConsumerGroupPartitionOwner
}

// NewConsumerGroupFence creates a local generation fence for group.
func NewConsumerGroupFence(group string, options ConsumerGroupFenceOptions) (*ConsumerGroupFence, error) {
	group = strings.TrimSpace(group)
	if group == "" {
		return nil, ErrConsumerGroupFenceInvalidGroup
	}
	maxPartitions := options.MaxPartitions
	if maxPartitions == 0 {
		maxPartitions = DefaultConsumerGroupFenceMaxPartitions
	}
	if maxPartitions < 1 || maxPartitions > maxConsumerGroupFencePartitions {
		return nil, ErrConsumerGroupFenceOptionsInvalid
	}
	fence := &ConsumerGroupFence{group: group, maxPartitions: maxPartitions}
	fence.state.Store(&consumerGroupFenceState{})
	return fence, nil
}

// Rebalance publishes a new assignment generation. A rejected assignment does
// not revoke the currently published generation.
func (fence *ConsumerGroupFence) Rebalance(assignments []ConsumerGroupPartitionOwner) (ConsumerGroupFenceSnapshot, error) {
	if fence == nil {
		return ConsumerGroupFenceSnapshot{}, ErrConsumerGroupFenceNil
	}
	normalized, denseOwners, err := normalizeConsumerGroupAssignments(assignments, fence.maxPartitions)
	if err != nil {
		return ConsumerGroupFenceSnapshot{}, err
	}

	fence.rebalanceMutex.Lock()
	defer fence.rebalanceMutex.Unlock()
	current := fence.state.Load()
	if current == nil {
		current = &consumerGroupFenceState{}
	}
	if current.generation == ^uint64(0) {
		return ConsumerGroupFenceSnapshot{}, ErrConsumerGroupFenceGenerationExhausted
	}
	next := &consumerGroupFenceState{generation: current.generation + 1}
	if denseOwners == nil && len(normalized) > 0 && consumerGroupFenceIsDense(normalized) {
		denseOwners = make([]string, len(normalized))
		for index, assignment := range normalized {
			denseOwners[index] = assignment.Member
		}
	}
	if denseOwners != nil {
		next.denseOwners = denseOwners
	} else if len(normalized) > 0 {
		next.sparseOwners = make(map[int32]string, len(normalized))
		next.sparseAssignments = normalized
		for _, assignment := range normalized {
			next.sparseOwners[assignment.Partition] = assignment.Member
		}
	}
	fence.state.Store(next)
	return fence.snapshotFromState(next), nil
}

func normalizeConsumerGroupAssignments(assignments []ConsumerGroupPartitionOwner, maxPartitions int) ([]ConsumerGroupPartitionOwner, []string, error) {
	if len(assignments) > maxPartitions {
		return nil, nil, ErrConsumerGroupFenceOptionsInvalid
	}
	if len(assignments) > 0 && consumerGroupFenceIsDense(assignments) {
		denseOwners := make([]string, len(assignments))
		for index, assignment := range assignments {
			member := strings.TrimSpace(assignment.Member)
			if member == "" {
				return nil, nil, ErrConsumerGroupFenceInvalidMember
			}
			denseOwners[index] = member
		}
		return nil, denseOwners, nil
	}
	normalized := append([]ConsumerGroupPartitionOwner(nil), assignments...)
	for index := range normalized {
		if normalized[index].Partition < 0 {
			return nil, nil, ErrConsumerGroupFenceInvalidPartition
		}
		member := strings.TrimSpace(normalized[index].Member)
		if member == "" {
			return nil, nil, ErrConsumerGroupFenceInvalidMember
		}
		normalized[index].Member = member
	}
	sort.Slice(normalized, func(left, right int) bool {
		return normalized[left].Partition < normalized[right].Partition
	})
	for index := 1; index < len(normalized); index++ {
		if normalized[index-1].Partition == normalized[index].Partition {
			return nil, nil, ErrConsumerGroupFenceDuplicatePartition
		}
	}
	return normalized, nil, nil
}

func consumerGroupFenceIsDense(assignments []ConsumerGroupPartitionOwner) bool {
	for index, assignment := range assignments {
		if assignment.Partition != int32(index) {
			return false
		}
	}
	return true
}

// Lease returns a generation-fenced lease for member and partition.
func (fence *ConsumerGroupFence) Lease(member string, partition int32) (ConsumerGroupLease, error) {
	if fence == nil {
		return ConsumerGroupLease{}, ErrConsumerGroupFenceNil
	}
	member = strings.TrimSpace(member)
	if member == "" {
		return ConsumerGroupLease{}, ErrConsumerGroupFenceInvalidMember
	}
	if partition < 0 {
		return ConsumerGroupLease{}, ErrConsumerGroupFenceInvalidPartition
	}
	state := fence.state.Load()
	owner, assigned := consumerGroupFenceOwner(state, partition)
	if !assigned {
		return ConsumerGroupLease{}, ErrConsumerGroupFencePartitionUnassigned
	}
	if owner != member {
		return ConsumerGroupLease{}, ErrConsumerGroupFenceWrongOwner
	}
	return ConsumerGroupLease{
		Group:      fence.group,
		Member:     member,
		Partition:  partition,
		Generation: state.generation,
	}, nil
}

// Validate checks a lease against the latest published assignment. It does
// not allocate and is safe to call concurrently with Rebalance.
func (fence *ConsumerGroupFence) Validate(lease ConsumerGroupLease) error {
	if fence == nil {
		return ErrConsumerGroupFenceNil
	}
	if lease.Group != fence.group {
		return ErrConsumerGroupFenceWrongGroup
	}
	if strings.TrimSpace(lease.Member) == "" {
		return ErrConsumerGroupFenceInvalidMember
	}
	if lease.Partition < 0 {
		return ErrConsumerGroupFenceInvalidPartition
	}
	state := fence.state.Load()
	if state == nil || lease.Generation != state.generation {
		return ErrConsumerGroupFenceStaleGeneration
	}
	owner, assigned := consumerGroupFenceOwner(state, lease.Partition)
	if !assigned {
		return ErrConsumerGroupFencePartitionUnassigned
	}
	if owner != lease.Member {
		return ErrConsumerGroupFenceWrongOwner
	}
	return nil
}

func consumerGroupFenceOwner(state *consumerGroupFenceState, partition int32) (string, bool) {
	if state == nil {
		return "", false
	}
	if state.denseOwners != nil {
		index := int(partition)
		if index < 0 || index >= len(state.denseOwners) {
			return "", false
		}
		return state.denseOwners[index], true
	}
	owner, ok := state.sparseOwners[partition]
	return owner, ok
}

// Snapshot returns a detached, partition-sorted assignment view.
func (fence *ConsumerGroupFence) Snapshot() ConsumerGroupFenceSnapshot {
	if fence == nil {
		return ConsumerGroupFenceSnapshot{}
	}
	return fence.snapshotFromState(fence.state.Load())
}

func (fence *ConsumerGroupFence) snapshotFromState(state *consumerGroupFenceState) ConsumerGroupFenceSnapshot {
	snapshot := ConsumerGroupFenceSnapshot{Group: fence.group}
	if state == nil {
		return snapshot
	}
	snapshot.Generation = state.generation
	if state.denseOwners != nil {
		snapshot.Assignments = make([]ConsumerGroupPartitionOwner, len(state.denseOwners))
		for index, member := range state.denseOwners {
			snapshot.Assignments[index] = ConsumerGroupPartitionOwner{Partition: int32(index), Member: member}
		}
		return snapshot
	}
	snapshot.Assignments = append([]ConsumerGroupPartitionOwner(nil), state.sparseAssignments...)
	return snapshot
}
