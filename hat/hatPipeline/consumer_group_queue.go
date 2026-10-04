package hatPipeline

import (
	"errors"
	"strings"
	"time"

	"hatrie_cache/hat/hatDataStructure"
)

var (
	// ErrConsumerGroupQueueNil indicates a method call on a nil queue.
	ErrConsumerGroupQueueNil = errors.New("hatPipeline: consumer group queue is nil")
	// ErrConsumerGroupQueueOptionsInvalid indicates an invalid queue setup.
	ErrConsumerGroupQueueOptionsInvalid = errors.New("hatPipeline: consumer group queue options are invalid")
	// ErrConsumerGroupQueuePartitionInvalid indicates a partition outside the queue.
	ErrConsumerGroupQueuePartitionInvalid = errors.New("hatPipeline: consumer group queue partition is invalid")
	// ErrConsumerGroupQueueEmpty indicates that the partition has no ready item.
	ErrConsumerGroupQueueEmpty = errors.New("hatPipeline: consumer group queue has no ready item")
	// ErrConsumerGroupQueueLeaseInvalid indicates that the queue token is unknown
	// or no longer active.
	ErrConsumerGroupQueueLeaseInvalid = errors.New("hatPipeline: consumer group queue lease is invalid")
)

// ConsumerGroupQueueOptions configures a partitioned visibility queue.
//
// CapacityPerPartition and VisibilityTimeout follow VisibilityQueue semantics:
// zero means unbounded capacity and the default timeout respectively. Epoch is
// shared by all partitions and is advanced by the caller when restoring a
// queue in a new process.
type ConsumerGroupQueueOptions struct {
	Group                string
	PartitionCount       int
	CapacityPerPartition int
	VisibilityTimeout    time.Duration
	Epoch                uint64
}

// ConsumerGroupQueueLease combines a fenced partition lease with the
// visibility-queue token required to acknowledge or retry the item.
type ConsumerGroupQueueLease[T any] struct {
	ConsumerGroupLease
	QueueToken hatDataStructure.VisibilityQueueLeaseToken
	Value      T
	Attempts   uint32
	LeaseUntil time.Time
}

// ConsumerGroupQueue composes partition ownership fencing with one
// VisibilityQueue per partition. It is intentionally not thread-safe; callers
// that share it across goroutines should serialize queue operations.
type ConsumerGroupQueue[T any] struct {
	fence      *ConsumerGroupFence
	partitions []*hatDataStructure.VisibilityQueue[T]
}

// NewConsumerGroupQueue creates a local consumer-group queue. Membership is
// published with Rebalance before a member can lease from a partition.
func NewConsumerGroupQueue[T any](options ConsumerGroupQueueOptions) (*ConsumerGroupQueue[T], error) {
	group := strings.TrimSpace(options.Group)
	if group == "" {
		return nil, ErrConsumerGroupFenceInvalidGroup
	}
	if options.PartitionCount < 1 || options.PartitionCount > maxConsumerGroupFencePartitions || options.CapacityPerPartition < 0 {
		return nil, ErrConsumerGroupQueueOptionsInvalid
	}
	fence, err := NewConsumerGroupFence(group, ConsumerGroupFenceOptions{MaxPartitions: options.PartitionCount})
	if err != nil {
		return nil, err
	}
	queue := &ConsumerGroupQueue[T]{
		fence:      fence,
		partitions: make([]*hatDataStructure.VisibilityQueue[T], options.PartitionCount),
	}
	for partition := range queue.partitions {
		queue.partitions[partition] = hatDataStructure.NewVisibilityQueueWithEpoch[T](options.CapacityPerPartition, options.VisibilityTimeout, options.Epoch)
	}
	return queue, nil
}

// Rebalance publishes a new owner generation. An old lease cannot acknowledge
// or retry after this call, while the underlying item remains available after
// its visibility timeout expires.
func (queue *ConsumerGroupQueue[T]) Rebalance(assignments []ConsumerGroupPartitionOwner) (ConsumerGroupFenceSnapshot, error) {
	if queue == nil || queue.fence == nil {
		return ConsumerGroupFenceSnapshot{}, ErrConsumerGroupQueueNil
	}
	for _, assignment := range assignments {
		if !queue.validPartition(int(assignment.Partition)) {
			return ConsumerGroupFenceSnapshot{}, ErrConsumerGroupQueuePartitionInvalid
		}
	}
	return queue.fence.Rebalance(assignments)
}

// Enqueue appends an immediately available item to one partition. It returns
// false for an invalid partition or a full partition.
func (queue *ConsumerGroupQueue[T]) Enqueue(partition int, value T) bool {
	if queue == nil || !queue.validPartition(partition) {
		return false
	}
	return queue.partitions[partition].Enqueue(value)
}

// Lease returns the next ready item after validating the member's current
// ownership generation.
func (queue *ConsumerGroupQueue[T]) Lease(member string, partition int, now time.Time) (ConsumerGroupQueueLease[T], error) {
	if queue == nil || queue.fence == nil {
		return ConsumerGroupQueueLease[T]{}, ErrConsumerGroupQueueNil
	}
	if !queue.validPartition(partition) {
		return ConsumerGroupQueueLease[T]{}, ErrConsumerGroupQueuePartitionInvalid
	}
	assignment, err := queue.fence.Lease(member, int32(partition))
	if err != nil {
		return ConsumerGroupQueueLease[T]{}, err
	}
	item, ok := queue.partitions[partition].LeaseWithToken(now)
	if !ok {
		return ConsumerGroupQueueLease[T]{}, ErrConsumerGroupQueueEmpty
	}
	return ConsumerGroupQueueLease[T]{
		ConsumerGroupLease: assignment,
		QueueToken:         item.Token,
		Value:              item.Value,
		Attempts:           item.Attempts,
		LeaseUntil:         item.LeaseUntil,
	}, nil
}

// Ack validates ownership and permanently removes a leased item.
func (queue *ConsumerGroupQueue[T]) Ack(lease ConsumerGroupQueueLease[T]) error {
	if queue == nil || queue.fence == nil {
		return ErrConsumerGroupQueueNil
	}
	if err := queue.fence.Validate(lease.ConsumerGroupLease); err != nil {
		return err
	}
	partition := int(lease.Partition)
	if !queue.validPartition(partition) {
		return ErrConsumerGroupQueuePartitionInvalid
	}
	if !queue.partitions[partition].AckToken(lease.QueueToken) {
		return ErrConsumerGroupQueueLeaseInvalid
	}
	return nil
}

// Nack validates ownership and makes a leased item available at readyAt.
func (queue *ConsumerGroupQueue[T]) Nack(lease ConsumerGroupQueueLease[T], readyAt time.Time) error {
	if queue == nil || queue.fence == nil {
		return ErrConsumerGroupQueueNil
	}
	if err := queue.fence.Validate(lease.ConsumerGroupLease); err != nil {
		return err
	}
	partition := int(lease.Partition)
	if !queue.validPartition(partition) {
		return ErrConsumerGroupQueuePartitionInvalid
	}
	if !queue.partitions[partition].NackToken(lease.QueueToken, readyAt) {
		return ErrConsumerGroupQueueLeaseInvalid
	}
	return nil
}

// PendingLen returns the number of ready items in a partition.
func (queue *ConsumerGroupQueue[T]) PendingLen(partition int) int {
	if queue == nil || !queue.validPartition(partition) {
		return 0
	}
	return queue.partitions[partition].PendingLen()
}

// LeaseLen returns the number of active leases in a partition.
func (queue *ConsumerGroupQueue[T]) LeaseLen(partition int) int {
	if queue == nil || !queue.validPartition(partition) {
		return 0
	}
	return queue.partitions[partition].LeaseLen()
}

// Len returns ready plus leased items across all partitions.
func (queue *ConsumerGroupQueue[T]) Len() int {
	if queue == nil {
		return 0
	}
	total := 0
	for _, partition := range queue.partitions {
		total += partition.Len()
	}
	return total
}

func (queue *ConsumerGroupQueue[T]) validPartition(partition int) bool {
	return partition >= 0 && partition < len(queue.partitions)
}
