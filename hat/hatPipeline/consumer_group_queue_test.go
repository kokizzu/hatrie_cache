package hatPipeline

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestConsumerGroupQueueLeaseAckAndRebalance(t *testing.T) {
	queue, err := NewConsumerGroupQueue[string](ConsumerGroupQueueOptions{
		Group:                "orders",
		PartitionCount:       2,
		CapacityPerPartition: 2,
		VisibilityTimeout:    time.Second,
		Epoch:                7,
	})
	if err != nil {
		t.Fatalf("NewConsumerGroupQueue() error = %v", err)
	}
	if _, err := queue.Rebalance([]ConsumerGroupPartitionOwner{
		{Partition: 0, Member: "worker-a"},
		{Partition: 1, Member: "worker-b"},
	}); err != nil {
		t.Fatalf("Rebalance() error = %v", err)
	}
	if !queue.Enqueue(0, "zero") || !queue.Enqueue(1, "one") {
		t.Fatal("Enqueue() rejected assigned partitions")
	}
	now := time.Unix(1_700_000_000, 0).UTC()
	lease, err := queue.Lease("worker-a", 0, now)
	if err != nil || lease.Value != "zero" || lease.Partition != 0 || lease.Generation != 1 {
		t.Fatalf("Lease() = %#v/%v, want worker-a partition 0 generation 1", lease, err)
	}
	if _, err := queue.Lease("worker-b", 0, now); !errors.Is(err, ErrConsumerGroupFenceWrongOwner) {
		t.Fatalf("wrong-owner Lease() error = %v, want %v", err, ErrConsumerGroupFenceWrongOwner)
	}
	if err := queue.Ack(lease); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}

	if !queue.Enqueue(0, "stale") {
		t.Fatal("Enqueue(stale) rejected")
	}
	stale, err := queue.Lease("worker-a", 0, now)
	if err != nil {
		t.Fatalf("second Lease() error = %v", err)
	}
	if _, err := queue.Rebalance([]ConsumerGroupPartitionOwner{
		{Partition: 0, Member: "worker-b"},
		{Partition: 1, Member: "worker-a"},
	}); err != nil {
		t.Fatalf("swap Rebalance() error = %v", err)
	}
	if err := queue.Ack(stale); !errors.Is(err, ErrConsumerGroupFenceStaleGeneration) {
		t.Fatalf("stale Ack() error = %v, want %v", err, ErrConsumerGroupFenceStaleGeneration)
	}
	retry, err := queue.Lease("worker-b", 0, now.Add(time.Second))
	if err != nil || retry.Value != "stale" {
		t.Fatalf("expired lease recovery = %#v/%v, want stale item", retry, err)
	}
	if err := queue.Ack(retry); err != nil {
		t.Fatalf("retry Ack() error = %v", err)
	}
	if queue.PendingLen(0) != 0 || queue.LeaseLen(0) != 0 || queue.Len() != 1 {
		t.Fatalf("queue lengths = pending %d leased %d total %d, want 0/0/1", queue.PendingLen(0), queue.LeaseLen(0), queue.Len())
	}
}

func TestConsumerGroupQueueRejectsInvalidPartition(t *testing.T) {
	if _, err := NewConsumerGroupQueue[int](ConsumerGroupQueueOptions{Group: "orders", PartitionCount: 1}); err != nil {
		t.Fatalf("constructor error = %v", err)
	}
	queue, err := NewConsumerGroupQueue[int](ConsumerGroupQueueOptions{Group: "orders", PartitionCount: 1})
	if err != nil {
		t.Fatal(err)
	}
	if queue.Enqueue(1, 1) {
		t.Fatal("Enqueue accepted an invalid partition")
	}
	if _, err := queue.Lease("worker", 1, time.Time{}); !errors.Is(err, ErrConsumerGroupQueuePartitionInvalid) {
		t.Fatalf("invalid Lease() error = %v, want %v", err, ErrConsumerGroupQueuePartitionInvalid)
	}
}

func TestConsumerGroupQueueNackAndValidation(t *testing.T) {
	queue, err := NewConsumerGroupQueue[string](ConsumerGroupQueueOptions{
		Group:             "orders",
		PartitionCount:    1,
		VisibilityTimeout: time.Minute,
	})
	if err != nil {
		t.Fatalf("NewConsumerGroupQueue() error = %v", err)
	}
	if _, err := queue.Rebalance([]ConsumerGroupPartitionOwner{{Partition: 0, Member: "worker"}}); err != nil {
		t.Fatalf("Rebalance() error = %v", err)
	}
	if !queue.Enqueue(0, "retry") {
		t.Fatal("Enqueue() rejected retry item")
	}
	now := time.Unix(1_700_000_000, 0).UTC()
	lease, err := queue.Lease("worker", 0, now)
	if err != nil || lease.Attempts != 1 {
		t.Fatalf("Lease() = %#v/%v, want first attempt", lease, err)
	}
	if err := queue.Nack(lease, now); err != nil {
		t.Fatalf("Nack() error = %v", err)
	}
	retry, err := queue.Lease("worker", 0, now)
	if err != nil || retry.Value != "retry" || retry.Attempts != 2 {
		t.Fatalf("retry Lease() = %#v/%v, want second attempt", retry, err)
	}
	if err := queue.Ack(retry); err != nil {
		t.Fatalf("retry Ack() error = %v", err)
	}
	if err := queue.Ack(retry); !errors.Is(err, ErrConsumerGroupQueueLeaseInvalid) {
		t.Fatalf("duplicate Ack() error = %v, want %v", err, ErrConsumerGroupQueueLeaseInvalid)
	}
	if _, err := queue.Lease("worker", 0, now); !errors.Is(err, ErrConsumerGroupQueueEmpty) {
		t.Fatalf("empty Lease() error = %v, want %v", err, ErrConsumerGroupQueueEmpty)
	}

	wrongOwner := retry
	wrongOwner.Member = "other"
	if err := queue.Ack(wrongOwner); !errors.Is(err, ErrConsumerGroupFenceWrongOwner) {
		t.Fatalf("wrong-owner Ack() error = %v, want %v", err, ErrConsumerGroupFenceWrongOwner)
	}
}

func TestConsumerGroupQueueRejectsInvalidOptionsAndRebalance(t *testing.T) {
	if _, err := NewConsumerGroupQueue[int](ConsumerGroupQueueOptions{}); !errors.Is(err, ErrConsumerGroupFenceInvalidGroup) {
		t.Fatalf("empty options error = %v, want %v", err, ErrConsumerGroupFenceInvalidGroup)
	}
	if _, err := NewConsumerGroupQueue[int](ConsumerGroupQueueOptions{Group: "orders"}); !errors.Is(err, ErrConsumerGroupQueueOptionsInvalid) {
		t.Fatalf("zero partitions error = %v, want %v", err, ErrConsumerGroupQueueOptionsInvalid)
	}
	if _, err := NewConsumerGroupQueue[int](ConsumerGroupQueueOptions{Group: "orders", PartitionCount: 1, CapacityPerPartition: -1}); !errors.Is(err, ErrConsumerGroupQueueOptionsInvalid) {
		t.Fatalf("negative capacity error = %v, want %v", err, ErrConsumerGroupQueueOptionsInvalid)
	}
	queue, err := NewConsumerGroupQueue[int](ConsumerGroupQueueOptions{Group: "orders", PartitionCount: 1})
	if err != nil {
		t.Fatalf("valid constructor error = %v", err)
	}
	if _, err := queue.Rebalance([]ConsumerGroupPartitionOwner{{Partition: 1, Member: "worker"}}); !errors.Is(err, ErrConsumerGroupQueuePartitionInvalid) {
		t.Fatalf("invalid Rebalance() error = %v, want %v", err, ErrConsumerGroupQueuePartitionInvalid)
	}
	snapshot, err := queue.Rebalance([]ConsumerGroupPartitionOwner{{Partition: 0, Member: "worker"}})
	if err != nil {
		t.Fatalf("valid Rebalance() error = %v", err)
	}
	if snapshot.Generation != 1 {
		t.Fatalf("valid Rebalance() generation = %d, want 1", snapshot.Generation)
	}
}

func TestConsumerGroupQueueNilReceiver(t *testing.T) {
	var queue *ConsumerGroupQueue[int]
	if _, err := queue.Rebalance(nil); !errors.Is(err, ErrConsumerGroupQueueNil) {
		t.Fatalf("nil Rebalance() error = %v", err)
	}
	if queue.Enqueue(0, 1) {
		t.Fatal("nil Enqueue() accepted an item")
	}
	if _, err := queue.Lease("worker", 0, time.Time{}); !errors.Is(err, ErrConsumerGroupQueueNil) {
		t.Fatalf("nil Lease() error = %v", err)
	}
	if err := queue.Ack(ConsumerGroupQueueLease[int]{}); !errors.Is(err, ErrConsumerGroupQueueNil) {
		t.Fatalf("nil Ack() error = %v", err)
	}
	if err := queue.Nack(ConsumerGroupQueueLease[int]{}, time.Time{}); !errors.Is(err, ErrConsumerGroupQueueNil) {
		t.Fatalf("nil Nack() error = %v", err)
	}
	if queue.PendingLen(0) != 0 || queue.LeaseLen(0) != 0 || queue.Len() != 0 {
		t.Fatal("nil queue lengths were not zero")
	}
}

func ExampleConsumerGroupQueue() {
	queue, err := NewConsumerGroupQueue[string](ConsumerGroupQueueOptions{
		Group:          "orders",
		PartitionCount: 1,
	})
	if err != nil {
		panic(err)
	}
	if _, err := queue.Rebalance([]ConsumerGroupPartitionOwner{{Partition: 0, Member: "worker-a"}}); err != nil {
		panic(err)
	}
	if !queue.Enqueue(0, "order-123") {
		panic("enqueue failed")
	}
	lease, err := queue.Lease("worker-a", 0, time.Unix(1_700_000_000, 0).UTC())
	if err != nil {
		panic(err)
	}
	fmt.Println(lease.Value)
	if err := queue.Ack(lease); err != nil {
		panic(err)
	}
	// Output: order-123
}
