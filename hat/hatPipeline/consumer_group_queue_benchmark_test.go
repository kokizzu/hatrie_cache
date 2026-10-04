package hatPipeline

import (
	"testing"
	"time"

	"hatrie_cache/hat/hatDataStructure"
)

func BenchmarkConsumerGroupManualLeaseAckBaseline(b *testing.B) {
	fence, err := NewConsumerGroupFence("orders", ConsumerGroupFenceOptions{MaxPartitions: 1})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := fence.Rebalance([]ConsumerGroupPartitionOwner{{Partition: 0, Member: "worker-a"}}); err != nil {
		b.Fatal(err)
	}
	queue := hatDataStructure.NewVisibilityQueue[string](1, time.Minute)
	if !queue.Enqueue("payload") {
		b.Fatal("initial enqueue failed")
	}
	now := time.Unix(1_700_000_000, 0).UTC()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		assignment, err := fence.Lease("worker-a", 0)
		if err != nil {
			b.Fatal(err)
		}
		lease, ok := queue.LeaseWithToken(now)
		if !ok {
			b.Fatal("manual lease returned no item")
		}
		if err := fence.Validate(assignment); err != nil {
			b.Fatal(err)
		}
		if !queue.AckToken(lease.Token) {
			b.Fatal("manual ack failed")
		}
		if !queue.Enqueue("payload") {
			b.Fatal("manual re-enqueue failed")
		}
	}
}

func BenchmarkConsumerGroupQueueLeaseAck(b *testing.B) {
	queue, err := NewConsumerGroupQueue[string](ConsumerGroupQueueOptions{
		Group:                "orders",
		PartitionCount:       1,
		CapacityPerPartition: 1,
		VisibilityTimeout:    time.Minute,
	})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := queue.Rebalance([]ConsumerGroupPartitionOwner{{Partition: 0, Member: "worker-a"}}); err != nil {
		b.Fatal(err)
	}
	if !queue.Enqueue(0, "payload") {
		b.Fatal("initial enqueue failed")
	}
	now := time.Unix(1_700_000_000, 0).UTC()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		lease, err := queue.Lease("worker-a", 0, now)
		if err != nil {
			b.Fatal(err)
		}
		if err := queue.Ack(lease); err != nil {
			b.Fatal(err)
		}
		if !queue.Enqueue(0, "payload") {
			b.Fatal("re-enqueue failed")
		}
	}
}
