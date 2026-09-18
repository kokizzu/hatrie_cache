package hatPipeline

import "testing"

func mz012BenchmarkAssignments() []ConsumerGroupPartitionOwner {
	assignments := make([]ConsumerGroupPartitionOwner, 64)
	for index := range assignments {
		assignments[index] = ConsumerGroupPartitionOwner{
			Partition: int32(index),
			Member:    "consumer-0",
		}
	}
	return assignments
}

func BenchmarkMZ012ConsumerGroupFenceValidate(b *testing.B) {
	fence, err := NewConsumerGroupFence("orders", ConsumerGroupFenceOptions{MaxPartitions: 64})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := fence.Rebalance(mz012BenchmarkAssignments()); err != nil {
		b.Fatal(err)
	}
	lease, err := fence.Lease("consumer-0", 31)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := fence.Validate(lease); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMZ012ConsumerGroupFenceRebalance(b *testing.B) {
	assignments := mz012BenchmarkAssignments()
	fence, err := NewConsumerGroupFence("orders", ConsumerGroupFenceOptions{MaxPartitions: 64})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := fence.Rebalance(assignments); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMZ012ConsumerGroupFenceSnapshot(b *testing.B) {
	fence, err := NewConsumerGroupFence("orders", ConsumerGroupFenceOptions{MaxPartitions: 64})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := fence.Rebalance(mz012BenchmarkAssignments()); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		snapshot := fence.Snapshot()
		if len(snapshot.Assignments) != 64 {
			b.Fatal("unexpected snapshot size")
		}
	}
}

func BenchmarkMZ012ConsumerGroupFenceSparseRebalance(b *testing.B) {
	assignments := []ConsumerGroupPartitionOwner{
		{Partition: 7, Member: "consumer-0"},
		{Partition: 19, Member: "consumer-1"},
		{Partition: 31, Member: "consumer-0"},
		{Partition: 43, Member: "consumer-1"},
	}
	fence, err := NewConsumerGroupFence("orders", ConsumerGroupFenceOptions{MaxPartitions: 64})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := fence.Rebalance(assignments); err != nil {
			b.Fatal(err)
		}
	}
}
