package hatSql

import (
	"context"
	"strconv"
	"testing"
)

func BenchmarkSQLSourceFrontierBarrierBaseline(b *testing.B) {
	partitions := make([]SQLSourceFrontierPartition, 1024)
	for i := range partitions {
		partitions[i] = SQLSourceFrontierPartition{Source: "source", Partition: strconv.Itoa(i)}
	}
	tracker, err := NewSQLSourceFrontierTracker(partitions)
	if err != nil {
		b.Fatal(err)
	}
	for _, partition := range partitions {
		if _, err := tracker.Observe(SQLSourceFrontier{
			Source:    partition.Source,
			Partition: partition.Partition,
			Frontier:  1024,
		}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !tracker.ReadyAt(1024) {
			b.Fatal("frontier is not ready")
		}
	}
}

func BenchmarkSQLSourceFrontierBarrier(b *testing.B) {
	partitions := make([]SQLSourceFrontierPartition, 1024)
	for i := range partitions {
		partitions[i] = SQLSourceFrontierPartition{Source: "source", Partition: strconv.Itoa(i)}
	}
	barrier, err := NewSQLSourceFrontierBarrierFromPartitions(partitions)
	if err != nil {
		b.Fatal(err)
	}
	updates := make([]SQLSourceFrontier, len(partitions))
	for i, partition := range partitions {
		updates[i] = SQLSourceFrontier{
			Source:    partition.Source,
			Partition: partition.Partition,
			Frontier:  1024,
		}
	}
	if _, err := barrier.ObserveBatch(updates); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := barrier.WaitForFrontier(ctx, 1024); err != nil {
			b.Fatal(err)
		}
	}
}
