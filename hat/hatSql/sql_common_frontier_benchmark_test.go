package hatSql

import (
	"strconv"
	"testing"
)

var sqlCommonFrontierBenchmarkSink uint64

func BenchmarkSQLCommonFrontier(b *testing.B) {
	frontiers := make([]uint64, 1024)
	for index := range frontiers {
		frontiers[index] = 1000
	}
	b.Run("scan_all_partitions", func(b *testing.B) {
		for iteration := 0; iteration < b.N; iteration++ {
			index := iteration % len(frontiers)
			frontiers[index] = uint64(1001 + iteration)
			minimum := frontiers[0]
			for _, frontier := range frontiers[1:] {
				if frontier < minimum {
					minimum = frontier
				}
			}
			sqlCommonFrontierBenchmarkSink = minimum
		}
	})
	b.Run("indexed_heap_update_and_read", func(b *testing.B) {
		partitions := make([]SQLSourceFrontierPartition, 1024)
		initial := make([]SQLSourceFrontier, len(partitions))
		partitionNames := make([]string, len(partitions))
		for index := range partitions {
			partitionNames[index] = strconv.Itoa(index)
			partition := SQLSourceFrontierPartition{Source: "orders", Partition: partitionNames[index]}
			partitions[index] = partition
			initial[index] = SQLSourceFrontier{Source: partition.Source, Partition: partition.Partition, Frontier: 1000}
		}
		tracker, err := NewSQLSourceFrontierTracker(partitions)
		if err != nil {
			b.Fatalf("NewSQLSourceFrontierTracker() error = %v", err)
		}
		if _, err := tracker.ObserveBatch(initial); err != nil {
			b.Fatalf("ObserveBatch(initial) error = %v", err)
		}
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			index := iteration % len(partitions)
			changed, err := tracker.Observe(SQLSourceFrontier{Source: "orders", Partition: partitionNames[index], Frontier: uint64(1001 + iteration)})
			if err != nil || !changed {
				b.Fatalf("Observe() = %v/%v, want true/nil", changed, err)
			}
			frontier, ready := tracker.CommonFrontier()
			if !ready {
				b.Fatal("CommonFrontier() is not ready")
			}
			sqlCommonFrontierBenchmarkSink = frontier
		}
	})
	if sqlCommonFrontierBenchmarkSink == 0 {
		b.Fatal("benchmark sink is zero")
	}
}
