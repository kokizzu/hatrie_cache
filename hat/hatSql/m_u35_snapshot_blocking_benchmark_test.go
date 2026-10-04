package hatSql

import "testing"

func BenchmarkSQLSourceFrontierObserve(b *testing.B) {
	tracker := benchmarkSQLSourceFrontierTracker(b)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := tracker.Observe(SQLSourceFrontier{Source: "orders", Partition: "eu", Frontier: uint64(index + 1)}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLSourceFrontierReadyAt(b *testing.B) {
	tracker := benchmarkSQLSourceFrontierTracker(b)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if !tracker.ReadyAt(1) {
			b.Fatal("ReadyAt(1) = false")
		}
	}
}

func benchmarkSQLSourceFrontierTracker(b *testing.B) *SQLSourceFrontierTracker {
	b.Helper()
	tracker, err := NewSQLSourceFrontierTracker([]SQLSourceFrontierPartition{{Source: "orders", Partition: "eu"}})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := tracker.Observe(SQLSourceFrontier{Source: "orders", Partition: "eu", Frontier: 1}); err != nil {
		b.Fatal(err)
	}
	return tracker
}
