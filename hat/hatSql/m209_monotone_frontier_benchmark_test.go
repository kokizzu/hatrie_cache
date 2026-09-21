package hatSql

import "testing"

var m209SQLFrontierObserveSink uint64

func BenchmarkM209SQLSourceFrontierObserve(b *testing.B) {
	tracker, err := NewSQLSourceFrontierTracker([]SQLSourceFrontierPartition{{Source: "orders", Partition: "0"}})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if _, err := tracker.Observe(SQLSourceFrontier{Source: "orders", Partition: "0", Frontier: uint64(index + 1)}); err != nil {
			b.Fatal(err)
		}
	}
	frontier, _ := tracker.CommonFrontier()
	m209SQLFrontierObserveSink = frontier
}
