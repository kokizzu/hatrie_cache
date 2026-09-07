package hatSql_test

import (
	"strconv"
	"testing"

	"hatrie_cache/hat/hatSql"
)

var sqlSourceOffsetTrackerBenchmarkAccepted bool

func BenchmarkSQLSourceOffsetTrackerAdvance(b *testing.B) {
	tracker := hatSql.NewSQLSourceOffsetTracker()
	partitions := make([]string, 256)
	for index := range partitions {
		partitions[index] = strconv.Itoa(index)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		accepted, err := tracker.Advance(hatSql.SQLSourceOffset{
			Source:    "events",
			Partition: partitions[index%len(partitions)],
			Offset:    uint64(index/len(partitions) + 1),
		})
		if err != nil {
			b.Fatal(err)
		}
		sqlSourceOffsetTrackerBenchmarkAccepted = accepted
	}
}
