package hatSql_test

import (
	"testing"

	hatSql "hatrie_cache/hat/hatSql"
)

func BenchmarkSQLSourceOffsetTrackerAdvanceTransaction(b *testing.B) {
	tracker := hatSql.NewSQLSourceOffsetTracker()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := tracker.AdvanceTransaction(hatSql.SQLSourceTransaction{
			ID: "transaction",
			Offsets: []hatSql.SQLSourceOffset{
				{Source: "events", Partition: "0", Offset: uint64(index + 1)},
				{Source: "events", Partition: "1", Offset: uint64(index + 1)},
			},
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLSourceOffsetTrackerAdvanceSeparate(b *testing.B) {
	tracker := hatSql.NewSQLSourceOffsetTracker()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := tracker.Advance(hatSql.SQLSourceOffset{Source: "events", Partition: "0", Offset: uint64(index + 1)}); err != nil {
			b.Fatal(err)
		}
		if _, err := tracker.Advance(hatSql.SQLSourceOffset{Source: "events", Partition: "1", Offset: uint64(index + 1)}); err != nil {
			b.Fatal(err)
		}
	}
}
