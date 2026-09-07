package hatSql_test

import (
	"testing"

	hatSql "hatrie_cache/hat/hatSql"
)

func BenchmarkSQLSinkProgressTrackerAcknowledgeBatch(b *testing.B) {
	tracker := hatSql.NewSQLSinkProgressTracker()
	progress := []hatSql.SQLSinkProgress{
		{Sink: "warehouse", Partition: "0"},
		{Sink: "warehouse", Partition: "1"},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		progress[0].Frontier = uint64(index + 1)
		progress[1].Frontier = uint64(index + 1)
		if _, err := tracker.AcknowledgeBatch(progress); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLSinkProgressTrackerAcknowledgeSeparate(b *testing.B) {
	tracker := hatSql.NewSQLSinkProgressTracker()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		frontier := uint64(index + 1)
		if _, err := tracker.Acknowledge(hatSql.SQLSinkProgress{Sink: "warehouse", Partition: "0", Frontier: frontier}); err != nil {
			b.Fatal(err)
		}
		if _, err := tracker.Acknowledge(hatSql.SQLSinkProgress{Sink: "warehouse", Partition: "1", Frontier: frontier}); err != nil {
			b.Fatal(err)
		}
	}
}
