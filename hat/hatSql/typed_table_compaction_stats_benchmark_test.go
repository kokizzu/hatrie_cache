package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

var typedTableCompactionStatsBenchmarkSink uint64

func BenchmarkTypedTableAggregateCompactionStats(b *testing.B) {
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: "compaction_stats_bench",
		Columns: []hatSql.TypedTableColumn{
			{Name: "value", Kind: hatSql.TypedTableInt64},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 128; index++ {
		if _, err := table.Upsert(string(rune('a'+index%26)), []hatSql.TypedTableValue{hatSql.TypedInt64(int64(index))}); err != nil {
			b.Fatal(err)
		}
	}
	arrangements, err := hatSql.NewTypedTableAggregateArrangements(table)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		stats := arrangements.CompactionStats()
		typedTableCompactionStatsBenchmarkSink = stats.CompactionDebt
	}
}
