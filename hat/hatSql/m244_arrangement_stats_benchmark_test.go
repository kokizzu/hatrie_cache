package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

var m244ArrangementStatsBenchmarkSink int

func BenchmarkM244TypedTableAggregateArrangementsStats(b *testing.B) {
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: "m244_stats_bench",
		Columns: []hatSql.TypedTableColumn{
			{Name: "team", Kind: hatSql.TypedTableString},
			{Name: "points", Kind: hatSql.TypedTableInt64},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 128; index++ {
		key := "key-" + string(rune('a'+index%26))
		if _, err := table.Upsert(key, []hatSql.TypedTableValue{hatSql.TypedString(key), hatSql.TypedInt64(int64(index))}); err != nil {
			b.Fatal(err)
		}
	}
	arrangements, err := hatSql.NewTypedTableAggregateArrangements(table)
	if err != nil {
		b.Fatal(err)
	}
	arrangement, err := arrangements.Acquire(hatSql.TypedTableAggregateDefinition{GroupBy: []string{"team"}, SumField: "points"})
	if err != nil {
		b.Fatal(err)
	}
	changes, _, err := table.ChangesAfter(0, 256)
	if err != nil {
		b.Fatal(err)
	}
	if err := arrangement.Apply(changes); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		stats := arrangements.Stats()
		m244ArrangementStatsBenchmarkSink = stats.ActiveDefinitions + len(stats.Arrangements)
	}
}
