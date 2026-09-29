package hatSql_test

import (
	"strconv"
	"testing"

	"hatrie_cache/hat/hatSql"
)

var m243ArrangementStatsBenchmarkSink uint64

func BenchmarkM243TypedTableAggregateArrangementsStats(b *testing.B) {
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: "m243_stats_bench",
		Columns: []hatSql.TypedTableColumn{
			{Name: "team", Kind: hatSql.TypedTableString},
			{Name: "points", Kind: hatSql.TypedTableInt64},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 128; index++ {
		key := "key-" + strconv.Itoa(index)
		team := "team-" + strconv.Itoa(index%8)
		if _, err := table.Upsert(key, []hatSql.TypedTableValue{hatSql.TypedString(team), hatSql.TypedInt64(int64(index))}); err != nil {
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
		stats, err := arrangement.Stats()
		if err != nil {
			b.Fatal(err)
		}
		m243ArrangementStatsBenchmarkSink = stats.EstimatedBytes
	}
}
