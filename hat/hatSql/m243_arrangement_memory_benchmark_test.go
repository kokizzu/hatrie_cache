package hatSql

import (
	"strconv"
	"testing"
)

var m243ArrangementStatsSink TypedTableAggregateArrangementStats

func BenchmarkM243ArrangementStats(b *testing.B) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "m243_benchmark",
		Columns: []TypedTableColumn{
			{Name: "team", Kind: TypedTableString},
			{Name: "points", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 256; index++ {
		if _, err := table.Upsert("row-"+strconv.Itoa(index), []TypedTableValue{
			TypedString("team-" + strconv.Itoa(index%32)),
			TypedInt64(int64(index)),
		}); err != nil {
			b.Fatal(err)
		}
	}
	arrangements, err := NewTypedTableAggregateArrangements(table)
	if err != nil {
		b.Fatal(err)
	}
	arrangement, err := arrangements.Acquire(TypedTableAggregateDefinition{
		GroupBy:       []string{"team"},
		SumField:      "points",
		MinField:      "points",
		MaxField:      "points",
		DistinctField: "points",
	})
	if err != nil {
		b.Fatal(err)
	}
	changes, _, err := table.ChangesAfter(0, 512)
	if err != nil {
		b.Fatal(err)
	}
	if err := arrangement.Apply(changes); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		stats, err := arrangement.Stats()
		if err != nil {
			b.Fatal(err)
		}
		m243ArrangementStatsSink = stats
	}
	arrangement.Release()
}
