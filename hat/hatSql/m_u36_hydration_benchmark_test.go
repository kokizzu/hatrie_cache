package hatSql

import "testing"

func BenchmarkM036AggregateHydrate(b *testing.B) {
	table, arrangement := benchmarkM036AggregateArrangement(b)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := table.Upsert("row", []TypedTableValue{TypedString("red"), TypedInt64(int64(index))}); err != nil {
			b.Fatal(err)
		}
		if _, err := arrangement.Hydrate(1); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkM036AggregateReadiness(b *testing.B) {
	table, arrangement := benchmarkM036AggregateArrangement(b)
	if _, err := table.Upsert("row", []TypedTableValue{TypedString("red"), TypedInt64(1)}); err != nil {
		b.Fatal(err)
	}
	if _, err := arrangement.Hydrate(1); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		status, err := arrangement.HydrationStatus()
		if err != nil {
			b.Fatal(err)
		}
		if !status.Ready {
			b.Fatal("HydrationStatus().Ready = false")
		}
	}
}

func benchmarkM036AggregateArrangement(b *testing.B) (*TypedTable, *TypedTableAggregateArrangement) {
	b.Helper()
	table, err := NewTypedTable(TypedTableSchema{
		Name: "scores",
		Columns: []TypedTableColumn{
			{Name: "team", Kind: TypedTableString},
			{Name: "points", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	arrangements, err := NewTypedTableAggregateArrangements(table)
	if err != nil {
		b.Fatal(err)
	}
	arrangement, err := arrangements.Acquire(TypedTableAggregateDefinition{GroupBy: []string{"team"}, SumField: "points"})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { arrangement.Release() })
	return table, arrangement
}
