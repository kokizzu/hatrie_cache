package hatSql

import "testing"

func BenchmarkMU036HydrateNoAdmission(b *testing.B) {
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
	for index := 0; index < 128; index++ {
		if _, err := table.Upsert("row", []TypedTableValue{TypedString("red"), TypedInt64(int64(index))}); err != nil {
			b.Fatal(err)
		}
	}
	arrangements, err := NewTypedTableAggregateArrangements(table)
	if err != nil {
		b.Fatal(err)
	}
	arrangement, err := arrangements.Acquire(TypedTableAggregateDefinition{GroupBy: []string{"team"}, SumField: "points"})
	if err != nil {
		b.Fatal(err)
	}
	defer arrangement.Release()
	if _, err := arrangement.Hydrate(0); err != nil {
		b.Fatal(err)
	}
	if err := table.CompactChangesThrough(128); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		b.StopTimer()
		change, err := table.Upsert("row", []TypedTableValue{TypedString("red"), TypedInt64(int64(index))})
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if _, err := arrangement.Hydrate(1); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		if err := table.CompactChangesThrough(change.Sequence); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
}
