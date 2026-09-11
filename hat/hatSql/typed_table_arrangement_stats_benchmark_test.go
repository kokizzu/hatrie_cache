package hatSql

import "testing"

var mz027ArrangementStatsSink TypedTableAggregateArrangementsStats

func BenchmarkMZ027ArrangementStats(b *testing.B) {
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
	for index := 0; index < 512; index++ {
		if _, err := table.Upsert("row-"+string(rune(index)), []TypedTableValue{TypedString("team"), TypedInt64(int64(index))}); err != nil {
			b.Fatal(err)
		}
	}
	arrangements, err := NewTypedTableAggregateArrangements(table)
	if err != nil {
		b.Fatal(err)
	}
	arrangement, err := arrangements.Acquire(TypedTableAggregateDefinition{GroupBy: []string{"team"}, SumField: "points", DistinctField: "points", DictionaryEncodeGroups: true})
	if err != nil {
		b.Fatal(err)
	}
	defer arrangement.Release()
	changes, _, err := table.ChangesAfter(0, 512)
	if err != nil {
		b.Fatal(err)
	}
	if err := arrangement.Apply(changes); err != nil {
		b.Fatal(err)
	}
	if rows := arrangement.Rows(); len(rows) != 1 {
		b.Fatalf("aggregate rows = %#v", rows)
	}
	leases := make([]*TypedTableAggregateArrangement, 0, 4)
	for index := 0; index < 4; index++ {
		lease, err := arrangements.Acquire(TypedTableAggregateDefinition{GroupBy: []string{"team"}, SumField: "points", DistinctField: "points", DictionaryEncodeGroups: true})
		if err != nil {
			b.Fatal(err)
		}
		leases = append(leases, lease)
	}
	defer func() {
		for _, lease := range leases {
			lease.Release()
		}
	}()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		mz027ArrangementStatsSink = arrangements.Stats()
	}
}
