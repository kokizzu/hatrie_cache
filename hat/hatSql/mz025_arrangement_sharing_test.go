package hatSql

import "testing"

func TestMZ025AggregateArrangementCanonicalSharing(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "mz025_arrangements",
		Columns: []TypedTableColumn{
			{Name: "team", Kind: TypedTableString},
			{Name: "region", Kind: TypedTableString},
			{Name: "amount", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	arrangements, err := NewTypedTableAggregateArrangements(table)
	if err != nil {
		t.Fatal(err)
	}
	first, err := arrangements.Acquire(TypedTableAggregateDefinition{
		GroupBy:  []string{" team "},
		SumField: " amount ",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()
	second, err := arrangements.Acquire(TypedTableAggregateDefinition{
		GroupBy:  []string{"team"},
		SumField: "amount",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()
	if first.entry != second.entry || arrangements.Active() != 1 {
		t.Fatalf("canonical equivalent definitions did not share: first=%p second=%p active=%d", first.entry, second.entry, arrangements.Active())
	}

	different, err := arrangements.Acquire(TypedTableAggregateDefinition{
		GroupBy:  []string{"region"},
		SumField: "amount",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer different.Release()
	if different.entry == first.entry || arrangements.Active() != 2 {
		t.Fatalf("different definition was incorrectly shared: first=%p different=%p active=%d", first.entry, different.entry, arrangements.Active())
	}

	if _, err := arrangements.Acquire(TypedTableAggregateDefinition{SumField: " "}); err == nil {
		t.Fatal("blank configured sum field was accepted")
	}
}

func BenchmarkMZ025AggregateArrangementAcquireRelease(b *testing.B) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "mz025_benchmark",
		Columns: []TypedTableColumn{
			{Name: "team", Kind: TypedTableString},
			{Name: "amount", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	arrangements, err := NewTypedTableAggregateArrangements(table)
	if err != nil {
		b.Fatal(err)
	}
	definition := TypedTableAggregateDefinition{GroupBy: []string{"team"}, SumField: "amount"}
	held, err := arrangements.Acquire(definition)
	if err != nil {
		b.Fatal(err)
	}
	defer held.Release()

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		lease, acquireErr := arrangements.Acquire(definition)
		if acquireErr != nil {
			b.Fatal(acquireErr)
		}
		lease.Release()
	}
}
