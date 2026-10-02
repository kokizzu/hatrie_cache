package hatSql

import (
	"context"
	"testing"
)

func BenchmarkMU036HydrationAdmission(b *testing.B) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "scores",
		Columns: []TypedTableColumn{{Name: "team", Kind: TypedTableString}},
	})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := table.Upsert("ada", []TypedTableValue{TypedString("red")}); err != nil {
		b.Fatal(err)
	}
	arrangements, err := NewTypedTableAggregateArrangements(table)
	if err != nil {
		b.Fatal(err)
	}
	arrangement, err := arrangements.Acquire(TypedTableAggregateDefinition{GroupBy: []string{"team"}})
	if err != nil {
		b.Fatal(err)
	}
	defer arrangement.Release()
	admission, err := NewTypedTableArrangementHydrationAdmission(1)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := admission.HydrateAggregate(context.Background(), "scores_by_team", arrangement, 0); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.Run("direct_freshness_baseline", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := arrangement.Freshness(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("ready_admission", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := admission.Admit(ctx, "scores_by_team"); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("progress_snapshot", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = admission.Snapshot()
		}
	})
}
