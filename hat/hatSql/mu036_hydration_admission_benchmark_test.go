package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

var mu036HydrationAdmissionSink uint64

func BenchmarkMU036ArrangementReadinessBaseline(b *testing.B) {
	arrangement := newMU036AggregateArrangement(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		freshness, err := arrangement.Freshness()
		if err != nil {
			b.Fatal(err)
		}
		mu036HydrationAdmissionSink += freshness.SourceSequence
	}
}

func BenchmarkMU036ArrangementReadinessAdmission(b *testing.B) {
	arrangement := newMU036AggregateArrangement(b)
	admission := hatSql.NewTypedTableArrangementHydrationAdmission()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		progress, err := admission.WaitReady(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		mu036HydrationAdmissionSink += progress.Target
	}
	_ = arrangement
}

func BenchmarkMU036HydrateBaseline(b *testing.B) {
	arrangement := newMU036AggregateArrangement(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		report, err := arrangement.Hydrate(1)
		if err != nil {
			b.Fatal(err)
		}
		mu036HydrationAdmissionSink += uint64(report.Applied)
	}
}

func BenchmarkMU036HydrateWithAdmission(b *testing.B) {
	arrangement := newMU036AggregateArrangement(b)
	admission := hatSql.NewTypedTableArrangementHydrationAdmission()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		report, err := arrangement.HydrateWithAdmission(context.Background(), admission, 1)
		if err != nil {
			b.Fatal(err)
		}
		mu036HydrationAdmissionSink += uint64(report.Applied)
	}
}

func newMU036AggregateArrangement(b *testing.B) *hatSql.TypedTableAggregateArrangement {
	b.Helper()
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name:    "bench",
		Columns: []hatSql.TypedTableColumn{{Name: "kind", Kind: hatSql.TypedTableString}},
	})
	if err != nil {
		b.Fatal(err)
	}
	arrangements, err := hatSql.NewTypedTableAggregateArrangements(table)
	if err != nil {
		b.Fatal(err)
	}
	arrangement, err := arrangements.Acquire(hatSql.TypedTableAggregateDefinition{GroupBy: []string{"kind"}})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { arrangement.Release() })
	return arrangement
}
