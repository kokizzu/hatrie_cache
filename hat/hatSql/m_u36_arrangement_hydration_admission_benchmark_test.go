package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkTypedTableAggregateArrangementFreshnessMU036(b *testing.B) {
	arrangement := newM036AggregateBenchmarkArrangement(b)
	defer arrangement.Release()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := arrangement.Freshness(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTypedTableAggregateArrangementHydrationStatusMU036(b *testing.B) {
	arrangement := newM036AggregateBenchmarkArrangement(b)
	defer arrangement.Release()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := arrangement.HydrationStatus(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTypedTableAggregateArrangementWaitForHydrationMU036(b *testing.B) {
	arrangement := newM036AggregateBenchmarkArrangement(b)
	defer arrangement.Release()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := arrangement.WaitForHydration(contextBackgroundMU036); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTypedTableJoinArrangementFreshnessMU036(b *testing.B) {
	arrangement := newM036JoinBenchmarkArrangement(b)
	defer arrangement.Release()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := arrangement.Freshness(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTypedTableJoinArrangementHydrationStatusMU036(b *testing.B) {
	arrangement := newM036JoinBenchmarkArrangement(b)
	defer arrangement.Release()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := arrangement.HydrationStatus(); err != nil {
			b.Fatal(err)
		}
	}
}

var contextBackgroundMU036 = context.Background()

func newM036AggregateBenchmarkArrangement(b *testing.B) *hatSql.TypedTableAggregateArrangement {
	b.Helper()
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name:    "benchmark",
		Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}},
	})
	if err != nil {
		b.Fatal(err)
	}
	arrangements, err := hatSql.NewTypedTableAggregateArrangements(table)
	if err != nil {
		b.Fatal(err)
	}
	arrangement, err := arrangements.Acquire(hatSql.TypedTableAggregateDefinition{GroupBy: []string{"team"}})
	if err != nil {
		b.Fatal(err)
	}
	return arrangement
}

func newM036JoinBenchmarkArrangement(b *testing.B) *hatSql.TypedTableJoinArrangement {
	b.Helper()
	left, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name:    "left-benchmark",
		Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}},
	})
	if err != nil {
		b.Fatal(err)
	}
	right, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name:    "right-benchmark",
		Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}},
	})
	if err != nil {
		b.Fatal(err)
	}
	arrangements, err := hatSql.NewTypedTableJoinArrangements(left, right)
	if err != nil {
		b.Fatal(err)
	}
	arrangement, err := arrangements.Acquire(hatSql.TypedTableJoinDefinition{LeftField: "team", RightField: "team"})
	if err != nil {
		b.Fatal(err)
	}
	return arrangement
}
