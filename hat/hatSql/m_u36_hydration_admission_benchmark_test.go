package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

var m036HydrationStatusBenchmarkSink bool

func BenchmarkM036AggregateHydrationStatus(b *testing.B) {
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name:    "scores",
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
	defer arrangement.Release()

	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		status, err := arrangement.HydrationStatus()
		if err != nil {
			b.Fatal(err)
		}
		m036HydrationStatusBenchmarkSink = status.Ready
	}
}
