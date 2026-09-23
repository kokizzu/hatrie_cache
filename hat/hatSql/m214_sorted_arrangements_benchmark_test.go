package hatSql_test

import (
	"fmt"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkM214DirectSortedArrangementConstruction(b *testing.B) {
	for i := 0; i < b.N; i++ {
		table := benchmarkM214SortedTable(b, i)
		arrangement, err := hatSql.NewTypedTableSortedArrangement(table, hatSql.TypedTableSortedArrangementDefinition{
			OrderBy: []hatSql.TypedTableSortedArrangementOrder{
				{Field: "team"},
				{Field: "score", Descending: true},
			},
		})
		if err != nil {
			b.Fatal(err)
		}
		if len(arrangement.RowsPage(0, 1)) != 1 {
			b.Fatal("expected one row")
		}
	}
}

func BenchmarkM214DirectCompatiblePrefixConstruction(b *testing.B) {
	table := benchmarkM214SortedTable(b, 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		arrangement, err := hatSql.NewTypedTableSortedArrangement(table, hatSql.TypedTableSortedArrangementDefinition{Field: "team"})
		if err != nil {
			b.Fatal(err)
		}
		if len(arrangement.RowsPage(0, 1)) != 1 {
			b.Fatal("expected one row")
		}
	}
}

func BenchmarkM214RegistryCompatiblePrefixReuse(b *testing.B) {
	table := benchmarkM214SortedTable(b, 0)
	registry, err := hatSql.NewTypedTableSortedArrangements(table)
	if err != nil {
		b.Fatal(err)
	}
	composite, err := registry.Acquire(hatSql.TypedTableSortedArrangementDefinition{
		OrderBy: []hatSql.TypedTableSortedArrangementOrder{
			{Field: "team"},
			{Field: "score", Descending: true},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	defer composite.Release()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		prefix, err := registry.Acquire(hatSql.TypedTableSortedArrangementDefinition{Field: "team"})
		if err != nil {
			b.Fatal(err)
		}
		if !prefix.Reused() || len(prefix.RowsPage(0, 1)) != 1 || !prefix.Release() {
			b.Fatal("expected a reused prefix lease")
		}
	}
}

func benchmarkM214SortedTable(tb testing.TB, seed int) *hatSql.TypedTable {
	tb.Helper()
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: fmt.Sprintf("m214_benchmark_%d", seed),
		Columns: []hatSql.TypedTableColumn{
			{Name: "team", Kind: hatSql.TypedTableString},
			{Name: "score", Kind: hatSql.TypedTableInt64},
		},
	})
	if err != nil {
		tb.Fatal(err)
	}
	for i := 0; i < 128; i++ {
		if _, err := table.Upsert(fmt.Sprintf("key-%03d", i), []hatSql.TypedTableValue{
			hatSql.TypedString(fmt.Sprintf("team-%02d", i%16)),
			hatSql.TypedInt64(int64(i)),
		}); err != nil {
			tb.Fatal(err)
		}
	}
	return table
}
