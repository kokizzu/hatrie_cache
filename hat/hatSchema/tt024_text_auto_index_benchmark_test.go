package hatSchema

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func benchmarkTT024MaterializedTextSource(b *testing.B, indexed bool) SQLResolverAdapter {
	b.Helper()
	source := NewMaterializedSource([]DerivedColumn{{Name: "id"}, {Name: "body"}})
	for rowIndex := 0; rowIndex < 20_000; rowIndex++ {
		body := "alpha filler document"
		if rowIndex%1000 == 0 {
			body = "alpha beta gamma target"
		}
		if _, err := source.Insert(Row{"id": int64(rowIndex), "body": body}); err != nil {
			b.Fatal(err)
		}
	}
	if indexed {
		if _, err := source.BuildTextIndex("body"); err != nil {
			b.Fatal(err)
		}
	}
	return SQLResolverAdapter{Sources: map[string]*MaterializedSource{"docs": source}}
}

func BenchmarkTT024MaterializedTextIndexSelection(b *testing.B) {
	query := "FROM CACHE('docs') AS doc WHERE CONTAINS(doc.body, 'alpha gamma') SELECT COUNT(*)"
	for _, benchmark := range []struct {
		name    string
		indexed bool
	}{
		{name: "scan_baseline", indexed: false},
		{name: "automatic_text_index", indexed: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			adapter := benchmarkTT024MaterializedTextSource(b, benchmark.indexed)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, adapter, hatSql.SQLQueryOptions{})
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != 1 {
					b.Fatalf("result rows = %d", len(result.Rows))
				}
			}
		})
	}
}
