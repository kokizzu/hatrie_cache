package hatSchema

import (
	"context"
	"fmt"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkTT024CrossFieldTextScan(b *testing.B) {
	_, adapter := benchmarkTT024CrossFieldTextSource(b, false)
	benchmarkTT024CrossFieldTextQuery(b, adapter)
}

func BenchmarkTT024CrossFieldTextIndexed(b *testing.B) {
	_, adapter := benchmarkTT024CrossFieldTextSource(b, true)
	benchmarkTT024CrossFieldTextQuery(b, adapter)
}

func benchmarkTT024CrossFieldTextSource(b *testing.B, indexed bool) (*MaterializedSource, SQLResolverAdapter) {
	b.Helper()
	source := NewMaterializedSource([]DerivedColumn{{Name: "id"}, {Name: "title"}, {Name: "body"}})
	for index := 0; index < 20_000; index++ {
		title := fmt.Sprintf("unrelated title %d", index)
		body := fmt.Sprintf("unrelated body %d", index)
		if index%1_000 == 0 {
			title = "quick brown release notes"
		}
		if index%1_000 == 500 {
			body = "lazy red fox release notes"
		}
		if index%2_000 == 0 {
			body = "lazy fox release notes"
		}
		if _, err := source.Insert(Row{"id": int64(index), "title": title, "body": body}); err != nil {
			b.Fatal(err)
		}
	}
	if indexed {
		if _, err := source.BuildTextIndex("title"); err != nil {
			b.Fatal(err)
		}
		if _, err := source.BuildTextIndex("body"); err != nil {
			b.Fatal(err)
		}
	}
	return source, SQLResolverAdapter{Sources: map[string]*MaterializedSource{"docs": source}}
}

func benchmarkTT024CrossFieldTextQuery(b *testing.B, adapter SQLResolverAdapter) {
	b.Helper()
	const query = "FROM CACHE('docs') AS doc WHERE CONTAINS_PHRASE(doc.title, 'quick brown') OR CONTAINS_PROXIMITY(doc.body, 'lazy fox', 1) SELECT doc.id"
	b.ReportAllocs()
	b.ReportMetric(20_000, "rows/source")
	b.ReportMetric(40, "rows/result")
	b.ResetTimer()
	for b.Loop() {
		result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, adapter, hatSql.SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 40 {
			b.Fatalf("rows = %d, want 40", len(result.Rows))
		}
	}
}
