package hatSchema

import (
	"context"
	"fmt"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkTT024PhraseScan(b *testing.B) {
	_, adapter := benchmarkTT024TextSource(b, false)
	benchmarkTT024PhraseQuery(b, adapter)
}

func BenchmarkTT024PhraseIndexed(b *testing.B) {
	_, adapter := benchmarkTT024TextSource(b, true)
	benchmarkTT024PhraseQuery(b, adapter)
}

func BenchmarkTT024TextIndexBuild(b *testing.B) {
	source, _ := benchmarkTT024TextSource(b, false)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := source.BuildTextIndex("body"); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkTT024TextSource(b *testing.B, indexed bool) (*MaterializedSource, SQLResolverAdapter) {
	b.Helper()
	source := NewMaterializedSource([]DerivedColumn{{Name: "id"}, {Name: "body"}})
	for index := 0; index < 20_000; index++ {
		body := fmt.Sprintf("alpha unrelated document %d", index)
		if index%1_000 == 0 {
			body = "alpha beta gamma release notes"
		}
		if _, err := source.Insert(Row{"id": int64(index), "body": body}); err != nil {
			b.Fatal(err)
		}
	}
	if indexed {
		if _, err := source.BuildTextIndex("body"); err != nil {
			b.Fatal(err)
		}
	}
	return source, SQLResolverAdapter{Sources: map[string]*MaterializedSource{"docs": source}}
}

func benchmarkTT024PhraseQuery(b *testing.B, adapter SQLResolverAdapter) {
	b.Helper()
	const query = "FROM CACHE('docs') AS doc WHERE CONTAINS_PHRASE(doc.body, 'alpha beta') SELECT doc.id"
	b.ReportAllocs()
	b.ReportMetric(20_000, "rows/source")
	b.ReportMetric(20, "rows/result")
	b.ResetTimer()
	for b.Loop() {
		result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, adapter, hatSql.SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 20 {
			b.Fatalf("rows = %d, want 20", len(result.Rows))
		}
	}
}
