package hatCache

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkCHU49SkipEnabledQuery(b *testing.B) {
	trie := benchmarkSQLJSONPathSkipTrie(b, true)
	query := "FROM CACHE('people') AS p WHERE JSON_VALUE(p.profile, '$.city') = 'target' SELECT p.id"
	if _, err := hatSql.ExecuteSQLQuery(query, trie); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := hatSql.ExecuteSQLQuery(query, trie)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 100 {
			b.Fatalf("result rows = %d, want 100", len(result.Rows))
		}
	}
}

func BenchmarkCHU49SkipEnabledExplain(b *testing.B) {
	trie := benchmarkSQLJSONPathSkipTrie(b, true)
	query := "EXPLAIN ANALYZE FROM CACHE('people') AS p WHERE JSON_VALUE(p.profile, '$.city') = 'target' SELECT p.id"
	if result, err := hatSql.ExecuteSQLQuery(query, trie); err != nil || len(result.Plan) == 0 {
		b.Fatalf("warmup result = %#v, error = %v", result, err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := hatSql.ExecuteSQLQuery(query, trie)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Plan) == 0 {
			b.Fatal("EXPLAIN ANALYZE returned no plan")
		}
	}
}
