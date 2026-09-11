package hatCache

import (
	"encoding/json"
	"fmt"
	"testing"
)

var benchmarkSQLTextPrefixRows int

func benchmarkSQLTextPrefixJSON(b *testing.B) string {
	b.Helper()
	rows := make([]map[string]interface{}, 10_000)
	for index := range rows {
		body := "ordinary database record"
		if index%100 == 0 {
			body = "cacheable target record"
		}
		rows[index] = map[string]interface{}{
			"id":   index,
			"body": body,
		}
	}
	data, err := json.Marshal(rows)
	if err != nil {
		b.Fatal(err)
	}
	return string(data)
}

func BenchmarkSQLTextPrefixScanVsIndex(b *testing.B) {
	data := benchmarkSQLTextPrefixJSON(b)
	query := `FROM CACHE('articles') AS article WHERE CONTAINS_PREFIX(article.body, 'cache') SELECT article.id`

	scanTrie := CreateHatTrie()
	b.Cleanup(scanTrie.Destroy)
	scanTrie.UpsertString("articles", data)

	indexTrie := CreateHatTrie()
	b.Cleanup(indexTrie.Destroy)
	indexTrie.UpsertString("articles", data)
	if err := indexTrie.CreateSQLJSONTextIndex("articles", "body"); err != nil {
		b.Fatal(err)
	}
	warmup, err := ExecuteSQLQuery(query, indexTrie)
	if err != nil {
		b.Fatal(err)
	}
	if len(warmup.Rows) != 100 {
		b.Fatalf("indexed warmup rows = %d, want 100", len(warmup.Rows))
	}
	indexTrie.sqlIndexMu.RLock()
	uniqueTokens := len(indexTrie.sqlJSONTextIndexes["articles"]["body"].tokenKeys)
	indexTrie.sqlIndexMu.RUnlock()

	b.Run("scan", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			result, err := ExecuteSQLQuery(query, scanTrie)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkSQLTextPrefixRows = len(result.Rows)
		}
	})
	b.Run("sorted_token_postings", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(uniqueTokens), "unique_tokens")
		for range b.N {
			result, err := ExecuteSQLQuery(query, indexTrie)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkSQLTextPrefixRows = len(result.Rows)
		}
	})
	if benchmarkSQLTextPrefixRows == 0 {
		b.Fatal(fmt.Errorf("benchmark result was empty"))
	}
}
