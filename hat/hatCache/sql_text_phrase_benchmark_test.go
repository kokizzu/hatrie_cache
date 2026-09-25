package hatCache

import (
	"encoding/json"
	"strings"
	"testing"
)

var benchmarkSQLTextPhraseRows int

func benchmarkSQLTextPhraseJSON(b *testing.B) string {
	b.Helper()
	rows := make([]map[string]interface{}, 20_000)
	for index := range rows {
		body := "alpha unrelated document section " + strings.Repeat("x", index%7+1)
		if index%500 == 0 {
			body = "alpha beta gamma release notes"
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

func BenchmarkCH026PhraseIndexed(b *testing.B) {
	data := benchmarkSQLTextPhraseJSON(b)
	query := `FROM CACHE('articles') AS article WHERE CONTAINS_PHRASE(article.body, 'alpha beta gamma') SELECT article.id`

	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	trie.UpsertString("articles", data)
	if err := trie.CreateSQLJSONTextIndex("articles", "body"); err != nil {
		b.Fatal(err)
	}
	warmup, err := ExecuteSQLQuery(query, trie)
	if err != nil {
		b.Fatal(err)
	}
	if len(warmup.Rows) != 40 {
		b.Fatalf("indexed warmup rows = %d, want 40", len(warmup.Rows))
	}

	trie.sqlIndexMu.RLock()
	index := trie.sqlJSONTextIndexes["articles"]["body"]
	positionPostings := 0
	for _, posting := range index.positions {
		positionPostings += len(posting)
	}
	trie.sqlIndexMu.RUnlock()
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(positionPostings), "position_postings")
	b.ReportMetric(float64(len(warmup.Rows)), "rows/result")
	for iteration := 0; iteration < b.N; iteration++ {
		result, err := ExecuteSQLQuery(query, trie)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkSQLTextPhraseRows = len(result.Rows)
	}
	if benchmarkSQLTextPhraseRows != 40 {
		b.Fatalf("indexed result rows = %d, want 40", benchmarkSQLTextPhraseRows)
	}
}

func BenchmarkTT024TextPhraseUnion(b *testing.B) {
	data := benchmarkSQLTextPhraseJSON(b)
	query := `FROM CACHE('articles') AS article WHERE CONTAINS_PHRASE(article.body, 'alpha beta') OR CONTAINS_PHRASE(article.body, 'release notes') SELECT article.id`

	b.Run("scan", func(b *testing.B) {
		trie := CreateHatTrie()
		b.Cleanup(trie.Destroy)
		trie.UpsertString("articles", data)
		warmup, err := ExecuteSQLQuery(query, trie)
		if err != nil {
			b.Fatal(err)
		}
		if len(warmup.Rows) != 40 {
			b.Fatalf("scan warmup rows = %d, want 40", len(warmup.Rows))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			result, err := ExecuteSQLQuery(query, trie)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkSQLTextPhraseRows = len(result.Rows)
		}
		if benchmarkSQLTextPhraseRows != 40 {
			b.Fatalf("scan result rows = %d, want 40", benchmarkSQLTextPhraseRows)
		}
	})

	b.Run("indexed_union", func(b *testing.B) {
		trie := CreateHatTrie()
		b.Cleanup(trie.Destroy)
		trie.UpsertString("articles", data)
		if err := trie.CreateSQLJSONTextIndex("articles", "body"); err != nil {
			b.Fatal(err)
		}
		warmup, err := ExecuteSQLQuery(query, trie)
		if err != nil {
			b.Fatal(err)
		}
		if len(warmup.Rows) != 40 {
			b.Fatalf("indexed warmup rows = %d, want 40", len(warmup.Rows))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			result, err := ExecuteSQLQuery(query, trie)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkSQLTextPhraseRows = len(result.Rows)
		}
		if benchmarkSQLTextPhraseRows != 40 {
			b.Fatalf("indexed union result rows = %d, want 40", benchmarkSQLTextPhraseRows)
		}
	})
}
