package hatCache

import (
	"strconv"
	"strings"
	"testing"
)

var sqlColumnarSegmentSkipBenchmarkResult SQLQueryResult

func BenchmarkSQLHatTrieColumnarSegmentSkip(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	var data strings.Builder
	data.Grow(280000)
	data.WriteByte('[')
	for row := 0; row < 20000; row++ {
		if row > 0 {
			data.WriteByte(',')
		}
		data.WriteString(`{"id":`)
		data.WriteString(strconv.Itoa(row))
		data.WriteByte('}')
	}
	data.WriteByte(']')
	trie.UpsertString("jobs", data.String())
	for warmup := 0; warmup < 2; warmup++ {
		if _, available, err := trie.ResolveSQLColumnarSource("CACHE", "jobs", []string{"id"}); err != nil || !available {
			b.Fatalf("warm-up ResolveSQLColumnarSource() available = %t, error = %v", available, err)
		}
	}
	query := "FROM CACHE('jobs') AS job WHERE job.id >= 19840 SELECT COUNT(*) AS total"
	noSkipQuery := "FROM CACHE('jobs') AS job WHERE job.id >= 0 SELECT COUNT(*) AS total"
	borrowed := sqlBorrowedColumnarImmutableResolver{trie: trie}

	b.Run("borrowed_full_scan", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			result, err := ExecuteSQLQuery(query, borrowed)
			if err != nil || len(result.Rows) != 1 || result.Rows[0]["total"] != int64(160) {
				b.Fatalf("ExecuteSQLQuery() result = %#v, error = %v", result, err)
			}
			sqlColumnarSegmentSkipBenchmarkResult = result
		}
	})
	b.Run("segment_pruned", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			result, err := ExecuteSQLQuery(query, trie)
			if err != nil || len(result.Rows) != 1 || result.Rows[0]["total"] != int64(160) {
				b.Fatalf("ExecuteSQLQuery() result = %#v, error = %v", result, err)
			}
			sqlColumnarSegmentSkipBenchmarkResult = result
		}
	})
	b.Run("borrowed_no_skip", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			result, err := ExecuteSQLQuery(noSkipQuery, borrowed)
			if err != nil || len(result.Rows) != 1 || result.Rows[0]["total"] != int64(20000) {
				b.Fatalf("ExecuteSQLQuery() result = %#v, error = %v", result, err)
			}
			sqlColumnarSegmentSkipBenchmarkResult = result
		}
	})
	b.Run("segment_no_skip", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			result, err := ExecuteSQLQuery(noSkipQuery, trie)
			if err != nil || len(result.Rows) != 1 || result.Rows[0]["total"] != int64(20000) {
				b.Fatalf("ExecuteSQLQuery() result = %#v, error = %v", result, err)
			}
			sqlColumnarSegmentSkipBenchmarkResult = result
		}
	})
}
