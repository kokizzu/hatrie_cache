package hatCache

import (
	"strconv"
	"strings"
	"testing"
)

type sqlBorrowedColumnarBaselineResolver struct{ trie *HatTrie }

func (resolver sqlBorrowedColumnarBaselineResolver) ResolveSQLSource(name, key string) ([]SQLRow, error) {
	return resolver.trie.ResolveSQLSource(name, key)
}

func (resolver sqlBorrowedColumnarBaselineResolver) ResolveSQLColumnarSource(name, key string, fields []string) (SQLColumnarBatch, bool, error) {
	return resolver.trie.ResolveSQLColumnarSource(name, key, fields)
}

var sqlBorrowedColumnarBenchmarkResult SQLQueryResult

func BenchmarkSQLHatTrieBorrowedColumnarLayout(b *testing.B) {
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
	query := "FROM CACHE('jobs') AS job WHERE job.id >= 10000 SELECT COUNT(*) AS total"
	for warmup := 0; warmup < 2; warmup++ {
		if _, available, err := trie.ResolveSQLColumnarSource("CACHE", "jobs", []string{"id"}); err != nil || !available {
			b.Fatalf("warm-up ResolveSQLColumnarSource() available = %t, error = %v", available, err)
		}
	}
	baseline := sqlBorrowedColumnarBaselineResolver{trie: trie}

	b.Run("defensive_copy", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			result, err := ExecuteSQLQuery(query, baseline)
			if err != nil || len(result.Rows) != 1 || result.Rows[0]["total"] != int64(10000) {
				b.Fatalf("ExecuteSQLQuery() result = %#v, error = %v", result, err)
			}
			sqlBorrowedColumnarBenchmarkResult = result
		}
	})
	b.Run("borrowed_immutable", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			result, err := ExecuteSQLQuery(query, trie)
			if err != nil || len(result.Rows) != 1 || result.Rows[0]["total"] != int64(10000) {
				b.Fatalf("ExecuteSQLQuery() result = %#v, error = %v", result, err)
			}
			sqlBorrowedColumnarBenchmarkResult = result
		}
	})
}
