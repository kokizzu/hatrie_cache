package hatCache

import (
	"strconv"
	"strings"
	"testing"
)

var sqlColumnarSortedProjectionBenchmarkResult SQLQueryResult

func BenchmarkSQLHatTrieColumnarSortedProjection(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	var data strings.Builder
	data.Grow(540000)
	data.WriteByte('[')
	for row := 0; row < 20000; row++ {
		if row > 0 {
			data.WriteByte(',')
		}
		data.WriteString(`{"id":`)
		data.WriteString(strconv.Itoa(row))
		data.WriteString(`,"score":`)
		data.WriteString(strconv.Itoa((row * 7919) % 20000))
		data.WriteByte('}')
	}
	data.WriteByte(']')
	trie.UpsertString("jobs", data.String())
	fields := []string{"score", "id"}
	for warmup := 0; warmup < 2; warmup++ {
		if _, available, err := trie.ResolveSQLColumnarSource("CACHE", "jobs", fields); err != nil || !available {
			b.Fatalf("warm-up ResolveSQLColumnarSource() available = %t, error = %v", available, err)
		}
	}
	query := "FROM CACHE('jobs') AS job SELECT job.id, job.score ORDER BY job.score ASC LIMIT 50"
	baseline := sqlBorrowedColumnarImmutableResolver{trie: trie}

	b.Run("direct_columnar_top_n", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			result, err := ExecuteSQLQuery(query, baseline)
			if err != nil || len(result.Rows) != 50 {
				b.Fatalf("ExecuteSQLQuery() result = %#v, error = %v", result, err)
			}
			sqlColumnarSortedProjectionBenchmarkResult = result
		}
	})

	for run := 0; run < sqlColumnarLayoutOrderCacheMinReads; run++ {
		if _, err := ExecuteSQLQuery(query, trie); err != nil {
			b.Fatalf("projection warm-up run %d error = %v", run, err)
		}
	}
	b.Run("warm_sorted_projection", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			result, err := ExecuteSQLQuery(query, trie)
			if err != nil || len(result.Rows) != 50 {
				b.Fatalf("ExecuteSQLQuery() result = %#v, error = %v", result, err)
			}
			sqlColumnarSortedProjectionBenchmarkResult = result
		}
	})

	batch, available, err := trie.BorrowSQLColumnarSource("CACHE", "jobs", fields)
	if err != nil || !available {
		b.Fatalf("BorrowSQLColumnarSource() available = %t, error = %v", available, err)
	}
	layoutKey := newSQLColumnarLayoutCacheKey("jobs", fields)
	orderKey := sqlColumnarLayoutOrderCacheKey{layout: layoutKey, field: "score"}
	b.Run("projection_build", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			cache := sqlColumnarLayoutCache{
				entries: map[sqlColumnarLayoutCacheKey]sqlColumnarLayoutCacheEntry{
					layoutKey: {batch: batch, sequence: 1},
				},
				orderObservations: map[sqlColumnarLayoutOrderCacheKey]uint8{orderKey: sqlColumnarLayoutOrderCacheMinReads - 1},
			}
			if _, ready := cache.observeOrder(layoutKey, "score"); !ready {
				b.Fatal("observeOrder() did not build sorted projection")
			}
		}
	})
}
