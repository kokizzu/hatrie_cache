package hatCache

import (
	"fmt"
	"strings"
	"testing"
)

var sqlColumnarStringBloomSegmentBenchmarkResult SQLQueryResult

func BenchmarkSQLHatTrieColumnarStringBloomSegmentSkip(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	var data strings.Builder
	data.Grow(620000)
	data.WriteByte('[')
	for row := 0; row < 20000; row++ {
		if row > 0 {
			data.WriteByte(',')
		}
		fmt.Fprintf(&data, `{"id":%d,"tag":"tag-%05d"}`, row, row)
	}
	data.WriteByte(']')
	trie.UpsertString("events", data.String())
	query := "FROM CACHE('events') AS event WHERE event.tag = 'tag-19999' SELECT event.id"
	for warmup := 0; warmup < 2; warmup++ {
		if _, err := ExecuteSQLQuery(query, trie); err != nil {
			b.Fatalf("warm-up ExecuteSQLQuery() error = %v", err)
		}
	}
	borrowed := sqlBorrowedColumnarImmutableResolver{trie: trie}

	b.Run("borrowed_full_scan", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			result, err := ExecuteSQLQuery(query, borrowed)
			if err != nil || len(result.Rows) != 1 || result.Rows[0]["id"] != float64(19999) {
				b.Fatalf("ExecuteSQLQuery() result = %#v, error = %v", result, err)
			}
			sqlColumnarStringBloomSegmentBenchmarkResult = result
		}
	})
	b.Run("bloom_segment_pruned", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			result, err := ExecuteSQLQuery(query, trie)
			if err != nil || len(result.Rows) != 1 || result.Rows[0]["id"] != float64(19999) {
				b.Fatalf("ExecuteSQLQuery() result = %#v, error = %v", result, err)
			}
			sqlColumnarStringBloomSegmentBenchmarkResult = result
		}
	})
}

func BenchmarkSQLHatTrieColumnarStringBloomSegmentNoSkip(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	var data strings.Builder
	data.Grow(620000)
	data.WriteByte('[')
	for row := 0; row < 20000; row++ {
		if row > 0 {
			data.WriteByte(',')
		}
		tag := fmt.Sprintf("tag-%05d", row)
		if row%256 == 0 {
			tag = "common"
		}
		fmt.Fprintf(&data, `{"id":%d,"tag":"%s"}`, row, tag)
	}
	data.WriteByte(']')
	trie.UpsertString("events", data.String())
	query := "FROM CACHE('events') AS event WHERE event.tag = 'common' SELECT event.id"
	for warmup := 0; warmup < 2; warmup++ {
		if _, err := ExecuteSQLQuery(query, trie); err != nil {
			b.Fatalf("warm-up ExecuteSQLQuery() error = %v", err)
		}
	}
	borrowed := sqlBorrowedColumnarImmutableResolver{trie: trie}

	b.Run("borrowed_full_scan", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			result, err := ExecuteSQLQuery(query, borrowed)
			if err != nil || len(result.Rows) != 79 {
				b.Fatalf("ExecuteSQLQuery() result = %#v, error = %v", result, err)
			}
			sqlColumnarStringBloomSegmentBenchmarkResult = result
		}
	})
	b.Run("bloom_no_skip", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			result, err := ExecuteSQLQuery(query, trie)
			if err != nil || len(result.Rows) != 79 {
				b.Fatalf("ExecuteSQLQuery() result = %#v, error = %v", result, err)
			}
			sqlColumnarStringBloomSegmentBenchmarkResult = result
		}
	})
}
