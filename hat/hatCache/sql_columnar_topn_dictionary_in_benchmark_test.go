package hatCache

import (
	"strconv"
	"strings"
	"testing"
)

var sqlColumnarTopNDictionaryINBenchmarkResult SQLQueryResult

func BenchmarkSQLColumnarTopNDictionaryLiteralIN(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	states := []string{"queued", "running", "done", "failed", "retry", "paused", "waiting", "cancelled"}
	var data strings.Builder
	data.Grow(840000)
	data.WriteByte('[')
	for row := 0; row < 20000; row++ {
		if row > 0 {
			data.WriteByte(',')
		}
		data.WriteString(`{"id":`)
		data.WriteString(strconv.Itoa(row))
		data.WriteString(`,"score":`)
		data.WriteString(strconv.Itoa((row * 7919) % 20000))
		data.WriteString(`,"state":"`)
		data.WriteString(states[row%len(states)])
		data.WriteString(`"}`)
	}
	data.WriteByte(']')
	trie.UpsertString("jobs", data.String())
	fields := []string{"score", "state", "id"}
	for warmup := 0; warmup < 2; warmup++ {
		if _, available, err := trie.ResolveSQLColumnarSource("CACHE", "jobs", fields); err != nil || !available {
			b.Fatalf("warm-up ResolveSQLColumnarSource() available = %t, error = %v", available, err)
		}
	}
	query := "FROM CACHE('jobs') AS job WHERE job.state IN ('queued', 'running', 'missing') SELECT job.id, job.score, job.state ORDER BY job.score DESC LIMIT 50"
	streaming := sqlColumnarTopNStreamingResolver{trie: trie}
	direct := sqlBorrowedColumnarImmutableResolver{trie: trie}

	for _, benchmark := range []struct {
		name     string
		resolver SQLSourceResolver
	}{
		{name: "streaming_baseline", resolver: streaming},
		{name: "direct_dictionary_top_n", resolver: direct},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				result, err := ExecuteSQLQuery(query, benchmark.resolver)
				if err != nil || len(result.Rows) != 50 {
					b.Fatalf("ExecuteSQLQuery() result = %#v, error = %v", result, err)
				}
				sqlColumnarTopNDictionaryINBenchmarkResult = result
			}
		})
	}
	for run := 0; run < sqlColumnarLayoutOrderCacheMinReads; run++ {
		if _, err := ExecuteSQLQuery(query, trie); err != nil {
			b.Fatalf("sorted-projection warm-up run %d error = %v", run, err)
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
			sqlColumnarTopNDictionaryINBenchmarkResult = result
		}
	})
}
