package hatCache

import (
	"context"
	"strconv"
	"strings"
	"testing"
)

type sqlColumnarTopNStreamingResolver struct{ trie *HatTrie }

func (resolver sqlColumnarTopNStreamingResolver) ResolveSQLSource(name, key string) ([]SQLRow, error) {
	return resolver.trie.ResolveSQLSource(name, key)
}

func (resolver sqlColumnarTopNStreamingResolver) StreamSQLSource(ctx context.Context, name, key string, visit func(SQLRow) error) error {
	return resolver.trie.StreamSQLSource(ctx, name, key, visit)
}

var sqlColumnarTopNLayoutPreferenceBenchmarkResult SQLQueryResult

func BenchmarkSQLHatTrieColumnarTopNLayoutPreference(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	var data strings.Builder
	data.Grow(130000)
	data.WriteByte('[')
	for row := 0; row < 4096; row++ {
		if row > 0 {
			data.WriteByte(',')
		}
		data.WriteString(`{"id":`)
		data.WriteString(strconv.Itoa(row))
		data.WriteString(`,"state":"`)
		data.WriteString([]string{"queued", "running", "done"}[row%3])
		data.WriteString(`"}`)
	}
	data.WriteByte(']')
	trie.UpsertString("jobs", data.String())
	fields := []string{"id", "state"}
	for warmup := 0; warmup < 2; warmup++ {
		if _, available, err := trie.ResolveSQLColumnarSource("CACHE", "jobs", fields); err != nil || !available {
			b.Fatalf("warm-up ResolveSQLColumnarSource() available = %t, error = %v", available, err)
		}
	}
	query := `FROM CACHE('jobs') AS job WHERE job.id >= 3072 AND job.id < 3328 SELECT job.id, job.state ORDER BY job.id DESC LIMIT 16`
	streaming := sqlColumnarTopNStreamingResolver{trie: trie}

	for _, benchmark := range []struct {
		name     string
		resolver SQLSourceResolver
	}{
		{name: "streaming_baseline", resolver: streaming},
		{name: "warm_columnar", resolver: trie},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				result, err := ExecuteSQLQuery(query, benchmark.resolver)
				if err != nil || len(result.Rows) != 16 || result.Rows[0]["id"] != float64(3327) {
					b.Fatalf("ExecuteSQLQuery() result = %#v, error = %v", result, err)
				}
				sqlColumnarTopNLayoutPreferenceBenchmarkResult = result
			}
		})
	}
}
