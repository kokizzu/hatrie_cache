package hatCache

import (
	"strconv"
	"strings"
	"testing"
)

type sqlMaterializedTopNFullResolver struct {
	trie *HatTrie
}

func (resolver sqlMaterializedTopNFullResolver) ResolveSQLSource(name, key string) ([]SQLRow, error) {
	return resolver.trie.ResolveSQLSource(name, key)
}

func BenchmarkExecuteSQLQueryMaterializedTopN(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	const rows = 20_000
	var data strings.Builder
	data.Grow(rows * 28)
	data.WriteByte('[')
	for index := 0; index < rows; index++ {
		if index > 0 {
			data.WriteByte(',')
		}
		data.WriteString(`{"id":`)
		data.WriteString(strconv.Itoa(index))
		data.WriteString(`,"score":`)
		data.WriteString(strconv.Itoa((index * 7) % rows))
		data.WriteByte('}')
	}
	data.WriteByte(']')
	trie.UpsertString("events", data.String())
	const query = "FROM CACHE('events') AS event SELECT event.id, event.score ORDER BY event.score DESC LIMIT 50"

	for _, benchmark := range []struct {
		name     string
		resolver SQLSourceResolver
	}{
		{name: "stream_top_n", resolver: trie},
		{name: "full_materialized", resolver: sqlMaterializedTopNFullResolver{trie: trie}},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := ExecuteSQLQuery(query, benchmark.resolver)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != 50 {
					b.Fatalf("rows = %d, want 50", len(result.Rows))
				}
			}
		})
	}
}
