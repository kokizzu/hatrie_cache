package hatCache

import (
	"strconv"
	"strings"
	"testing"
)

func BenchmarkSQLTypedIndexBaseline(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	const rows = 100_000
	var data strings.Builder
	data.Grow(rows * 32)
	data.WriteByte('[')
	for index := 0; index < rows; index++ {
		if index > 0 {
			data.WriteByte(',')
		}
		data.WriteString(`{"id":`)
		data.WriteString(strconv.Itoa(index))
		data.WriteString(`,"state":"ready"}`)
	}
	data.WriteByte(']')
	trie.UpsertString("events", data.String())
	if err := trie.CreateSQLJSONFieldIndex("events", "id"); err != nil {
		b.Fatal(err)
	}
	rangeQuery := "FROM CACHE('events') AS event WHERE event.id >= 99900 AND event.id < 99910 SELECT event.id"
	orderQuery := "FROM CACHE('events') AS event ORDER BY event.id DESC LIMIT 10 SELECT event.id"
	if result, err := ExecuteSQLQuery(rangeQuery, trie); err != nil || len(result.Rows) != 10 {
		b.Fatalf("indexed range warmup = %#v, %v", result, err)
	}
	if result, err := ExecuteSQLQuery(orderQuery, trie); err != nil || len(result.Rows) != 10 {
		b.Fatalf("indexed order warmup = %#v, %v", result, err)
	}
	fullScan := sqlRowsOnlyResolver{trie: trie}
	for _, benchmark := range []struct {
		name     string
		query    string
		resolver SQLSourceResolver
	}{
		{name: "range/indexed", query: rangeQuery, resolver: trie},
		{name: "range/full_scan", query: rangeQuery, resolver: fullScan},
		{name: "order_limit/indexed", query: orderQuery, resolver: trie},
		{name: "order_limit/full_scan", query: orderQuery, resolver: fullScan},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := ExecuteSQLQuery(benchmark.query, benchmark.resolver); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
