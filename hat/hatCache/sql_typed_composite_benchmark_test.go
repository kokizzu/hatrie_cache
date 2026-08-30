package hatCache

import (
	"strconv"
	"strings"
	"testing"
)

var sqlTypedCompositeBenchmarkResult interface{}

func benchmarkSQLTypedCompositeTrie(b *testing.B, indexed bool) *HatTrie {
	b.Helper()
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	var data strings.Builder
	data.Grow(20_000 * 56)
	data.WriteByte('[')
	for row := 0; row < 20_000; row++ {
		if row > 0 {
			data.WriteByte(',')
		}
		data.WriteString(`{"id":`)
		data.WriteString(strconv.Itoa(row))
		data.WriteString(`,"tenant_id":`)
		data.WriteString(strconv.Itoa(row % 128))
		data.WriteString(`,"created_at":`)
		data.WriteString(strconv.Itoa(row))
		data.WriteByte('}')
	}
	data.WriteByte(']')
	trie.UpsertString("events", data.String())
	if indexed {
		if err := trie.CreateSQLTypedJSONIndex(SQLJSONIndexSpec{
			CacheKey: "events",
			Fields:   []string{"tenant_id", "created_at"},
			Type:     SQLIndexInt64,
		}); err != nil {
			b.Fatal(err)
		}
	}
	return trie
}

func BenchmarkSQLTypedInt64CompositePrefixRange(b *testing.B) {
	const query = "FROM CACHE('events') AS event WHERE event.tenant_id = 7 AND event.created_at >= 19000 SELECT event.id"
	baseline := benchmarkSQLTypedCompositeTrie(b, false)
	indexed := benchmarkSQLTypedCompositeTrie(b, true)
	if result, err := ExecuteSQLQuery(query, indexed); err != nil || len(result.Rows) == 0 {
		b.Fatalf("indexed warmup = %#v, %v", result, err)
	}
	b.Run("scan", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			result, err := ExecuteSQLQuery(query, baseline)
			if err != nil {
				b.Fatal(err)
			}
			sqlTypedCompositeBenchmarkResult = result
		}
	})
	b.Run("typed_composite", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			result, err := ExecuteSQLQuery(query, indexed)
			if err != nil {
				b.Fatal(err)
			}
			sqlTypedCompositeBenchmarkResult = result
		}
	})
}
