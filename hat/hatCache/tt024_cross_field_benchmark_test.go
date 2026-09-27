package hatCache

import (
	"strconv"
	"strings"
	"testing"
)

const tt024CrossFieldCacheBenchmarkQuery = `
FROM CACHE('docs') AS doc
WHERE (CONTAINS_PHRASE(doc.title, 'alpha beta') AND doc.kind = 'a')
   OR (CONTAINS_PROXIMITY(doc.body, 'gamma delta', 1) AND doc.kind = 'b')
SELECT doc.id`

type tt024CacheFullScanResolver struct {
	trie *HatTrie
}

func (resolver tt024CacheFullScanResolver) ResolveSQLSource(name, key string) ([]SQLRow, error) {
	return resolver.trie.ResolveSQLSource(name, key)
}

func newTT024CrossFieldCacheBenchmarkTrie(b *testing.B) *HatTrie {
	b.Helper()
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)

	const rows = 50000
	var data strings.Builder
	data.Grow(rows * 120)
	data.WriteByte('[')
	for index := 0; index < rows; index++ {
		if index > 0 {
			data.WriteByte(',')
		}
		kind := "other"
		title := "unrelated filler title"
		body := "unrelated filler body"
		if index%200 == 0 {
			kind = "a"
			title = "alpha beta"
		} else if index%201 == 0 {
			kind = "b"
			body = "gamma delta"
		}
		data.WriteString(`{"id":`)
		data.WriteString(strconv.Itoa(index))
		data.WriteString(`,"kind":"`)
		data.WriteString(kind)
		data.WriteString(`","title":"`)
		data.WriteString(title)
		data.WriteString(`","body":"`)
		data.WriteString(body)
		data.WriteString(`"}`)
	}
	data.WriteByte(']')
	trie.UpsertString("docs", data.String())
	for _, field := range []string{"title", "body"} {
		if err := trie.CreateSQLJSONTextIndex("docs", field); err != nil {
			b.Fatal(err)
		}
	}
	return trie
}

var tt024CrossFieldCacheBenchmarkSink SQLQueryResult

func BenchmarkTT024CrossFieldHatTrieFullScan(b *testing.B) {
	trie := newTT024CrossFieldCacheBenchmarkTrie(b)
	resolver := tt024CacheFullScanResolver{trie: trie}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLQuery(tt024CrossFieldCacheBenchmarkQuery, resolver)
		if err != nil {
			b.Fatal(err)
		}
		tt024CrossFieldCacheBenchmarkSink = result
	}
}

func BenchmarkTT024CrossFieldHatTrieIndexedUnion(b *testing.B) {
	trie := newTT024CrossFieldCacheBenchmarkTrie(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLQuery(tt024CrossFieldCacheBenchmarkQuery, trie)
		if err != nil {
			b.Fatal(err)
		}
		tt024CrossFieldCacheBenchmarkSink = result
	}
}
