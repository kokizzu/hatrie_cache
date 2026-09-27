package hatCache

import (
	"encoding/json"
	"testing"
)

const tt024TextIntersectionQuery = `
FROM CACHE('articles') AS article
WHERE CONTAINS_PHRASE(article.title, 'quick brown')
  AND CONTAINS_PHRASE(article.body, 'lazy fox')
SELECT article.id`

var tt024TextIntersectionBenchmarkSink int

type tt024SingleTextResolver struct {
	trie *HatTrie
}

func (resolver tt024SingleTextResolver) ResolveSQLSource(name, key string) ([]SQLRow, error) {
	return resolver.trie.ResolveSQLSource(name, key)
}

func (resolver tt024SingleTextResolver) ResolveSQLTextProximitySource(name, key, field, query string, maxGap int) ([]SQLRow, bool, error) {
	return resolver.trie.ResolveSQLTextProximitySource(name, key, field, query, maxGap)
}

func newTT024TextIntersectionBenchmarkTrie(b *testing.B) *HatTrie {
	b.Helper()
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	rows := make([]struct {
		ID    int    `json:"id"`
		Title string `json:"title"`
		Body  string `json:"body"`
	}, 10000)
	for index := range rows {
		rows[index].ID = index
		rows[index].Title = "unrelated title"
		rows[index].Body = "unrelated body"
		if index%10 == 0 {
			rows[index].Title = "quick brown fox"
		}
		if index%100 == 0 {
			rows[index].Body = "lazy fox"
		}
	}
	payload, err := json.Marshal(rows)
	if err != nil {
		b.Fatal(err)
	}
	trie.UpsertString("articles", string(payload))
	for _, field := range []string{"title", "body"} {
		if err := trie.CreateSQLJSONTextIndex("articles", field); err != nil {
			b.Fatal(err)
		}
	}
	return trie
}

func BenchmarkTT024TextIntersectionIndexed(b *testing.B) {
	trie := newTT024TextIntersectionBenchmarkTrie(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQuery(tt024TextIntersectionQuery, trie)
		if err != nil {
			b.Fatal(err)
		}
		tt024TextIntersectionBenchmarkSink = len(result.Rows)
	}
}

func BenchmarkTT024TextIntersectionSingleIndexControl(b *testing.B) {
	trie := newTT024TextIntersectionBenchmarkTrie(b)
	resolver := tt024SingleTextResolver{trie: trie}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQuery(tt024TextIntersectionQuery, resolver)
		if err != nil {
			b.Fatal(err)
		}
		tt024TextIntersectionBenchmarkSink = len(result.Rows)
	}
}
