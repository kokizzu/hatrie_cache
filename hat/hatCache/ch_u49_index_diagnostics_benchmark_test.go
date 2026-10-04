package hatCache

import (
	"fmt"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkCHU49ExplainFieldIndex(b *testing.B) {
	trie := benchmarkCHU49IndexTrie(b, "field")
	benchmarkCHU49Explain(b, trie, "FROM CACHE('people') AS person WHERE person.team = 'team-3' SELECT person.id")
}

func BenchmarkCHU49ExplainTypedInt64Index(b *testing.B) {
	trie := benchmarkCHU49IndexTrie(b, "typed_int64")
	benchmarkCHU49Explain(b, trie, "FROM CACHE('people') AS person WHERE person.age = 3 SELECT person.id")
}

func BenchmarkCHU49ExplainBitmapIndex(b *testing.B) {
	trie := benchmarkCHU49IndexTrie(b, "bitmap")
	benchmarkCHU49Explain(b, trie, "FROM CACHE('people') AS person WHERE person.team = 'team-3' SELECT person.id")
}

func benchmarkCHU49Explain(b *testing.B, trie *HatTrie, query string) {
	b.Helper()
	query = "EXPLAIN ANALYZE " + query
	if result, err := hatSql.ExecuteSQLQuery(query, trie); err != nil || len(result.Plan) == 0 {
		b.Fatalf("warmup result = %#v, error = %v", result, err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := hatSql.ExecuteSQLQuery(query, trie)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Plan) == 0 {
			b.Fatal("EXPLAIN ANALYZE returned no plan")
		}
	}
}

func benchmarkCHU49IndexTrie(b *testing.B, kind string) *HatTrie {
	b.Helper()
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	var data strings.Builder
	data.WriteByte('[')
	for index := 0; index < 10000; index++ {
		if index > 0 {
			data.WriteByte(',')
		}
		fmt.Fprintf(&data, `{"id":%d,"team":"team-%d","age":%d}`, index, index%10, index%10)
	}
	data.WriteByte(']')
	trie.UpsertString("people", data.String())
	switch kind {
	case "field":
		if err := trie.CreateSQLJSONFieldIndex("people", "team"); err != nil {
			b.Fatal(err)
		}
	case "typed_int64":
		if err := trie.CreateSQLTypedJSONIndex(SQLJSONIndexSpec{CacheKey: "people", Fields: []string{"age"}, Type: SQLIndexInt64}); err != nil {
			b.Fatal(err)
		}
	case "bitmap":
		if err := trie.CreateSQLJSONBitmapIndex("people", "team"); err != nil {
			b.Fatal(err)
		}
	default:
		b.Fatalf("unknown index kind %q", kind)
	}
	return trie
}
