package hatCache

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func benchmarkSQLJSONPathSkipTrie(b *testing.B, enabled bool) *HatTrie {
	b.Helper()
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	var data strings.Builder
	data.Grow(2 << 20)
	data.WriteByte('[')
	for index := 0; index < 20_000; index++ {
		if index > 0 {
			data.WriteByte(',')
		}
		city := "target"
		if index >= 100 {
			city = "city-" + strconv.Itoa(index)
		}
		fmt.Fprintf(&data, `{"id":%d,"profile":{"city":%q}}`, index, city)
	}
	data.WriteByte(']')
	trie.UpsertString("people", data.String())
	if enabled {
		if err := trie.CreateSQLJSONPathSkipIndex(SQLJSONPathSkipIndexSpec{
			CacheKey:       "people",
			Paths:          []string{"$.profile.city"},
			RowsPerSegment: 256,
			BitsPerSegment: 512,
		}); err != nil {
			b.Fatal(err)
		}
	}
	return trie
}

func BenchmarkSQLJSONPathSkipEquality(b *testing.B) {
	query := "FROM CACHE('people') AS p WHERE JSON_VALUE(p.profile, '$.city') = 'target' SELECT p.id"
	for _, enabled := range []bool{false, true} {
		name := "FullJSONScan"
		if enabled {
			name = "PathSkipMetadata"
		}
		b.Run(name, func(b *testing.B) {
			trie := benchmarkSQLJSONPathSkipTrie(b, enabled)
			if _, err := hatSql.ExecuteSQLQuery(query, trie); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				result, err := hatSql.ExecuteSQLQuery(query, trie)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != 100 {
					b.Fatalf("result rows = %d, want 100", len(result.Rows))
				}
			}
		})
	}
}
