package hatCache

import (
	"strconv"
	"strings"
	"testing"
)

func BenchmarkSQLJSONLikePrefix(b *testing.B) {
	data := sqlPrefixBenchmarkData(10000)
	query := "FROM CACHE('people') AS person WHERE person.name LIKE 'alpha%' SELECT COUNT(*)"

	b.Run("scan_baseline", func(b *testing.B) {
		trie := CreateHatTrie()
		b.Cleanup(trie.Destroy)
		trie.UpsertString("people", data)
		if err := trie.CreateSQLJSONFieldIndex("people", "name"); err != nil {
			b.Fatalf("CreateSQLJSONFieldIndex() error = %v", err)
		}
		resolver := SQLSourceResolverFunc(func(name, key string) ([]SQLRow, error) {
			return trie.ResolveSQLSource(name, key)
		})
		if _, err := ExecuteSQLQuery(query, resolver); err != nil {
			b.Fatalf("baseline warmup error = %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := ExecuteSQLQuery(query, resolver); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("ordered_prefix_index", func(b *testing.B) {
		trie := CreateHatTrie()
		b.Cleanup(trie.Destroy)
		trie.UpsertString("people", data)
		if err := trie.CreateSQLJSONFieldIndex("people", "name"); err != nil {
			b.Fatalf("CreateSQLJSONFieldIndex() error = %v", err)
		}
		if _, err := ExecuteSQLQuery(query, trie); err != nil {
			b.Fatalf("indexed warmup error = %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := ExecuteSQLQuery(query, trie); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func sqlPrefixBenchmarkData(rows int) string {
	var data strings.Builder
	data.WriteByte('[')
	for id := 0; id < rows; id++ {
		if id > 0 {
			data.WriteByte(',')
		}
		data.WriteString(`{"id":`)
		data.WriteString(strconv.Itoa(id))
		data.WriteString(`,"name":"`)
		if id%20 == 0 {
			data.WriteString("alpha-")
		} else {
			data.WriteString("omega-")
		}
		data.WriteString(strconv.Itoa(id))
		data.WriteString(`"}`)
	}
	data.WriteByte(']')
	return data.String()
}
