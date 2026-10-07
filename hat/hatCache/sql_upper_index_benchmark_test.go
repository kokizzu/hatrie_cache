package hatCache

import (
	"strconv"
	"strings"
	"testing"
)

func benchmarkSQLUpperIndexTrie(b *testing.B, indexed bool) *HatTrie {
	b.Helper()
	const rows = 10_000
	var data strings.Builder
	data.Grow(rows * 32)
	data.WriteByte('[')
	for index := 0; index < rows; index++ {
		if index > 0 {
			data.WriteByte(',')
		}
		name := "Grace"
		if index%100 == 0 {
			name = "Ada"
		}
		data.WriteString(`{"id":`)
		data.WriteString(strconv.Itoa(index))
		data.WriteString(`,"name":"`)
		data.WriteString(name)
		data.WriteString(`"}`)
	}
	data.WriteByte(']')

	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	trie.UpsertString("people", data.String())
	if indexed {
		if err := trie.CreateSQLJSONUpperIndex("people", "name"); err != nil {
			b.Fatal(err)
		}
	}
	return trie
}

func BenchmarkSQLJSONUpperIndexQuery(b *testing.B) {
	const query = "FROM CACHE('people') AS person WHERE UPPER(person.name) = 'ADA' SELECT person.id"
	for _, benchmark := range []struct {
		name    string
		indexed bool
	}{
		{name: "scan", indexed: false},
		{name: "indexed", indexed: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			trie := benchmarkSQLUpperIndexTrie(b, benchmark.indexed)
			warmup, err := ExecuteSQLQuery(query, trie)
			if err != nil || len(warmup.Rows) != 100 {
				b.Fatalf("warmup rows = %d, error = %v", len(warmup.Rows), err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := ExecuteSQLQuery(query, trie)
				if err != nil || len(result.Rows) != 100 {
					b.Fatalf("query rows = %d, error = %v", len(result.Rows), err)
				}
			}
		})
	}
}
