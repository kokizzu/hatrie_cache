package hatCache

import (
	"strconv"
	"strings"
	"testing"
)

func benchmarkSQLJSONIndexINSource(rows int) string {
	var source strings.Builder
	source.Grow(rows * 16)
	source.WriteByte('[')
	for row := 0; row < rows; row++ {
		if row > 0 {
			source.WriteByte(',')
		}
		source.WriteString(`{"id":`)
		source.WriteString(strconv.Itoa(row))
		source.WriteByte('}')
	}
	source.WriteByte(']')
	return source.String()
}

func BenchmarkSQLJSONIndexLiteralIN(b *testing.B) {
	const rows = 10_000
	const query = "FROM CACHE('people') AS person WHERE person.id IN (997, 1997, 2997, 3997, 4997, 5997, 6997, 7997, 8997, 9997) SELECT person.id"
	source := benchmarkSQLJSONIndexINSource(rows)
	for _, indexed := range []bool{false, true} {
		name := "scan"
		if indexed {
			name = "index_union"
		}
		b.Run(name, func(b *testing.B) {
			trie := CreateHatTrie()
			b.Cleanup(trie.Destroy)
			trie.UpsertString("people", source)
			if indexed {
				if err := trie.CreateSQLJSONFieldIndex("people", "id"); err != nil {
					b.Fatal(err)
				}
				if result, err := ExecuteSQLQuery(query, trie); err != nil || len(result.Rows) != 10 {
					b.Fatalf("warm ExecuteSQLQuery() = %#v, %v", result, err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := ExecuteSQLQuery(query, trie)
				if err != nil || len(result.Rows) != 10 {
					b.Fatalf("ExecuteSQLQuery() = %#v, %v", result, err)
				}
			}
		})
	}
}
