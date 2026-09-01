package hatCache

import (
	"fmt"
	"strings"
	"testing"
)

func benchmarkSQLJSONLowerIndexSource(rows int) string {
	var source strings.Builder
	source.Grow(rows * 32)
	source.WriteByte('[')
	for row := 0; row < rows; row++ {
		if row > 0 {
			source.WriteByte(',')
		}
		name := fmt.Sprintf("person-%03d", row%100)
		if row%2 == 0 {
			name = strings.ToUpper(name)
		}
		fmt.Fprintf(&source, `{"id":%d,"name":%q}`, row, name)
	}
	source.WriteByte(']')
	return source.String()
}

func BenchmarkSQLJSONLowerIndexEquality(b *testing.B) {
	const rows = 10_000
	const query = "FROM CACHE('people') AS person WHERE LOWER(person.name) = 'person-042' SELECT person.id"
	source := benchmarkSQLJSONLowerIndexSource(rows)
	for _, indexed := range []bool{false, true} {
		name := "scan"
		if indexed {
			name = "lower_index"
		}
		b.Run(name, func(b *testing.B) {
			trie := CreateHatTrie()
			b.Cleanup(trie.Destroy)
			trie.UpsertString("people", source)
			if indexed {
				if err := trie.CreateSQLJSONLowerIndex("people", "name"); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := ExecuteSQLQuery(query, trie)
				if err != nil || len(result.Rows) != rows/100 {
					b.Fatalf("ExecuteSQLQuery() = %#v, %v", result, err)
				}
			}
		})
	}
}

func BenchmarkSQLJSONLowerIndexLiteralIN(b *testing.B) {
	const rows = 10_000
	const query = "FROM CACHE('people') AS person WHERE LOWER(person.name) IN ('person-042', 'person-043', 'person-044') SELECT person.id"
	source := benchmarkSQLJSONLowerIndexSource(rows)
	for _, indexed := range []bool{false, true} {
		name := "scan"
		if indexed {
			name = "lower_index_union"
		}
		b.Run(name, func(b *testing.B) {
			trie := CreateHatTrie()
			b.Cleanup(trie.Destroy)
			trie.UpsertString("people", source)
			if indexed {
				if err := trie.CreateSQLJSONLowerIndex("people", "name"); err != nil {
					b.Fatal(err)
				}
				if result, err := ExecuteSQLQuery(query, trie); err != nil || len(result.Rows) != rows/100*3 {
					b.Fatalf("warm ExecuteSQLQuery() = %#v, %v", result, err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := ExecuteSQLQuery(query, trie)
				if err != nil || len(result.Rows) != rows/100*3 {
					b.Fatalf("ExecuteSQLQuery() = %#v, %v", result, err)
				}
			}
		})
	}
}
