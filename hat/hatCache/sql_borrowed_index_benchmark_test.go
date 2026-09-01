package hatCache

import (
	"strconv"
	"strings"
	"testing"
)

type sqlCopiedIndexedJoinResolver struct{ *HatTrie }

func (resolver sqlCopiedIndexedJoinResolver) BorrowSQLIndexedSource(string, string, string, interface{}) ([]SQLRow, bool, error) {
	return nil, false, nil
}

func benchmarkSQLIndexedJoinSource(rows int, dimension bool) string {
	var source strings.Builder
	source.Grow(rows * 48)
	source.WriteByte('[')
	for row := 0; row < rows; row++ {
		if row > 0 {
			source.WriteByte(',')
		}
		source.WriteString(`{"id":`)
		source.WriteString(strconv.Itoa(row))
		source.WriteString(`,"team":"team-`)
		source.WriteString(strconv.Itoa(row))
		source.WriteByte('"')
		if dimension {
			source.WriteString(`,"name":"dimension-`)
			source.WriteString(strconv.Itoa(row))
			source.WriteByte('"')
		}
		source.WriteByte('}')
	}
	source.WriteByte(']')
	return source.String()
}

func BenchmarkSQLBorrowedIndexedJoin(b *testing.B) {
	const facts = 100
	const dimensions = 10_000
	const query = "FROM CACHE('facts') AS fact JOIN CACHE('dimensions') AS dimension ON fact.team = dimension.team SELECT fact.id, dimension.name"
	factSource := benchmarkSQLIndexedJoinSource(facts, false)
	dimensionSource := benchmarkSQLIndexedJoinSource(dimensions, true)
	for _, benchmark := range []struct {
		name     string
		borrowed bool
	}{
		{name: "copied_postings"},
		{name: "borrowed_postings", borrowed: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			trie := CreateHatTrie()
			b.Cleanup(trie.Destroy)
			trie.UpsertString("facts", factSource)
			trie.UpsertString("dimensions", dimensionSource)
			if err := trie.CreateSQLJSONFieldIndex("dimensions", "team"); err != nil {
				b.Fatal(err)
			}
			resolver := sqlCopiedIndexedJoinResolver{HatTrie: trie}
			if benchmark.borrowed {
				if result, err := ExecuteSQLQuery(query, trie); err != nil || len(result.Rows) != facts {
					b.Fatalf("warm borrowed join = %#v, %v", result, err)
				}
			} else if result, err := ExecuteSQLQuery(query, resolver); err != nil || len(result.Rows) != facts {
				b.Fatalf("warm copied join = %#v, %v", result, err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var err error
				if benchmark.borrowed {
					_, err = ExecuteSQLQuery(query, trie)
				} else {
					_, err = ExecuteSQLQuery(query, resolver)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
