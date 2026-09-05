package hatCache

import (
	"strconv"
	"strings"
	"testing"
)

var sqlJSONMultikeyBenchmarkSink SQLQueryResult

func benchmarkSQLJSONMultikeySource(rows int) string {
	var source strings.Builder
	source.Grow(rows * 48)
	source.WriteByte('[')
	for index := 0; index < rows; index++ {
		if index != 0 {
			source.WriteByte(',')
		}
		source.WriteString(`{"id":`)
		source.WriteString(strconv.Itoa(index))
		source.WriteString(`,"tags":["tag","`)
		if index%100 == 0 {
			source.WriteString("target")
		} else {
			source.WriteString("other")
		}
		source.WriteString(`"]}`)
	}
	source.WriteByte(']')
	return source.String()
}

func benchmarkSQLJSONMultikeyQuery(b *testing.B, indexed bool) {
	b.Helper()
	trie := CreateHatTrie()
	defer trie.Destroy()
	trie.UpsertString("people", benchmarkSQLJSONMultikeySource(10_000))
	query := "FROM CACHE('people') AS person WHERE ARRAY_CONTAINS(person.tags, 'target') SELECT person.id"
	if indexed {
		if err := trie.CreateSQLJSONMultikeyIndex("people", "tags"); err != nil {
			b.Fatal(err)
		}
		if _, err := ExecuteSQLQuery(query, trie); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLQuery(query, trie)
		if err != nil {
			b.Fatal(err)
		}
		sqlJSONMultikeyBenchmarkSink = result
	}
}

func BenchmarkSQLJSONMultikeyScanQuery(b *testing.B) {
	benchmarkSQLJSONMultikeyQuery(b, false)
}

func BenchmarkSQLJSONMultikeyIndexQuery(b *testing.B) {
	benchmarkSQLJSONMultikeyQuery(b, true)
}
