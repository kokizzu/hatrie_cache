package hatCache

import (
	"fmt"
	"strings"
	"testing"
)

func BenchmarkSQLColumnarDictionaryGroupAggregateOrder(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	var values strings.Builder
	values.WriteByte('[')
	for id := 0; id < 4096; id++ {
		if id > 0 {
			values.WriteByte(',')
		}
		state := id % 32
		if _, err := fmt.Fprintf(&values, `{"state":"state-%02d","value":%d}`, state, state+1); err != nil {
			b.Fatal(err)
		}
	}
	values.WriteByte(']')
	trie.UpsertString("jobs", values.String())
	query := "FROM CACHE('jobs') AS job SELECT job.state, COUNT(*) AS count, SUM(job.value) AS total, AVG(job.value) AS average GROUP BY job.state ORDER BY total DESC LIMIT 8"
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if _, err := ExecuteSQLQuery(query, trie); err != nil {
			b.Fatal(err)
		}
	}
}
