package hatCache

import (
	"fmt"
	"strings"
	"testing"
)

func BenchmarkSQLColumnarDictionaryGroupAggregateWithoutOrder(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	var values strings.Builder
	values.WriteByte('[')
	states := []string{"running", "queued", "done", "failed", "paused", "retrying", "archived", "cancelled"}
	for row := 0; row < 20_000; row++ {
		if row > 0 {
			values.WriteByte(',')
		}
		if _, err := fmt.Fprintf(&values, `{"state":%q,"value":%d}`, states[row%len(states)], row%1000); err != nil {
			b.Fatal(err)
		}
	}
	values.WriteByte(']')
	trie.UpsertString("jobs", values.String())
	query := "FROM CACHE('jobs') AS job SELECT job.state, COUNT(*) AS total, SUM(job.value) AS value_sum GROUP BY job.state"
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		result, err := ExecuteSQLQuery(query, trie)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != len(states) {
			b.Fatalf("row count = %d, want %d", len(result.Rows), len(states))
		}
	}
}
