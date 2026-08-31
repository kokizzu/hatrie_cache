package hatCache

import (
	"fmt"
	"strings"
	"testing"
)

func BenchmarkSQLColumnarDictionaryDistinctLiteralIN(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	var values strings.Builder
	values.WriteByte('[')
	states := []string{"queued", "running", "done", "failed", "paused", "retrying", "archived", "cancelled"}
	for row := 0; row < 20_000; row++ {
		if row > 0 {
			values.WriteByte(',')
		}
		if _, err := fmt.Fprintf(&values, `{"state":%q}`, states[row%len(states)]); err != nil {
			b.Fatal(err)
		}
	}
	values.WriteByte(']')
	trie.UpsertString("jobs", values.String())
	query := "FROM CACHE('jobs') AS job WHERE job.state IN ('queued', 'running', 'missing') SELECT DISTINCT job.state ORDER BY job.state"
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		result, err := ExecuteSQLQuery(query, trie)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 2 {
			b.Fatalf("row count = %d, want 2", len(result.Rows))
		}
	}
}
