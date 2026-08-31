package hatCache

import (
	"fmt"
	"strings"
	"testing"
)

func BenchmarkSQLColumnarDictionaryLiteralINNumericConjunction(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	var values strings.Builder
	values.WriteByte('[')
	states := []string{"queued", "running", "done", "failed", "paused", "retrying", "archived", "cancelled"}
	for row := 0; row < 20_000; row++ {
		if row > 0 {
			values.WriteByte(',')
		}
		if _, err := fmt.Fprintf(&values, `{"id":%d,"score":%d,"state":%q}`, row, row%1000, states[row%len(states)]); err != nil {
			b.Fatal(err)
		}
	}
	values.WriteByte(']')
	trie.UpsertString("jobs", values.String())
	query := "FROM CACHE('jobs') AS job WHERE job.state IN ('queued', 'running', 'missing') AND job.score >= 800 SELECT job.id, job.state"
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		result, err := ExecuteSQLQuery(query, trie)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 1_000 {
			b.Fatalf("row count = %d, want 1000", len(result.Rows))
		}
	}
}
