package hatCache

import (
	"fmt"
	"strings"
	"testing"
)

func BenchmarkSQLColumnarDictionaryGroupAggregateLiteralIN(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	var values strings.Builder
	values.WriteByte('[')
	states := []string{"queued", "running", "done", "failed", "paused", "retrying", "archived", "cancelled"}
	for row := 0; row < 20_000; row++ {
		if row > 0 {
			values.WriteByte(',')
		}
		if _, err := fmt.Fprintf(&values, `{"owner":"owner-%02d","score":%d,"state":%q}`, row%32, row%1000, states[row%len(states)]); err != nil {
			b.Fatal(err)
		}
	}
	values.WriteByte(']')
	trie.UpsertString("jobs", values.String())
	query := "FROM CACHE('jobs') AS job WHERE job.state IN ('queued', 'running', 'missing') SELECT job.owner, COUNT(*) AS total, SUM(job.score) AS score_sum GROUP BY job.owner ORDER BY score_sum DESC LIMIT 8"
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		result, err := ExecuteSQLQuery(query, trie)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 8 {
			b.Fatalf("row count = %d, want 8", len(result.Rows))
		}
	}
}
