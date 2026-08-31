package hatCache

import (
	"fmt"
	"strings"
	"testing"
)

func BenchmarkSQLColumnarDictionaryLiteralOR(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	var values strings.Builder
	values.WriteByte('[')
	states := []string{"queued", "running", "done", "failed", "paused", "retrying", "archived", "cancelled"}
	for row := 0; row < 20_000; row++ {
		if row > 0 {
			values.WriteByte(',')
		}
		if _, err := fmt.Fprintf(&values, `{"id":%d,"state":%q}`, row, states[row%len(states)]); err != nil {
			b.Fatal(err)
		}
	}
	values.WriteByte(']')
	trie.UpsertString("jobs", values.String())
	query := "FROM CACHE('jobs') AS job WHERE job.state = 'queued' OR job.state = 'running' SELECT job.id, job.state LIMIT 100"
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		result, err := ExecuteSQLQuery(query, trie)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 100 {
			b.Fatalf("row count = %d, want 100", len(result.Rows))
		}
	}
}
