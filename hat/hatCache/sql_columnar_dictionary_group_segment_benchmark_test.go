package hatCache

import (
	"fmt"
	"strings"
	"testing"
)

func BenchmarkSQLColumnarDictionaryGroupSegmentSkip(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	var values strings.Builder
	values.WriteByte('[')
	for id := 0; id < 4096; id++ {
		if id > 0 {
			values.WriteByte(',')
		}
		if _, err := fmt.Fprintf(&values, `{"state":%q,"id":%d,"value":%d}`, []string{"queued", "running", "done"}[id%3], id, id%97); err != nil {
			b.Fatal(err)
		}
	}
	values.WriteByte(']')
	trie.UpsertString("jobs", values.String())
	fields := []string{"id", "state", "value"}
	for warmup := 0; warmup < 2; warmup++ {
		if _, available, err := trie.ResolveSQLColumnarSource("CACHE", "jobs", fields); err != nil || !available {
			b.Fatalf("warm-up ResolveSQLColumnarSource() available = %t, error = %v", available, err)
		}
	}
	query := "FROM CACHE('jobs') AS job WHERE job.id >= 3072 AND job.id < 3328 SELECT job.state, COUNT(*) AS count, SUM(job.value) AS total, MIN(job.value) AS minimum, MAX(job.value) AS maximum GROUP BY job.state ORDER BY job.state"
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if _, err := ExecuteSQLQuery(query, trie); err != nil {
			b.Fatal(err)
		}
	}
}
