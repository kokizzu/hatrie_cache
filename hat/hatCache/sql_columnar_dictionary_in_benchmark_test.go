package hatCache

import (
	"strconv"
	"strings"
	"testing"
)

var sqlColumnarDictionaryINBenchmarkResult SQLQueryResult

func BenchmarkSQLColumnarDictionaryLiteralIN(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	states := []string{"queued", "running", "done", "failed", "retry", "paused", "waiting", "cancelled"}
	var data strings.Builder
	data.Grow(560000)
	data.WriteByte('[')
	for row := 0; row < 20000; row++ {
		if row > 0 {
			data.WriteByte(',')
		}
		data.WriteString(`{"id":`)
		data.WriteString(strconv.Itoa(row))
		data.WriteString(`,"state":"`)
		data.WriteString(states[row%len(states)])
		data.WriteString(`"}`)
	}
	data.WriteByte(']')
	trie.UpsertString("jobs", data.String())
	query := "FROM CACHE('jobs') AS job WHERE job.state IN ('queued', 'running', 'waiting', 'missing') SELECT job.id, job.state"

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result, err := ExecuteSQLQuery(query, trie)
		if err != nil || len(result.Rows) != 7500 {
			b.Fatalf("ExecuteSQLQuery() result = %#v, error = %v", result, err)
		}
		sqlColumnarDictionaryINBenchmarkResult = result
	}
}
