package hatCache

import (
	"strconv"
	"strings"
	"testing"
)

var sqlColumnarAggregateDictionaryINBenchmarkResult SQLQueryResult

func BenchmarkSQLColumnarNumericAggregateDictionaryLiteralIN(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	states := []string{"queued", "running", "done", "failed", "retry", "paused", "waiting", "cancelled"}
	var data strings.Builder
	data.Grow(840000)
	data.WriteByte('[')
	for row := 0; row < 20000; row++ {
		if row > 0 {
			data.WriteByte(',')
		}
		data.WriteString(`{"score":`)
		data.WriteString(strconv.Itoa(row % 1000))
		data.WriteString(`,"state":"`)
		data.WriteString(states[row%len(states)])
		data.WriteString(`"}`)
	}
	data.WriteByte(']')
	trie.UpsertString("jobs", data.String())
	query := "FROM CACHE('jobs') AS job WHERE job.state IN ('queued', 'running', 'missing') SELECT COUNT(*) AS total, SUM(job.score) AS score_sum"

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result, err := ExecuteSQLQuery(query, trie)
		if err != nil || len(result.Rows) != 1 || result.Rows[0]["total"] != int64(5000) {
			b.Fatalf("ExecuteSQLQuery() result = %#v, error = %v", result, err)
		}
		sqlColumnarAggregateDictionaryINBenchmarkResult = result
	}
}
