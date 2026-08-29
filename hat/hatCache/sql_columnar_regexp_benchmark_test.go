package hatCache

import "testing"

func BenchmarkSQLColumnarRegexpFilter(b *testing.B) {
	trie, _ := benchmarkSQLColumnarJobs(b)
	query := "FROM CACHE('jobs') AS job WHERE job.state REGEXP '^queued' SELECT job.id, job.name"
	b.ResetTimer()
	for b.Loop() {
		result, err := ExecuteSQLQuery(query, trie)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) == 0 {
			b.Fatal("regexp filter returned no rows")
		}
	}
}
