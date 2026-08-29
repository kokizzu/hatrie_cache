package hatCache

import "testing"

func BenchmarkSQLColumnarMixedConjunction(b *testing.B) {
	trie, _ := benchmarkSQLColumnarJobs(b)
	query := "FROM CACHE('jobs') AS job WHERE job.state LIKE 'queued%' AND job.id >= 1024 SELECT job.id, job.name"
	b.ResetTimer()
	for b.Loop() {
		result, err := ExecuteSQLQuery(query, trie)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) == 0 {
			b.Fatal("mixed conjunction returned no rows")
		}
	}
}
