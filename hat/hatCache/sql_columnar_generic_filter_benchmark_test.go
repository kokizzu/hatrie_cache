package hatCache

import "testing"

func BenchmarkSQLColumnarGenericFilter(b *testing.B) {
	trie, _ := benchmarkSQLColumnarJobs(b)
	query := "FROM CACHE('jobs') AS job WHERE job.state LIKE 'queued%' SELECT job.id, job.name"
	b.ResetTimer()
	for b.Loop() {
		result, err := ExecuteSQLQuery(query, trie)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) == 0 {
			b.Fatal("generic filter returned no rows")
		}
	}
}
