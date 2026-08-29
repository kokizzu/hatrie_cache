package hatCache

import "testing"

func BenchmarkSQLColumnarNumericAggregateConjunction(b *testing.B) {
	trie, _ := benchmarkSQLColumnarJobs(b)
	query := "FROM CACHE('jobs') AS job WHERE job.id >= 1024 AND job.id < 2048 SELECT COUNT(*) AS count, SUM(job.id) AS total, AVG(job.id) AS average, MIN(job.id) AS minimum, MAX(job.id) AS maximum"
	b.Run("columnar", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := ExecuteSQLQuery(query, trie); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("materialized", func(b *testing.B) {
		resolver := sqlRowsOnlyResolver{trie: trie}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := ExecuteSQLQuery(query, resolver); err != nil {
				b.Fatal(err)
			}
		}
	})
}
