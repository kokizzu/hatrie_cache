package hatCache

import "testing"

func BenchmarkSQLColumnarNumericConjunction(b *testing.B) {
	trie, _ := benchmarkSQLColumnarJobs(b)
	query := "FROM CACHE('jobs') AS job WHERE job.id >= 1024 AND job.id < 2048 SELECT job.id LIMIT 32"
	genericQuery := "FROM CACHE('jobs') AS job WHERE job.id >= 1024 AND job.id < 2048 AND job.id IS NOT NULL SELECT job.id LIMIT 32"
	b.Run("columnar", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := ExecuteSQLQuery(query, trie); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("generic_columnar", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := ExecuteSQLQuery(genericQuery, trie); err != nil {
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
