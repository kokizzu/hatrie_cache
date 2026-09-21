package hatSql

import (
	"testing"
)

func BenchmarkCH042StorageAwareSample(b *testing.B) {
	rows := sampleRows(10000)
	sampledRows := sqlSampleRows(rows, sqlTableSample{mode: "BERNOULLI", value: 10, seed: 7})
	query := `SELECT id FROM CACHE('events') TABLESAMPLE BERNOULLI (10) REPEATABLE (7)`

	b.Run("legacy-materialize", func(b *testing.B) {
		resolver := SourceResolverFunc(func(string, string) ([]Row, error) {
			return rows, nil
		})
		b.ReportAllocs()
		for range b.N {
			result, err := ExecuteSQLQuery(query, resolver)
			if err != nil {
				b.Fatal(err)
			}
			if len(result.Rows) != len(sampledRows) {
				b.Fatalf("legacy sample rows = %d, want %d", len(result.Rows), len(sampledRows))
			}
		}
	})

	b.Run("storage-aware", func(b *testing.B) {
		resolver := &ch042SampledResolver{rows: rows, sampledRows: sampledRows}
		b.ReportAllocs()
		for range b.N {
			result, err := ExecuteSQLQuery(query, resolver)
			if err != nil {
				b.Fatal(err)
			}
			if len(result.Rows) != len(sampledRows) {
				b.Fatalf("storage-aware sample rows = %d, want %d", len(result.Rows), len(sampledRows))
			}
		}
	})
}
