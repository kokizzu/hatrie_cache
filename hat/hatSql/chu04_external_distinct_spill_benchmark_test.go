package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

const chu04BenchmarkQuery = `
FROM EXTERNAL('events') AS event
SELECT DISTINCT event.id`

func BenchmarkCHU04ExternalDistinctBaselineAndStreaming(b *testing.B) {
	const rowCount = 4096

	b.Run("materialized_baseline", func(b *testing.B) {
		resolver := &chu02GeneratedExternalResolver{count: rowCount}
		b.ReportAllocs()
		for range b.N {
			result, err := hatSql.ExecuteSQLQueryContext(context.Background(), chu04BenchmarkQuery, resolver, hatSql.QueryOptions{})
			if err != nil {
				b.Fatal(err)
			}
			if len(result.Rows) != rowCount {
				b.Fatalf("result rows = %d, want %d", len(result.Rows), rowCount)
			}
		}
	})

	b.Run("streaming_external_spill", func(b *testing.B) {
		resolver := &chu02GeneratedExternalResolver{count: rowCount}
		options := hatSql.QueryOptions{
			MaxSetBytes:    16 << 10,
			SpillDirectory: b.TempDir(),
			MaxSpillBytes:  64 << 20,
		}
		b.ReportAllocs()
		for range b.N {
			outputRows := 0
			err := hatSql.ExecuteSQLQueryRows(context.Background(), chu04BenchmarkQuery, resolver, nil, options, func(_ []string, row hatSql.SQLRow) error {
				if row["id"] == nil {
					b.Fatal("streamed row has no id")
				}
				outputRows++
				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
			if outputRows != rowCount {
				b.Fatalf("result rows = %d, want %d", outputRows, rowCount)
			}
		}
	})
}
