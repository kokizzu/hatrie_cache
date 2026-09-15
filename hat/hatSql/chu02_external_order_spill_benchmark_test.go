package hatSql_test

import (
	"context"
	"fmt"
	"testing"

	"hatrie_cache/hat/hatSql"
)

const chu02BenchmarkQuery = `
FROM EXTERNAL('events') AS event
SELECT event.id, event.payload
ORDER BY event.id DESC`

type chu02GeneratedExternalResolver struct {
	count int
}

func (resolver *chu02GeneratedExternalResolver) ResolveSQLSource(string, string) ([]hatSql.Row, error) {
	return resolver.materializedRows(), nil
}

func (resolver *chu02GeneratedExternalResolver) ResolveSQLExternalSource(string) ([]hatSql.Row, error) {
	return resolver.materializedRows(), nil
}

func (resolver *chu02GeneratedExternalResolver) StreamSQLExternalSource(ctx context.Context, _ string, visit func(hatSql.Row) error) error {
	for index := 0; index < resolver.count; index++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(chu02BenchmarkRow(index)); err != nil {
			return err
		}
	}
	return nil
}

func (resolver *chu02GeneratedExternalResolver) materializedRows() []hatSql.Row {
	rows := make([]hatSql.Row, resolver.count)
	for index := range rows {
		rows[index] = chu02BenchmarkRow(index)
	}
	return rows
}

func chu02BenchmarkRow(index int) hatSql.Row {
	return hatSql.Row{
		"id":      int64(index),
		"payload": fmt.Sprintf("payload-%04d", index),
	}
}

func BenchmarkCHU02ExternalOrderByBaselineAndStreaming(b *testing.B) {
	const rowCount = 4096

	b.Run("materialized_baseline", func(b *testing.B) {
		resolver := &chu02GeneratedExternalResolver{count: rowCount}
		b.ReportAllocs()
		for range b.N {
			result, err := hatSql.ExecuteSQLQueryContext(context.Background(), chu02BenchmarkQuery, resolver, hatSql.QueryOptions{})
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
			MaxSortBytes:   16 << 10,
			SpillDirectory: b.TempDir(),
			MaxSpillBytes:  64 << 20,
		}
		b.ReportAllocs()
		for range b.N {
			outputRows := 0
			err := hatSql.ExecuteSQLQueryRows(context.Background(), chu02BenchmarkQuery, resolver, nil, options, func(_ []string, row hatSql.SQLRow) error {
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
