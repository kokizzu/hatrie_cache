package hatSql_test

import (
	"context"
	"fmt"
	"testing"

	"hatrie_cache/hat/hatSql"
)

type chg02StarBenchmarkResolver struct {
	rows        []hatSql.Row
	streamCalls int
}

func (resolver *chg02StarBenchmarkResolver) ResolveSQLSource(string, string) ([]hatSql.Row, error) {
	return resolver.rows, nil
}

func (resolver *chg02StarBenchmarkResolver) StreamSQLSource(ctx context.Context, _, _ string, visit func(hatSql.Row) error) error {
	resolver.streamCalls++
	for _, row := range resolver.rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(row); err != nil {
			return err
		}
	}
	return nil
}

func BenchmarkCHG02ExternalOrderBySelectStar(b *testing.B) {
	query := `
FROM CACHE('events') AS event
SELECT *
ORDER BY event.id DESC`
	rows := make([]hatSql.Row, 2048)
	for index := range rows {
		rows[index] = hatSql.Row{
			"id":      int64(index),
			"payload": fmt.Sprintf("payload-%04d", index),
		}
	}
	b.Run("materialized_baseline", func(b *testing.B) {
		resolver := &chg02StarBenchmarkResolver{rows: rows}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, hatSql.QueryOptions{})
			if err != nil || len(result.Rows) != len(rows) {
				b.Fatalf("materialized result rows=%d err=%v", len(result.Rows), err)
			}
		}
	})
	b.Run("streaming_spill", func(b *testing.B) {
		resolver := &chg02StarBenchmarkResolver{rows: rows}
		options := hatSql.QueryOptions{
			MaxSortBytes:   16 << 10,
			SpillDirectory: b.TempDir(),
			MaxSpillBytes:  64 << 20,
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			outputRows := 0
			err := hatSql.ExecuteSQLQueryRows(context.Background(), query, resolver, nil, options, func(_ []string, row hatSql.SQLRow) error {
				if row["id"] == nil {
					b.Fatal("streamed row has no id")
				}
				outputRows++
				return nil
			})
			if err != nil || outputRows != len(rows) {
				b.Fatalf("streamed result rows=%d err=%v", outputRows, err)
			}
		}
		b.StopTimer()
		if resolver.streamCalls == 0 {
			b.Fatal("streaming benchmark did not use StreamSQLSource")
		}
	})
}
