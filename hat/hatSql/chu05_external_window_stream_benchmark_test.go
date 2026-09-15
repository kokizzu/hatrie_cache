package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

const chu05ExternalWindowBenchmarkQuery = `
FROM EXTERNAL('events') AS event
SELECT event.id,
       ROW_NUMBER() OVER () AS row_number,
       SUM(event.value) OVER () AS running_sum,
       LAG(event.value) OVER () AS previous_value`

type chu05BenchmarkExternalResolver struct {
	rows []hatSql.Row
}

func (resolver *chu05BenchmarkExternalResolver) ResolveSQLSource(string, string) ([]hatSql.Row, error) {
	return resolver.rows, nil
}

func (resolver *chu05BenchmarkExternalResolver) ResolveSQLExternalSource(string) ([]hatSql.Row, error) {
	return resolver.rows, nil
}

func (resolver *chu05BenchmarkExternalResolver) StreamSQLExternalSource(ctx context.Context, _ string, visit func(hatSql.Row) error) error {
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

func chu05ExternalWindowBenchmarkRows(count int) []hatSql.Row {
	rows := make([]hatSql.Row, count)
	for index := range rows {
		rows[index] = hatSql.Row{
			"id":    int64(index + 1),
			"value": int64(index%97 + 1),
		}
	}
	return rows
}

var chu05ExternalWindowBenchmarkSink int

func BenchmarkCHU05ExternalWindowMaterialized(b *testing.B) {
	resolver := &chu05BenchmarkExternalResolver{rows: chu05ExternalWindowBenchmarkRows(4096)}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := hatSql.ExecuteSQLQueryContext(context.Background(), chu05ExternalWindowBenchmarkQuery, resolver, hatSql.QueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != len(resolver.rows) {
			b.Fatalf("materialized rows = %d, want %d", len(result.Rows), len(resolver.rows))
		}
		chu05ExternalWindowBenchmarkSink = len(result.Rows)
	}
}

func BenchmarkCHU05ExternalWindowStreaming(b *testing.B) {
	resolver := &chu05BenchmarkExternalResolver{rows: chu05ExternalWindowBenchmarkRows(4096)}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		rows := 0
		err := hatSql.ExecuteSQLQueryRows(context.Background(), chu05ExternalWindowBenchmarkQuery, resolver, nil, hatSql.QueryOptions{}, func([]string, hatSql.SQLRow) error {
			rows++
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
		if rows != len(resolver.rows) {
			b.Fatalf("streamed rows = %d, want %d", rows, len(resolver.rows))
		}
		chu05ExternalWindowBenchmarkSink = rows
	}
}
