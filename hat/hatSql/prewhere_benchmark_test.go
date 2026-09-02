package hatSql

import (
	"context"
	"strings"
	"testing"
)

const benchmarkSQLPrewhereQuery = "FROM CACHE('events') AS e WHERE e.keep = true SELECT e.id, e.category LIMIT 100"

type benchmarkSQLPrewhereMaterializedResolver struct {
	rows []SQLRow
}

func (resolver benchmarkSQLPrewhereMaterializedResolver) ResolveSQLSource(kind, key string) ([]SQLRow, error) {
	return resolver.rows, nil
}

type benchmarkSQLPrewhereStreamResolver struct {
	rows []SQLRow
}

func (resolver benchmarkSQLPrewhereStreamResolver) ResolveSQLSource(kind, key string) ([]SQLRow, error) {
	return resolver.rows, nil
}

func (resolver benchmarkSQLPrewhereStreamResolver) StreamSQLSource(ctx context.Context, kind, key string, visit func(Row) error) error {
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

func benchmarkSQLPrewhereRows() []SQLRow {
	rows := make([]SQLRow, 20_000)
	payload := strings.Repeat("unused-payload-", 64)
	for index := range rows {
		rows[index] = SQLRow{
			"category": "events",
			"id":       index,
			"keep":     index < 1_000 && index%10 == 0,
			"payload":  payload,
		}
	}
	return rows
}

func benchmarkSQLPrewhereRun(b *testing.B, resolver SQLSourceResolver) {
	b.Helper()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLQueryContext(context.Background(), benchmarkSQLPrewhereQuery, resolver, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 100 || result.Rows[0]["id"] != 0 || result.Rows[99]["id"] != 990 {
			b.Fatalf("unexpected result: rows=%d first=%v last=%v", len(result.Rows), result.Rows[0]["id"], result.Rows[len(result.Rows)-1]["id"])
		}
	}
}

func BenchmarkSQLPrewhereMaterialized(b *testing.B) {
	rows := benchmarkSQLPrewhereRows()
	b.ReportAllocs()
	benchmarkSQLPrewhereRun(b, benchmarkSQLPrewhereMaterializedResolver{rows: rows})
}

func BenchmarkSQLPrewhereStreamed(b *testing.B) {
	rows := benchmarkSQLPrewhereRows()
	b.ReportAllocs()
	benchmarkSQLPrewhereRun(b, benchmarkSQLPrewhereStreamResolver{rows: rows})
}
