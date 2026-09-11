package hatSql

import (
	"context"
	"testing"
)

var ch001PrewhereSink SQLQueryResult

func ch001PrewhereRows() []SQLRow {
	rows := make([]SQLRow, 20000)
	for index := range rows {
		rows[index] = SQLRow{
			"id":      int64(index),
			"keep":    index%16 == 0,
			"score":   int64(index % 1000),
			"payload": "payload-value-that-is-not-needed-for-rejected-rows",
		}
	}
	return rows
}

type ch001PrewhereStreamResolver struct{ rows []SQLRow }

func (resolver ch001PrewhereStreamResolver) ResolveSQLSource(string, string) ([]SQLRow, error) {
	return resolver.rows, nil
}

func (resolver ch001PrewhereStreamResolver) StreamSQLSource(ctx context.Context, _ string, _ string, visit func(Row) error) error {
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

func ch001PrewhereBenchmarkQuery() string {
	return "FROM CACHE('items') AS item PREWHERE item.keep = true WHERE item.payload LIKE '%needle%' AND item.score >= 900 SELECT item.id, item.payload"
}

func ch001PrewhereBaselineQuery() string {
	return "FROM CACHE('items') AS item WHERE item.payload LIKE '%needle%' AND item.score >= 900 AND item.keep = true SELECT item.id, item.payload"
}

func ch001PrewhereBenchmarkRows(b *testing.B) (SQLSourceResolver, string) {
	b.Helper()
	rows := ch001PrewhereRows()
	return ch001PrewhereStreamResolver{rows: rows}, ch001PrewhereBenchmarkQuery()
}

func BenchmarkSQLExplicitPrewhere(b *testing.B) {
	resolver, query := ch001PrewhereBenchmarkRows(b)
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		ch001PrewhereSink = result
	}
}

func BenchmarkSQLExplicitPrewhereBaseline(b *testing.B) {
	resolver, _ := ch001PrewhereBenchmarkRows(b)
	query := ch001PrewhereBaselineQuery()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		ch001PrewhereSink = result
	}
}

func BenchmarkSQLExplicitPrewhereFallback(b *testing.B) {
	rows := ch001PrewhereRows()
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	_, query := ch001PrewhereBenchmarkRows(b)
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		ch001PrewhereSink = result
	}
}

func BenchmarkSQLExplicitPrewhereFallbackBaseline(b *testing.B) {
	rows := ch001PrewhereRows()
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	query := ch001PrewhereBaselineQuery()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		ch001PrewhereSink = result
	}
}
