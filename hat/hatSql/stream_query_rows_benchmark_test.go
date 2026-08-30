package hatSql

import (
	"context"
	"testing"
)

var sqlQueryRowsBenchmarkCount int

func BenchmarkSQLQueryRowsSimpleFilter(b *testing.B) {
	rows := make([]Row, 20_000)
	for index := range rows {
		rows[index] = Row{"id": int64(index), "score": int64(index % 64)}
	}
	resolver := sqlQueryRowsTestResolver{rows: rows}
	const query = "FROM CACHE('events') AS event WHERE event.score >= 32 SELECT event.id"
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		count := 0
		err := ExecuteSQLQueryRows(context.Background(), query, resolver, nil, SQLQueryOptions{}, func([]string, SQLRow) error {
			count++
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
		sqlQueryRowsBenchmarkCount = count
	}
}
