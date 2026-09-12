package hatSql

import (
	"context"
	"testing"
)

func BenchmarkCH028BaselineQueryExecution(b *testing.B) {
	query := "FROM VALUES (1), (2), (3), (4), (5), (6), (7), (8) AS values(id) SELECT id"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := ExecuteSQLQueryContext(context.Background(), query, nil, SQLQueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCH028MaxThreadsQueryExecution(b *testing.B) {
	query := "FROM VALUES (1), (2), (3), (4), (5), (6), (7), (8) AS values(id) SELECT id SETTINGS max_threads = 2"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := ExecuteSQLQueryContext(context.Background(), query, nil, SQLQueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}
