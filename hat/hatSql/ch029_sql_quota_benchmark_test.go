package hatSql

import (
	"context"
	"testing"
)

func BenchmarkCH029BaselineQueryExecution(b *testing.B) {
	query := "FROM VALUES (1), (2), (3), (4), (5), (6), (7), (8) AS values(id) SELECT id"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := ExecuteSQLQueryContext(context.Background(), query, nil, SQLQueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCH029QuotaQueryExecution(b *testing.B) {
	query := "FROM VALUES (1), (2), (3), (4), (5), (6), (7), (8) AS values(id) SELECT id"
	registry, err := NewSQLQuotaRegistry(SQLQuotaRegistryOptions{Limits: SQLQuotaLimits{MaxQueries: b.N + 1}})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := ExecuteSQLQueryContext(context.Background(), query, nil, SQLQueryOptions{Quota: registry, QuotaKey: "benchmark"}); err != nil {
			b.Fatal(err)
		}
	}
}
