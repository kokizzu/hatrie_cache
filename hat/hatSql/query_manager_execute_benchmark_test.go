package hatSql

import (
	"context"
	"testing"
)

func BenchmarkCH031SQLQueryManagerDefaultExecute(b *testing.B) {
	manager := NewSQLQueryManager(256)
	query := "FROM VALUES (1) AS item(value) SELECT value"
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := manager.Execute(context.Background(), query, nil, nil, SQLQueryOptions{QueryID: "benchmark-query"}); err != nil {
			b.Fatal(err)
		}
	}
}
