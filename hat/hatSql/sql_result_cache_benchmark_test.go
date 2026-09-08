package hatSql

import (
	"context"
	"testing"
)

func BenchmarkExecuteSQLResultCacheHit(b *testing.B) {
	resolver := &sqlResultCacheVersionedResolver{version: "v1"}
	for index := 0; index < 1_024; index++ {
		resolver.rows = append(resolver.rows, Row{"id": int64(index), "group": "hot"})
	}
	query := "SELECT id, group FROM CACHE('events') WHERE id >= 0"
	options := SQLQueryOptions{ResultCache: NewSQLResultCache(1)}
	if _, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, options); err != nil {
		b.Fatalf("seed query error = %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, options)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != len(resolver.rows) {
			b.Fatalf("rows = %d, want %d", len(result.Rows), len(resolver.rows))
		}
	}
}

func BenchmarkExecuteSQLResultCacheControl(b *testing.B) {
	resolver := &sqlResultCacheVersionedResolver{version: "v1"}
	for index := 0; index < 1_024; index++ {
		resolver.rows = append(resolver.rows, Row{"id": int64(index), "group": "hot"})
	}
	query := "SELECT id, group FROM CACHE('events') WHERE id >= 0"
	b.ReportAllocs()
	for range b.N {
		result, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != len(resolver.rows) {
			b.Fatalf("rows = %d, want %d", len(result.Rows), len(resolver.rows))
		}
	}
}
