package hatSql

import (
	"context"
	"testing"
)

func benchmarkSQLSubqueryResultCacheRows() []Row {
	rows := make([]Row, 512)
	for index := range rows {
		rows[index] = Row{
			"id":      int64(index),
			"payload": "payload-value-that-makes-the-inner-result-nontrivial",
		}
	}
	return rows
}

func benchmarkSQLSubqueryResultCache(b *testing.B, options SQLQueryOptions) {
	resolver := &sqlResultCacheVersionedResolver{
		rows:    benchmarkSQLSubqueryResultCacheRows(),
		version: "v1",
	}
	query := "FROM (FROM CACHE('events') SELECT id, payload) AS inner_rows SELECT inner_rows.id"
	if _, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, options); err != nil {
		b.Fatalf("benchmark warmup error = %v", err)
	}
	resolver.sourceCalls = 0
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, options); err != nil {
			b.Fatalf("benchmark query error = %v", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(resolver.sourceCalls), "source_calls")
}

func BenchmarkSQLSubqueryResultCacheDisabled(b *testing.B) {
	benchmarkSQLSubqueryResultCache(b, SQLQueryOptions{})
}

func BenchmarkSQLSubqueryResultCacheEnabled(b *testing.B) {
	benchmarkSQLSubqueryResultCache(b, SQLQueryOptions{SubqueryResultCache: NewSQLResultCache(1)})
}
