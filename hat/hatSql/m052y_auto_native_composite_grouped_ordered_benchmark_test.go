package hatSql

import (
	"context"
	"testing"
)

func BenchmarkCompiledSQLAutomaticNativeCompositeGroupedOrdered(b *testing.B) {
	query, resolver := m052yCompositeGroupedOrderedBenchmarkSetup(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := query.Execute(context.Background(), resolver, nil, SQLQueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCompiledSQLAutomaticNativeCompositeGroupedOrderedFallback(b *testing.B) {
	query, resolver := m052yCompositeGroupedOrderedBenchmarkSetup(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := query.Execute(context.Background(), resolver, nil, SQLQueryOptions{DisableNativeDataflow: true}); err != nil {
			b.Fatal(err)
		}
	}
}

func m052yCompositeGroupedOrderedBenchmarkSetup(b *testing.B) (*CompiledSQLQuery, SQLSourceResolver) {
	b.Helper()
	rows := make([]SQLRow, 20000)
	for index := range rows {
		rows[index] = SQLRow{
			"group_id": int64(index % 257),
			"tier":     int64(index % 4),
			"value":    int64(index % 97),
		}
	}
	query, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.group_id, src.tier, COUNT(*) AS total, SUM(src.value) AS total_value GROUP BY src.group_id, src.tier HAVING COUNT(*) > 1 ORDER BY total DESC, group_id ASC, tier ASC LIMIT 16 OFFSET 32")
	if err != nil {
		b.Fatal(err)
	}
	return query, SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
}
