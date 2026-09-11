package hatSql

import (
	"context"
	"testing"
)

func BenchmarkCompiledSQLAutomaticNativeGrouped(b *testing.B) {
	compiled, resolver := m052sGroupedBenchmarkSetup(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCompiledSQLAutomaticNativeGroupedFallback(b *testing.B) {
	compiled, resolver := m052sGroupedBenchmarkSetup(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{DisableNativeDataflow: true}); err != nil {
			b.Fatal(err)
		}
	}
}

func m052sGroupedBenchmarkSetup(b *testing.B) (*CompiledSQLQuery, SQLSourceResolver) {
	b.Helper()
	rows := make([]SQLRow, 20000)
	for index := range rows {
		rows[index] = SQLRow{
			"group_id": int64(index % 257),
			"value":    int64(index % 97),
		}
	}
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.group_id, COUNT(*) AS total, SUM(src.value) AS total_value GROUP BY src.group_id")
	if err != nil {
		b.Fatal(err)
	}
	return compiled, SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
}
