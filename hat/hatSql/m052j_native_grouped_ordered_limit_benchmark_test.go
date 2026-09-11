package hatSql

import (
	"context"
	"testing"
)

var m052jGroupedOrderedLimitSink []SQLRow

func m052jGroupedOrderedLimitRows() []SQLRow {
	rows := make([]SQLRow, 20_000)
	for index := range rows {
		rows[index] = SQLRow{
			"group": int64(index % 512),
			"value": int64(index % 1_000),
		}
	}
	return rows
}

func BenchmarkCompiledSQLGroupedOrderedLimitBaseline(b *testing.B) {
	rows := m052jGroupedOrderedLimitRows()
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.group AS bucket, COUNT(*) AS total, SUM(src.value) AS total_value GROUP BY src.group ORDER BY total DESC, bucket ASC LIMIT 32 OFFSET 128")
	if err != nil {
		b.Fatalf("compile SQL: %v", err)
	}
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatalf("execute grouped ordered query: %v", err)
		}
		m052jGroupedOrderedLimitSink = result.Rows
	}
}

func BenchmarkCompiledSQLGroupedOrderedLimitNative(b *testing.B) {
	rows := m052jGroupedOrderedLimitRows()
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.group AS bucket, COUNT(*) AS total, SUM(src.value) AS total_value GROUP BY src.group ORDER BY total DESC, bucket ASC LIMIT 32 OFFSET 128")
	if err != nil {
		b.Fatalf("compile SQL: %v", err)
	}
	native, err := compiled.CompileNativeDataflow()
	if err != nil {
		b.Fatalf("compile native dataflow: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := native.Execute(context.Background(), rows)
		if err != nil {
			b.Fatalf("execute native grouped ordered query: %v", err)
		}
		m052jGroupedOrderedLimitSink = result
	}
}
