package hatSql

import (
	"context"
	"testing"
)

var m052kGroupedHavingSink []SQLRow

func m052kGroupedHavingRows() []SQLRow {
	rows := make([]SQLRow, 20_000)
	for index := range rows {
		rows[index] = SQLRow{
			"group": int64(index % 512),
			"value": int64(index % 1_000),
		}
	}
	return rows
}

func BenchmarkCompiledSQLGroupedHavingOrderedLimitBaseline(b *testing.B) {
	rows := m052kGroupedHavingRows()
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.group AS bucket, COUNT(*) AS total, SUM(src.value) AS total_value GROUP BY src.group HAVING COUNT(*) >= 40 ORDER BY total DESC, bucket ASC LIMIT 8 OFFSET 4")
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
			b.Fatalf("execute grouped HAVING query: %v", err)
		}
		m052kGroupedHavingSink = result.Rows
	}
}

func BenchmarkCompiledSQLGroupedHavingOrderedLimitNative(b *testing.B) {
	rows := m052kGroupedHavingRows()
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.group AS bucket, COUNT(*) AS total, SUM(src.value) AS total_value GROUP BY src.group HAVING COUNT(*) >= 40 ORDER BY total DESC, bucket ASC LIMIT 8 OFFSET 4")
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
			b.Fatalf("execute native grouped HAVING query: %v", err)
		}
		m052kGroupedHavingSink = result
	}
}
