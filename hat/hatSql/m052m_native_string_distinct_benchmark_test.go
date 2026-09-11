package hatSql

import (
	"context"
	"strconv"
	"testing"
)

var m052mNativeStringDistinctSink []SQLRow

func m052mNativeStringDistinctRows() []SQLRow {
	rows := make([]SQLRow, 4_096)
	for index := range rows {
		rows[index] = SQLRow{
			"region": "region-" + strconv.Itoa(index%257),
			"value":  int64(index%1000 - 500),
		}
	}
	return rows
}

func BenchmarkCompiledSQLNativeStringDistinctBaseline(b *testing.B) {
	rows := m052mNativeStringDistinctRows()
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT DISTINCT src.region AS region WHERE src.value >= 0")
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
			b.Fatalf("execute ordinary string DISTINCT query: %v", err)
		}
		m052mNativeStringDistinctSink = result.Rows
	}
}

func BenchmarkCompiledSQLNativeStringDistinctNative(b *testing.B) {
	rows := m052mNativeStringDistinctRows()
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT DISTINCT src.region AS region WHERE src.value >= 0")
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
			b.Fatalf("execute native string DISTINCT query: %v", err)
		}
		m052mNativeStringDistinctSink = result
	}
}
