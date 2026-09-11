package hatSql

import (
	"context"
	"testing"
)

var m052lNativeStringGroupSink []SQLRow

func m052lNativeStringGroupRows() []SQLRow {
	regions := []string{"apac", "eu", "us", "latam", "mea", "india", "canada", "oceania"}
	rows := make([]SQLRow, 20_000)
	for index := range rows {
		rows[index] = SQLRow{
			"region": regions[index%len(regions)],
			"value":  int64(index % 1_000),
		}
	}
	return rows
}

func BenchmarkCompiledSQLNativeStringGroupBaseline(b *testing.B) {
	rows := m052lNativeStringGroupRows()
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.region AS region, COUNT(*) AS total, SUM(src.value) AS total_value GROUP BY src.region HAVING COUNT(*) >= 2 ORDER BY total DESC, region ASC LIMIT 4 OFFSET 1")
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
			b.Fatalf("execute ordinary string grouped query: %v", err)
		}
		m052lNativeStringGroupSink = result.Rows
	}
}

func BenchmarkCompiledSQLNativeStringGroupNative(b *testing.B) {
	rows := m052lNativeStringGroupRows()
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.region AS region, COUNT(*) AS total, SUM(src.value) AS total_value GROUP BY src.region HAVING COUNT(*) >= 2 ORDER BY total DESC, region ASC LIMIT 4 OFFSET 1")
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
			b.Fatalf("execute native string grouped query: %v", err)
		}
		m052lNativeStringGroupSink = result
	}
}
