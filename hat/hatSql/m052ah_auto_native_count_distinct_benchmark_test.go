package hatSql

import (
	"context"
	"testing"
)

var m052AHBenchmarkRowsSink []SQLRow

func BenchmarkM052AHAutoNativeCountDistinct(b *testing.B) {
	m052AHBenchmarkCountDistinct(b, SQLQueryOptions{})
}

func BenchmarkM052AHFallbackCountDistinct(b *testing.B) {
	m052AHBenchmarkCountDistinct(b, SQLQueryOptions{DisableNativeDataflow: true})
}

func m052AHBenchmarkCountDistinct(b *testing.B, options SQLQueryOptions) {
	rows := make([]SQLRow, 8192)
	for index := range rows {
		rows[index] = SQLRow{
			"group": int64(index % 128),
			"value": int64(index % 64),
		}
	}
	query := "FROM CACHE('items') AS src SELECT src.group, COUNT(DISTINCT src.value) AS unique_count GROUP BY src.group"
	compiled, err := CompileSQLQuery(query)
	if err != nil {
		b.Fatal(err)
	}
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := compiled.Execute(context.Background(), resolver, nil, options)
		if err != nil {
			b.Fatal(err)
		}
		m052AHBenchmarkRowsSink = result.Rows
	}
}
