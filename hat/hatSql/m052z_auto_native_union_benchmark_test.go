package hatSql

import (
	"context"
	"testing"
)

var m052zAutoNativeUnionBenchmarkSink SQLQueryResult

func BenchmarkM052ZAutomaticNativeUnionBranches(b *testing.B) {
	rows := make([]SQLRow, 4096)
	for index := range rows {
		rows[index] = SQLRow{
			"id":    int64(index),
			"value": int64(index),
		}
	}
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.id, src.value WHERE src.value >= 2048 UNION ALL FROM CACHE('items') AS src SELECT src.id, src.value WHERE src.value < 2048")
	if err != nil {
		b.Fatalf("compile union SQL: %v", err)
	}
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})

	for name, options := range map[string]SQLQueryOptions{
		"baseline_general_executor": {DisableNativeDataflow: true},
		"automatic_native_branches": {},
	} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := compiled.Execute(context.Background(), resolver, nil, options)
				if err != nil {
					b.Fatal(err)
				}
				m052zAutoNativeUnionBenchmarkSink = result
			}
		})
	}
}
