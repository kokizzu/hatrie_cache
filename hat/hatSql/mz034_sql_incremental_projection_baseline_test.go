package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

var benchmarkMZ034SQLProjectionResult hatSql.SQLQueryResult

func BenchmarkMZ034RebuildSQLProjection(b *testing.B) {
	rows := mz034SQLProjectionBenchmarkRows()
	resolver := hatSql.SQLSourceResolverFunc(func(_ string, key string) ([]hatSql.SQLRow, error) {
		if key != "items" {
			return nil, nil
		}
		return hatSql.CloneRows(rows), nil
	})
	compiled, err := hatSql.CompileSQLQuery("FROM CACHE('items') SELECT id AS id, value + 1 AS next_value WHERE value >= 0")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := compiled.Execute(context.Background(), resolver, nil, hatSql.SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		benchmarkMZ034SQLProjectionResult = result
	}
}

func mz034SQLProjectionBenchmarkRows() []hatSql.SQLRow {
	rows := make([]hatSql.SQLRow, 10_000)
	for index := range rows {
		rows[index] = hatSql.SQLRow{"id": index, "value": int64(index % 100)}
	}
	return rows
}
