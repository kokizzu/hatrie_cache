package hatSql

import (
	"context"
	"testing"
)

var mz034SQLGroupBenchmarkSink SQLQueryResult

func mz034SQLGroupBenchmarkRows(count int) []SQLRow {
	rows := make([]SQLRow, count)
	for index := range rows {
		rows[index] = SQLRow{
			"group": "group-" + string(rune('a'+index%32)),
			"score": int64(index%97 + 1),
		}
	}
	return rows
}

// BenchmarkMZ034RebuildSQLGroupAggregate is the pre-incremental baseline:
// every measurement rebuilds all grouped state from the 10,000-row source.
func BenchmarkMZ034RebuildSQLGroupAggregate(b *testing.B) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.group AS bucket, COUNT(*) AS total, SUM(src.score) AS total_score GROUP BY src.group")
	if err != nil {
		b.Fatalf("compile SQL: %v", err)
	}
	rows := mz034SQLGroupBenchmarkRows(10_000)
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	b.ReportAllocs()
	for range b.N {
		result, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		mz034SQLGroupBenchmarkSink = result
	}
}
