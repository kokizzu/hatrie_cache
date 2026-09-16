package hatSql_test

import (
	"context"
	"fmt"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkCHG01UnorderedGroupBaseline(b *testing.B) {
	query := chg01GroupQuery(2048)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, nil, hatSql.SQLQueryOptions{})
		if err != nil || len(result.Rows) != 2048 {
			b.Fatalf("unbounded grouped aggregate: rows=%d err=%v", len(result.Rows), err)
		}
	}
}

func BenchmarkCHG01BoundedUnorderedGroup(b *testing.B) {
	query := chg01GroupQuery(2048)
	spillDirectory := b.TempDir()
	options := hatSql.SQLQueryOptions{
		MaxGroupBytes:  16 << 10,
		SpillDirectory: spillDirectory,
		MaxSpillBytes:  64 << 20,
	}
	b.ReportAllocs()
	supported := 0
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, nil, options)
		if err == nil && len(result.Rows) == 2048 {
			supported++
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(supported)/float64(b.N), "supported")
}

func chg01GroupQuery(groups int) string {
	query := "FROM VALUES "
	for index := 0; index < groups; index++ {
		if index > 0 {
			query += ", "
		}
		query += fmt.Sprintf("('group-%04d')", index)
	}
	return query + " AS src(region) SELECT src.region, COUNT(*) AS total GROUP BY src.region"
}
