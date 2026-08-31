package hatSql_test

import (
	"context"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkSQLProjectionAdvisor(b *testing.B) {
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"team": "blue", "points": int64(5)}}, nil
	})
	b.Run("disabled", func(b *testing.B) {
		for index := 0; index < b.N; index++ {
			if _, err := hatSql.ExecuteQueryParameters(context.Background(), "FROM CACHE('events') SELECT team WHERE points > 0", resolver, nil, hatSql.QueryOptions{}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("enabled", func(b *testing.B) {
		advisor := hatSql.NewSQLProjectionAdvisor(1)
		options := hatSql.QueryOptions{ProjectionAdvisor: advisor, QueryID: "team_totals", SlowQueryThreshold: time.Nanosecond}
		for index := 0; index < b.N; index++ {
			if _, err := hatSql.ExecuteQueryParameters(context.Background(), "FROM CACHE('events') SELECT team WHERE points > 0", resolver, nil, options); err != nil {
				b.Fatal(err)
			}
		}
	})
}
