package hatSql_test

import (
	"context"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestSQLProjectionAdvisorRecordsExecutionLatency(t *testing.T) {
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"team": "blue", "points": int64(5)}}, nil
	})
	advisor := hatSql.NewSQLProjectionAdvisor(1)
	options := hatSql.QueryOptions{
		ProjectionAdvisor:  advisor,
		QueryID:            "team_totals",
		SlowQueryThreshold: time.Nanosecond,
	}
	if _, err := hatSql.ExecuteQueryParameters(context.Background(), "FROM CACHE('events') SELECT team WHERE points > 0", resolver, nil, options); err != nil {
		t.Fatal(err)
	}
	recommendations := advisor.Recommendations()
	if len(recommendations) != 1 {
		t.Fatalf("Recommendations() length = %d, want 1", len(recommendations))
	}
	if recommendations[0].TotalElapsed <= 0 || recommendations[0].AverageElapsed <= 0 {
		t.Fatalf("execution feedback = %#v, want positive elapsed values", recommendations[0])
	}
}
