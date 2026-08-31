package hatSql_test

import (
	"context"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestSQLProjectionAdvisorRecommendsRepeatedSlowCacheQuery(t *testing.T) {
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"team": "blue", "points": int64(5)}}, nil
	})
	advisor := hatSql.NewSQLProjectionAdvisor(2)
	options := hatSql.QueryOptions{ProjectionAdvisor: advisor, QueryID: "team_totals", SlowQueryThreshold: time.Nanosecond}
	for run := 0; run < 2; run++ {
		if _, err := hatSql.ExecuteQueryParameters(context.Background(), "FROM CACHE('events') SELECT team WHERE points > 0", resolver, nil, options); err != nil {
			t.Fatal(err)
		}
	}
	recommendations := advisor.Recommendations()
	if len(recommendations) != 1 || recommendations[0].QueryID != "team_totals" || len(recommendations[0].Dependencies) != 1 || recommendations[0].Dependencies[0] != "events" || recommendations[0].SlowQueries != 2 {
		t.Fatalf("Recommendations() = %#v", recommendations)
	}
}

func TestSQLProjectionAdvisorRequiresCallerQueryID(t *testing.T) {
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"team": "blue"}}, nil
	})
	advisor := hatSql.NewSQLProjectionAdvisor(1)
	if _, err := hatSql.ExecuteQueryParameters(context.Background(), "FROM CACHE('events') SELECT team", resolver, nil, hatSql.QueryOptions{ProjectionAdvisor: advisor, SlowQueryThreshold: time.Nanosecond}); err != nil {
		t.Fatal(err)
	}
	if recommendations := advisor.Recommendations(); len(recommendations) != 0 {
		t.Fatalf("Recommendations() = %#v, want none without QueryID", recommendations)
	}
}
