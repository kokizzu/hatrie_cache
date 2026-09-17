package hatSql_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestSQLProjectionAdvisorCapturesWorkloadShape(t *testing.T) {
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"team": "blue", "points": int64(5), "region": "apac"}}, nil
	})
	advisor := hatSql.NewSQLProjectionAdvisor(4)
	options := hatSql.QueryOptions{
		ProjectionAdvisor:  advisor,
		QueryID:            "team_totals",
		SlowQueryThreshold: time.Nanosecond,
	}
	query := `FROM CACHE('events') AS e SELECT e.team, SUM(e.points) AS total WHERE e.region = 'apac' GROUP BY e.team ORDER BY e.team DESC`
	for range 2 {
		if _, err := hatSql.ExecuteQueryParameters(context.Background(), query, resolver, nil, options); err != nil {
			t.Fatal(err)
		}
	}

	recommendations := advisor.Recommendations()
	if len(recommendations) != 1 {
		t.Fatalf("Recommendations() length = %d, want 1", len(recommendations))
	}
	recommendation := recommendations[0]
	if !reflect.DeepEqual(recommendation.Fields, []string{"e.points", "e.region", "e.team"}) {
		t.Fatalf("Fields = %#v", recommendation.Fields)
	}
	if !reflect.DeepEqual(recommendation.FilterFields, []string{"e.region"}) {
		t.Fatalf("FilterFields = %#v", recommendation.FilterFields)
	}
	if !reflect.DeepEqual(recommendation.GroupByFields, []string{"e.team"}) {
		t.Fatalf("GroupByFields = %#v", recommendation.GroupByFields)
	}
	if !reflect.DeepEqual(recommendation.OrderByFields, []string{"e.team"}) {
		t.Fatalf("OrderByFields = %#v", recommendation.OrderByFields)
	}
}

func TestSQLProjectionAdvisorKeepsDifferentShapesSeparate(t *testing.T) {
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"team": "blue", "region": "apac", "points": int64(5)}}, nil
	})
	advisor := hatSql.NewSQLProjectionAdvisor(4)
	options := hatSql.QueryOptions{
		ProjectionAdvisor:  advisor,
		QueryID:            "dashboard",
		SlowQueryThreshold: time.Nanosecond,
	}
	queries := []string{
		`FROM CACHE('events') SELECT team WHERE points > 0`,
		`FROM CACHE('events') SELECT region WHERE points > 0`,
	}
	for _, query := range queries {
		for range 2 {
			if _, err := hatSql.ExecuteQueryParameters(context.Background(), query, resolver, nil, options); err != nil {
				t.Fatal(err)
			}
		}
	}

	recommendations := advisor.Recommendations()
	if len(recommendations) != 2 {
		t.Fatalf("Recommendations() length = %d, want 2: %#v", len(recommendations), recommendations)
	}
	if !reflect.DeepEqual(recommendations[0].Fields, []string{"points", "team"}) || !reflect.DeepEqual(recommendations[1].Fields, []string{"points", "region"}) {
		t.Fatalf("Recommendations() = %#v", recommendations)
	}
}
