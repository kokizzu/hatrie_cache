package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestQueryAlertRulesEvaluateThresholdsAndMissingRows(t *testing.T) {
	rows := map[string][]hatSql.Row{
		"metrics":    {{"latency": int64(95)}},
		"heartbeats": {},
	}
	resolver := hatSql.SourceResolverFunc(func(_ string, key string) ([]hatSql.Row, error) {
		return hatSql.CloneRows(rows[key]), nil
	})
	rules, err := hatSql.NewQueryAlertRules(resolver, hatSql.QueryOptions{}, 4)
	if err != nil {
		t.Fatal(err)
	}
	if err := rules.Create(hatSql.QueryAlertRule{
		Name:      "high-latency",
		Query:     "FROM CACHE('metrics') SELECT latency",
		Threshold: &hatSql.QueryAlertThreshold{Column: "latency", Operator: ">=", Value: 90},
	}); err != nil {
		t.Fatal(err)
	}
	if err := rules.Create(hatSql.QueryAlertRule{
		Name:            "missing-heartbeat",
		Query:           "FROM CACHE('heartbeats') SELECT id",
		ExpectedMinRows: 1,
	}); err != nil {
		t.Fatal(err)
	}

	latency, err := rules.Evaluate(context.Background(), "high-latency")
	if err != nil || !latency.Triggered || latency.OutputRows != 1 || len(latency.Plan) == 0 {
		t.Fatalf("threshold Evaluate() = %#v, %v", latency, err)
	}
	missing, err := rules.Evaluate(context.Background(), "missing-heartbeat")
	if err != nil || !missing.Triggered || missing.OutputRows != 0 || missing.Reason != "expected at least 1 rows, got 0" {
		t.Fatalf("missing Evaluate() = %#v, %v", missing, err)
	}
	if history := rules.History(); len(history) != 2 || !history[0].Triggered || !history[1].Triggered {
		t.Fatalf("History() = %#v", history)
	}

	rows["metrics"] = []hatSql.Row{{"latency": int64(50)}}
	latency, err = rules.Evaluate(context.Background(), "high-latency")
	if err != nil || latency.Triggered || latency.Reason != "no row matched latency >= 90" {
		t.Fatalf("non-triggering Evaluate() = %#v, %v", latency, err)
	}
}
