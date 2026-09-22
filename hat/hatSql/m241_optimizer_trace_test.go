package hatSql_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestM241OptimizerTraceReportsRuleApplicationsAndRejectedAlternatives(t *testing.T) {
	query := "EXPLAIN FROM CACHE('orders') AS o WHERE o.id = 1 SELECT o.id"
	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, &optimizerRuleResolver{}, hatSql.SQLQueryOptions{
		OptimizerTrace: true,
		Optimizer: hatSql.NewSQLQueryOptimizer(
			func(plan *hatSql.SQLQueryOptimizationContext) error {
				plan.RejectAlternative("bitmap index", "requires a composite key")
				plan.IndexHint = hatSql.SQLIndexHint{Source: "o", Field: "id", Mode: hatSql.SQLIndexHintForce}
				return nil
			},
			func(plan *hatSql.SQLQueryOptimizationContext) error {
				plan.RejectAlternative("full scan", "an applicable index hint was selected")
				return nil
			},
		),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.OptimizerTrace == nil {
		t.Fatal("optimizer trace is nil")
	}
	if result.OptimizerTrace.Format != "hatrie-cache-sql-optimizer-trace/v1" {
		t.Fatalf("optimizer trace format = %q", result.OptimizerTrace.Format)
	}
	events := result.OptimizerTrace.Events
	if len(events) != 4 {
		t.Fatalf("optimizer trace events = %#v, want two rule and two alternative events", events)
	}
	if events[0].Rule != 1 || events[0].Kind != "rule" || events[0].Action != "applied" {
		t.Fatalf("first optimizer event = %#v", events[0])
	}
	if events[1].Rule != 1 || events[1].Kind != "alternative" || events[1].Action != "rejected" || events[1].Expression != "bitmap index" {
		t.Fatalf("first rejected alternative = %#v", events[1])
	}
	if events[2].Rule != 2 || events[2].Kind != "rule" || events[2].Action != "applied" {
		t.Fatalf("second optimizer event = %#v", events[2])
	}
	if events[3].Rule != 2 || events[3].Kind != "alternative" || events[3].Action != "rejected" || events[3].Reason == "" {
		t.Fatalf("second rejected alternative = %#v", events[3])
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"optimizer_trace"`) || !strings.Contains(string(encoded), `"action":"rejected"`) {
		t.Fatalf("optimizer trace JSON = %s", encoded)
	}
}

func TestM241OptimizerTraceIsOffByDefault(t *testing.T) {
	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), "EXPLAIN FROM VALUES (1) AS values(id) SELECT id", nil, hatSql.SQLQueryOptions{
		Optimizer: hatSql.NewSQLQueryOptimizer(func(*hatSql.SQLQueryOptimizationContext) error { return nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.OptimizerTrace != nil {
		t.Fatalf("default optimizer trace = %#v, want nil", result.OptimizerTrace)
	}
}
