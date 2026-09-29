package hatSql_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestM241OptimizerTraceCapturesRuleApplicationsAndFinalHint(t *testing.T) {
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"id": int64(1)}}, nil
	})
	optimizer := hatSql.NewSQLQueryOptimizer(
		func(plan *hatSql.SQLQueryOptimizationContext) error {
			plan.IndexHint = hatSql.SQLIndexHint{Source: "o", Field: "id", Mode: hatSql.SQLIndexHintForbid}
			return nil
		},
		func(*hatSql.SQLQueryOptimizationContext) error { return nil },
	)
	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), "FROM CACHE('items') AS o SELECT o.id", resolver, hatSql.SQLQueryOptions{
		Optimizer:      optimizer,
		OptimizerTrace: &hatSql.SQLOptimizerTraceOptions{MaxEntries: 4},
	})
	if err != nil {
		t.Fatalf("query error = %v", err)
	}
	if result.OptimizerTrace == nil {
		t.Fatal("optimizer trace is nil")
	}
	trace := result.OptimizerTrace
	if trace.Format != hatSql.SQLOptimizerTraceFormat || len(trace.Entries) != 2 {
		t.Fatalf("trace = %#v, want format and two entries", trace)
	}
	if trace.Entries[0].Rule != 1 || trace.Entries[0].Status != "applied" || trace.Entries[0].After.Field != "id" {
		t.Fatalf("first trace entry = %#v", trace.Entries[0])
	}
	if trace.Entries[1].Rule != 2 || trace.Entries[1].Status != "no_change" {
		t.Fatalf("second trace entry = %#v", trace.Entries[1])
	}
	if trace.FinalHint == nil || trace.FinalHint.Field != "id" || trace.FinalHint.Mode != string(hatSql.SQLIndexHintForbid) {
		t.Fatalf("final hint = %#v", trace.FinalHint)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if !containsM241(string(encoded), `"optimizer_trace"`) || !containsM241(string(encoded), `"rejected_alternatives"`) {
		t.Fatalf("result JSON = %s", encoded)
	}
}

func TestM241OptimizerTraceCapturesRuleErrorAndBoundsEntries(t *testing.T) {
	wantErr := errors.New("rule stopped")
	optimizer := hatSql.NewSQLQueryOptimizer(
		func(*hatSql.SQLQueryOptimizationContext) error { return wantErr },
		func(*hatSql.SQLQueryOptimizationContext) error { return nil },
	)
	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), "SELECT * FROM VALUES (1)", nil, hatSql.SQLQueryOptions{
		Optimizer:      optimizer,
		OptimizerTrace: &hatSql.SQLOptimizerTraceOptions{MaxEntries: 1},
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want rule error", err)
	}
	if result.OptimizerTrace == nil || len(result.OptimizerTrace.Entries) != 1 || !result.OptimizerTrace.Truncated {
		t.Fatalf("bounded error trace = %#v", result.OptimizerTrace)
	}
	if result.OptimizerTrace.Entries[0].Status != "error" {
		t.Fatalf("error trace entry = %#v", result.OptimizerTrace.Entries[0])
	}
}

func TestM241OptimizerTraceIsDisabledByDefault(t *testing.T) {
	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), "SELECT * FROM VALUES (1)", nil, hatSql.SQLQueryOptions{})
	if err != nil {
		t.Fatalf("query error = %v", err)
	}
	if result.OptimizerTrace != nil {
		t.Fatalf("default optimizer trace = %#v, want nil", result.OptimizerTrace)
	}
}

func containsM241(value, fragment string) bool {
	for index := 0; index+len(fragment) <= len(value); index++ {
		if value[index:index+len(fragment)] == fragment {
			return true
		}
	}
	return false
}
