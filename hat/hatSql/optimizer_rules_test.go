package hatSql_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

type optimizerRuleResolver struct {
	indexed  int
	resolved int
}

func (resolver *optimizerRuleResolver) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
	if name != "CACHE" || key != "orders" {
		return nil, fmt.Errorf("unexpected source %s(%q)", name, key)
	}
	resolver.resolved++
	return []hatSql.Row{{"id": 1, "region": "us"}}, nil
}

func (resolver *optimizerRuleResolver) ResolveSQLIndexedSource(name, key, field string, value interface{}) ([]hatSql.Row, bool, error) {
	if name != "CACHE" || key != "orders" || field != "id" {
		return nil, false, nil
	}
	resolver.indexed++
	return []hatSql.Row{{"id": 1, "region": "us"}}, true, nil
}

func TestOptimizerRuleCanSelectIndex(t *testing.T) {
	resolver := &optimizerRuleResolver{}
	called := 0
	query := "FROM CACHE('orders') AS o WHERE o.id = 1 SELECT o.id"
	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, hatSql.SQLQueryOptions{
		Optimizer: hatSql.NewSQLQueryOptimizer(
			func(plan *hatSql.SQLQueryOptimizationContext) error {
				called++
				if plan.Source != query || len(plan.Plan) == 0 {
					return fmt.Errorf("unexpected optimizer context: %#v", plan)
				}
				plan.IndexHint = hatSql.SQLIndexHint{Source: "o", Field: "id", Mode: hatSql.SQLIndexHintForce}
				return nil
			},
		),
	})
	if err != nil {
		t.Fatal(err)
	}
	if called != 1 {
		t.Fatalf("optimizer rule calls = %d, want 1", called)
	}
	if resolver.indexed != 2 {
		t.Fatalf("indexed calls = %d, want 2 (validation and execution)", resolver.indexed)
	}
	want := hatSql.SQLQueryResult{Columns: []string{"id"}, Rows: []hatSql.Row{{"id": 1}}}
	if !reflect.DeepEqual(result.Columns, want.Columns) || !reflect.DeepEqual(result.Rows, want.Rows) {
		t.Fatalf("result = %#v, want %#v", result, want)
	}
}

func TestOptimizerRuleErrorPreventsSourceAccess(t *testing.T) {
	resolver := &optimizerRuleResolver{}
	_, err := hatSql.ExecuteSQLQueryContext(context.Background(), "FROM CACHE('orders') AS o SELECT o.id", resolver, hatSql.SQLQueryOptions{
		Optimizer: hatSql.NewSQLQueryOptimizer(func(*hatSql.SQLQueryOptimizationContext) error {
			return fmt.Errorf("rule rejected query")
		}),
	})
	if err == nil || err.Error() != "SQL optimizer rule 1: rule rejected query" {
		t.Fatalf("error = %v, want optimizer rule error", err)
	}
	if resolver.resolved != 0 || resolver.indexed != 0 {
		t.Fatalf("resolver calls = (%d resolved, %d indexed), want no source access", resolver.resolved, resolver.indexed)
	}
}

func TestOptimizerRuleOutputIsValidated(t *testing.T) {
	_, err := hatSql.ExecuteSQLQueryContext(context.Background(), "FROM CACHE('orders') AS o SELECT o.id", &optimizerRuleResolver{}, hatSql.SQLQueryOptions{
		Optimizer: hatSql.NewSQLQueryOptimizer(func(plan *hatSql.SQLQueryOptimizationContext) error {
			plan.IndexHint = hatSql.SQLIndexHint{Field: "id"}
			return nil
		}),
	})
	if err == nil || err.Error() != "SQL optimizer rule output: SQL index hint mode must be FORCE or FORBID" {
		t.Fatalf("error = %v, want invalid rule output", err)
	}
}

func TestExplicitIndexHintWinsOverOptimizerRule(t *testing.T) {
	resolver := &optimizerRuleResolver{}
	query := "FROM CACHE('orders') AS o WHERE o.id = 1 SELECT o.id"
	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, hatSql.SQLQueryOptions{
		IndexHint: hatSql.SQLIndexHint{Source: "o", Field: "id", Mode: hatSql.SQLIndexHintForce},
		Optimizer: hatSql.NewSQLQueryOptimizer(func(plan *hatSql.SQLQueryOptimizationContext) error {
			plan.IndexHint = hatSql.SQLIndexHint{Source: "o", Field: "missing", Mode: hatSql.SQLIndexHintForce}
			return nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Rows, []hatSql.Row{{"id": 1}}) || resolver.indexed != 2 {
		t.Fatalf("result = %#v, indexed calls = %d, want id result and two id lookups", result.Rows, resolver.indexed)
	}
}
