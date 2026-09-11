package hatSql

import (
	"context"
	"reflect"
	"testing"
)

func TestCompiledSQLAutomaticNativeAggregateUsesSafePath(t *testing.T) {
	rows := []SQLRow{
		{"id": int64(1), "value": int64(2)},
		{"id": int64(2), "value": int64(3)},
		{"id": int64(3), "value": nil},
	}
	query := "FROM CACHE('items') AS src SELECT COUNT(*) AS total, SUM(src.value) AS total_value"
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	auto, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{
		Observer: SQLQueryObserverFunc(func(SQLQueryEvent) {}),
	})
	if err != nil {
		t.Fatalf("automatic aggregate: %v", err)
	}
	fallback, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{
		DisableNativeDataflow: true,
	})
	if err != nil {
		t.Fatalf("aggregate fallback: %v", err)
	}
	if !reflect.DeepEqual(auto.Rows, fallback.Rows) {
		t.Fatalf("automatic rows = %#v, fallback rows = %#v", auto.Rows, fallback.Rows)
	}
	if !m052qPlanHasNode(auto.Plan, "NATIVE DATAFLOW") {
		t.Fatalf("automatic plan = %#v, want NATIVE DATAFLOW", auto.Plan)
	}
}

func TestCompiledSQLAutomaticNativeDistinctUsesSafePath(t *testing.T) {
	rows := []SQLRow{
		{"id": int64(1), "category": int64(2)},
		{"id": int64(2), "category": int64(2)},
		{"id": int64(3), "category": int64(1)},
	}
	query := "FROM CACHE('items') AS src SELECT DISTINCT src.category"
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	auto, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{
		Observer: SQLQueryObserverFunc(func(SQLQueryEvent) {}),
	})
	if err != nil {
		t.Fatalf("automatic distinct: %v", err)
	}
	fallback, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{
		DisableNativeDataflow: true,
	})
	if err != nil {
		t.Fatalf("distinct fallback: %v", err)
	}
	if !reflect.DeepEqual(auto.Rows, fallback.Rows) {
		t.Fatalf("automatic rows = %#v, fallback rows = %#v", auto.Rows, fallback.Rows)
	}
	if !m052qPlanHasNode(auto.Plan, "NATIVE DATAFLOW") {
		t.Fatalf("automatic plan = %#v, want NATIVE DATAFLOW", auto.Plan)
	}
}

func TestCompiledSQLAutomaticNativeOperatorsKeepUnsupportedGroupedQueriesOnFallback(t *testing.T) {
	rows := []SQLRow{{"id": int64(1)}, {"id": int64(1)}}
	query := "FROM CACHE('items') AS src SELECT src.id, COUNT(*) AS total GROUP BY src.id HAVING total > 1"
	result, err := ExecuteSQLQueryContext(context.Background(), query, SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	}), SQLQueryOptions{Observer: SQLQueryObserverFunc(func(SQLQueryEvent) {})})
	if err != nil {
		t.Fatalf("grouped query: %v", err)
	}
	if m052qPlanHasNode(result.Plan, "NATIVE DATAFLOW") {
		t.Fatalf("grouped plan unexpectedly used automatic native dataflow: %#v", result.Plan)
	}
}

func TestCompiledSQLAutomaticNativeOperatorsHonorDisableOption(t *testing.T) {
	rows := []SQLRow{{"value": int64(1)}, {"value": int64(1)}}
	query := "FROM CACHE('items') AS src SELECT COUNT(*) AS total"
	result, err := ExecuteSQLQueryContext(context.Background(), query, SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	}), SQLQueryOptions{
		DisableNativeDataflow: true,
		Observer:              SQLQueryObserverFunc(func(SQLQueryEvent) {}),
	})
	if err != nil {
		t.Fatalf("disabled aggregate: %v", err)
	}
	if m052qPlanHasNode(result.Plan, "NATIVE DATAFLOW") {
		t.Fatalf("disabled plan unexpectedly used automatic native dataflow: %#v", result.Plan)
	}
}

func m052qPlanHasNode(plan []SQLExplainStep, node string) bool {
	for _, step := range plan {
		if step.Node == node {
			return true
		}
	}
	return false
}
