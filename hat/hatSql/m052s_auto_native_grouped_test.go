package hatSql

import (
	"context"
	"reflect"
	"testing"
)

func TestCompiledSQLAutomaticNativeGroupedUsesSafePath(t *testing.T) {
	rows := []SQLRow{
		{"id": int64(1), "value": int64(2)},
		{"id": int64(1), "value": int64(3)},
		{"id": int64(2), "value": nil},
		{"id": int64(2), "value": int64(5)},
	}
	query := "FROM CACHE('items') AS src SELECT src.id, COUNT(*) AS total, SUM(src.value) AS total_value GROUP BY src.id"
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	auto, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{
		Observer: SQLQueryObserverFunc(func(SQLQueryEvent) {}),
	})
	if err != nil {
		t.Fatalf("automatic grouped query: %v", err)
	}
	fallback, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{
		DisableNativeDataflow: true,
	})
	if err != nil {
		t.Fatalf("grouped fallback: %v", err)
	}
	if !reflect.DeepEqual(auto.Rows, fallback.Rows) {
		t.Fatalf("automatic rows = %#v, fallback rows = %#v", auto.Rows, fallback.Rows)
	}
	if !m052qPlanHasNode(auto.Plan, "NATIVE DATAFLOW") {
		t.Fatalf("automatic plan = %#v, want NATIVE DATAFLOW", auto.Plan)
	}
}

func TestCompiledSQLAutomaticNativeCompositeGroupedUsesSafePath(t *testing.T) {
	rows := []SQLRow{
		{"region": "apac", "tier": int64(1)},
		{"region": "apac", "tier": int64(1)},
		{"region": "apac", "tier": int64(2)},
	}
	query := "FROM CACHE('items') AS src SELECT src.region, src.tier, COUNT(*) AS total GROUP BY src.region, src.tier"
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	auto, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{
		Observer: SQLQueryObserverFunc(func(SQLQueryEvent) {}),
	})
	if err != nil {
		t.Fatalf("automatic composite grouped query: %v", err)
	}
	fallback, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{
		DisableNativeDataflow: true,
	})
	if err != nil {
		t.Fatalf("composite grouped fallback: %v", err)
	}
	if !reflect.DeepEqual(auto.Rows, fallback.Rows) {
		t.Fatalf("automatic composite rows = %#v, fallback rows = %#v", auto.Rows, fallback.Rows)
	}
	if !m052qPlanHasNode(auto.Plan, "NATIVE DATAFLOW") {
		t.Fatalf("automatic composite plan = %#v, want NATIVE DATAFLOW", auto.Plan)
	}
}
