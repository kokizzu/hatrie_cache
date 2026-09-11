package hatSql

import (
	"context"
	"reflect"
	"testing"
)

func TestCompiledSQLAutomaticNativeOrderedUsesSafePath(t *testing.T) {
	rows := []SQLRow{
		{"id": int64(1), "score": int64(10)},
		{"id": int64(2), "score": int64(40)},
		{"id": int64(3), "score": int64(30)},
		{"id": int64(4), "score": int64(20)},
	}
	query := "FROM CACHE('items') AS src SELECT src.id, src.score ORDER BY src.score DESC LIMIT 2 OFFSET 1"
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	auto, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{
		Observer: SQLQueryObserverFunc(func(SQLQueryEvent) {}),
	})
	if err != nil {
		t.Fatalf("automatic ordered query: %v", err)
	}
	fallback, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{
		DisableNativeDataflow: true,
	})
	if err != nil {
		t.Fatalf("ordered fallback: %v", err)
	}
	if !reflect.DeepEqual(auto.Rows, fallback.Rows) {
		t.Fatalf("automatic rows = %#v, fallback rows = %#v", auto.Rows, fallback.Rows)
	}
	if !m052qPlanHasNode(auto.Plan, "NATIVE DATAFLOW") {
		t.Fatalf("automatic plan = %#v, want NATIVE DATAFLOW", auto.Plan)
	}
}

func TestCompiledSQLAutomaticNativeOrderedPreservesCompositeTies(t *testing.T) {
	rows := []SQLRow{
		{"id": int64(3), "score": int64(20)},
		{"id": int64(1), "score": int64(20)},
		{"id": int64(4), "score": int64(10)},
		{"id": int64(2), "score": int64(20)},
	}
	query := "FROM CACHE('items') AS src SELECT src.id, src.score ORDER BY src.score DESC, src.id ASC LIMIT 2 OFFSET 1"
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	auto, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{
		Observer: SQLQueryObserverFunc(func(SQLQueryEvent) {}),
	})
	if err != nil {
		t.Fatalf("automatic composite ordered query: %v", err)
	}
	fallback, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{
		DisableNativeDataflow: true,
	})
	if err != nil {
		t.Fatalf("composite ordered fallback: %v", err)
	}
	if !reflect.DeepEqual(auto.Rows, fallback.Rows) {
		t.Fatalf("automatic rows = %#v, fallback rows = %#v", auto.Rows, fallback.Rows)
	}
	if !m052qPlanHasNode(auto.Plan, "NATIVE DATAFLOW") {
		t.Fatalf("automatic composite plan = %#v, want NATIVE DATAFLOW", auto.Plan)
	}
}

func TestCompiledSQLAutomaticNativeOrderedKeepsUnboundedOrderOnFallback(t *testing.T) {
	rows := []SQLRow{{"id": int64(1), "score": int64(10)}, {"id": int64(2), "score": int64(20)}}
	query := "FROM CACHE('items') AS src SELECT src.id ORDER BY src.score DESC"
	result, err := ExecuteSQLQueryContext(context.Background(), query, SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	}), SQLQueryOptions{Observer: SQLQueryObserverFunc(func(SQLQueryEvent) {})})
	if err != nil {
		t.Fatalf("unbounded ordered query: %v", err)
	}
	if m052qPlanHasNode(result.Plan, "NATIVE DATAFLOW") {
		t.Fatalf("unbounded plan unexpectedly used automatic native dataflow: %#v", result.Plan)
	}
}

func TestCompiledSQLAutomaticNativeOrderedHonorsDisableOption(t *testing.T) {
	rows := []SQLRow{{"id": int64(1), "score": int64(10)}, {"id": int64(2), "score": int64(20)}}
	query := "FROM CACHE('items') AS src SELECT src.id ORDER BY src.score DESC LIMIT 1"
	result, err := ExecuteSQLQueryContext(context.Background(), query, SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	}), SQLQueryOptions{
		DisableNativeDataflow: true,
		Observer:              SQLQueryObserverFunc(func(SQLQueryEvent) {}),
	})
	if err != nil {
		t.Fatalf("disabled ordered query: %v", err)
	}
	if m052qPlanHasNode(result.Plan, "NATIVE DATAFLOW") {
		t.Fatalf("disabled plan unexpectedly used automatic native dataflow: %#v", result.Plan)
	}
}
