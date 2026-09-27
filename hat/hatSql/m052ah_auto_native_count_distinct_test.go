package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestM052AHAutoNativeCountDistinctMatchesFallback(t *testing.T) {
	rows := []SQLRow{
		{"group": "a", "value": int64(1)},
		{"group": "a", "value": int64(1)},
		{"group": "a", "value": int64(2)},
		{"group": "a", "value": nil},
		{"group": "b", "value": int64(2)},
		{"group": "b", "value": int64(3)},
		{"group": "b", "value": int64(3)},
	}
	query := "FROM CACHE('items') AS src SELECT src.group, COUNT(DISTINCT src.value) AS unique_count GROUP BY src.group"
	compiled, err := CompileSQLQuery(query)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	native, err := compiled.CompileNativeDataflow()
	if err != nil {
		t.Fatalf("compile native dataflow: %v", err)
	}
	nativeResult, err := native.Execute(context.Background(), rows)
	if err != nil {
		t.Fatalf("native dataflow: %v", err)
	}

	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	auto, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{
		Observer: SQLQueryObserverFunc(func(SQLQueryEvent) {}),
	})
	if err != nil {
		t.Fatalf("automatic native dataflow: %v", err)
	}
	fallback, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{
		DisableNativeDataflow: true,
	})
	if err != nil {
		t.Fatalf("fallback: %v", err)
	}
	if !reflect.DeepEqual(nativeResult, fallback.Rows) {
		t.Fatalf("native rows = %#v, fallback rows = %#v", nativeResult, fallback.Rows)
	}
	if !reflect.DeepEqual(auto.Rows, fallback.Rows) {
		t.Fatalf("automatic rows = %#v, fallback rows = %#v", auto.Rows, fallback.Rows)
	}
	if !m052qPlanHasNode(auto.Plan, "NATIVE DATAFLOW") {
		t.Fatalf("automatic plan = %#v, want NATIVE DATAFLOW", auto.Plan)
	}
}

func TestM052AHNativeCountDistinctPreservesStringAndNullKeys(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') SELECT COUNT(DISTINCT value) AS unique_count")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	native, err := compiled.CompileNativeDataflow()
	if err != nil {
		t.Fatalf("compile native dataflow: %v", err)
	}
	got, err := native.Execute(context.Background(), []SQLRow{
		{"value": "blue"},
		{"value": "blue"},
		{"value": "red"},
		{"value": nil},
	})
	if err != nil {
		t.Fatalf("native dataflow: %v", err)
	}
	want := []SQLRow{{"unique_count": int64(2)}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("native rows = %#v, want %#v", got, want)
	}
}

func TestM052AHNativeCountDistinctRejectsUnsupportedRuntimeKey(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') SELECT COUNT(DISTINCT value) AS unique_count")
	if err != nil {
		t.Fatalf("compile query: %v", err)
	}
	native, err := compiled.CompileNativeDataflow()
	if err != nil {
		t.Fatalf("compile native dataflow: %v", err)
	}
	_, err = native.Execute(context.Background(), []SQLRow{{"value": true}})
	if !errors.Is(err, ErrSQLNativeDataflowUnsupported) {
		t.Fatalf("native unsupported key error = %v, want %v", err, ErrSQLNativeDataflowUnsupported)
	}
}

func TestM052AHAutoNativeCountDistinctFallsBackForUnsupportedRuntimeKey(t *testing.T) {
	rows := []SQLRow{
		{"value": 1.5},
		{"value": 1.5},
		{"value": 2.5},
	}
	query := "FROM CACHE('items') AS src SELECT COUNT(DISTINCT src.value) AS unique_count"
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	auto, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{
		Observer: SQLQueryObserverFunc(func(SQLQueryEvent) {}),
	})
	if err != nil {
		t.Fatalf("automatic unsupported distinct: %v", err)
	}
	fallback, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{
		DisableNativeDataflow: true,
	})
	if err != nil {
		t.Fatalf("fallback unsupported distinct: %v", err)
	}
	if !reflect.DeepEqual(auto.Rows, fallback.Rows) {
		t.Fatalf("automatic rows = %#v, fallback rows = %#v", auto.Rows, fallback.Rows)
	}
	if m052qPlanHasNode(auto.Plan, "NATIVE DATAFLOW") {
		t.Fatalf("automatic plan unexpectedly used native dataflow: %#v", auto.Plan)
	}
}
