package hatSql

import (
	"context"
	"reflect"
	"testing"
)

func TestCompiledSQLAutomaticNativeDistinctLimitUsesSafePath(t *testing.T) {
	rows := []SQLRow{
		{"value": int64(0)},
		{"value": int64(0)},
		{"value": int64(1)},
		{"value": int64(2)},
		{"value": int64(2)},
		{"value": int64(3)},
		{"value": int64(4)},
	}
	query := "FROM CACHE('items') AS src SELECT DISTINCT src.value LIMIT 2 OFFSET 1"
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	var automaticEvent SQLQueryEvent
	automatic, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{
		Observer: SQLQueryObserverFunc(func(event SQLQueryEvent) {
			automaticEvent = event
		}),
	})
	if err != nil {
		t.Fatalf("automatic distinct limit query: %v", err)
	}
	fallback, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{DisableNativeDataflow: true})
	if err != nil {
		t.Fatalf("fallback distinct limit query: %v", err)
	}
	if !reflect.DeepEqual(automatic.Columns, fallback.Columns) || !reflect.DeepEqual(automatic.Rows, fallback.Rows) {
		t.Fatalf("automatic result = %#v, fallback = %#v", automatic, fallback)
	}
	if !sqlQueryEventHasOperator(automaticEvent, "NATIVE DATAFLOW") {
		t.Fatalf("automatic operators = %#v, want NATIVE DATAFLOW", automaticEvent.Operators)
	}
}

func TestCompiledSQLAutomaticNativeCompositeDistinctLimitUsesSafePath(t *testing.T) {
	rows := []SQLRow{
		{"region": "apac", "tier": int64(1)},
		{"region": "apac", "tier": int64(1)},
		{"region": "eu", "tier": int64(1)},
		{"region": "us", "tier": int64(2)},
	}
	query := "FROM CACHE('items') AS src SELECT DISTINCT src.region, src.tier LIMIT 1 OFFSET 1"
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	var automaticEvent SQLQueryEvent
	automatic, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{
		Observer: SQLQueryObserverFunc(func(event SQLQueryEvent) {
			automaticEvent = event
		}),
	})
	if err != nil {
		t.Fatalf("automatic composite distinct limit query: %v", err)
	}
	fallback, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{DisableNativeDataflow: true})
	if err != nil {
		t.Fatalf("fallback composite distinct limit query: %v", err)
	}
	if !reflect.DeepEqual(automatic.Columns, fallback.Columns) || !reflect.DeepEqual(automatic.Rows, fallback.Rows) {
		t.Fatalf("automatic composite result = %#v, fallback = %#v", automatic, fallback)
	}
	if !sqlQueryEventHasOperator(automaticEvent, "NATIVE DATAFLOW") {
		t.Fatalf("automatic composite operators = %#v, want NATIVE DATAFLOW", automaticEvent.Operators)
	}
}
