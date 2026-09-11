package hatSql

import (
	"context"
	"reflect"
	"testing"
)

func TestCompiledSQLAutomaticNativeAggregateLimitUsesSafePath(t *testing.T) {
	rows := []SQLRow{
		{"value": int64(1)},
		{"value": int64(2)},
		{"value": int64(3)},
	}
	query := "FROM CACHE('items') AS src SELECT COUNT(*) AS total, SUM(src.value) AS total_value LIMIT 1"
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
		t.Fatalf("automatic aggregate limit query: %v", err)
	}
	fallback, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{DisableNativeDataflow: true})
	if err != nil {
		t.Fatalf("fallback aggregate limit query: %v", err)
	}
	if !reflect.DeepEqual(automatic.Columns, fallback.Columns) || !reflect.DeepEqual(automatic.Rows, fallback.Rows) {
		t.Fatalf("automatic result = %#v, fallback = %#v", automatic, fallback)
	}
	if !sqlQueryEventHasOperator(automaticEvent, "NATIVE DATAFLOW") {
		t.Fatalf("automatic operators = %#v, want NATIVE DATAFLOW", automaticEvent.Operators)
	}
}

func TestCompiledSQLAutomaticNativeAggregateOffsetPreservesEmptyResult(t *testing.T) {
	query := "FROM CACHE('items') AS src SELECT COUNT(*) AS total LIMIT 1 OFFSET 1"
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return []SQLRow{{"value": int64(1)}}, nil
	})
	var automaticEvent SQLQueryEvent
	automatic, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{
		Observer: SQLQueryObserverFunc(func(event SQLQueryEvent) {
			automaticEvent = event
		}),
	})
	if err != nil {
		t.Fatalf("automatic aggregate offset query: %v", err)
	}
	fallback, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{DisableNativeDataflow: true})
	if err != nil {
		t.Fatalf("fallback aggregate offset query: %v", err)
	}
	if !reflect.DeepEqual(automatic.Columns, fallback.Columns) || !reflect.DeepEqual(automatic.Rows, fallback.Rows) {
		t.Fatalf("automatic offset result = %#v, fallback = %#v", automatic, fallback)
	}
	if !sqlQueryEventHasOperator(automaticEvent, "NATIVE DATAFLOW") {
		t.Fatalf("automatic offset operators = %#v, want NATIVE DATAFLOW", automaticEvent.Operators)
	}
}
