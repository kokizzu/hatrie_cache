package hatSql

import (
	"context"
	"reflect"
	"testing"
)

func TestCompiledSQLAutomaticNativeCompositeGroupedOrderedUsesSafePath(t *testing.T) {
	rows := []SQLRow{
		{"region": "apac", "tier": int64(1)},
		{"region": "apac", "tier": int64(1)},
		{"region": "eu", "tier": int64(2)},
		{"region": "eu", "tier": int64(2)},
		{"region": "us", "tier": int64(3)},
	}
	query := "FROM CACHE('items') AS src SELECT src.region, src.tier, COUNT(*) AS total GROUP BY src.region, src.tier HAVING COUNT(*) > 1 ORDER BY total DESC, region ASC, tier ASC LIMIT 2"
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
		t.Fatalf("automatic composite grouped ordered query: %v", err)
	}
	fallback, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{DisableNativeDataflow: true})
	if err != nil {
		t.Fatalf("fallback composite grouped ordered query: %v", err)
	}
	if !reflect.DeepEqual(automatic.Columns, fallback.Columns) || !reflect.DeepEqual(automatic.Rows, fallback.Rows) {
		t.Fatalf("automatic result = %#v, fallback = %#v", automatic, fallback)
	}
	if !sqlQueryEventHasOperator(automaticEvent, "NATIVE DATAFLOW") {
		t.Fatalf("automatic operators = %#v, want NATIVE DATAFLOW", automaticEvent.Operators)
	}
}

func TestCompiledSQLAutomaticNativeCompositeGroupedQualifiedOrderKeepsFallback(t *testing.T) {
	query := "FROM CACHE('items') AS src SELECT src.region, src.tier, COUNT(*) AS total GROUP BY src.region, src.tier ORDER BY src.region ASC LIMIT 1"
	var event SQLQueryEvent
	_, err := ExecuteSQLQueryContext(context.Background(), query, SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return []SQLRow{{"region": "apac", "tier": int64(1)}}, nil
	}), SQLQueryOptions{Observer: SQLQueryObserverFunc(func(observed SQLQueryEvent) {
		event = observed
	})})
	if err != nil {
		t.Fatalf("unsupported composite grouped ordered query: %v", err)
	}
	if sqlQueryEventHasOperator(event, "NATIVE DATAFLOW") {
		t.Fatalf("qualified order operators = %#v, unexpectedly used NATIVE DATAFLOW", event.Operators)
	}
}
