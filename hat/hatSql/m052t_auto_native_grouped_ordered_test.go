package hatSql

import (
	"context"
	"reflect"
	"testing"
)

func TestCompiledSQLAutomaticNativeGroupedOrderedUsesSafePath(t *testing.T) {
	rows := []SQLRow{
		{"id": int64(1)},
		{"id": int64(1)},
		{"id": int64(1)},
		{"id": int64(2)},
		{"id": int64(2)},
		{"id": int64(3)},
	}
	query := "FROM CACHE('items') AS src SELECT src.id, COUNT(*) AS total GROUP BY src.id HAVING COUNT(*) > 1 ORDER BY total DESC, id ASC LIMIT 2"
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
		t.Fatalf("automatic grouped ordered query: %v", err)
	}
	fallback, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{DisableNativeDataflow: true})
	if err != nil {
		t.Fatalf("fallback grouped ordered query: %v", err)
	}
	if !reflect.DeepEqual(automatic.Columns, fallback.Columns) || !reflect.DeepEqual(automatic.Rows, fallback.Rows) {
		t.Fatalf("automatic result = %#v, fallback = %#v", automatic, fallback)
	}
	if !sqlQueryEventHasOperator(automaticEvent, "NATIVE DATAFLOW") {
		t.Fatalf("automatic operators = %#v, want NATIVE DATAFLOW", automaticEvent.Operators)
	}
}

func TestCompiledSQLAutomaticNativeGroupedOrderedKeepsUnsupportedOrderOnFallback(t *testing.T) {
	rows := []SQLRow{
		{"id": int64(1)},
		{"id": int64(1)},
		{"id": int64(2)},
	}
	query := "FROM CACHE('items') AS src SELECT src.id, COUNT(*) AS total GROUP BY src.id ORDER BY src.id DESC LIMIT 1"
	var event SQLQueryEvent
	result, err := ExecuteSQLQueryContext(context.Background(), query, SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	}), SQLQueryOptions{Observer: SQLQueryObserverFunc(func(observed SQLQueryEvent) {
		event = observed
	})})
	if err != nil {
		t.Fatalf("unsupported grouped ordered query: %v", err)
	}
	if len(result.Rows) != 1 || sqlQueryEventHasOperator(event, "NATIVE DATAFLOW") {
		t.Fatalf("result/operators = %#v / %#v, want ordinary grouped ordered execution", result.Rows, event.Operators)
	}
}
