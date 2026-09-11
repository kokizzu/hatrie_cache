package hatSql

import (
	"context"
	"reflect"
	"testing"
)

func TestCompiledSQLAutomaticNativeScalarLimitUsesSafePath(t *testing.T) {
	rows := make([]SQLRow, 256)
	for index := range rows {
		rows[index] = SQLRow{"id": int64(index), "value": int64(index)}
	}
	query := "FROM CACHE('items') AS src SELECT src.id, src.value WHERE src.value >= 0 LIMIT 8 OFFSET 120"
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
		t.Fatalf("automatic scalar limit query: %v", err)
	}
	fallback, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{DisableNativeDataflow: true})
	if err != nil {
		t.Fatalf("fallback scalar limit query: %v", err)
	}
	if !reflect.DeepEqual(automatic.Columns, fallback.Columns) || !reflect.DeepEqual(automatic.Rows, fallback.Rows) {
		t.Fatalf("automatic result = %#v, fallback = %#v", automatic, fallback)
	}
	if !sqlQueryEventHasOperator(automaticEvent, "NATIVE DATAFLOW") {
		t.Fatalf("automatic operators = %#v, want NATIVE DATAFLOW", automaticEvent.Operators)
	}
}

func TestCompiledSQLAutomaticNativeScalarLimitWithTiesKeepsFallback(t *testing.T) {
	query := "FROM CACHE('items') AS src SELECT src.id, src.value ORDER BY src.value LIMIT 1 WITH TIES"
	var event SQLQueryEvent
	_, err := ExecuteSQLQueryContext(context.Background(), query, SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return []SQLRow{{"id": int64(1), "value": int64(1)}, {"id": int64(2), "value": int64(1)}}, nil
	}), SQLQueryOptions{Observer: SQLQueryObserverFunc(func(observed SQLQueryEvent) {
		event = observed
	})})
	if err != nil {
		t.Fatalf("scalar limit with ties query: %v", err)
	}
	if sqlQueryEventHasOperator(event, "NATIVE DATAFLOW") {
		t.Fatalf("limit with ties operators = %#v, unexpectedly used NATIVE DATAFLOW", event.Operators)
	}
}
