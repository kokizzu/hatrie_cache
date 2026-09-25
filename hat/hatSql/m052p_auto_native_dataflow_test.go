package hatSql

import (
	"context"
	"reflect"
	"testing"
)

func TestCompiledSQLAutomaticNativeDataflowUsesSafePath(t *testing.T) {
	rows := []SQLRow{
		{"id": int64(1), "value": int64(1)},
		{"id": int64(2), "value": int64(2)},
		{"id": int64(3), "value": int64(3)},
	}
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.id, src.value WHERE src.value >= 2")
	if err != nil {
		t.Fatalf("compile SQL: %v", err)
	}
	var event SQLQueryEvent
	result, err := compiled.Execute(context.Background(), SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	}), nil, SQLQueryOptions{
		Observer: SQLQueryObserverFunc(func(observed SQLQueryEvent) {
			event = observed
		}),
	})
	if err != nil {
		t.Fatalf("execute SQL: %v", err)
	}
	want := SQLQueryResult{
		Columns: []string{"id", "value"},
		Rows: []SQLRow{
			{"id": int64(2), "value": int64(2)},
			{"id": int64(3), "value": int64(3)},
		},
	}
	if !reflect.DeepEqual(result.Columns, want.Columns) || !reflect.DeepEqual(result.Rows, want.Rows) {
		t.Fatalf("automatic result = %#v, want %#v", result, want)
	}
	if !sqlQueryEventHasOperator(event, "NATIVE DATAFLOW") {
		t.Fatalf("automatic operators = %#v, want NATIVE DATAFLOW", event.Operators)
	}
}

func TestCompiledSQLAutomaticNativeDataflowFallbackPreservesResult(t *testing.T) {
	rows := []SQLRow{
		{"id": int64(1), "value": int64(1)},
		{"id": int64(2), "value": int64(2)},
	}
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.id, src.value WHERE src.value >= 2")
	if err != nil {
		t.Fatalf("compile SQL: %v", err)
	}
	var event SQLQueryEvent
	result, err := compiled.Execute(context.Background(), SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	}), nil, SQLQueryOptions{
		DisableNativeDataflow: true,
		Observer: SQLQueryObserverFunc(func(observed SQLQueryEvent) {
			event = observed
		}),
	})
	if err != nil {
		t.Fatalf("execute fallback SQL: %v", err)
	}
	wantRows := []SQLRow{{"id": int64(2), "value": int64(2)}}
	if !reflect.DeepEqual(result.Rows, wantRows) {
		t.Fatalf("fallback rows = %#v, want %#v", result.Rows, wantRows)
	}
	if sqlQueryEventHasOperator(event, "NATIVE DATAFLOW") {
		t.Fatalf("fallback operators = %#v, unexpectedly used NATIVE DATAFLOW", event.Operators)
	}
}

func TestCompiledSQLAutomaticNativeDataflowUsesGroupedSafePath(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.group, COUNT(*) AS total GROUP BY src.group")
	if err != nil {
		t.Fatalf("compile SQL: %v", err)
	}
	var event SQLQueryEvent
	result, err := compiled.Execute(context.Background(), SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return []SQLRow{
			{"group": int64(1)},
			{"group": int64(1)},
			{"group": int64(2)},
		}, nil
	}), nil, SQLQueryOptions{
		Observer: SQLQueryObserverFunc(func(observed SQLQueryEvent) {
			event = observed
		}),
	})
	if err != nil {
		t.Fatalf("execute grouped SQL: %v", err)
	}
	if len(result.Rows) != 2 || !sqlQueryEventHasOperator(event, "NATIVE DATAFLOW") {
		t.Fatalf("grouped result/operators = %#v / %#v, want NATIVE DATAFLOW", result.Rows, event.Operators)
	}
}

func TestCompiledSQLAutomaticNativeDataflowAppliesToUnionBranches(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.id, src.value WHERE src.value >= 2 UNION ALL FROM CACHE('items') AS src SELECT src.id, src.value WHERE src.value < 2")
	if err != nil {
		t.Fatalf("compile union SQL: %v", err)
	}
	var event SQLQueryEvent
	result, err := compiled.Execute(context.Background(), SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return []SQLRow{
			{"id": int64(1), "value": int64(1)},
			{"id": int64(2), "value": int64(2)},
		}, nil
	}), nil, SQLQueryOptions{
		Observer: SQLQueryObserverFunc(func(observed SQLQueryEvent) {
			event = observed
		}),
	})
	if err != nil {
		t.Fatalf("execute union SQL: %v", err)
	}
	want := []SQLRow{
		{"id": int64(2), "value": int64(2)},
		{"id": int64(1), "value": int64(1)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("union rows = %#v, want %#v", result.Rows, want)
	}
	nativeOperators := 0
	for _, operator := range event.Operators {
		if operator.Node == "NATIVE DATAFLOW" {
			nativeOperators++
		}
	}
	if nativeOperators != 2 {
		t.Fatalf("union native operators = %d, operators = %#v, want 2 native branches", nativeOperators, event.Operators)
	}

	event = SQLQueryEvent{}
	result, err = compiled.Execute(context.Background(), SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return []SQLRow{{"id": int64(1), "value": int64(1)}, {"id": int64(2), "value": int64(2)}}, nil
	}), nil, SQLQueryOptions{
		DisableNativeDataflow: true,
		Observer: SQLQueryObserverFunc(func(observed SQLQueryEvent) {
			event = observed
		}),
	})
	if err != nil {
		t.Fatalf("execute disabled union SQL: %v", err)
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("disabled union rows = %#v, want %#v", result.Rows, want)
	}
	if sqlQueryEventHasOperator(event, "NATIVE DATAFLOW") {
		t.Fatalf("disabled union operators = %#v, unexpectedly used NATIVE DATAFLOW", event.Operators)
	}
}

func TestCompiledSQLAutomaticNativeDataflowPreservesSetOperationSemantics(t *testing.T) {
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return []SQLRow{
			{"id": int64(1), "value": int64(1)},
			{"id": int64(2), "value": int64(2)},
			{"id": int64(3), "value": int64(3)},
		}, nil
	})
	tests := []struct {
		name  string
		query string
		want  []SQLRow
	}{
		{
			name:  "union",
			query: "FROM CACHE('items') AS src SELECT src.id WHERE src.value >= 2 UNION FROM CACHE('items') AS src SELECT src.id WHERE src.value >= 3",
			want:  []SQLRow{{"id": int64(2)}, {"id": int64(3)}},
		},
		{
			name:  "intersect all",
			query: "FROM CACHE('items') AS src SELECT src.id WHERE src.value >= 2 INTERSECT ALL FROM CACHE('items') AS src SELECT src.id WHERE src.value >= 3",
			want:  []SQLRow{{"id": int64(3)}},
		},
		{
			name:  "except all",
			query: "FROM CACHE('items') AS src SELECT src.id WHERE src.value >= 2 EXCEPT ALL FROM CACHE('items') AS src SELECT src.id WHERE src.value >= 3",
			want:  []SQLRow{{"id": int64(2)}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := ExecuteSQLQueryContext(context.Background(), test.query, resolver, SQLQueryOptions{})
			if err != nil {
				t.Fatalf("execute %s: %v", test.name, err)
			}
			if !reflect.DeepEqual(result.Rows, test.want) {
				t.Fatalf("%s rows = %#v, want %#v", test.name, result.Rows, test.want)
			}
		})
	}
}

func TestCompiledSQLAutomaticNativeDataflowHonorsResultBudgets(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.id, src.value WHERE src.value >= 1")
	if err != nil {
		t.Fatalf("compile SQL: %v", err)
	}
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return []SQLRow{{"id": int64(1), "value": int64(1)}, {"id": int64(2), "value": int64(2)}}, nil
	})
	if _, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{MaxRows: 1}); err == nil {
		t.Fatal("MaxRows unexpectedly allowed an oversized automatic source")
	}
	if _, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{MaxResultBytes: 1}); err == nil {
		t.Fatal("MaxResultBytes unexpectedly allowed an oversized automatic result")
	}
}

func TestCompiledSQLAutomaticNativeDataflowPreservesEmptyResultShape(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.id WHERE src.id > 10")
	if err != nil {
		t.Fatalf("compile SQL: %v", err)
	}
	for name, rows := range map[string][]SQLRow{
		"nil source":   nil,
		"no matches":   {{"id": int64(1)}},
		"empty source": {},
	} {
		t.Run(name, func(t *testing.T) {
			resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
				return rows, nil
			})
			automatic, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{})
			if err != nil {
				t.Fatalf("execute automatic SQL: %v", err)
			}
			fallback, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{DisableNativeDataflow: true})
			if err != nil {
				t.Fatalf("execute fallback SQL: %v", err)
			}
			if !reflect.DeepEqual(automatic.Columns, fallback.Columns) || !reflect.DeepEqual(automatic.Rows, fallback.Rows) {
				t.Fatalf("automatic result = %#v, fallback = %#v", automatic, fallback)
			}
		})
	}
}

func sqlQueryEventHasOperator(event SQLQueryEvent, node string) bool {
	for _, operator := range event.Operators {
		if operator.Node == node {
			return true
		}
	}
	return false
}
