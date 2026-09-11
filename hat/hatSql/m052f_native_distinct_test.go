package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestCompiledSQLNativeDataflowMatchesDistinctRows(t *testing.T) {
	rows := []SQLRow{
		{"group": int64(2), "value": int64(20)},
		{"group": int64(1), "value": int64(10)},
		{"group": int64(2), "value": int64(-1)},
		{"group": nil, "value": int64(5)},
		{"group": nil, "value": nil},
		{"group": int64(3), "value": int64(-1)},
	}
	query := "FROM CACHE('items') AS src SELECT DISTINCT src.group AS bucket WHERE src.value >= 0"
	compiled, err := CompileSQLQuery(query)
	if err != nil {
		t.Fatalf("compile SQL: %v", err)
	}
	native, err := compiled.CompileNativeDataflow()
	if err != nil {
		t.Fatalf("compile native dataflow: %v", err)
	}
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	ordinary, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("execute ordinary DISTINCT query: %v", err)
	}
	actual, err := native.Execute(context.Background(), rows)
	if err != nil {
		t.Fatalf("execute native DISTINCT query: %v", err)
	}
	if !reflect.DeepEqual(actual, ordinary.Rows) {
		t.Fatalf("native rows = %#v, ordinary rows = %#v", actual, ordinary.Rows)
	}
}

func TestCompiledSQLNativeDataflowDistinctRejectsUnsupportedShapes(t *testing.T) {
	queries := []string{
		"FROM CACHE('items') AS src SELECT DISTINCT src.group AS bucket, src.value",
		"FROM CACHE('items') AS src SELECT DISTINCT src.group AS bucket ORDER BY bucket",
		"FROM CACHE('items') AS src SELECT DISTINCT LOWER(src.group) AS bucket",
	}
	for _, source := range queries {
		compiled, err := CompileSQLQuery(source)
		if err != nil {
			t.Fatalf("compile %q: %v", source, err)
		}
		if _, err := compiled.CompileNativeDataflow(); !errors.Is(err, ErrSQLNativeDataflowUnsupported) {
			t.Errorf("CompileNativeDataflow(%q) error = %v, want %v", source, err, ErrSQLNativeDataflowUnsupported)
		}
	}
}

func TestCompiledSQLNativeDataflowDistinctChecksContext(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT DISTINCT src.group AS bucket")
	if err != nil {
		t.Fatalf("compile SQL: %v", err)
	}
	native, err := compiled.CompileNativeDataflow()
	if err != nil {
		t.Fatalf("compile native dataflow: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := native.Execute(canceled, []SQLRow{{"group": int64(1)}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled native execution error = %v, want %v", err, context.Canceled)
	}
}

func TestCompiledSQLNativeDataflowDistinctRejectsUnsupportedRuntimeKey(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT DISTINCT src.group AS bucket")
	if err != nil {
		t.Fatalf("compile SQL: %v", err)
	}
	native, err := compiled.CompileNativeDataflow()
	if err != nil {
		t.Fatalf("compile native dataflow: %v", err)
	}
	if _, err := native.Execute(context.Background(), []SQLRow{{"group": "unsupported"}}); !errors.Is(err, ErrSQLNativeDataflowUnsupported) {
		t.Fatalf("unsupported native DISTINCT key error = %v, want %v", err, ErrSQLNativeDataflowUnsupported)
	}
}
