package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestCompiledSQLNativeDataflowMatchesStringDistinctRows(t *testing.T) {
	rows := []SQLRow{
		{"region": "eu", "value": int64(20)},
		{"region": "apac", "value": int64(-1)},
		{"region": "eu", "value": int64(10)},
		{"region": nil, "value": int64(5)},
		{"region": nil, "value": nil},
		{"region": "", "value": int64(7)},
		{"region": "", "value": int64(8)},
		{"region": "us", "value": int64(3)},
	}
	query := "FROM CACHE('items') AS src SELECT DISTINCT src.region AS region WHERE src.value >= 0"
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

func TestCompiledSQLNativeDataflowStringDistinctChecksContext(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT DISTINCT src.region AS region")
	if err != nil {
		t.Fatalf("compile SQL: %v", err)
	}
	native, err := compiled.CompileNativeDataflow()
	if err != nil {
		t.Fatalf("compile native dataflow: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := native.Execute(canceled, []SQLRow{{"region": "eu"}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled native execution error = %v, want %v", err, context.Canceled)
	}
}
