package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestCompiledSQLNativeDataflowMatchesCompositeDistinctRows(t *testing.T) {
	rows := []SQLRow{
		{"region": "eu", "tier": int64(1), "value": int64(20)},
		{"region": "eu", "tier": int64(1), "value": int64(-1)},
		{"region": "eu", "tier": int64(2), "value": int64(10)},
		{"region": nil, "tier": int64(1), "value": int64(5)},
		{"region": nil, "tier": int64(1), "value": nil},
		{"region": "", "tier": int64(0), "value": int64(7)},
		{"region": "", "tier": int64(0), "value": int64(8)},
		{"region": "us", "tier": int64(3), "value": int64(3)},
	}
	query := "FROM CACHE('items') AS src SELECT DISTINCT src.region AS region, src.tier AS tier WHERE src.value >= 0"
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
		t.Fatalf("execute ordinary composite DISTINCT query: %v", err)
	}
	actual, err := native.Execute(context.Background(), rows)
	if err != nil {
		t.Fatalf("execute native composite DISTINCT query: %v", err)
	}
	if !reflect.DeepEqual(actual, ordinary.Rows) {
		t.Fatalf("native rows = %#v, ordinary rows = %#v", actual, ordinary.Rows)
	}
}

func TestCompiledSQLNativeDataflowCompositeDistinctChecksContext(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT DISTINCT src.region AS region, src.tier AS tier")
	if err != nil {
		t.Fatalf("compile SQL: %v", err)
	}
	native, err := compiled.CompileNativeDataflow()
	if err != nil {
		t.Fatalf("compile native dataflow: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := native.Execute(canceled, []SQLRow{{"region": "eu", "tier": int64(1)}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled native execution error = %v, want %v", err, context.Canceled)
	}
}

func TestCompiledSQLNativeDataflowCompositeDistinctRejectsUnsupportedRuntimeKey(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT DISTINCT src.region AS region, src.tier AS tier")
	if err != nil {
		t.Fatalf("compile SQL: %v", err)
	}
	native, err := compiled.CompileNativeDataflow()
	if err != nil {
		t.Fatalf("compile native dataflow: %v", err)
	}
	if _, err := native.Execute(context.Background(), []SQLRow{{"region": []byte("eu"), "tier": int64(1)}}); !errors.Is(err, ErrSQLNativeDataflowUnsupported) {
		t.Fatalf("unsupported native composite DISTINCT key error = %v, want %v", err, ErrSQLNativeDataflowUnsupported)
	}
}
