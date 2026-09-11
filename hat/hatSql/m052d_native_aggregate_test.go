package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestCompiledSQLNativeDataflowMatchesGlobalAggregates(t *testing.T) {
	rows := []SQLRow{
		{"id": int64(0), "value": nil},
		{"id": int64(1), "value": int64(10)},
		{"id": int64(2), "value": int64(20)},
		{"id": int64(3), "value": nil},
	}
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT COUNT(*) AS total, COUNT(src.value) AS present, SUM(src.value) AS sum, AVG(src.value) AS average, MIN(src.value) AS minimum, MAX(src.value) AS maximum WHERE src.id >= 1")
	if err != nil {
		t.Fatalf("compile SQL: %v", err)
	}
	native, err := compiled.CompileNativeDataflow()
	if err != nil {
		t.Fatalf("compile native dataflow: %v", err)
	}
	got, err := native.Execute(context.Background(), rows)
	if err != nil {
		t.Fatalf("execute native dataflow: %v", err)
	}
	wantResult, err := compiled.Execute(context.Background(), SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	}), nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("execute compiled SQL: %v", err)
	}
	if !reflect.DeepEqual(got, wantResult.Rows) {
		t.Fatalf("native rows = %#v, compiled rows = %#v", got, wantResult.Rows)
	}

	empty, err := native.Execute(context.Background(), nil)
	if err != nil {
		t.Fatalf("execute empty native dataflow: %v", err)
	}
	emptyResult, err := compiled.Execute(context.Background(), SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return nil, nil
	}), nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("execute empty compiled SQL: %v", err)
	}
	if !reflect.DeepEqual(empty, emptyResult.Rows) {
		t.Fatalf("empty native rows = %#v, compiled rows = %#v", empty, emptyResult.Rows)
	}
}

func TestCompiledSQLNativeDataflowAggregateRejectsUnsupportedShapes(t *testing.T) {
	queries := []string{
		"FROM CACHE('items') AS src SELECT src.id, COUNT(*) AS total",
		"FROM CACHE('items') AS src SELECT src.id, COUNT(*) AS total GROUP BY src.id",
		"FROM CACHE('items') AS src SELECT COUNT(*) AS total ORDER BY total",
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

func TestCompiledSQLNativeDataflowAggregateChecksContext(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT COUNT(*) AS total")
	if err != nil {
		t.Fatalf("compile SQL: %v", err)
	}
	native, err := compiled.CompileNativeDataflow()
	if err != nil {
		t.Fatalf("compile native dataflow: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := native.Execute(canceled, []SQLRow{{"id": int64(1)}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled native execution error = %v, want %v", err, context.Canceled)
	}
}
