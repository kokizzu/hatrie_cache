package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestCompiledSQLNativeDataflowMatchesLimitedRows(t *testing.T) {
	rows := []SQLRow{
		{"id": int64(1), "value": int64(-1)},
		{"id": int64(2), "value": int64(10)},
		{"id": int64(3), "value": int64(20)},
		{"id": int64(4), "value": int64(-1)},
		{"id": int64(5), "value": int64(30)},
		{"id": int64(6), "value": int64(40)},
		{"id": int64(7), "value": int64(50)},
	}
	query := "FROM CACHE('items') AS src SELECT src.id AS id, src.value AS value WHERE src.value >= 0 LIMIT 3 OFFSET 1"
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
		t.Fatalf("execute ordinary limited query: %v", err)
	}
	actual, err := native.Execute(context.Background(), rows)
	if err != nil {
		t.Fatalf("execute native limited query: %v", err)
	}
	if !reflect.DeepEqual(actual, ordinary.Rows) {
		t.Fatalf("native rows = %#v, ordinary rows = %#v", actual, ordinary.Rows)
	}
}

func TestCompiledSQLNativeDataflowLimitChecksContext(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.id AS id LIMIT 1")
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

func TestCompiledSQLNativeDataflowLimitZeroReturnsEmpty(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.id AS id LIMIT 0")
	if err != nil {
		t.Fatalf("compile SQL: %v", err)
	}
	native, err := compiled.CompileNativeDataflow()
	if err != nil {
		t.Fatalf("compile native dataflow: %v", err)
	}
	actual, err := native.Execute(context.Background(), []SQLRow{{"id": int64(1)}})
	if err != nil {
		t.Fatalf("execute native LIMIT 0 query: %v", err)
	}
	if len(actual) != 0 {
		t.Fatalf("native LIMIT 0 rows = %#v, want empty", actual)
	}
}

func TestCompiledSQLNativeDataflowLimitRejectsGroupedShape(t *testing.T) {
	queries := []string{
		"FROM CACHE('items') AS src SELECT src.group AS bucket, COUNT(*) GROUP BY src.group LIMIT 1",
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
