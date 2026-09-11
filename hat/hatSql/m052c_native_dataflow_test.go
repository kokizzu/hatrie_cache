package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestCompiledSQLNativeDataflowMatchesCompiledExecutor(t *testing.T) {
	rows := []SQLRow{
		{"id": int64(1), "value": nil},
		{"id": int64(2), "value": int64(10)},
		{"id": int64(2), "value": int64(20)},
		{"id": int64(3), "value": int64(5)},
	}
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.id, src.value WHERE src.value >= 10")
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
	if !reflect.DeepEqual(rows, []SQLRow{
		{"id": int64(1), "value": nil},
		{"id": int64(2), "value": int64(10)},
		{"id": int64(2), "value": int64(20)},
		{"id": int64(3), "value": int64(5)},
	}) {
		t.Fatalf("native execution mutated input rows: %#v", rows)
	}

	second, err := native.Execute(context.Background(), []SQLRow{{"id": int64(4), "value": int64(40)}})
	if err != nil {
		t.Fatalf("reuse native dataflow: %v", err)
	}
	if want := []SQLRow{{"id": int64(4), "value": int64(40)}}; !reflect.DeepEqual(second, want) {
		t.Fatalf("second native rows = %#v, want %#v", second, want)
	}
}

func TestCompiledSQLNativeDataflowMatchesScalarExpressions(t *testing.T) {
	rows := []SQLRow{
		{"id": int64(0)},
		{"id": int64(1)},
		{"id": int64(2)},
		{"id": int64(3)},
	}
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.id + 1 AS next WHERE src.id > 0 AND src.id < 3")
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
	want, err := compiled.Execute(context.Background(), SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	}), nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("execute compiled SQL: %v", err)
	}
	if !reflect.DeepEqual(got, want.Rows) {
		t.Fatalf("native rows = %#v, compiled rows = %#v", got, want.Rows)
	}
}

func TestCompiledSQLNativeDataflowRejectsUnsupportedPlans(t *testing.T) {
	queries := []string{
		"FROM CACHE('items') AS src SELECT COUNT(*)",
		"FROM CACHE('items') AS src SELECT src.id ORDER BY src.id",
		"FROM CACHE('items') AS src JOIN CACHE('other') AS other ON src.id = other.id SELECT src.id",
		"FROM CACHE('items') AS src SELECT LOWER(src.name)",
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

func TestCompiledSQLNativeDataflowPreservesScalarNullSemantics(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.id WHERE src.id / 0 > 1")
	if err != nil {
		t.Fatalf("CompileSQLQuery() error = %v", err)
	}
	native, err := compiled.CompileNativeDataflow()
	if err != nil {
		t.Fatalf("CompileNativeDataflow() error = %v", err)
	}
	nativeRows, err := native.Execute(context.Background(), []SQLRow{{"id": int64(1)}})
	if err != nil {
		t.Fatalf("native Execute() error = %v", err)
	}
	compiledResult, err := compiled.Execute(context.Background(), SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return []SQLRow{{"id": int64(1)}}, nil
	}), nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("compiled Execute() error = %v", err)
	}
	if !reflect.DeepEqual(nativeRows, compiledResult.Rows) {
		t.Fatalf("native rows = %#v, compiled rows = %#v", nativeRows, compiledResult.Rows)
	}
}

func TestCompiledSQLNativeDataflowChecksContextAndTypedInput(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.id WHERE src.id >= 1")
	if err != nil {
		t.Fatalf("compile SQL: %v", err)
	}
	native, err := compiled.CompileNativeDataflow()
	if err != nil {
		t.Fatalf("compile native dataflow: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := native.Execute(canceled, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled native execution error = %v, want %v", err, context.Canceled)
	}

	typedCompiled, err := CompileSQLQuery("FROM CACHE('items') AS src(id INTEGER) SELECT src.id")
	if err != nil {
		t.Fatalf("compile typed SQL: %v", err)
	}
	typedNative, err := typedCompiled.CompileNativeDataflow()
	if err != nil {
		t.Fatalf("compile typed native dataflow: %v", err)
	}
	if _, err := typedNative.Execute(context.Background(), []SQLRow{{"id": "wrong"}}); err == nil {
		t.Fatal("typed native execution accepted an invalid input value")
	}
}

func TestCompiledSQLNativeDataflowNilQuery(t *testing.T) {
	var compiled *CompiledSQLQuery
	if _, err := compiled.CompileNativeDataflow(); err == nil {
		t.Fatal("nil query compiled into native dataflow")
	}
}
