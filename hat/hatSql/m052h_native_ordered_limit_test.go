package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestCompiledSQLNativeDataflowMatchesOrderedLimitedRows(t *testing.T) {
	rows := []SQLRow{
		{"id": int64(1), "score": int64(10)},
		{"id": int64(2), "score": int64(30)},
		{"id": int64(3), "score": int64(20)},
		{"id": int64(4), "score": int64(30)},
		{"id": int64(5), "score": int64(5)},
		{"id": int64(6), "score": int64(40)},
		{"id": int64(7), "score": int64(30)},
	}
	query := "FROM CACHE('items') AS src SELECT src.id AS id, src.score AS score WHERE src.id >= 0 ORDER BY src.score DESC LIMIT 2 OFFSET 1"
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
		t.Fatalf("execute ordinary ordered query: %v", err)
	}
	actual, err := native.Execute(context.Background(), rows)
	if err != nil {
		t.Fatalf("execute native ordered query: %v", err)
	}
	if !reflect.DeepEqual(actual, ordinary.Rows) {
		t.Fatalf("native rows = %#v, ordinary rows = %#v", actual, ordinary.Rows)
	}
}

func TestCompiledSQLNativeDataflowOrderedLimitMatchesNullOrdering(t *testing.T) {
	rows := []SQLRow{
		{"id": int64(1), "score": nil},
		{"id": int64(2), "score": int64(-1)},
		{"id": int64(3), "score": int64(0)},
		{"id": int64(4), "score": nil},
		{"id": int64(5), "score": int64(2)},
	}
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.id AS id, src.score AS score ORDER BY src.score ASC LIMIT 4")
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
		t.Fatalf("execute ordinary NULL ordering query: %v", err)
	}
	actual, err := native.Execute(context.Background(), rows)
	if err != nil {
		t.Fatalf("execute native NULL ordering query: %v", err)
	}
	if !reflect.DeepEqual(actual, ordinary.Rows) {
		t.Fatalf("native rows = %#v, ordinary rows = %#v", actual, ordinary.Rows)
	}
}

func TestCompiledSQLNativeDataflowOrderedLimitChecksContext(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.id AS id ORDER BY src.id LIMIT 1")
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

func TestCompiledSQLNativeDataflowOrderedLimitRejectsUnsupportedShapes(t *testing.T) {
	queries := []string{
		"FROM CACHE('items') AS src SELECT src.id AS id ORDER BY src.id",
		"FROM CACHE('items') AS src SELECT src.id AS id ORDER BY src.id, src.score LIMIT 1",
		"FROM CACHE('items') AS src SELECT src.id AS id ORDER BY LOWER(src.id) LIMIT 1",
		"FROM CACHE('items') AS src SELECT src.score AS rank ORDER BY rank LIMIT 1",
		"FROM CACHE('items') AS src SELECT src.group AS bucket, COUNT(*) GROUP BY src.group ORDER BY src.group LIMIT 1",
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
