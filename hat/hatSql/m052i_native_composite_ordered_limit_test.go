package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestCompiledSQLNativeDataflowMatchesCompositeOrderedLimitedRows(t *testing.T) {
	rows := []SQLRow{
		{"id": int64(1), "region": int64(2), "score": int64(10)},
		{"id": int64(2), "region": int64(1), "score": int64(30)},
		{"id": int64(3), "region": int64(1), "score": int64(20)},
		{"id": int64(4), "region": int64(2), "score": int64(30)},
		{"id": int64(5), "region": int64(1), "score": int64(30)},
		{"id": int64(6), "region": int64(2), "score": int64(40)},
		{"id": int64(7), "region": int64(1), "score": nil},
	}
	query := "FROM CACHE('items') AS src SELECT src.id AS id, src.region AS region, src.score AS score ORDER BY src.region ASC, src.score DESC LIMIT 3 OFFSET 1"
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
		t.Fatalf("execute ordinary composite ordered query: %v", err)
	}
	actual, err := native.Execute(context.Background(), rows)
	if err != nil {
		t.Fatalf("execute native composite ordered query: %v", err)
	}
	if !reflect.DeepEqual(actual, ordinary.Rows) {
		t.Fatalf("native rows = %#v, ordinary rows = %#v", actual, ordinary.Rows)
	}
}

func TestCompiledSQLNativeDataflowCompositeOrderedLimitChecksContext(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.id AS id ORDER BY src.region ASC, src.id DESC LIMIT 1")
	if err != nil {
		t.Fatalf("compile SQL: %v", err)
	}
	native, err := compiled.CompileNativeDataflow()
	if err != nil {
		t.Fatalf("compile native dataflow: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := native.Execute(canceled, []SQLRow{{"id": int64(1), "region": int64(1)}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled native execution error = %v, want %v", err, context.Canceled)
	}
}

func TestCompiledSQLNativeDataflowCompositeOrderedLimitRejectsUnsupportedShapes(t *testing.T) {
	queries := []string{
		"FROM CACHE('items') AS src SELECT src.id AS id ORDER BY src.id, LOWER(src.region) LIMIT 1",
		"FROM CACHE('items') AS src SELECT src.region AS bucket, COUNT(*) GROUP BY src.region ORDER BY src.region, src.bucket LIMIT 1",
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
