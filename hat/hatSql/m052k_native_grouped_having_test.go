package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestCompiledSQLNativeDataflowMatchesGroupedHavingOrderedLimitedRows(t *testing.T) {
	rows := []SQLRow{
		{"group": int64(2), "value": int64(20)},
		{"group": int64(1), "value": int64(10)},
		{"group": int64(2), "value": nil},
		{"group": nil, "value": int64(5)},
		{"group": int64(1), "value": int64(30)},
		{"group": nil, "value": nil},
		{"group": int64(3), "value": int64(7)},
	}
	query := "FROM CACHE('items') AS src SELECT src.group AS bucket, COUNT(*) AS total, SUM(src.value) AS total_value GROUP BY src.group HAVING COUNT(*) >= 2 ORDER BY total DESC, bucket ASC LIMIT 2 OFFSET 0"
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
		t.Fatalf("execute ordinary grouped HAVING query: %v", err)
	}
	actual, err := native.Execute(context.Background(), rows)
	if err != nil {
		t.Fatalf("execute native grouped HAVING query: %v", err)
	}
	if !reflect.DeepEqual(actual, ordinary.Rows) {
		t.Fatalf("native rows = %#v, ordinary rows = %#v", actual, ordinary.Rows)
	}
}

func TestCompiledSQLNativeDataflowGroupedHavingOrderedLimitChecksContext(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.group AS bucket, COUNT(*) AS total GROUP BY src.group HAVING COUNT(*) > 0 ORDER BY total DESC LIMIT 1")
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

func TestCompiledSQLNativeDataflowGroupedHavingRejectsUnsupportedShapes(t *testing.T) {
	queries := []string{
		"FROM CACHE('items') AS src SELECT src.group AS bucket, COUNT(*) AS total GROUP BY src.group HAVING total > 0 ORDER BY total DESC LIMIT 1",
		"FROM CACHE('items') AS src SELECT src.group AS bucket, COUNT(*) AS total GROUP BY src.group HAVING src.group > 0 ORDER BY total DESC LIMIT 1",
		"FROM CACHE('items') AS src SELECT src.group AS bucket, COUNT(*) AS total GROUP BY src.group HAVING LOWER(bucket) = 'x' ORDER BY total DESC LIMIT 1",
		"FROM CACHE('items') AS src SELECT src.group AS bucket, COUNT(*) AS total GROUP BY src.group HAVING missing > 0 ORDER BY total DESC LIMIT 1",
		"FROM CACHE('items') AS src SELECT src.group AS bucket, COUNT(*) AS total GROUP BY src.group HAVING SUM(src.value) > 0 ORDER BY total DESC LIMIT 1",
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
