package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestCompiledSQLNativeDataflowMatchesGroupedAggregates(t *testing.T) {
	rows := []SQLRow{
		{"group": int64(2), "value": int64(20)},
		{"group": int64(1), "value": int64(10)},
		{"group": int64(2), "value": nil},
		{"group": nil, "value": int64(5)},
		{"group": int64(1), "value": int64(30)},
		{"group": nil, "value": nil},
	}
	query := "FROM CACHE('items') AS src SELECT src.group AS bucket, COUNT(*) AS total, COUNT(src.value) AS present, SUM(src.value) AS sum, AVG(src.value) AS average, MIN(src.value) AS minimum, MAX(src.value) AS maximum GROUP BY src.group"
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
		t.Fatalf("execute ordinary grouped query: %v", err)
	}
	actual, err := native.Execute(context.Background(), rows)
	if err != nil {
		t.Fatalf("execute native grouped query: %v", err)
	}
	if !reflect.DeepEqual(actual, ordinary.Rows) {
		t.Fatalf("native rows = %#v, ordinary rows = %#v", actual, ordinary.Rows)
	}
}

func TestCompiledSQLNativeDataflowGroupRejectsUnsupportedShapes(t *testing.T) {
	queries := []string{
		"FROM CACHE('items') AS src SELECT src.group AS bucket, COUNT(*) AS total GROUP BY src.group ORDER BY bucket",
		"FROM CACHE('items') AS src SELECT src.group, src.value, COUNT(*) AS total GROUP BY src.group",
		"FROM CACHE('items') AS src SELECT src.group AS bucket, COUNT(*) AS total GROUP BY src.group HAVING total > 1",
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

func TestCompiledSQLNativeDataflowGroupChecksContext(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.group, COUNT(*) AS total GROUP BY src.group")
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

func TestCompiledSQLNativeDataflowGroupRejectsUnsupportedRuntimeKey(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.group, COUNT(*) AS total GROUP BY src.group")
	if err != nil {
		t.Fatalf("compile SQL: %v", err)
	}
	native, err := compiled.CompileNativeDataflow()
	if err != nil {
		t.Fatalf("compile native dataflow: %v", err)
	}
	if _, err := native.Execute(context.Background(), []SQLRow{{"group": struct{}{}}}); !errors.Is(err, ErrSQLNativeDataflowUnsupported) {
		t.Fatalf("unsupported native group key error = %v, want %v", err, ErrSQLNativeDataflowUnsupported)
	}
}
