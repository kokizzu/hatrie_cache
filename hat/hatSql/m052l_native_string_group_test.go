package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestCompiledSQLNativeDataflowMatchesStringGroupedHavingOrderedRows(t *testing.T) {
	rows := []SQLRow{
		{"region": "eu", "value": int64(20)},
		{"region": "apac", "value": int64(10)},
		{"region": "eu", "value": nil},
		{"region": nil, "value": int64(5)},
		{"region": "apac", "value": int64(30)},
		{"region": nil, "value": nil},
		{"region": "us", "value": int64(7)},
		{"region": "", "value": int64(2)},
		{"region": "", "value": int64(3)},
	}
	query := "FROM CACHE('items') AS src SELECT src.region AS region, COUNT(*) AS total, SUM(src.value) AS total_value GROUP BY src.region HAVING COUNT(*) >= 2 ORDER BY total DESC, region ASC LIMIT 3 OFFSET 0"
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

func TestCompiledSQLNativeDataflowStringGroupChecksContext(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.region AS region, COUNT(*) AS total GROUP BY src.region HAVING COUNT(*) > 0 ORDER BY total DESC LIMIT 1")
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
