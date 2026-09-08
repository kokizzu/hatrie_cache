package hatSql

import (
	"context"
	"reflect"
	"testing"
)

func TestCompileSQLQueryExecutesReusableImmutablePlan(t *testing.T) {
	const source = "SELECT name FROM CACHE('users') WHERE score >= $1"
	compiled, err := CompileSQLQuery(source)
	if err != nil {
		t.Fatalf("CompileSQLQuery() error = %v", err)
	}
	if compiled.Source() != source {
		t.Fatalf("Source() = %q, want %q", compiled.Source(), source)
	}
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) {
		return []Row{{"name": "Ada", "score": int64(7)}}, nil
	})

	result, err := compiled.Execute(context.Background(), resolver, []interface{}{int64(7)}, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if want := []Row{{"name": "Ada"}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("Execute() rows = %#v, want %#v", result.Rows, want)
	}
	result, err = compiled.Execute(context.Background(), resolver, []interface{}{int64(8)}, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("second Execute() error = %v", err)
	}
	if len(result.Rows) != 0 {
		t.Fatalf("second Execute() rows = %#v, want empty", result.Rows)
	}
}

func TestCompileSQLQueryStreamsAndRejectsInvalidPlans(t *testing.T) {
	compiled, err := CompileSQLQuery("SELECT name FROM CACHE('users') WHERE score >= $1")
	if err != nil {
		t.Fatalf("CompileSQLQuery() error = %v", err)
	}
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) {
		return []Row{{"name": "Ada", "score": int64(7)}}, nil
	})
	var columns []string
	var rows []Row
	err = compiled.ExecuteRows(context.Background(), resolver, []interface{}{int64(7)}, SQLQueryOptions{}, func(gotColumns []string, row Row) error {
		columns = append([]string(nil), gotColumns...)
		rows = append(rows, row)
		return nil
	})
	if err != nil {
		t.Fatalf("ExecuteRows() error = %v", err)
	}
	if want := []string{"name"}; !reflect.DeepEqual(columns, want) {
		t.Fatalf("ExecuteRows() columns = %#v, want %#v", columns, want)
	}
	if want := []Row{{"name": "Ada"}}; !reflect.DeepEqual(rows, want) {
		t.Fatalf("ExecuteRows() rows = %#v, want %#v", rows, want)
	}

	if _, err := CompileSQLQuery("SELECT"); err == nil {
		t.Fatal("CompileSQLQuery() accepted invalid SQL")
	}
}
