package hatSql

import (
	"context"
	"fmt"
	"reflect"
	"testing"
)

func TestCompiledSQLQueryReadOnlyTemplateEligibility(t *testing.T) {
	staticQuery, err := CompileSQLQuery("FROM VALUES (1), (2) AS values(id) SELECT id")
	if err != nil {
		t.Fatal(err)
	}
	if !staticQuery.readOnlyTemplateEligible(nil, SQLQueryOptions{}) {
		t.Fatal("static compiled query should use the immutable template path")
	}
	if staticQuery.readOnlyTemplateEligible([]interface{}{int64(1)}, SQLQueryOptions{}) {
		t.Fatal("parameter values must use the bound clone path")
	}
	if staticQuery.readOnlyTemplateEligible(nil, SQLQueryOptions{Collation: SQLCollationUnicodeCI}) {
		t.Fatal("non-default collation must use an execution copy")
	}
	if staticQuery.readOnlyTemplateEligible(nil, SQLQueryOptions{Collation: SQLCollationBinary}) {
		t.Fatal("explicit collation selection must use an execution copy")
	}

	parameterized, err := CompileSQLQuery("FROM VALUES ($1) AS values(id) SELECT id")
	if err != nil {
		t.Fatal(err)
	}
	if parameterized.readOnlyTemplateEligible(nil, SQLQueryOptions{}) {
		t.Fatal("parameterized compiled query must use the bound clone path")
	}
}

func TestCompiledSQLQueryParameterizedTemplateStillBindsWithoutMutation(t *testing.T) {
	query, err := CompileSQLQuery("FROM VALUES ($1) AS values(id) SELECT id")
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []int64{7, 11} {
		result, err := query.Execute(context.Background(), nil, []interface{}{value}, SQLQueryOptions{})
		if err != nil {
			t.Fatal(err)
		}
		want := []SQLRow{{"id": value}}
		if !reflect.DeepEqual(result.Rows, want) {
			t.Fatalf("parameter %d rows = %#v, want %#v", value, result.Rows, want)
		}
	}
	if !query.hasParameters {
		t.Fatal("parameterized template was not marked as parameterized")
	}
}

func TestCompiledSQLQueryReadOnlyTemplateStreamsRows(t *testing.T) {
	query, err := CompileSQLQuery("FROM VALUES (1), (2), (3) AS values(id) SELECT id WHERE id >= 2")
	if err != nil {
		t.Fatal(err)
	}
	var rows []SQLRow
	err = query.ExecuteRows(context.Background(), nil, nil, SQLQueryOptions{}, func(_ []string, row SQLRow) error {
		rows = append(rows, row)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []SQLRow{{"id": int64(2)}, {"id": int64(3)}}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("streamed rows = %#v, want %#v", rows, want)
	}
}

func TestCompiledSQLQueryReadOnlyTemplateConcurrentExecution(t *testing.T) {
	query, err := CompileSQLQuery("FROM VALUES (1), (2), (3), (4) AS values(id) SELECT id WHERE id > 1")
	if err != nil {
		t.Fatal(err)
	}
	const workers = 8
	const executions = 50
	errors := make(chan error, workers)
	for range workers {
		go func() {
			for range executions {
				result, err := query.Execute(context.Background(), nil, nil, SQLQueryOptions{})
				if err != nil {
					errors <- err
					return
				}
				if len(result.Rows) != 3 {
					errors <- fmt.Errorf("rows = %d, want 3", len(result.Rows))
					return
				}
			}
			errors <- nil
		}()
	}
	for range workers {
		if err := <-errors; err != nil {
			t.Fatal(err)
		}
	}
}

func TestCompiledSQLQueryReadOnlyTemplateExecutionPreservesRowsAndTemplate(t *testing.T) {
	query, err := CompileSQLQuery("FROM VALUES (1), (2), (3) AS values(id) SELECT id WHERE id >= 2")
	if err != nil {
		t.Fatal(err)
	}
	want := SQLQueryResult{Columns: []string{"id"}, Rows: []SQLRow{{"id": int64(2)}, {"id": int64(3)}}}
	for range 3 {
		got, err := query.Execute(context.Background(), nil, nil, SQLQueryOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got.Rows, want.Rows) || !reflect.DeepEqual(got.Columns, want.Columns) {
			t.Fatalf("compiled rows = %#v/%#v, want %#v/%#v", got.Columns, got.Rows, want.Columns, want.Rows)
		}
	}
	if query.template.indexHint.Mode != "" {
		t.Fatalf("read-only execution mutated template index hint: %#v", query.template.indexHint)
	}
}

var compiledTemplateReuseBenchmarkResult SQLQueryResult

func BenchmarkCompiledSQLQueryTemplateReuse(b *testing.B) {
	query, err := CompileSQLQuery("FROM VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (6, 'f'), (7, 'g'), (8, 'h'), (9, 'i'), (10, 'j'), (11, 'k'), (12, 'l'), (13, 'm'), (14, 'n'), (15, 'o'), (16, 'p') AS values(id, name) SELECT id, LOWER(name) AS name WHERE id >= 4")
	if err != nil {
		b.Fatal(err)
	}
	b.Run("clone_control", func(b *testing.B) {
		b.ReportAllocs()
		options := SQLQueryOptions{compiledTemplate: query.template}
		b.ResetTimer()
		for range b.N {
			result, err := ExecuteSQLQueryParameters(context.Background(), query.source, nil, nil, options)
			if err != nil {
				b.Fatal(err)
			}
			compiledTemplateReuseBenchmarkResult = result
		}
	})
	b.Run("read_only_handle", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			result, err := query.Execute(context.Background(), nil, nil, SQLQueryOptions{})
			if err != nil {
				b.Fatal(err)
			}
			compiledTemplateReuseBenchmarkResult = result
		}
	})
}
