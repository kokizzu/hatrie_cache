package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type chu63CaseProjectionResolver struct {
	batch         ColumnarBatch
	rows          []Row
	columnarCalls int
	rowCalls      int
	rejectRows    bool
}

func (resolver *chu63CaseProjectionResolver) ResolveSQLSource(string, string) ([]Row, error) {
	resolver.rowCalls++
	if resolver.rejectRows {
		return nil, errors.New("row source fallback is not allowed")
	}
	return resolver.rows, nil
}

func (resolver *chu63CaseProjectionResolver) ResolveSQLColumnarSource(string, string, []string) (ColumnarBatch, bool, error) {
	resolver.columnarCalls++
	return resolver.batch, true, nil
}

func chu63CaseRows(batch ColumnarBatch) []Row {
	rows := make([]Row, batch.Rows)
	for row := range rows {
		value, _ := batch.Value("value", row)
		rows[row] = Row{"value": value}
	}
	return rows
}

func TestCHU63CaseProjectionUsesColumnarSource(t *testing.T) {
	values := []interface{}{int64(1), nil, int64(10), int64(20)}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
	batch.PackNumericColumns()
	if _, ok := batch.NumericColumns["value"]; !ok {
		t.Fatal("numeric fixture was not packed")
	}

	resolver := &chu63CaseProjectionResolver{batch: batch, rejectRows: true}
	result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT CASE WHEN value >= 10 THEN 'high' ELSE 'low' END AS band FROM CACHE('items')", resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if resolver.columnarCalls == 0 {
		t.Fatal("CASE projection did not resolve a columnar source")
	}
	if resolver.rowCalls != 0 {
		t.Fatalf("CASE projection used row-source fallback %d times", resolver.rowCalls)
	}
	want := []SQLRow{{"band": "low"}, {"band": "low"}, {"band": "high"}, {"band": "high"}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestCHU63CaseProjectionPreservesNullAndPaging(t *testing.T) {
	values := []interface{}{int64(1), nil, int64(10), int64(20)}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
	batch.PackNumericColumns()
	for _, test := range []struct {
		name  string
		query string
		want  []SQLRow
	}{
		{name: "missing else", query: "SELECT CASE WHEN value >= 10 THEN 1 END AS result FROM CACHE('items')", want: []SQLRow{{"result": nil}, {"result": nil}, {"result": int64(1)}, {"result": int64(1)}}},
		{name: "offset limit", query: "SELECT CASE WHEN value >= 10 THEN 'high' ELSE 'low' END AS result FROM CACHE('items') LIMIT 2 OFFSET 1", want: []SQLRow{{"result": "low"}, {"result": "high"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			resolver := &chu63CaseProjectionResolver{batch: batch, rejectRows: true}
			result, err := ExecuteSQLQueryParameters(context.Background(), test.query, resolver, nil, SQLQueryOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if resolver.rowCalls != 0 || resolver.columnarCalls == 0 {
				t.Fatalf("source calls = columnar %d, row %d; want columnar-only", resolver.columnarCalls, resolver.rowCalls)
			}
			if !reflect.DeepEqual(result.Rows, test.want) {
				t.Fatalf("rows = %#v, want %#v", result.Rows, test.want)
			}
		})
	}
}

func TestCHU63CaseProjectionStreamsWithoutRowFallback(t *testing.T) {
	values := []interface{}{int64(2), nil, int64(5)}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
	batch.PackNumericColumns()
	resolver := &chu63CaseProjectionResolver{batch: batch, rejectRows: true}
	var columns []string
	var rows []SQLRow
	err := ExecuteSQLQueryRows(context.Background(), "SELECT CASE WHEN value >= 5 THEN 'yes' ELSE 'no' END AS result FROM CACHE('items')", resolver, nil, SQLQueryOptions{}, func(gotColumns []string, row SQLRow) error {
		if columns == nil {
			columns = append([]string(nil), gotColumns...)
		}
		rows = append(rows, row)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if resolver.rowCalls != 0 || resolver.columnarCalls == 0 {
		t.Fatalf("source calls = columnar %d, row %d; want columnar-only", resolver.columnarCalls, resolver.rowCalls)
	}
	if !reflect.DeepEqual(columns, []string{"result"}) {
		t.Fatalf("columns = %#v, want %#v", columns, []string{"result"})
	}
	want := []SQLRow{{"result": "no"}, {"result": "no"}, {"result": "yes"}}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("rows = %#v, want %#v", rows, want)
	}
}

func TestCHU63CaseProjectionFallsBackForUnsupportedShape(t *testing.T) {
	values := []interface{}{int64(1), int64(2)}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
	resolver := &chu63CaseProjectionResolver{batch: batch, rows: chu63CaseRows(batch)}
	result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT CASE WHEN value >= 2 THEN 'high' WHEN value >= 1 THEN 'mid' ELSE 'low' END AS result FROM CACHE('items')", resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if resolver.rowCalls == 0 || resolver.columnarCalls != 0 {
		t.Fatalf("source calls = columnar %d, row %d; want row-only fallback", resolver.columnarCalls, resolver.rowCalls)
	}
	want := []SQLRow{{"result": "mid"}, {"result": "high"}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}
