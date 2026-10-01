package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type chu62ArithmeticProjectionResolver struct {
	batch         ColumnarBatch
	rows          []Row
	columnarCalls int
	rowCalls      int
	rejectRows    bool
}

func (resolver *chu62ArithmeticProjectionResolver) ResolveSQLSource(string, string) ([]Row, error) {
	resolver.rowCalls++
	if resolver.rejectRows {
		return nil, errors.New("row source fallback is not allowed")
	}
	return resolver.rows, nil
}

func (resolver *chu62ArithmeticProjectionResolver) ResolveSQLColumnarSource(string, string, []string) (ColumnarBatch, bool, error) {
	resolver.columnarCalls++
	return resolver.batch, true, nil
}

func chu62ArithmeticRows(batch ColumnarBatch) []Row {
	rows := make([]Row, batch.Rows)
	for row := range rows {
		value, _ := batch.Value("value", row)
		rows[row] = Row{"value": value}
	}
	return rows
}

func TestCHU62ArithmeticProjectionUsesColumnarSource(t *testing.T) {
	values := []interface{}{int64(1), nil, int64(3), int64(4)}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
	batch.PackNumericColumns()
	if _, ok := batch.NumericColumns["value"]; !ok {
		t.Fatal("numeric fixture was not packed")
	}

	resolver := &chu62ArithmeticProjectionResolver{batch: batch, rejectRows: true}
	result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT value + 1 AS incremented FROM CACHE('items')", resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if resolver.columnarCalls == 0 {
		t.Fatal("arithmetic projection did not resolve a columnar source")
	}
	if resolver.rowCalls != 0 {
		t.Fatalf("arithmetic projection used row-source fallback %d times", resolver.rowCalls)
	}
	want := []SQLRow{{"incremented": int64(2)}, {"incremented": nil}, {"incremented": int64(4)}, {"incremented": int64(5)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestCHU62ArithmeticProjectionPreservesOperators(t *testing.T) {
	values := []interface{}{int64(3), nil, int64(8)}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
	batch.PackNumericColumns()
	for _, test := range []struct {
		name  string
		query string
		want  []SQLRow
	}{
		{name: "subtract", query: "SELECT value - 2 AS result FROM CACHE('items')", want: []SQLRow{{"result": int64(1)}, {"result": nil}, {"result": int64(6)}}},
		{name: "multiply reversed", query: "SELECT 2 * value AS result FROM CACHE('items')", want: []SQLRow{{"result": int64(6)}, {"result": nil}, {"result": int64(16)}}},
		{name: "divide by zero", query: "SELECT value / 0 AS result FROM CACHE('items')", want: []SQLRow{{"result": nil}, {"result": nil}, {"result": nil}}},
		{name: "modulo", query: "SELECT value % 2 AS result FROM CACHE('items')", want: []SQLRow{{"result": int64(1)}, {"result": nil}, {"result": int64(0)}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			resolver := &chu62ArithmeticProjectionResolver{batch: batch, rejectRows: true}
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

func TestCHU62ArithmeticProjectionFallsBackForUnsupportedShape(t *testing.T) {
	values := []interface{}{int64(1), int64(2)}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
	resolver := &chu62ArithmeticProjectionResolver{batch: batch, rows: chu62ArithmeticRows(batch)}
	result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT value + value AS result FROM CACHE('items')", resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if resolver.rowCalls == 0 || resolver.columnarCalls != 0 {
		t.Fatalf("source calls = columnar %d, row %d; want row-only fallback", resolver.columnarCalls, resolver.rowCalls)
	}
	want := []SQLRow{{"result": int64(2)}, {"result": int64(4)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestCHU62ArithmeticProjectionPreservesFloatAndPaging(t *testing.T) {
	values := []interface{}{1.5, nil, 4.0, 8.5}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
	batch.PackNumericColumns()
	for _, test := range []struct {
		name  string
		query string
		want  []SQLRow
	}{
		{name: "float", query: "SELECT value * 0.5 AS result FROM CACHE('items')", want: []SQLRow{{"result": 0.75}, {"result": nil}, {"result": 2.0}, {"result": 4.25}}},
		{name: "offset limit", query: "SELECT value + 1 AS result FROM CACHE('items') LIMIT 2 OFFSET 1", want: []SQLRow{{"result": nil}, {"result": 5.0}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			resolver := &chu62ArithmeticProjectionResolver{batch: batch, rejectRows: true}
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

func TestCHU62ArithmeticProjectionStreamsWithoutRowFallback(t *testing.T) {
	values := []interface{}{int64(2), nil, int64(5)}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
	batch.PackNumericColumns()
	resolver := &chu62ArithmeticProjectionResolver{batch: batch, rejectRows: true}
	var columns []string
	var rows []SQLRow
	err := ExecuteSQLQueryRows(context.Background(), "SELECT value + 3 AS result FROM CACHE('items')", resolver, nil, SQLQueryOptions{}, func(gotColumns []string, row SQLRow) error {
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
	want := []SQLRow{{"result": int64(5)}, {"result": nil}, {"result": int64(8)}}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("rows = %#v, want %#v", rows, want)
	}
}
