package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type chu61NullablePredicateResolver struct {
	batch         ColumnarBatch
	rows          []Row
	columnarCalls int
	rowCalls      int
	rejectRows    bool
}

func (resolver *chu61NullablePredicateResolver) ResolveSQLSource(string, string) ([]Row, error) {
	resolver.rowCalls++
	if resolver.rejectRows {
		return nil, errors.New("row source fallback is not allowed")
	}
	return resolver.rows, nil
}

func (resolver *chu61NullablePredicateResolver) ResolveSQLColumnarSource(string, string, []string) (ColumnarBatch, bool, error) {
	resolver.columnarCalls++
	return resolver.batch, true, nil
}

func chu61NullableRows(batch ColumnarBatch) []Row {
	rows := make([]Row, batch.Rows)
	for row := range rows {
		value, _ := batch.Value("value", row)
		rows[row] = Row{"value": value}
	}
	return rows
}

func TestCHU61NullablePredicatesUseColumnarValidityBitmap(t *testing.T) {
	values := []interface{}{nil, int64(1), nil, int64(2), nil}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
	batch.PackNumericColumns()
	if _, ok := batch.NumericColumns["value"]; !ok {
		t.Fatal("numeric fixture was not packed")
	}

	for _, test := range []struct {
		name  string
		query string
		want  []SQLRow
	}{
		{name: "is null", query: "SELECT value FROM CACHE('items') WHERE value IS NULL", want: []SQLRow{{"value": nil}, {"value": nil}, {"value": nil}}},
		{name: "is not null", query: "SELECT value FROM CACHE('items') WHERE value IS NOT NULL", want: []SQLRow{{"value": int64(1)}, {"value": int64(2)}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			parsed, err := parseSQLQueryParameters(test.query, nil)
			if err != nil {
				t.Fatal(err)
			}
			kernel, ok := sqlColumnarNullKernelForQuery(parsed.where, parsed.from.alias, batch)
			if !ok {
				t.Fatal("packed validity bitmap was not admitted to the nullable kernel")
			}
			if got := kernel.matches(0); got != (test.name == "is null") {
				t.Fatalf("row 0 nullable match = %v, want %v", got, test.name == "is null")
			}
			resolver := &chu61NullablePredicateResolver{batch: batch, rejectRows: true}
			result, err := ExecuteSQLQueryParameters(context.Background(), test.query, resolver, nil, SQLQueryOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if resolver.columnarCalls == 0 {
				t.Fatal("nullable predicate did not resolve a columnar source")
			}
			if resolver.rowCalls != 0 {
				t.Fatalf("nullable predicate used row-source fallback %d times", resolver.rowCalls)
			}
			if !reflect.DeepEqual(result.Rows, test.want) {
				t.Fatalf("rows = %#v, want %#v", result.Rows, test.want)
			}
		})
	}
}

func TestCHU61NullablePredicatesPreserveLegacyRows(t *testing.T) {
	values := []interface{}{nil, int64(1), nil, int64(2)}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
	resolver := &chu61NullablePredicateResolver{batch: batch, rows: chu61NullableRows(batch)}

	result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT value FROM CACHE('items') WHERE value IS NULL", resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []SQLRow{{"value": nil}, {"value": nil}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("legacy rows = %#v, want %#v", result.Rows, want)
	}
}

func TestCHU61NullablePredicatesSupportOtherValidityBitmaps(t *testing.T) {
	for _, test := range []struct {
		name  string
		batch ColumnarBatch
		query string
		want  []SQLRow
	}{
		{
			name: "boolean bitmap",
			batch: func() ColumnarBatch {
				batch := ColumnarBatch{Columns: map[string][]interface{}{"active": {nil, true, nil, false}}, Rows: 4}
				batch.PackBooleanColumns()
				return batch
			}(),
			query: "SELECT active FROM CACHE('items') WHERE active IS NULL",
			want:  []SQLRow{{"active": nil}, {"active": nil}},
		},
		{
			name: "dense nullable bitmap",
			batch: func() ColumnarBatch {
				batch := ColumnarBatch{Columns: map[string][]interface{}{"value": {nil, "a", nil, "b", nil, nil, "c", nil}}, Rows: 8}
				batch.PackNullableColumns()
				return batch
			}(),
			query: "SELECT value FROM CACHE('items') WHERE value IS NOT NULL",
			want:  []SQLRow{{"value": "a"}, {"value": "b"}, {"value": "c"}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			resolver := &chu61NullablePredicateResolver{batch: test.batch, rejectRows: true}
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

func TestCHU61NullablePredicateKernelRejectsMalformedBitmaps(t *testing.T) {
	for index, test := range []struct {
		validity []byte
		rows     int
		isNull   bool
		want     bool
	}{
		{validity: nil, rows: 4, isNull: false, want: true},
		{validity: []byte{0x0a}, rows: 4, isNull: true, want: true},
		{validity: []byte{}, rows: 4, isNull: true, want: false},
		{validity: []byte{0x80}, rows: 7, isNull: true, want: false},
	} {
		_, ok := newSQLColumnarNullPredicateKernel(test.validity, test.rows, test.isNull)
		if ok != test.want {
			t.Fatalf("case %d admitted = %v, want %v", index, ok, test.want)
		}
	}

	kernel, ok := newSQLColumnarNullPredicateKernel([]byte{0x0a}, 4, true)
	if !ok {
		t.Fatal("validity bitmap was rejected")
	}
	for row, want := range []bool{true, false, true, false} {
		if got := kernel.matches(row); got != want {
			t.Fatalf("row %d match = %v, want %v", row, got, want)
		}
	}
}
