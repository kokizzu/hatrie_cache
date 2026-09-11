package hatSql

import (
	"context"
	"reflect"
	"testing"
)

func TestSQLColumnarBooleanPredicateKernelMatchesPackedRows(t *testing.T) {
	values := []interface{}{true, false, nil, true, nil, false}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"active": values}, Rows: len(values)}
	batch.PackBooleanColumns()
	column := batch.BoolColumns["active"]

	cases := []struct {
		name     string
		operator string
		value    bool
		want     []int
	}{
		{name: "equal true", operator: "=", value: true, want: []int{0, 3}},
		{name: "equal false", operator: "=", value: false, want: []int{1, 5}},
		{name: "not equal true", operator: "!=", value: true, want: []int{1, 5}},
		{name: "not equal false", operator: "<>", value: false, want: []int{0, 3}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			kernel, ok := newSQLColumnarBooleanPredicateKernel(column, test.operator, test.value)
			if !ok {
				t.Fatalf("newSQLColumnarBooleanPredicateKernel() rejected %q", test.operator)
			}
			got := make([]int, 0, len(test.want))
			for row := 0; row < column.Rows; row++ {
				if kernel.matches(row) {
					got = append(got, row)
				}
			}
			if len(got) != len(test.want) {
				t.Fatalf("matched rows = %#v, want %#v", got, test.want)
			}
			for index := range got {
				if got[index] != test.want[index] {
					t.Fatalf("matched rows = %#v, want %#v", got, test.want)
				}
			}
		})
	}
}

func TestSQLColumnarBooleanPredicateKernelSupportsReversedOperands(t *testing.T) {
	values := []interface{}{true, false, nil, true}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"active": values}, Rows: len(values)}
	batch.PackBooleanColumns()

	for _, test := range []struct {
		name  string
		query string
		want  []SQLRow
	}{
		{name: "equal", query: "SELECT active FROM CACHE('items') WHERE true = active", want: []SQLRow{{"active": true}, {"active": true}}},
		{name: "not equal", query: "SELECT active FROM CACHE('items') WHERE false != active", want: []SQLRow{{"active": true}, {"active": true}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := ExecuteSQLQueryParameters(context.Background(), test.query, packedBoolQueryResolver{batch: batch}, nil, SQLQueryOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(result.Rows, test.want) {
				t.Fatalf("rows = %#v, want %#v", result.Rows, test.want)
			}
		})
	}
}

func TestSQLColumnarBooleanPredicateKernelFallsBackForLegacyColumns(t *testing.T) {
	values := []interface{}{true, false, nil, true}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"active": values}, Rows: len(values)}
	result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT active FROM CACHE('items') WHERE active = true", packedBoolQueryResolver{batch: batch}, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := []SQLRow{{"active": true}, {"active": true}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("legacy rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLColumnarBooleanPredicateKernelRejectsInvalidInput(t *testing.T) {
	cases := []ColumnarBoolColumn{
		{Bits: nil, Rows: 8},
		{Bits: []byte{1}, Validity: []byte{}, Rows: 8},
		{Bits: []byte{0x81}, Rows: 7},
	}
	for index, column := range cases {
		if _, ok := newSQLColumnarBooleanPredicateKernel(column, "=", true); ok {
			t.Fatalf("case %d unexpectedly admitted malformed column", index)
		}
	}
	valid := ColumnarBoolColumn{Bits: []byte{1}, Rows: 1}
	if _, ok := newSQLColumnarBooleanPredicateKernel(valid, "LIKE", true); ok {
		t.Fatal("unsupported operator was admitted")
	}
	legacy := ColumnarBatch{Columns: map[string][]interface{}{"active": {true}}, Rows: 1}
	if _, ok := sqlColumnarBooleanKernelForQuery(sqlExpr{}, "", legacy); ok {
		t.Fatal("empty query expression was admitted")
	}
}
