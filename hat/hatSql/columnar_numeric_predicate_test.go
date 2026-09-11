package hatSql

import (
	"math"
	"testing"
)

func TestSQLColumnarNumericPredicateKernelMatchesPackedRows(t *testing.T) {
	values := []interface{}{int64(-2), nil, int64(0), int64(3), int64(8)}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
	batch.PackNumericColumns()
	column, ok := batch.NumericColumns["value"]
	if !ok {
		t.Fatal("numeric column was not packed")
	}

	cases := []struct {
		name      string
		operator  string
		threshold float64
		want      []int
	}{
		{name: "equal", operator: "=", threshold: 3, want: []int{3}},
		{name: "not equal", operator: "!=", threshold: 3, want: []int{0, 2, 4}},
		{name: "less", operator: "<", threshold: 3, want: []int{0, 2}},
		{name: "less or equal", operator: "<=", threshold: 3, want: []int{0, 2, 3}},
		{name: "greater", operator: ">", threshold: 0, want: []int{3, 4}},
		{name: "greater or equal", operator: ">=", threshold: 0, want: []int{2, 3, 4}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			kernel, ok := newSQLColumnarNumericPredicateKernel(column, test.operator, test.threshold)
			if !ok {
				t.Fatalf("newSQLColumnarNumericPredicateKernel() rejected %q", test.operator)
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

func TestSQLColumnarNumericPredicateKernelPreservesFloatSemantics(t *testing.T) {
	values := []interface{}{math.Copysign(0, -1), math.Inf(1), math.NaN(), nil, -12.5}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
	batch.PackNumericColumns()
	column := batch.NumericColumns["value"]

	kernel, ok := newSQLColumnarNumericPredicateKernel(column, "=", math.Copysign(0, -1))
	if !ok || !kernel.matches(0) || kernel.matches(1) || kernel.matches(2) || kernel.matches(3) || kernel.matches(4) {
		t.Fatal("float equality kernel changed SQL comparison semantics")
	}

	kernel, ok = newSQLColumnarNumericPredicateKernel(column, "!=", math.NaN())
	if !ok || !kernel.matches(0) || !kernel.matches(1) || !kernel.matches(2) || kernel.matches(3) || !kernel.matches(4) {
		t.Fatal("NaN inequality kernel changed SQL comparison semantics")
	}
}

func TestSQLColumnarNumericPredicateKernelRejectsInvalidInput(t *testing.T) {
	cases := []ColumnarNumericColumn{
		{Kind: ColumnarNumericInt64, Data: make([]byte, 7), Rows: 1},
		{Kind: ColumnarNumericKind(99), Data: make([]byte, 8), Rows: 1},
		{Kind: ColumnarNumericInt64, Data: make([]byte, 8), Rows: 1, Validity: []byte{}},
	}
	for index, column := range cases {
		if _, ok := newSQLColumnarNumericPredicateKernel(column, "=", 1); ok {
			t.Fatalf("case %d unexpectedly admitted malformed column", index)
		}
	}
	valid := ColumnarNumericColumn{Kind: ColumnarNumericInt64, Data: make([]byte, 8), Rows: 1}
	if _, ok := newSQLColumnarNumericPredicateKernel(valid, "LIKE", 1); ok {
		t.Fatal("unsupported operator was admitted")
	}
	legacy := ColumnarBatch{Columns: map[string][]interface{}{"value": {int64(1)}}, Rows: 1}
	if _, ok := newSQLColumnarNumericPredicateKernel(ColumnarNumericColumn{}, "=", 1); ok {
		t.Fatal("empty column was admitted")
	}
	if _, ok := sqlColumnarNumericPredicateKernels(legacy, []sqlColumnarNumericFilter{{field: "value", operator: "=", value: 1}}); ok {
		t.Fatal("legacy column unexpectedly selected the packed kernel")
	}
}
