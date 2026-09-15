package hatSql

import (
	"math"
	"testing"
)

func TestCHU17DenseIntegerInProgramUsesBoundedBitmap(t *testing.T) {
	args := make([]sqlExpr, 64)
	for index := range args {
		args[index] = sqlExpr{kind: "literal", value: int64(1000 + index)}
	}
	expr := sqlExpr{kind: "in", op: "IN", args: args}
	prepareSQLInExpr(&expr)
	if expr.inProgram == nil || expr.inProgram.mode != sqlInProgramNumericBitmap || len(expr.inProgram.numericBitmap) == 0 {
		t.Fatalf("dense integer program = %#v, want bounded numeric bitmap", expr.inProgram)
	}
	for _, test := range []struct {
		name string
		left interface{}
		op   string
		want interface{}
	}{
		{name: "minimum", left: int64(1000), op: "IN", want: true},
		{name: "maximum as float", left: float64(1063), op: "IN", want: true},
		{name: "missing", left: int64(1064), op: "IN", want: false},
		{name: "fraction", left: float64(1000.5), op: "IN", want: false},
		{name: "not found", left: int64(999), op: "NOT IN", want: true},
		{name: "formatted text fallback", left: "1000", op: "IN", want: true},
		{name: "NaN preserves legacy numeric comparison", left: math.NaN(), op: "IN", want: true},
		{name: "NULL", left: nil, op: "IN", want: nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := expr.inProgram.evaluate(test.op, test.left, SQLCollationBinary); got != test.want {
				t.Fatalf("evaluate(%v, %v) = %v, want %v", test.op, test.left, got, test.want)
			}
		})
	}
}

func TestCHU17InProgramKeepsUnsuitedListsOnExistingModes(t *testing.T) {
	sparse := make([]sqlExpr, 32)
	for index := range sparse {
		sparse[index] = sqlExpr{kind: "literal", value: int64(index * 1_000_000)}
	}
	sparseExpr := sqlExpr{kind: "in", op: "IN", args: sparse}
	prepareSQLInExpr(&sparseExpr)
	if sparseExpr.inProgram == nil || sparseExpr.inProgram.mode != sqlInProgramNumericSearch || len(sparseExpr.inProgram.numericBitmap) != 0 {
		t.Fatalf("sparse integer program = %#v, want existing numeric search", sparseExpr.inProgram)
	}

	small := make([]sqlExpr, 7)
	for index := range small {
		small[index] = sqlExpr{kind: "literal", value: int64(index)}
	}
	smallExpr := sqlExpr{kind: "in", op: "IN", args: small}
	prepareSQLInExpr(&smallExpr)
	if smallExpr.inProgram == nil || smallExpr.inProgram.mode != sqlInProgramLinear || len(smallExpr.inProgram.numericBitmap) != 0 {
		t.Fatalf("small integer program = %#v, want existing linear search", smallExpr.inProgram)
	}
}
