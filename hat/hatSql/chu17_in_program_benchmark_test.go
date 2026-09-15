package hatSql

import (
	"sort"
	"testing"
)

var chu17BenchmarkResult interface{}

func chu17DenseIntegerValues(count int) []interface{} {
	values := make([]interface{}, count)
	for index := range values {
		values[index] = int64(100000 + index)
	}
	return values
}

func chu17DenseIntegerProbes() []interface{} {
	return []interface{}{int64(100000), int64(100127), int64(104096), int64(109999), int64(110000), float64(105555), float64(105555.5)}
}

func BenchmarkCHU17DenseIntegerSortedSearch(b *testing.B) {
	program := &sqlInProgram{values: chu17DenseIntegerValues(10000), mode: sqlInProgramNumericSearch}
	probes := chu17DenseIntegerProbes()
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(len(program.values)*16), "program_bytes")
	for index := 0; index < b.N; index++ {
		chu17BenchmarkResult = program.evaluate("IN", probes[index%len(probes)], SQLCollationBinary)
	}
}

func BenchmarkCHU17DenseIntegerBitmapSearch(b *testing.B) {
	args := make([]sqlExpr, 10000)
	for index := range args {
		args[index] = sqlExpr{kind: "literal", value: int64(100000 + index)}
	}
	expr := sqlExpr{kind: "in", op: "IN", args: args}
	prepareSQLInExpr(&expr)
	if expr.inProgram == nil || expr.inProgram.mode != sqlInProgramNumericBitmap {
		b.Fatal("dense integer program did not select bitmap mode")
	}
	program := expr.inProgram
	probes := chu17DenseIntegerProbes()
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(len(program.numericValues)*8+len(program.numericBitmap)*8), "program_bytes")
	for index := 0; index < b.N; index++ {
		chu17BenchmarkResult = program.evaluate("IN", probes[index%len(probes)], SQLCollationBinary)
	}
}

func BenchmarkCHU17DenseIntegerSortedBuild(b *testing.B) {
	args := make([]sqlExpr, 10000)
	for index := range args {
		args[index] = sqlExpr{kind: "literal", value: int64(100000 + index)}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		values := make([]interface{}, len(args))
		for index, argument := range args {
			values[index] = argument.value
		}
		sort.Slice(values, func(left, right int) bool {
			leftValue, _ := sqlNumber(values[left])
			rightValue, _ := sqlNumber(values[right])
			return leftValue < rightValue
		})
		program := &sqlInProgram{values: values, mode: sqlInProgramNumericSearch}
		chu17BenchmarkResult = program
	}
}

func BenchmarkCHU17DenseIntegerBitmapBuild(b *testing.B) {
	args := make([]sqlExpr, 10000)
	for index := range args {
		args[index] = sqlExpr{kind: "literal", value: int64(100000 + index)}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		expr := sqlExpr{kind: "in", op: "IN", args: args}
		prepareSQLInExpr(&expr)
		if expr.inProgram == nil || expr.inProgram.mode != sqlInProgramNumericBitmap {
			b.Fatal("dense integer program did not select bitmap mode")
		}
		chu17BenchmarkResult = expr.inProgram
	}
}
