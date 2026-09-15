package hatSql

import (
	"math"
	"sort"
)

const (
	sqlInProgramLinear = iota
	sqlInProgramNumericSearch
	sqlInProgramStringSearch
	sqlInProgramNumericBitmap
)

const (
	sqlInProgramSearchMinimum        = 8
	sqlInProgramBitmapMinimum        = 16
	sqlInProgramBitmapMaximumSpan    = 1 << 20
	sqlInProgramBitmapMaximumDensity = 32
	sqlInProgramBitmapMaximumInteger = int64(1 << 53)
)

type sqlInProgram struct {
	values        []interface{}
	numericValues []int64
	numericBitmap []uint64
	numericMin    int64
	mode          int
}

func prepareSQLInExpr(expr *sqlExpr) {
	if expr == nil {
		return
	}
	expr.inProgram = nil
	if expr.kind != "in" || len(expr.args) == 0 {
		return
	}
	if _, integer := sqlInProgramIntegerLiteral(expr.args[0].value); integer {
		integerValues, ok := sqlInProgramDenseIntegerCandidates(expr.args)
		if !ok {
			integerValues = nil
		}
		if minimum, bitmap, compactValues, ok := sqlInProgramNumericBitmapFor(integerValues); ok {
			expr.inProgram = &sqlInProgram{
				mode:          sqlInProgramNumericBitmap,
				numericMin:    minimum,
				numericBitmap: bitmap,
				numericValues: compactValues,
			}
			return
		}
	}
	values := make([]interface{}, len(expr.args))
	for index, argument := range expr.args {
		if argument.kind != "literal" {
			return
		}
		values[index] = argument.value
	}
	program := &sqlInProgram{values: values}
	if len(values) >= sqlInProgramSearchMinimum {
		allNumeric := true
		allString := true
		for _, value := range values {
			number, numeric := sqlNumber(value)
			if !numeric || math.IsNaN(number) {
				allNumeric = false
			}
			if _, stringValue := value.(string); !stringValue {
				allString = false
			}
		}
		switch {
		case allNumeric:
			sort.Slice(values, func(left, right int) bool {
				leftValue, _ := sqlNumber(values[left])
				rightValue, _ := sqlNumber(values[right])
				return leftValue < rightValue
			})
			program.mode = sqlInProgramNumericSearch
		case allString:
			sort.Slice(values, func(left, right int) bool {
				return values[left].(string) < values[right].(string)
			})
			program.mode = sqlInProgramStringSearch
		}
	}
	expr.inProgram = program
}

func sqlInProgramDenseIntegerCandidates(args []sqlExpr) ([]int64, bool) {
	if len(args) < sqlInProgramBitmapMinimum {
		return nil, false
	}
	var minimum, maximum int64
	for index, argument := range args {
		if argument.kind != "literal" {
			return nil, false
		}
		value, ok := sqlInProgramIntegerLiteral(argument.value)
		if !ok {
			return nil, false
		}
		if index == 0 || value < minimum {
			minimum = value
		}
		if index == 0 || value > maximum {
			maximum = value
		}
	}
	if minimum < -sqlInProgramBitmapMaximumInteger || maximum > sqlInProgramBitmapMaximumInteger {
		return nil, false
	}
	span := uint64(maximum) - uint64(minimum) + 1
	if span == 0 || span > sqlInProgramBitmapMaximumSpan || span > uint64(len(args))*sqlInProgramBitmapMaximumDensity {
		return nil, false
	}
	values := make([]int64, len(args))
	for index, argument := range args {
		values[index], _ = sqlInProgramIntegerLiteral(argument.value)
	}
	return values, true
}

func (program *sqlInProgram) evaluate(op string, left interface{}, collation SQLCollation) interface{} {
	if program == nil {
		return sqlInValueWithCollation(op, left, nil, collation)
	}
	switch program.mode {
	case sqlInProgramNumericBitmap:
		if value, ok := sqlInProgramIntegerProbe(left); ok {
			if value >= program.numericMin {
				offset := uint64(value - program.numericMin)
				word := offset >> 6
				if word < uint64(len(program.numericBitmap)) && program.numericBitmap[word]&(uint64(1)<<(offset&63)) != 0 {
					return op != "NOT IN"
				}
			}
			return op == "NOT IN"
		}
		if number, ok := sqlNumber(left); ok {
			if math.IsNaN(number) {
				return op != "NOT IN"
			}
			return op == "NOT IN"
		}
		return sqlInIntegerValuesWithCollation(op, left, program.numericValues, collation)
	case sqlInProgramNumericSearch:
		value, ok := sqlNumber(left)
		if ok && !math.IsNaN(value) {
			index := sort.Search(len(program.values), func(index int) bool {
				candidate, _ := sqlNumber(program.values[index])
				return candidate >= value
			})
			if index < len(program.values) {
				candidate, _ := sqlNumber(program.values[index])
				if candidate == value {
					return op != "NOT IN"
				}
			}
			return op == "NOT IN"
		}
	case sqlInProgramStringSearch:
		if value, ok := left.(string); ok && collation.normalized() == SQLCollationBinary {
			index := sort.Search(len(program.values), func(index int) bool {
				return program.values[index].(string) >= value
			})
			if index < len(program.values) && program.values[index].(string) == value {
				return op != "NOT IN"
			}
			return op == "NOT IN"
		}
	}
	return sqlInValueWithCollation(op, left, program.values, collation)
}

func sqlInProgramIntegerLiteral(value interface{}) (int64, bool) {
	switch value := value.(type) {
	case int:
		return int64(value), true
	case int64:
		return value, true
	default:
		return 0, false
	}
}

func sqlInProgramIntegerProbe(value interface{}) (int64, bool) {
	if integer, ok := sqlInProgramIntegerLiteral(value); ok {
		if integer < -sqlInProgramBitmapMaximumInteger || integer > sqlInProgramBitmapMaximumInteger {
			return 0, false
		}
		return integer, true
	}
	number, ok := sqlNumber(value)
	if !ok || math.IsNaN(number) || math.IsInf(number, 0) || math.Trunc(number) != number || number < -float64(sqlInProgramBitmapMaximumInteger) || number > float64(sqlInProgramBitmapMaximumInteger) {
		return 0, false
	}
	return int64(number), true
}

func sqlInProgramNumericBitmapFor(values []int64) (int64, []uint64, []int64, bool) {
	if len(values) < sqlInProgramBitmapMinimum || len(values) == 0 {
		return 0, nil, nil, false
	}
	sort.Slice(values, func(left, right int) bool { return values[left] < values[right] })
	if values[0] < -sqlInProgramBitmapMaximumInteger || values[len(values)-1] > sqlInProgramBitmapMaximumInteger {
		return 0, nil, nil, false
	}
	compact := values[:1]
	for _, value := range values[1:] {
		if value != compact[len(compact)-1] {
			compact = append(compact, value)
		}
	}
	if len(compact) != cap(compact) {
		compact = append([]int64(nil), compact...)
	}
	span := uint64(values[len(values)-1]) - uint64(values[0]) + 1
	if span == 0 || span > sqlInProgramBitmapMaximumSpan || span > uint64(len(compact))*sqlInProgramBitmapMaximumDensity {
		return 0, nil, nil, false
	}
	bitmap := make([]uint64, (span+63)/64)
	for _, value := range compact {
		offset := uint64(value - values[0])
		bitmap[offset>>6] |= uint64(1) << (offset & 63)
	}
	return values[0], bitmap, compact, true
}

func sqlInIntegerValuesWithCollation(op string, left interface{}, values []int64, collation SQLCollation) interface{} {
	if left == nil {
		return nil
	}
	unknown := false
	for _, value := range values {
		comparison := sqlBinaryValueWithCollation("=", left, value, collation)
		if comparison == true {
			return op != "NOT IN"
		}
		if comparison == nil {
			unknown = true
		}
	}
	if unknown {
		return nil
	}
	return op == "NOT IN"
}
