package hatSql

import (
	"math"
	"sort"
)

const (
	sqlInProgramLinear = iota
	sqlInProgramNumericSearch
	sqlInProgramStringSearch
)

const sqlInProgramSearchMinimum = 8

type sqlInProgram struct {
	values []interface{}
	mode   int
}

func prepareSQLInExpr(expr *sqlExpr) {
	if expr == nil {
		return
	}
	expr.inProgram = nil
	if expr.kind != "in" || len(expr.args) == 0 {
		return
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

func (program *sqlInProgram) evaluate(op string, left interface{}, collation SQLCollation) interface{} {
	if program == nil {
		return sqlInValueWithCollation(op, left, nil, collation)
	}
	switch program.mode {
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
