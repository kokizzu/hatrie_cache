package hatSql

import "fmt"

// sqlAggregateOrNullBase maps ClickHouse-style OrNull combinators to the
// existing aggregate implementation while retaining their empty-result rule.
func sqlAggregateOrNullBase(name string) (string, bool) {
	switch name {
	case "COUNT_OR_NULL":
		return "COUNT", true
	case "SUM_OR_NULL":
		return "SUM", true
	case "AVG_OR_NULL":
		return "AVG", true
	case "MIN_OR_NULL":
		return "MIN", true
	case "MAX_OR_NULL":
		return "MAX", true
	default:
		return "", false
	}
}

func evalSQLAggregateOrNull(expr sqlExpr, group []sqlExecRow) (interface{}, error) {
	base, ok := sqlAggregateOrNullBase(expr.name)
	if !ok {
		return nil, fmt.Errorf("unknown OrNull aggregate %q", expr.name)
	}
	aggregateRows, err := sqlAggregateFilterRows(expr, group)
	if err != nil {
		return nil, err
	}
	if base == "COUNT" {
		if len(expr.args) > 1 {
			return nil, fmt.Errorf("%s expects zero or one argument", expr.name)
		}
		if len(expr.args) == 0 || expr.args[0].kind == "star" {
			if len(aggregateRows) == 0 {
				return nil, nil
			}
			return int64(len(aggregateRows)), nil
		}
		var count int64
		for _, row := range aggregateRows {
			value := evalSQLExpr(expr.args[0], []sqlExecRow{row}, row)
			if err := sqlExpressionError(value); err != nil {
				return nil, err
			}
			if value != nil {
				count++
			}
		}
		if count == 0 {
			return nil, nil
		}
		return count, nil
	}
	if len(expr.args) != 1 || expr.args[0].kind == "star" {
		return nil, fmt.Errorf("%s expects one argument", expr.name)
	}
	var result float64
	var count int64
	seen := false
	for _, row := range aggregateRows {
		value := evalSQLExpr(expr.args[0], []sqlExecRow{row}, row)
		if err := sqlExpressionError(value); err != nil {
			return nil, err
		}
		number, ok := sqlNumber(value)
		if !ok {
			continue
		}
		if !seen {
			result, seen = number, true
		} else if base == "SUM" || base == "AVG" {
			result += number
		} else if base == "MIN" && number < result {
			result = number
		} else if base == "MAX" && number > result {
			result = number
		}
		count++
	}
	if !seen {
		return nil, nil
	}
	if base == "AVG" {
		return result / float64(count), nil
	}
	return result, nil
}
