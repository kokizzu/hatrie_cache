package hatSql

import (
	"fmt"
	"sort"
	"strings"
)

type sqlIncrementalWindowRow struct {
	index       int
	partition   string
	orderValues []interface{}
	argument    interface{}
}

// sqlTryIncrementalWindowAggregate handles the append-only frame shape whose
// aggregate state can be updated once per ordered row. More complex frames
// retain the established evaluator because they need row exclusion, RANGE
// boundaries, or removals from the frame.
func sqlTryIncrementalWindowAggregate(item sqlSelectItem, column int, columns []string, out []sqlQueryOutput) (bool, error) {
	if item.expr.window == nil || len(item.expr.args) != 1 {
		return false, nil
	}
	switch item.expr.name {
	case "SUM", "AVG", "MIN", "MAX":
	default:
		return false, nil
	}
	if sqlExprHasCustomFunction(item.expr.args[0], nil) || sqlExprHasWindow(item.expr.args[0]) {
		return false, nil
	}
	frame := item.expr.window.frame
	if frame != nil && (frame.kind != "ROWS" || frame.start.kind != "UNBOUNDED PRECEDING" || frame.end.kind != "CURRENT ROW" || frame.exclude != "" && frame.exclude != "NO OTHERS") {
		return false, nil
	}
	prepared := make([]sqlIncrementalWindowRow, len(out))
	partitions := make(map[string][]int, len(out))
	for index, output := range out {
		row := sqlExecRow{}
		if len(output.group) > 0 {
			row = output.group[0]
		}
		prepared[index] = sqlIncrementalWindowRow{index: index}
		parts := make([]string, len(item.expr.window.partition))
		for partIndex, expression := range item.expr.window.partition {
			value := evalSQLExpr(expression, output.group, row)
			if err := sqlExpressionError(value); err != nil {
				return true, err
			}
			parts[partIndex] = fmt.Sprintf("%#v", value)
		}
		prepared[index].partition = strings.Join(parts, "\x00")
		prepared[index].orderValues = make([]interface{}, len(item.expr.window.order))
		for orderIndex, order := range item.expr.window.order {
			value := evalSQLExpr(order.expr, output.group, row)
			if err := sqlExpressionError(value); err != nil {
				return true, err
			}
			prepared[index].orderValues[orderIndex] = value
		}
		value := evalSQLExpr(item.expr.args[0], output.group, row)
		if err := sqlExpressionError(value); err != nil {
			return true, err
		}
		prepared[index].argument = value
		partitions[prepared[index].partition] = append(partitions[prepared[index].partition], index)
	}
	for _, indexes := range partitions {
		sort.SliceStable(indexes, func(left, right int) bool {
			leftRow, rightRow := prepared[indexes[left]], prepared[indexes[right]]
			for orderIndex, order := range item.expr.window.order {
				if less, decided := sqlOrderLess(order, leftRow.orderValues[orderIndex], rightRow.orderValues[orderIndex]); decided {
					return less
				}
			}
			return false
		})
		var sum, minimum, maximum float64
		count := 0
		for _, preparedIndex := range indexes {
			value, ok := sqlNumber(prepared[preparedIndex].argument)
			if ok {
				if count == 0 {
					minimum, maximum = value, value
				} else {
					if value < minimum {
						minimum = value
					}
					if value > maximum {
						maximum = value
					}
				}
				sum += value
				count++
			}
			var aggregate interface{}
			if count > 0 {
				switch item.expr.name {
				case "SUM":
					aggregate = sum
				case "AVG":
					aggregate = sum / float64(count)
				case "MIN":
					aggregate = minimum
				case "MAX":
					aggregate = maximum
				}
			}
			out[prepared[preparedIndex].index].row[columns[column]] = aggregate
		}
	}
	return true, nil
}
