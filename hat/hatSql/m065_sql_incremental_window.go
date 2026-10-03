package hatSql

import "math"

// sqlApplyIncrementalWindowAggregate evaluates the common append-only rolling
// SUM/AVG shape with one value transition per row. It deliberately declines
// frames with exclusions, RANGE semantics, or a following bound so the
// general evaluator remains the source of truth for those cases.
func sqlApplyIncrementalWindowAggregate(expression sqlExpr, indexes []int, out []sqlQueryOutput, column string) (bool, error) {
	if expression.window == nil || (expression.name != "SUM" && expression.name != "AVG") || len(expression.args) != 1 {
		return false, nil
	}
	frame := expression.window.frame
	if frame == nil || frame.kind != "ROWS" || frame.end.kind != "CURRENT ROW" || frame.exclude != "" && frame.exclude != "NO OTHERS" {
		return false, nil
	}
	preceding := 0
	unbounded := false
	switch frame.start.kind {
	case "UNBOUNDED PRECEDING":
		unbounded = true
	case "PRECEDING":
		if frame.start.offset < 0 {
			return false, nil
		}
		preceding = frame.start.offset
	default:
		return false, nil
	}
	if len(indexes) == 0 {
		return true, nil
	}
	if !unbounded && preceding >= len(indexes)-1 {
		unbounded = true
	}

	var ring []sqlIncrementalWindowValue
	if !unbounded {
		ring = make([]sqlIncrementalWindowValue, preceding+1)
	}
	var sum float64
	validCount := 0
	nanCount := 0
	positiveInfCount := 0
	negativeInfCount := 0
	for position, index := range indexes {
		if !unbounded {
			slot := &ring[position%len(ring)]
			if slot.valid {
				if slot.nan {
					nanCount--
				} else if slot.positiveInf {
					positiveInfCount--
				} else if slot.negativeInf {
					negativeInfCount--
				} else {
					sum -= slot.value
				}
				validCount--
			}
			*slot = sqlIncrementalWindowValue{}
		}
		row := sqlExecRow{}
		if len(out[index].group) > 0 {
			row = out[index].group[0]
		}
		value := evalSQLExpr(expression.args[0], out[index].group, row)
		if err := sqlExpressionError(value); err != nil {
			return true, err
		}
		if number, ok := sqlNumber(value); ok {
			validCount++
			entry := sqlIncrementalWindowValue{value: number, valid: true}
			switch {
			case math.IsNaN(number):
				entry.nan = true
				nanCount++
			case math.IsInf(number, 1):
				entry.positiveInf = true
				positiveInfCount++
			case math.IsInf(number, -1):
				entry.negativeInf = true
				negativeInfCount++
			default:
				sum += number
			}
			if !unbounded {
				ring[position%len(ring)] = entry
			}
		}
		out[index].row[column] = sqlIncrementalWindowAggregateValue(expression.name, sum, validCount, nanCount, positiveInfCount, negativeInfCount)
	}
	return true, nil
}

type sqlIncrementalWindowValue struct {
	value       float64
	valid       bool
	nan         bool
	positiveInf bool
	negativeInf bool
}

func sqlIncrementalWindowAggregateValue(name string, sum float64, validCount, nanCount, positiveInfCount, negativeInfCount int) interface{} {
	if validCount == 0 {
		return nil
	}
	if nanCount > 0 || positiveInfCount > 0 && negativeInfCount > 0 {
		return math.NaN()
	}
	if positiveInfCount > 0 {
		return math.Inf(1)
	}
	if negativeInfCount > 0 {
		return math.Inf(-1)
	}
	if name == "AVG" {
		return sum / float64(validCount)
	}
	return sum
}
