package hatSql

import (
	"container/heap"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

type sqlQueryOutput struct {
	row   SQLRow
	group []sqlExecRow
}

type sqlLimitByState struct {
	limitBy      *sqlLimitBy
	counts       map[string]int
	simpleCounts map[interface{}]int
}

func newSQLLimitByState(limitBy *sqlLimitBy) *sqlLimitByState {
	if limitBy == nil {
		return nil
	}
	state := &sqlLimitByState{limitBy: limitBy, counts: make(map[string]int)}
	if len(limitBy.expressions) == 1 && limitBy.expressions[0].kind == "field" && limitBy.expressions[0].collation.normalized() == SQLCollationBinary {
		state.simpleCounts = make(map[interface{}]int)
	}
	return state
}

func (state *sqlLimitByState) accept(row SQLRow, group []sqlExecRow) (bool, error) {
	if state == nil || state.limitBy == nil {
		return true, nil
	}
	if state.simpleCounts != nil {
		value := evalOutputOrder(state.limitBy.expressions[0], row, group)
		if err := sqlExpressionError(value); err != nil {
			return false, err
		}
		if key, ok := sqlLimitByComparableValue(value); ok {
			return state.acceptSimpleKey(key), nil
		}
	}
	key, err := sqlLimitByKey(state.limitBy, row, group)
	if err != nil {
		return false, err
	}
	return state.acceptKey(key), nil
}

func (state *sqlLimitByState) acceptSimpleKey(key interface{}) bool {
	if state.limitBy.limit == 0 {
		return false
	}
	count := state.simpleCounts[key]
	if count >= state.limitBy.limit {
		return false
	}
	state.simpleCounts[key] = count + 1
	return true
}

func (state *sqlLimitByState) acceptKey(key string) bool {
	if state == nil || state.limitBy == nil {
		return true
	}
	if state.limitBy.limit == 0 {
		return false
	}
	count := state.counts[key]
	if count >= state.limitBy.limit {
		return false
	}
	state.counts[key] = count + 1
	return true
}

func sqlLimitByKey(limitBy *sqlLimitBy, row SQLRow, group []sqlExecRow) (string, error) {
	if limitBy == nil {
		return "", nil
	}
	parts := make([]string, len(limitBy.expressions))
	for index, expression := range limitBy.expressions {
		value := evalOutputOrder(expression, row, group)
		if err := sqlExpressionError(value); err != nil {
			return "", err
		}
		part := sqlCollationValueKey(expression.collation, value)
		parts[index] = strconv.Itoa(len(part)) + ":" + part
	}
	return strings.Join(parts, ";"), nil
}

func sqlLimitByComparableValue(value interface{}) (interface{}, bool) {
	if value == nil {
		return nil, true
	}
	typeOf := reflect.TypeOf(value)
	if !typeOf.Comparable() {
		return nil, false
	}
	switch typed := value.(type) {
	case float32:
		if math.IsNaN(float64(typed)) {
			return nil, false
		}
	case float64:
		if math.IsNaN(typed) {
			return nil, false
		}
	}
	return value, true
}

func sqlLimitByTopN(outputs []sqlQueryOutput, limitBy *sqlLimitBy, order []sqlOrder, maxSortBytes int) ([]sqlQueryOutput, bool, error) {
	if limitBy == nil || limitBy.limit <= 0 || len(order) == 0 || len(outputs) <= limitBy.limit {
		return outputs, false, nil
	}
	if maxSortBytes > 0 && sqlQueryOutputsBytes(outputs) > maxSortBytes {
		return outputs, false, nil
	}

	groups := make(map[string]*sqlTopNStreamHeap)
	simpleGroups := make(map[interface{}]*sqlTopNStreamHeap)
	simpleKey := len(limitBy.expressions) == 1 && limitBy.expressions[0].kind == "field" && limitBy.expressions[0].collation.normalized() == SQLCollationBinary
	newGroup := func() *sqlTopNStreamHeap {
		result := &sqlTopNStreamHeap{items: make([]sqlTopNStreamItem, 0, limitBy.limit), order: order}
		heap.Init(result)
		return result
	}
	for ordinal, output := range outputs {
		var group *sqlTopNStreamHeap
		if simpleKey {
			value := evalOutputOrder(limitBy.expressions[0], output.row, output.group)
			if err := sqlExpressionError(value); err != nil {
				return nil, false, err
			}
			if key, ok := sqlLimitByComparableValue(value); ok {
				group = simpleGroups[key]
				if group == nil {
					group = newGroup()
					simpleGroups[key] = group
				}
			} else {
				key, err := sqlLimitByKey(limitBy, output.row, output.group)
				if err != nil {
					return nil, false, err
				}
				group = groups[key]
				if group == nil {
					group = newGroup()
					groups[key] = group
				}
			}
		} else {
			key, err := sqlLimitByKey(limitBy, output.row, output.group)
			if err != nil {
				return nil, false, err
			}
			group = groups[key]
			if group == nil {
				group = newGroup()
				groups[key] = group
			}
		}

		candidate := sqlTopNStreamItem{row: output.row, group: output.group, ordinal: ordinal}
		if len(order) == 1 {
			value := evalOutputOrder(order[0].expr, output.row, output.group)
			if err := sqlExpressionError(value); err != nil {
				return nil, false, err
			}
			candidate.key = value
		} else {
			candidate.keys = make([]interface{}, len(order))
			for index, item := range order {
				value := evalOutputOrder(item.expr, output.row, output.group)
				if err := sqlExpressionError(value); err != nil {
					return nil, false, err
				}
				candidate.keys[index] = value
			}
		}
		if group.Len() < limitBy.limit {
			heap.Push(group, candidate)
			continue
		}
		if sqlTopNStreamBefore(candidate, group.items[0], order) {
			group.items[0] = candidate
			heap.Fix(group, 0)
		}
	}

	selectedItems := make([]sqlTopNStreamItem, 0, len(groups)*limitBy.limit+len(simpleGroups)*limitBy.limit)
	for _, group := range groups {
		selectedItems = append(selectedItems, group.items...)
	}
	for _, group := range simpleGroups {
		selectedItems = append(selectedItems, group.items...)
	}
	sort.SliceStable(selectedItems, func(left, right int) bool {
		return sqlTopNStreamBefore(selectedItems[left], selectedItems[right], order)
	})
	selected := make([]sqlQueryOutput, 0, len(selectedItems))
	for _, item := range selectedItems {
		selected = append(selected, sqlQueryOutput{row: item.row, group: item.group})
	}
	return selected, true, nil
}

func sqlQueryOutputsBytes(outputs []sqlQueryOutput) int {
	bytes := 0
	for _, output := range outputs {
		bytes += sqlRowBytes(output.row)
	}
	return bytes
}
