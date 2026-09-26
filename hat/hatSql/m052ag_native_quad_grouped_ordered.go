package hatSql

import (
	"container/heap"
	"context"
	"fmt"
	"sort"
	"strings"
)

type nativeSQLDataflowQuadGroupedOrderedPlan struct {
	group           nativeSQLDataflowQuadGroupPlan
	having          sqlExpr
	orders          []sqlOrder
	orderProjection []int
}

func nativeSQLDataflowQuadGroupedOrderedPlanFor(query *sqlQuery) (nativeSQLDataflowQuadGroupedOrderedPlan, bool) {
	if query == nil || query.limit < 0 || query.limitWithTies || len(query.orderBy) == 0 || len(query.groupBy) != 4 || query.distinct || sqlQueryHasWindow(query) || query.where.window != nil || sqlExprHasAggregate(query.where) || sqlExprHasCustomFunction(query.where, nil) {
		return nativeSQLDataflowQuadGroupedOrderedPlan{}, false
	}
	group, ok := nativeSQLDataflowQuadGroupPlanFor(query)
	if !ok {
		return nativeSQLDataflowQuadGroupedOrderedPlan{}, false
	}
	columns := sqlColumns(query.selects)
	having := query.having
	if having.kind != "" {
		having, ok = nativeSQLDataflowRewriteGroupedHaving(having, query, columns)
		if !ok || !sqlStreamScalarExpr(having) || sqlExprHasAggregate(having) || sqlExprHasCustomFunction(having, nil) {
			return nativeSQLDataflowQuadGroupedOrderedPlan{}, false
		}
	}
	orderProjection := make([]int, len(query.orderBy))
	for orderIndex, order := range query.orderBy {
		if order.expr.kind != "field" || order.expr.qualifier != "" {
			return nativeSQLDataflowQuadGroupedOrderedPlan{}, false
		}
		projection := -1
		for selectIndex, item := range query.selects {
			matches := item.alias != "" && strings.EqualFold(item.alias, order.expr.name)
			if !matches && item.alias == "" && strings.EqualFold(columns[selectIndex], order.expr.name) {
				matches = true
			}
			if !matches {
				continue
			}
			if projection >= 0 {
				return nativeSQLDataflowQuadGroupedOrderedPlan{}, false
			}
			projection = selectIndex
		}
		if projection < 0 {
			return nativeSQLDataflowQuadGroupedOrderedPlan{}, false
		}
		orderProjection[orderIndex] = projection
	}
	return nativeSQLDataflowQuadGroupedOrderedPlan{
		group:           group,
		having:          having,
		orders:          append([]sqlOrder(nil), query.orderBy...),
		orderProjection: orderProjection,
	}, true
}

func executeNativeSQLDataflowQuadGroupedOrdered(ctx context.Context, query *sqlQuery, initial []SQLRow, plan nativeSQLDataflowQuadGroupedOrderedPlan) ([]SQLRow, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if query.limit == 0 {
		return []SQLRow{}, nil
	}
	grouped, err := executeNativeSQLDataflowQuadGroups(ctx, query, initial, plan.group)
	if err != nil {
		return nil, err
	}
	capacity := sqlTopNStreamCapacity(query, len(grouped))
	columns := sqlColumns(query.selects)
	candidates := sqlTopNStreamHeap{items: make([]sqlTopNStreamItem, 0, capacity), order: plan.orders}
	heap.Init(&candidates)
	for ordinal, row := range grouped {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if plan.having.kind != "" {
			havingValue, err := evalSQLStreamExpr(plan.having, newSQLSingleSourceExecRow("", row), nil)
			if err != nil {
				return nil, fmt.Errorf("native dataflow HAVING group %d: %w", ordinal+1, err)
			}
			if !sqlTruthy(havingValue) {
				continue
			}
		}
		candidate := sqlTopNStreamItem{row: row, ordinal: ordinal}
		if len(plan.orderProjection) == 1 {
			candidate.key = row[columns[plan.orderProjection[0]]]
		} else {
			candidate.keys = make([]interface{}, len(plan.orderProjection))
			for orderIndex, projection := range plan.orderProjection {
				candidate.keys[orderIndex] = row[columns[projection]]
			}
		}
		if candidates.Len() < capacity {
			heap.Push(&candidates, candidate)
			continue
		}
		if sqlTopNStreamBefore(candidate, candidates.items[0], plan.orders) {
			candidates.items[0] = candidate
			heap.Fix(&candidates, 0)
		}
	}
	sort.SliceStable(candidates.items, func(left, right int) bool {
		return sqlTopNStreamBefore(candidates.items[left], candidates.items[right], plan.orders)
	})
	start := query.offset
	if start > len(candidates.items) {
		start = len(candidates.items)
	}
	end := len(candidates.items)
	if query.limit < end-start {
		end = start + query.limit
	}
	result := make([]SQLRow, 0, end-start)
	for _, candidate := range candidates.items[start:end] {
		result = append(result, candidate.row)
	}
	return result, nil
}

type nativeSQLDataflowQuadGroupPlan struct {
	groups              [4]sqlExpr
	projectionGroup     []int
	projectionAggregate []int
	aggregates          []sqlStreamAggregate
}

func nativeSQLDataflowQuadGroupPlanFor(query *sqlQuery) (nativeSQLDataflowQuadGroupPlan, bool) {
	if query == nil || len(query.groupBy) != 4 || len(query.selects) == 0 || query.groupBy[0].kind != "field" || query.groupBy[1].kind != "field" || query.groupBy[2].kind != "field" || query.groupBy[3].kind != "field" || query.where.window != nil || sqlExprHasAggregate(query.where) || sqlExprHasCustomFunction(query.where, nil) {
		return nativeSQLDataflowQuadGroupPlan{}, false
	}
	plan := nativeSQLDataflowQuadGroupPlan{
		projectionGroup:     make([]int, len(query.selects)),
		projectionAggregate: make([]int, len(query.selects)),
	}
	for index := range plan.projectionGroup {
		plan.projectionGroup[index] = -1
		plan.projectionAggregate[index] = -1
	}
	copy(plan.groups[:], query.groupBy)
	hasGroupProjection := [4]bool{}
	for index, item := range query.selects {
		groupProjection := -1
		for groupIndex, group := range plan.groups {
			if !sqlSameField(item.expr, group) {
				continue
			}
			if hasGroupProjection[groupIndex] {
				return nativeSQLDataflowQuadGroupPlan{}, false
			}
			groupProjection = groupIndex
			break
		}
		if groupProjection >= 0 {
			plan.projectionGroup[index] = groupProjection
			hasGroupProjection[groupProjection] = true
			continue
		}
		aggregate, ok := nativeSQLDataflowAggregateExpression(item.expr)
		if !ok {
			return nativeSQLDataflowQuadGroupPlan{}, false
		}
		plan.projectionAggregate[index] = len(plan.aggregates)
		plan.aggregates = append(plan.aggregates, aggregate)
	}
	for _, present := range hasGroupProjection {
		if !present {
			return nativeSQLDataflowQuadGroupPlan{}, false
		}
	}
	return plan, true
}

type nativeSQLDataflowQuadGroupState struct {
	values          [4]interface{}
	aggregateOffset int
}

func executeNativeSQLDataflowQuadGroups(ctx context.Context, query *sqlQuery, initial []SQLRow, plan nativeSQLDataflowQuadGroupPlan) ([]SQLRow, error) {
	indexes := make(map[nativeSQLDataflowQuadDistinctKey]int, len(initial))
	groups := make([]nativeSQLDataflowQuadGroupState, 0)
	aggregates := make([]sqlStreamAggregate, 0)
	execRows := make([]sqlExecRow, 1)
	for index, input := range initial {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		row := input
		if len(query.from.fieldTypes) != 0 {
			validated, err := validateSQLSourceFieldTypeRow(*query.from, input, index+1)
			if err != nil {
				return nil, err
			}
			row = validated
		}
		execRow := newSQLSingleSourceExecRow(query.from.alias, row)
		execRows[0] = execRow
		if query.where.kind != "" {
			whereValue := evalSQLExpr(query.where, execRows, execRow)
			if err := sqlExpressionError(whereValue); err != nil {
				return nil, fmt.Errorf("native dataflow WHERE row %d: %w", index+1, err)
			}
			if !sqlTruthy(whereValue) {
				continue
			}
		}
		values := [4]interface{}{}
		keys := nativeSQLDataflowQuadDistinctKey{}
		for groupIndex, group := range plan.groups {
			value := evalSQLExpr(group, execRows, execRow)
			if err := sqlExpressionError(value); err != nil {
				return nil, fmt.Errorf("native dataflow GROUP BY row %d column %d: %w", index+1, groupIndex+1, err)
			}
			key, ok := nativeSQLDataflowDistinctKeyFor(value)
			if !ok {
				return nil, fmt.Errorf("%w: GROUP BY key type %T", ErrSQLNativeDataflowUnsupported, value)
			}
			values[groupIndex] = value
			switch groupIndex {
			case 0:
				keys.first = key
			case 1:
				keys.second = key
			case 2:
				keys.third = key
			case 3:
				keys.fourth = key
			}
		}
		groupIndex, found := indexes[keys]
		if !found {
			groupIndex = len(groups)
			indexes[keys] = groupIndex
			groups = append(groups, nativeSQLDataflowQuadGroupState{
				values:          values,
				aggregateOffset: len(aggregates),
			})
			for _, aggregate := range plan.aggregates {
				aggregates = append(aggregates, cloneNativeSQLDataflowAggregate(aggregate))
			}
		}
		group := groups[groupIndex]
		for aggregateIndex := range plan.aggregates {
			if err := aggregates[group.aggregateOffset+aggregateIndex].addWithGroup(execRows, execRow); err != nil {
				return nil, fmt.Errorf("native dataflow aggregate row %d column %d: %w", index+1, aggregateIndex+1, err)
			}
		}
	}
	columns := sqlColumns(query.selects)
	result := make([]SQLRow, 0, len(groups))
	for groupIndex := range groups {
		group := groups[groupIndex]
		row := make(SQLRow, len(columns))
		for selectIndex := range query.selects {
			if projection := plan.projectionGroup[selectIndex]; projection >= 0 {
				row[columns[selectIndex]] = group.values[projection]
				continue
			}
			aggregateIndex := plan.projectionAggregate[selectIndex]
			row[columns[selectIndex]] = aggregates[group.aggregateOffset+aggregateIndex].result()
		}
		result = append(result, row)
	}
	return result, nil
}

type nativeSQLDataflowQuadDistinctKey struct {
	first  nativeSQLDataflowDistinctKey
	second nativeSQLDataflowDistinctKey
	third  nativeSQLDataflowDistinctKey
	fourth nativeSQLDataflowDistinctKey
}
