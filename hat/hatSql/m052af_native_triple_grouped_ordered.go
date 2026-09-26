package hatSql

import (
	"container/heap"
	"context"
	"fmt"
	"sort"
	"strings"
)

type nativeSQLDataflowTripleGroupedOrderedPlan struct {
	group           nativeSQLDataflowTripleGroupPlan
	having          sqlExpr
	orders          []sqlOrder
	orderProjection []int
}

func nativeSQLDataflowTripleGroupedOrderedPlanFor(query *sqlQuery) (nativeSQLDataflowTripleGroupedOrderedPlan, bool) {
	if query == nil || query.limit < 0 || query.limitWithTies || len(query.orderBy) == 0 || len(query.groupBy) != 3 || query.distinct || sqlQueryHasWindow(query) || query.where.window != nil || sqlExprHasAggregate(query.where) || sqlExprHasCustomFunction(query.where, nil) {
		return nativeSQLDataflowTripleGroupedOrderedPlan{}, false
	}
	group, ok := nativeSQLDataflowTripleGroupPlanFor(query)
	if !ok {
		return nativeSQLDataflowTripleGroupedOrderedPlan{}, false
	}
	columns := sqlColumns(query.selects)
	having := query.having
	if having.kind != "" {
		having, ok = nativeSQLDataflowRewriteGroupedHaving(having, query, columns)
		if !ok || !sqlStreamScalarExpr(having) || sqlExprHasAggregate(having) || sqlExprHasCustomFunction(having, nil) {
			return nativeSQLDataflowTripleGroupedOrderedPlan{}, false
		}
	}
	orderProjection := make([]int, len(query.orderBy))
	for orderIndex, order := range query.orderBy {
		if order.expr.kind != "field" || order.expr.qualifier != "" {
			return nativeSQLDataflowTripleGroupedOrderedPlan{}, false
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
				return nativeSQLDataflowTripleGroupedOrderedPlan{}, false
			}
			projection = selectIndex
		}
		if projection < 0 {
			return nativeSQLDataflowTripleGroupedOrderedPlan{}, false
		}
		orderProjection[orderIndex] = projection
	}
	return nativeSQLDataflowTripleGroupedOrderedPlan{
		group:           group,
		having:          having,
		orders:          append([]sqlOrder(nil), query.orderBy...),
		orderProjection: orderProjection,
	}, true
}

func executeNativeSQLDataflowTripleGroupedOrdered(ctx context.Context, query *sqlQuery, initial []SQLRow, plan nativeSQLDataflowTripleGroupedOrderedPlan) ([]SQLRow, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if query.limit == 0 {
		return []SQLRow{}, nil
	}
	grouped, err := executeNativeSQLDataflowTripleGroups(ctx, query, initial, plan.group)
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
