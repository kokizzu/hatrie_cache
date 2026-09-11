package hatSql

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// ErrSQLNativeDataflowUnsupported reports a compiled query that cannot use the
// built-in batch runtime without changing SQL semantics.
var ErrSQLNativeDataflowUnsupported = errors.New("hatSql: native dataflow shape is unsupported")

// CompileNativeDataflow creates an opt-in built-in executor for a compiled
// single-source query. Execute receives an already-resolved source batch,
// then fuses supported filter/project, finite ordered pages, global aggregate,
// grouped aggregate, and distinct stages without invoking the resolver. The
// regular SQL executor remains the path for all other query shapes.
func (query *CompiledSQLQuery) CompileNativeDataflow() (*SQLDataflowExecutor, error) {
	if query == nil || query.template == nil {
		return nil, fmt.Errorf("compiled SQL query is required")
	}
	if err := validateNativeSQLDataflowQuery(query.template); err != nil {
		return nil, err
	}
	return &SQLDataflowExecutor{
		plan:        query.LowerDataflow(),
		nativeQuery: query.template,
	}, nil
}

func validateNativeSQLDataflowQuery(query *sqlQuery) error {
	if query == nil || query.from == nil {
		return fmt.Errorf("%w: one source is required", ErrSQLNativeDataflowUnsupported)
	}
	if query.from.kind != "CACHE" && query.from.kind != "KEYS" {
		return fmt.Errorf("%w: source kind %q", ErrSQLNativeDataflowUnsupported, query.from.kind)
	}
	if query.explain || query.sample != nil || len(query.ctes) != 0 || len(query.joins) != 0 || len(query.unions) != 0 {
		return fmt.Errorf("%w: query contains non-scalar stages", ErrSQLNativeDataflowUnsupported)
	}
	if len(query.groupingSets) != 0 || len(query.groupingDimensions) != 0 || query.limitBy != nil || query.limitWithTies || sqlQueryHasWithFill(query) {
		return fmt.Errorf("%w: query requires materialized state", ErrSQLNativeDataflowUnsupported)
	}
	if query.where.kind != "" && !sqlStreamScalarExpr(query.where) {
		return fmt.Errorf("%w: WHERE expression is not scalar", ErrSQLNativeDataflowUnsupported)
	}
	if len(query.groupBy) != 0 && len(query.orderBy) != 0 {
		if len(query.groupBy) == 2 {
			if _, ok := nativeSQLDataflowCompositeGroupedOrderedPlanFor(query); !ok {
				return fmt.Errorf("%w: grouped ordered query shape", ErrSQLNativeDataflowUnsupported)
			}
		} else if _, ok := nativeSQLDataflowGroupedOrderedPlanFor(query); !ok {
			return fmt.Errorf("%w: grouped ordered query shape", ErrSQLNativeDataflowUnsupported)
		}
		return nil
	}
	if query.having.kind != "" {
		return fmt.Errorf("%w: query requires materialized state", ErrSQLNativeDataflowUnsupported)
	}
	if len(query.orderBy) != 0 {
		if _, ok := nativeSQLDataflowOrderedPlanFor(query); !ok {
			return fmt.Errorf("%w: ordered query shape", ErrSQLNativeDataflowUnsupported)
		}
		return nil
	}
	hasOutputWindow := query.limit >= 0 || query.offset > 0
	if query.distinct {
		if _, ok := nativeSQLDataflowDistinctPlanFor(query); !ok {
			return fmt.Errorf("%w: DISTINCT query shape", ErrSQLNativeDataflowUnsupported)
		}
		return nil
	}
	if len(query.groupBy) != 0 {
		if hasOutputWindow {
			return fmt.Errorf("%w: grouped query requires materialized state", ErrSQLNativeDataflowUnsupported)
		}
		if len(query.groupBy) == 2 {
			if _, ok := nativeSQLDataflowCompositeGroupPlanFor(query); !ok {
				return fmt.Errorf("%w: grouped query shape", ErrSQLNativeDataflowUnsupported)
			}
		} else if _, ok := nativeSQLDataflowGroupPlanFor(query); !ok {
			return fmt.Errorf("%w: grouped query shape", ErrSQLNativeDataflowUnsupported)
		}
		return nil
	}
	if _, ok := nativeSQLDataflowAggregatePlan(query); ok {
		return nil
	}
	if len(query.selects) == 0 {
		return fmt.Errorf("%w: projection is empty", ErrSQLNativeDataflowUnsupported)
	}
	for _, item := range query.selects {
		if item.expr.kind == "star" || !sqlStreamScalarExpr(item.expr) {
			return fmt.Errorf("%w: SELECT expression is not scalar", ErrSQLNativeDataflowUnsupported)
		}
	}
	return nil
}

type nativeSQLDataflowGroupPlan struct {
	group               sqlExpr
	projectionAggregate []int
	aggregates          []sqlStreamAggregate
}

type nativeSQLDataflowCompositeGroupPlan struct {
	groups              [2]sqlExpr
	projectionGroup     []int
	projectionAggregate []int
	aggregates           []sqlStreamAggregate
}

type nativeSQLDataflowGroupedOrderedPlan struct {
	group           nativeSQLDataflowGroupPlan
	having          sqlExpr
	orders          []sqlOrder
	orderProjection []int
}

type nativeSQLDataflowCompositeGroupedOrderedPlan struct {
	group           nativeSQLDataflowCompositeGroupPlan
	having          sqlExpr
	orders          []sqlOrder
	orderProjection []int
}

type nativeSQLDataflowDistinctPlan struct {
	field      sqlExpr
	fields     [2]sqlExpr
	fieldCount int
}

type nativeSQLDataflowOrderedPlan struct {
	orders []sqlOrder
}

func nativeSQLDataflowOrderedPlanFor(query *sqlQuery) (nativeSQLDataflowOrderedPlan, bool) {
	if query == nil || query.limit < 0 || query.limitWithTies || len(query.orderBy) == 0 || query.distinct || len(query.groupBy) != 0 || sqlQueryHasAggregate(query) || sqlQueryHasWindow(query) || query.where.window != nil || sqlExprHasAggregate(query.where) || sqlExprHasCustomFunction(query.where, nil) {
		return nativeSQLDataflowOrderedPlan{}, false
	}
	if len(query.selects) == 0 {
		return nativeSQLDataflowOrderedPlan{}, false
	}
	for _, order := range query.orderBy {
		if order.expr.kind != "field" {
			return nativeSQLDataflowOrderedPlan{}, false
		}
		if order.expr.qualifier == "" {
			for _, item := range query.selects {
				if item.alias != "" && strings.EqualFold(item.alias, order.expr.name) {
					return nativeSQLDataflowOrderedPlan{}, false
				}
			}
		}
	}
	for _, item := range query.selects {
		if item.expr.kind == "star" || !sqlStreamScalarExpr(item.expr) {
			return nativeSQLDataflowOrderedPlan{}, false
		}
	}
	return nativeSQLDataflowOrderedPlan{orders: append([]sqlOrder(nil), query.orderBy...)}, true
}

func nativeSQLDataflowDistinctPlanFor(query *sqlQuery) (nativeSQLDataflowDistinctPlan, bool) {
	if query == nil || !query.distinct || len(query.selects) == 0 || len(query.selects) > 2 || query.where.window != nil || sqlExprHasAggregate(query.where) || sqlExprHasCustomFunction(query.where, nil) {
		return nativeSQLDataflowDistinctPlan{}, false
	}
	if len(query.selects) == 1 {
		if query.selects[0].expr.kind != "field" {
			return nativeSQLDataflowDistinctPlan{}, false
		}
		return nativeSQLDataflowDistinctPlan{field: query.selects[0].expr, fieldCount: 1}, true
	}
	plan := nativeSQLDataflowDistinctPlan{fieldCount: 2}
	for index, item := range query.selects {
		if item.expr.kind != "field" {
			return nativeSQLDataflowDistinctPlan{}, false
		}
		plan.fields[index] = item.expr
	}
	return plan, true
}

func nativeSQLDataflowGroupPlanFor(query *sqlQuery) (nativeSQLDataflowGroupPlan, bool) {
	if query == nil || len(query.groupBy) != 1 || query.groupBy[0].kind != "field" || len(query.selects) == 0 || query.where.window != nil || sqlExprHasAggregate(query.where) || sqlExprHasCustomFunction(query.where, nil) {
		return nativeSQLDataflowGroupPlan{}, false
	}
	plan := nativeSQLDataflowGroupPlan{
		group:               query.groupBy[0],
		projectionAggregate: make([]int, len(query.selects)),
	}
	for index := range plan.projectionAggregate {
		plan.projectionAggregate[index] = -1
	}
	hasGroupProjection := false
	for index, item := range query.selects {
		if sqlSameField(item.expr, plan.group) {
			hasGroupProjection = true
			continue
		}
		aggregate, ok := nativeSQLDataflowAggregateExpression(item.expr)
		if !ok {
			return nativeSQLDataflowGroupPlan{}, false
		}
		plan.projectionAggregate[index] = len(plan.aggregates)
		plan.aggregates = append(plan.aggregates, aggregate)
	}
	if !hasGroupProjection {
		return nativeSQLDataflowGroupPlan{}, false
	}
	return plan, true
}

func nativeSQLDataflowCompositeGroupPlanFor(query *sqlQuery) (nativeSQLDataflowCompositeGroupPlan, bool) {
	if query == nil || len(query.groupBy) != 2 || len(query.selects) == 0 || query.groupBy[0].kind != "field" || query.groupBy[1].kind != "field" || query.where.window != nil || sqlExprHasAggregate(query.where) || sqlExprHasCustomFunction(query.where, nil) {
		return nativeSQLDataflowCompositeGroupPlan{}, false
	}
	plan := nativeSQLDataflowCompositeGroupPlan{
		projectionGroup:     make([]int, len(query.selects)),
		projectionAggregate: make([]int, len(query.selects)),
	}
	for index := range plan.projectionGroup {
		plan.projectionGroup[index] = -1
		plan.projectionAggregate[index] = -1
	}
	plan.groups[0] = query.groupBy[0]
	plan.groups[1] = query.groupBy[1]
	hasGroupProjection := [2]bool{}
	for index, item := range query.selects {
		groupProjection := -1
		for groupIndex, group := range plan.groups {
			if !sqlSameField(item.expr, group) {
				continue
			}
			if hasGroupProjection[groupIndex] {
				return nativeSQLDataflowCompositeGroupPlan{}, false
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
			return nativeSQLDataflowCompositeGroupPlan{}, false
		}
		plan.projectionAggregate[index] = len(plan.aggregates)
		plan.aggregates = append(plan.aggregates, aggregate)
	}
	if !hasGroupProjection[0] || !hasGroupProjection[1] {
		return nativeSQLDataflowCompositeGroupPlan{}, false
	}
	return plan, true
}

func nativeSQLDataflowGroupedOrderedPlanFor(query *sqlQuery) (nativeSQLDataflowGroupedOrderedPlan, bool) {
	if query == nil || query.limit < 0 || query.limitWithTies || len(query.orderBy) == 0 || len(query.groupBy) != 1 || query.distinct || sqlQueryHasWindow(query) || query.where.window != nil || sqlExprHasAggregate(query.where) || sqlExprHasCustomFunction(query.where, nil) {
		return nativeSQLDataflowGroupedOrderedPlan{}, false
	}
	group, ok := nativeSQLDataflowGroupPlanFor(query)
	if !ok {
		return nativeSQLDataflowGroupedOrderedPlan{}, false
	}
	columns := sqlColumns(query.selects)
	having := query.having
	if having.kind != "" {
		var ok bool
		having, ok = nativeSQLDataflowRewriteGroupedHaving(having, query, columns)
		if !ok || !sqlStreamScalarExpr(having) || sqlExprHasAggregate(having) || sqlExprHasCustomFunction(having, nil) {
			return nativeSQLDataflowGroupedOrderedPlan{}, false
		}
	}
	orderProjection := make([]int, len(query.orderBy))
	for orderIndex, order := range query.orderBy {
		if order.expr.kind != "field" || order.expr.qualifier != "" {
			return nativeSQLDataflowGroupedOrderedPlan{}, false
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
				return nativeSQLDataflowGroupedOrderedPlan{}, false
			}
			projection = selectIndex
		}
		if projection < 0 {
			return nativeSQLDataflowGroupedOrderedPlan{}, false
		}
		orderProjection[orderIndex] = projection
	}
	return nativeSQLDataflowGroupedOrderedPlan{
		group:           group,
		having:          having,
		orders:          append([]sqlOrder(nil), query.orderBy...),
		orderProjection: orderProjection,
	}, true
}

func nativeSQLDataflowCompositeGroupedOrderedPlanFor(query *sqlQuery) (nativeSQLDataflowCompositeGroupedOrderedPlan, bool) {
	if query == nil || query.limit < 0 || query.limitWithTies || len(query.orderBy) == 0 || len(query.groupBy) != 2 || query.distinct || sqlQueryHasWindow(query) || query.where.window != nil || sqlExprHasAggregate(query.where) || sqlExprHasCustomFunction(query.where, nil) {
		return nativeSQLDataflowCompositeGroupedOrderedPlan{}, false
	}
	group, ok := nativeSQLDataflowCompositeGroupPlanFor(query)
	if !ok {
		return nativeSQLDataflowCompositeGroupedOrderedPlan{}, false
	}
	columns := sqlColumns(query.selects)
	having := query.having
	if having.kind != "" {
		var ok bool
		having, ok = nativeSQLDataflowRewriteGroupedHaving(having, query, columns)
		if !ok || !sqlStreamScalarExpr(having) || sqlExprHasAggregate(having) || sqlExprHasCustomFunction(having, nil) {
			return nativeSQLDataflowCompositeGroupedOrderedPlan{}, false
		}
	}
	orderProjection := make([]int, len(query.orderBy))
	for orderIndex, order := range query.orderBy {
		if order.expr.kind != "field" || order.expr.qualifier != "" {
			return nativeSQLDataflowCompositeGroupedOrderedPlan{}, false
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
				return nativeSQLDataflowCompositeGroupedOrderedPlan{}, false
			}
			projection = selectIndex
		}
		if projection < 0 {
			return nativeSQLDataflowCompositeGroupedOrderedPlan{}, false
		}
		orderProjection[orderIndex] = projection
	}
	return nativeSQLDataflowCompositeGroupedOrderedPlan{
		group:           group,
		having:          having,
		orders:          append([]sqlOrder(nil), query.orderBy...),
		orderProjection: orderProjection,
	}, true
}

func nativeSQLDataflowRewriteGroupedHaving(expr sqlExpr, query *sqlQuery, columns []string) (sqlExpr, bool) {
	if expr.kind == "field" || expr.kind == "star" || expr.query != nil || expr.window != nil || expr.filter != nil || len(expr.cases) != 0 {
		return sqlExpr{}, false
	}
	if expr.kind == "func" {
		for index, item := range query.selects {
			if item.expr.kind != "func" {
				continue
			}
			if _, ok := nativeSQLDataflowAggregateExpression(item.expr); !ok {
				continue
			}
			if sqlExpressionsStructurallyEqual(expr, item.expr) {
				return sqlExpr{kind: "field", name: columns[index]}, true
			}
		}
		name := strings.ToUpper(expr.name)
		switch name {
		case "COUNT", "SUM", "AVG", "MIN", "MAX":
			return sqlExpr{}, false
		}
	}
	rewritten := expr
	if expr.left != nil {
		left, ok := nativeSQLDataflowRewriteGroupedHaving(*expr.left, query, columns)
		if !ok {
			return sqlExpr{}, false
		}
		rewritten.left = &left
	}
	if expr.right != nil {
		right, ok := nativeSQLDataflowRewriteGroupedHaving(*expr.right, query, columns)
		if !ok {
			return sqlExpr{}, false
		}
		rewritten.right = &right
	}
	if len(expr.args) != 0 {
		rewritten.args = make([]sqlExpr, len(expr.args))
		for index, argument := range expr.args {
			rewrittenArgument, ok := nativeSQLDataflowRewriteGroupedHaving(argument, query, columns)
			if !ok {
				return sqlExpr{}, false
			}
			rewritten.args[index] = rewrittenArgument
		}
	}
	return rewritten, true
}

func nativeSQLDataflowAggregateExpression(expr sqlExpr) (sqlStreamAggregate, bool) {
	if expr.kind != "func" || expr.window != nil || expr.filter != nil || sqlExprHasCustomFunction(expr, nil) {
		return sqlStreamAggregate{}, false
	}
	aggregate := sqlStreamAggregate{name: expr.name}
	switch expr.name {
	case "COUNT":
		if len(expr.args) > 1 {
			return sqlStreamAggregate{}, false
		}
		if len(expr.args) == 1 && expr.args[0].kind != "star" {
			argument := expr.args[0]
			if !sqlStreamScalarExpr(argument) {
				return sqlStreamAggregate{}, false
			}
			aggregate.arg = &argument
		}
	case "SUM", "AVG", "MIN", "MAX":
		if len(expr.args) != 1 || expr.args[0].kind == "star" || !sqlStreamScalarExpr(expr.args[0]) {
			return sqlStreamAggregate{}, false
		}
		argument := expr.args[0]
		aggregate.arg = &argument
	default:
		return sqlStreamAggregate{}, false
	}
	return aggregate, true
}

func nativeSQLDataflowAggregatePlan(query *sqlQuery) ([]sqlStreamAggregate, bool) {
	if query == nil || len(query.groupBy) != 0 || len(query.selects) == 0 || query.where.window != nil || sqlExprHasAggregate(query.where) || sqlExprHasCustomFunction(query.where, nil) {
		return nil, false
	}
	aggregates := make([]sqlStreamAggregate, len(query.selects))
	for index, item := range query.selects {
		aggregate, ok := nativeSQLDataflowAggregateExpression(item.expr)
		if !ok {
			return nil, false
		}
		aggregates[index] = aggregate
	}
	return aggregates, true
}

func executeNativeSQLDataflow(ctx context.Context, query *sqlQuery, initial []SQLRow) ([]SQLRow, error) {
	if plan, ok := nativeSQLDataflowCompositeGroupedOrderedPlanFor(query); ok {
		return executeNativeSQLDataflowCompositeGroupedOrdered(ctx, query, initial, plan)
	}
	if plan, ok := nativeSQLDataflowGroupedOrderedPlanFor(query); ok {
		return executeNativeSQLDataflowGroupedOrdered(ctx, query, initial, plan)
	}
	if plan, ok := nativeSQLDataflowOrderedPlanFor(query); ok {
		return executeNativeSQLDataflowOrdered(ctx, query, initial, plan)
	}
	if plan, ok := nativeSQLDataflowDistinctPlanFor(query); ok {
		if plan.fieldCount == 2 {
			return executeNativeSQLDataflowCompositeDistinct(ctx, query, initial, plan)
		}
		return executeNativeSQLDataflowDistinct(ctx, query, initial, plan)
	}
	if plan, ok := nativeSQLDataflowGroupPlanFor(query); ok {
		return executeNativeSQLDataflowGroups(ctx, query, initial, plan)
	}
	if plan, ok := nativeSQLDataflowCompositeGroupPlanFor(query); ok {
		return executeNativeSQLDataflowCompositeGroups(ctx, query, initial, plan)
	}
	if aggregates, ok := nativeSQLDataflowAggregatePlan(query); ok {
		return executeNativeSQLDataflowAggregates(ctx, query, initial, aggregates)
	}
	columns := sqlColumns(query.selects)
	capacity := len(initial)
	if query.limit >= 0 && capacity > query.limit {
		capacity = query.limit
	}
	result := make([]SQLRow, 0, capacity)
	offset := query.offset
	for index, input := range initial {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if query.limit == 0 {
			return result, nil
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
		if query.where.kind != "" {
			matched, err := evalSQLStreamExpr(query.where, execRow, nil)
			if err != nil {
				return nil, fmt.Errorf("native dataflow WHERE row %d: %w", index+1, err)
			}
			if !sqlTruthy(matched) {
				continue
			}
		}
		if offset > 0 {
			offset--
			continue
		}
		projected := make(SQLRow, len(columns))
		for selectIndex, item := range query.selects {
			value, err := evalSQLStreamExpr(item.expr, execRow, nil)
			if err != nil {
				return nil, fmt.Errorf("native dataflow SELECT row %d column %d: %w", index+1, selectIndex+1, err)
			}
			projected[columns[selectIndex]] = value
		}
		result = append(result, projected)
		if query.limit >= 0 && len(result) >= query.limit {
			break
		}
	}
	return result, nil
}

func executeNativeSQLDataflowOrdered(ctx context.Context, query *sqlQuery, initial []SQLRow, plan nativeSQLDataflowOrderedPlan) ([]SQLRow, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if query.limit == 0 {
		return []SQLRow{}, nil
	}
	capacity := sqlTopNStreamCapacity(query, len(initial))
	candidates := sqlTopNStreamHeap{items: make([]sqlTopNStreamItem, 0, capacity), order: plan.orders}
	heap.Init(&candidates)
	ordinal := 0
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
		if query.where.kind != "" {
			matched, err := evalSQLStreamExpr(query.where, execRow, nil)
			if err != nil {
				return nil, fmt.Errorf("native dataflow WHERE row %d: %w", index+1, err)
			}
			if !sqlTruthy(matched) {
				continue
			}
		}
		candidate := sqlTopNStreamItem{row: row, ordinal: ordinal}
		if len(plan.orders) == 1 {
			orderValue, err := evalSQLStreamExpr(plan.orders[0].expr, execRow, nil)
			if err != nil {
				return nil, fmt.Errorf("native dataflow ORDER BY row %d: %w", index+1, err)
			}
			candidate.key = orderValue
		} else {
			candidate.keys = make([]interface{}, len(plan.orders))
			for orderIndex, order := range plan.orders {
				orderValue, err := evalSQLStreamExpr(order.expr, execRow, nil)
				if err != nil {
					return nil, fmt.Errorf("native dataflow ORDER BY row %d column %d: %w", index+1, orderIndex+1, err)
				}
				candidate.keys[orderIndex] = orderValue
			}
		}
		ordinal++
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
	columns := sqlColumns(query.selects)
	result := make([]SQLRow, 0, end-start)
	for _, candidate := range candidates.items[start:end] {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		execRow := newSQLSingleSourceExecRow(query.from.alias, candidate.row)
		projected := make(SQLRow, len(columns))
		for selectIndex, item := range query.selects {
			value, err := evalSQLStreamExpr(item.expr, execRow, nil)
			if err != nil {
				return nil, fmt.Errorf("native dataflow SELECT row column %d: %w", selectIndex+1, err)
			}
			projected[columns[selectIndex]] = value
		}
		result = append(result, projected)
	}
	return result, nil
}

func executeNativeSQLDataflowGroupedOrdered(ctx context.Context, query *sqlQuery, initial []SQLRow, plan nativeSQLDataflowGroupedOrderedPlan) ([]SQLRow, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if query.limit == 0 {
		return []SQLRow{}, nil
	}
	grouped, err := executeNativeSQLDataflowGroups(ctx, query, initial, plan.group)
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

func executeNativeSQLDataflowCompositeGroupedOrdered(ctx context.Context, query *sqlQuery, initial []SQLRow, plan nativeSQLDataflowCompositeGroupedOrderedPlan) ([]SQLRow, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if query.limit == 0 {
		return []SQLRow{}, nil
	}
	grouped, err := executeNativeSQLDataflowCompositeGroups(ctx, query, initial, plan.group)
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

func executeNativeSQLDataflowAggregates(ctx context.Context, query *sqlQuery, initial []SQLRow, aggregates []sqlStreamAggregate) ([]SQLRow, error) {
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
			value := evalSQLExpr(query.where, []sqlExecRow{execRow}, execRow)
			if err := sqlExpressionError(value); err != nil {
				return nil, fmt.Errorf("native dataflow WHERE row %d: %w", index+1, err)
			}
			if !sqlTruthy(value) {
				continue
			}
		}
		for aggregateIndex := range aggregates {
			if err := aggregates[aggregateIndex].addWithGroup(execRows, execRow); err != nil {
				return nil, fmt.Errorf("native dataflow aggregate row %d column %d: %w", index+1, aggregateIndex+1, err)
			}
		}
	}
	columns := sqlColumns(query.selects)
	row := SQLRow{}
	for index, aggregate := range aggregates {
		row[columns[index]] = aggregate.result()
	}
	if query.limit == 0 || query.offset > 0 {
		return []SQLRow{}, nil
	}
	return []SQLRow{row}, nil
}

type nativeSQLDataflowGroupState struct {
	key             int64
	value           interface{}
	null            bool
	aggregateOffset int
}

func nativeSQLDataflowIntegerGroupKey(value interface{}) (int64, bool, bool) {
	switch value := value.(type) {
	case nil:
		return 0, true, true
	case int:
		return int64(value), false, true
	case int8:
		return int64(value), false, true
	case int16:
		return int64(value), false, true
	case int32:
		return int64(value), false, true
	case int64:
		return value, false, true
	case uint:
		if uint64(value) > uint64(^uint64(0)>>1) {
			return 0, false, false
		}
		return int64(value), false, true
	case uint8:
		return int64(value), false, true
	case uint16:
		return int64(value), false, true
	case uint32:
		return int64(value), false, true
	case uint64:
		if value > uint64(^uint64(0)>>1) {
			return 0, false, false
		}
		return int64(value), false, true
	default:
		return 0, false, false
	}
}

func executeNativeSQLDataflowGroups(ctx context.Context, query *sqlQuery, initial []SQLRow, plan nativeSQLDataflowGroupPlan) ([]SQLRow, error) {
	indexes := make(map[int64]int, len(initial))
	var stringIndexes map[string]int
	groups := make([]nativeSQLDataflowGroupState, 0)
	aggregates := make([]sqlStreamAggregate, 0)
	execRows := make([]sqlExecRow, 1)
	nullGroup := -1
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
			value := evalSQLExpr(query.where, []sqlExecRow{execRow}, execRow)
			if err := sqlExpressionError(value); err != nil {
				return nil, fmt.Errorf("native dataflow WHERE row %d: %w", index+1, err)
			}
			if !sqlTruthy(value) {
				continue
			}
		}
		value := evalSQLExpr(plan.group, []sqlExecRow{execRow}, execRow)
		if err := sqlExpressionError(value); err != nil {
			return nil, fmt.Errorf("native dataflow GROUP BY row %d: %w", index+1, err)
		}
		groupValue, isNull, ok := nativeSQLDataflowIntegerGroupKey(value)
		if !ok {
			if _, stringOK := value.(string); !stringOK {
				return nil, fmt.Errorf("%w: GROUP BY key type %T", ErrSQLNativeDataflowUnsupported, value)
			}
		}
		groupIndex := -1
		if isNull {
			groupIndex = nullGroup
		} else if stringValue, stringOK := value.(string); stringOK {
			if existing, found := stringIndexes[stringValue]; found {
				groupIndex = existing
			}
		} else if existing, found := indexes[groupValue]; found {
			groupIndex = existing
		}
		if groupIndex < 0 {
			groupIndex = len(groups)
			if isNull {
				nullGroup = groupIndex
		} else if stringValue, stringOK := value.(string); stringOK {
				if stringIndexes == nil {
					stringIndexes = make(map[string]int, len(initial))
				}
				stringIndexes[stringValue] = groupIndex
			} else {
				indexes[groupValue] = groupIndex
			}
			group := nativeSQLDataflowGroupState{key: groupValue, value: value, null: isNull, aggregateOffset: len(aggregates)}
			groups = append(groups, group)
			aggregates = append(aggregates, plan.aggregates...)
		}
		group := groups[groupIndex]
		for _, aggregateIndex := range plan.projectionAggregate {
			if aggregateIndex < 0 {
				continue
			}
			if err := aggregates[group.aggregateOffset+aggregateIndex].addWithGroup(execRows, execRow); err != nil {
				return nil, fmt.Errorf("native dataflow aggregate row %d: %w", index+1, err)
			}
		}
	}
	columns := sqlColumns(query.selects)
	result := make([]SQLRow, 0, len(groups))
	for _, group := range groups {
		row := make(SQLRow, len(columns))
		for selectIndex, aggregateIndex := range plan.projectionAggregate {
			if aggregateIndex < 0 {
				if group.null {
					row[columns[selectIndex]] = nil
				} else {
					row[columns[selectIndex]] = group.value
				}
				continue
			}
			row[columns[selectIndex]] = aggregates[group.aggregateOffset+aggregateIndex].result()
		}
		result = append(result, row)
	}
	return result, nil
}

type nativeSQLDataflowCompositeGroupState struct {
	values          [2]interface{}
	aggregateOffset int
}

func executeNativeSQLDataflowCompositeGroups(ctx context.Context, query *sqlQuery, initial []SQLRow, plan nativeSQLDataflowCompositeGroupPlan) ([]SQLRow, error) {
	indexes := make(map[nativeSQLDataflowCompositeDistinctKey]int, len(initial))
	groups := make([]nativeSQLDataflowCompositeGroupState, 0)
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
		values := [2]interface{}{}
		keys := nativeSQLDataflowCompositeDistinctKey{}
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
			if groupIndex == 0 {
				keys.first = key
			} else {
				keys.second = key
			}
		}
		groupIndex, found := indexes[keys]
		if !found {
			groupIndex = len(groups)
			indexes[keys] = groupIndex
			groups = append(groups, nativeSQLDataflowCompositeGroupState{
				values:          values,
				aggregateOffset: len(aggregates),
			})
			aggregates = append(aggregates, plan.aggregates...)
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

const (
	nativeSQLDataflowDistinctKeyNull uint8 = iota
	nativeSQLDataflowDistinctKeyInteger
	nativeSQLDataflowDistinctKeyString
)

type nativeSQLDataflowDistinctKey struct {
	kind        uint8
	integer     int64
	stringValue string
}

type nativeSQLDataflowCompositeDistinctKey struct {
	first  nativeSQLDataflowDistinctKey
	second nativeSQLDataflowDistinctKey
}

func nativeSQLDataflowDistinctKeyFor(value interface{}) (nativeSQLDataflowDistinctKey, bool) {
	integer, isNull, ok := nativeSQLDataflowIntegerGroupKey(value)
	if ok {
		if isNull {
			return nativeSQLDataflowDistinctKey{kind: nativeSQLDataflowDistinctKeyNull}, true
		}
		return nativeSQLDataflowDistinctKey{kind: nativeSQLDataflowDistinctKeyInteger, integer: integer}, true
	}
	stringValue, ok := value.(string)
	if !ok {
		return nativeSQLDataflowDistinctKey{}, false
	}
	return nativeSQLDataflowDistinctKey{kind: nativeSQLDataflowDistinctKeyString, stringValue: stringValue}, true
}

func executeNativeSQLDataflowCompositeDistinct(ctx context.Context, query *sqlQuery, initial []SQLRow, plan nativeSQLDataflowDistinctPlan) ([]SQLRow, error) {
	if query.limit == 0 {
		return []SQLRow{}, nil
	}
	offset := query.offset
	seenCapacity := len(initial)
	if query.limit >= 0 && offset < len(initial) {
		remaining := len(initial) - offset
		if query.limit < remaining {
			seenCapacity = offset + query.limit
		}
	}
	resultCapacity := len(initial)
	if query.limit >= 0 && query.limit < resultCapacity {
		resultCapacity = query.limit
	}
	seen := make(map[nativeSQLDataflowCompositeDistinctKey]struct{}, seenCapacity)
	columns := sqlColumns(query.selects)
	result := make([]SQLRow, 0, resultCapacity)
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
		firstValue := evalSQLExpr(plan.fields[0], execRows, execRow)
		if err := sqlExpressionError(firstValue); err != nil {
			return nil, fmt.Errorf("native dataflow DISTINCT row %d column 1: %w", index+1, err)
		}
		secondValue := evalSQLExpr(plan.fields[1], execRows, execRow)
		if err := sqlExpressionError(secondValue); err != nil {
			return nil, fmt.Errorf("native dataflow DISTINCT row %d column 2: %w", index+1, err)
		}
		firstKey, ok := nativeSQLDataflowDistinctKeyFor(firstValue)
		if !ok {
			return nil, fmt.Errorf("%w: DISTINCT key type %T", ErrSQLNativeDataflowUnsupported, firstValue)
		}
		secondKey, ok := nativeSQLDataflowDistinctKeyFor(secondValue)
		if !ok {
			return nil, fmt.Errorf("%w: DISTINCT key type %T", ErrSQLNativeDataflowUnsupported, secondValue)
		}
		key := nativeSQLDataflowCompositeDistinctKey{first: firstKey, second: secondKey}
		if _, found := seen[key]; found {
			continue
		}
		seen[key] = struct{}{}
		if offset > 0 {
			offset--
			continue
		}
		projected := make(SQLRow, len(columns))
		projected[columns[0]] = firstValue
		projected[columns[1]] = secondValue
		result = append(result, projected)
		if query.limit >= 0 && len(result) >= query.limit {
			break
		}
	}
	return result, nil
}

func executeNativeSQLDataflowDistinct(ctx context.Context, query *sqlQuery, initial []SQLRow, plan nativeSQLDataflowDistinctPlan) ([]SQLRow, error) {
	if query.limit == 0 {
		return []SQLRow{}, nil
	}
	offset := query.offset
	seenCapacity := len(initial)
	if query.limit >= 0 && offset < len(initial) {
		remaining := len(initial) - offset
		if query.limit < remaining {
			seenCapacity = offset + query.limit
		}
	}
	resultCapacity := len(initial)
	if query.limit >= 0 && query.limit < resultCapacity {
		resultCapacity = query.limit
	}
	seen := make(map[int64]struct{}, seenCapacity)
	var stringSeen map[string]struct{}
	seenNull := false
	columns := sqlColumns(query.selects)
	result := make([]SQLRow, 0, resultCapacity)
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
		value := evalSQLExpr(plan.field, execRows, execRow)
		if err := sqlExpressionError(value); err != nil {
			return nil, fmt.Errorf("native dataflow DISTINCT row %d: %w", index+1, err)
		}
		key, isNull, ok := nativeSQLDataflowIntegerGroupKey(value)
		if !ok {
			stringValue, stringOK := value.(string)
			if !stringOK {
				return nil, fmt.Errorf("%w: DISTINCT key type %T", ErrSQLNativeDataflowUnsupported, value)
			}
			if stringSeen != nil {
				if _, found := stringSeen[stringValue]; found {
					continue
				}
			} else {
				stringSeen = make(map[string]struct{}, len(initial))
			}
			stringSeen[stringValue] = struct{}{}
		} else if isNull {
			if seenNull {
				continue
			}
			seenNull = true
		} else {
			if _, found := seen[key]; found {
				continue
			}
			seen[key] = struct{}{}
		}
		if offset > 0 {
			offset--
			continue
		}
		result = append(result, SQLRow{columns[0]: value})
		if query.limit >= 0 && len(result) >= query.limit {
			break
		}
	}
	return result, nil
}
