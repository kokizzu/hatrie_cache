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
	if len(query.groupingSets) != 0 || len(query.groupingDimensions) != 0 || query.having.kind != "" || query.limitBy != nil || query.limitWithTies || sqlQueryHasWithFill(query) {
		return fmt.Errorf("%w: query requires materialized state", ErrSQLNativeDataflowUnsupported)
	}
	if query.where.kind != "" && !sqlStreamScalarExpr(query.where) {
		return fmt.Errorf("%w: WHERE expression is not scalar", ErrSQLNativeDataflowUnsupported)
	}
	if len(query.orderBy) != 0 {
		if len(query.groupBy) != 0 {
			if _, ok := nativeSQLDataflowGroupedOrderedPlanFor(query); !ok {
				return fmt.Errorf("%w: grouped ordered query shape", ErrSQLNativeDataflowUnsupported)
			}
			return nil
		}
		if _, ok := nativeSQLDataflowOrderedPlanFor(query); !ok {
			return fmt.Errorf("%w: ordered query shape", ErrSQLNativeDataflowUnsupported)
		}
		return nil
	}
	hasOutputWindow := query.limit >= 0 || query.offset > 0
	if query.distinct {
		if hasOutputWindow {
			return fmt.Errorf("%w: DISTINCT query requires materialized state", ErrSQLNativeDataflowUnsupported)
		}
		if _, ok := nativeSQLDataflowDistinctPlanFor(query); !ok {
			return fmt.Errorf("%w: DISTINCT query shape", ErrSQLNativeDataflowUnsupported)
		}
		return nil
	}
	if len(query.groupBy) != 0 {
		if hasOutputWindow {
			return fmt.Errorf("%w: grouped query requires materialized state", ErrSQLNativeDataflowUnsupported)
		}
		if _, ok := nativeSQLDataflowGroupPlanFor(query); !ok {
			return fmt.Errorf("%w: grouped query shape", ErrSQLNativeDataflowUnsupported)
		}
		return nil
	}
	if _, ok := nativeSQLDataflowAggregatePlan(query); ok {
		if hasOutputWindow {
			return fmt.Errorf("%w: aggregate query requires materialized state", ErrSQLNativeDataflowUnsupported)
		}
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

type nativeSQLDataflowGroupedOrderedPlan struct {
	group           nativeSQLDataflowGroupPlan
	orders          []sqlOrder
	orderProjection []int
}

type nativeSQLDataflowDistinctPlan struct {
	field sqlExpr
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
	if query == nil || !query.distinct || len(query.selects) != 1 || query.selects[0].expr.kind != "field" || query.where.window != nil || sqlExprHasAggregate(query.where) || sqlExprHasCustomFunction(query.where, nil) {
		return nativeSQLDataflowDistinctPlan{}, false
	}
	return nativeSQLDataflowDistinctPlan{field: query.selects[0].expr}, true
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

func nativeSQLDataflowGroupedOrderedPlanFor(query *sqlQuery) (nativeSQLDataflowGroupedOrderedPlan, bool) {
	if query == nil || query.limit < 0 || query.limitWithTies || len(query.orderBy) == 0 || len(query.groupBy) != 1 || query.distinct || sqlQueryHasWindow(query) || query.where.window != nil || sqlExprHasAggregate(query.where) || sqlExprHasCustomFunction(query.where, nil) {
		return nativeSQLDataflowGroupedOrderedPlan{}, false
	}
	group, ok := nativeSQLDataflowGroupPlanFor(query)
	if !ok {
		return nativeSQLDataflowGroupedOrderedPlan{}, false
	}
	columns := sqlColumns(query.selects)
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
		orders:          append([]sqlOrder(nil), query.orderBy...),
		orderProjection: orderProjection,
	}, true
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
	if plan, ok := nativeSQLDataflowGroupedOrderedPlanFor(query); ok {
		return executeNativeSQLDataflowGroupedOrdered(ctx, query, initial, plan)
	}
	if plan, ok := nativeSQLDataflowOrderedPlanFor(query); ok {
		return executeNativeSQLDataflowOrdered(ctx, query, initial, plan)
	}
	if plan, ok := nativeSQLDataflowDistinctPlanFor(query); ok {
		return executeNativeSQLDataflowDistinct(ctx, query, initial, plan)
	}
	if plan, ok := nativeSQLDataflowGroupPlanFor(query); ok {
		return executeNativeSQLDataflowGroups(ctx, query, initial, plan)
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
			return nil, fmt.Errorf("%w: GROUP BY key type %T", ErrSQLNativeDataflowUnsupported, value)
		}
		groupIndex := -1
		if isNull {
			groupIndex = nullGroup
		} else if existing, found := indexes[groupValue]; found {
			groupIndex = existing
		}
		if groupIndex < 0 {
			groupIndex = len(groups)
			if isNull {
				nullGroup = groupIndex
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

func executeNativeSQLDataflowDistinct(ctx context.Context, query *sqlQuery, initial []SQLRow, plan nativeSQLDataflowDistinctPlan) ([]SQLRow, error) {
	seen := make(map[int64]struct{}, len(initial))
	seenNull := false
	columns := sqlColumns(query.selects)
	result := make([]SQLRow, 0, len(initial))
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
			return nil, fmt.Errorf("%w: DISTINCT key type %T", ErrSQLNativeDataflowUnsupported, value)
		}
		if isNull {
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
		result = append(result, SQLRow{columns[0]: value})
	}
	return result, nil
}
