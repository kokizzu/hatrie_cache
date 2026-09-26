package hatSql

import (
	"context"
	"fmt"
)

type nativeSQLDataflowTripleGroupPlan struct {
	groups              [3]sqlExpr
	projectionGroup     []int
	projectionAggregate []int
	aggregates          []sqlStreamAggregate
}

func nativeSQLDataflowTripleGroupPlanFor(query *sqlQuery) (nativeSQLDataflowTripleGroupPlan, bool) {
	if query == nil || len(query.groupBy) != 3 || len(query.selects) == 0 || query.groupBy[0].kind != "field" || query.groupBy[1].kind != "field" || query.groupBy[2].kind != "field" || query.where.window != nil || sqlExprHasAggregate(query.where) || sqlExprHasCustomFunction(query.where, nil) {
		return nativeSQLDataflowTripleGroupPlan{}, false
	}
	plan := nativeSQLDataflowTripleGroupPlan{
		projectionGroup:     make([]int, len(query.selects)),
		projectionAggregate: make([]int, len(query.selects)),
	}
	for index := range plan.projectionGroup {
		plan.projectionGroup[index] = -1
		plan.projectionAggregate[index] = -1
	}
	copy(plan.groups[:], query.groupBy)
	hasGroupProjection := [3]bool{}
	for index, item := range query.selects {
		groupProjection := -1
		for groupIndex, group := range plan.groups {
			if !sqlSameField(item.expr, group) {
				continue
			}
			if hasGroupProjection[groupIndex] {
				return nativeSQLDataflowTripleGroupPlan{}, false
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
			return nativeSQLDataflowTripleGroupPlan{}, false
		}
		plan.projectionAggregate[index] = len(plan.aggregates)
		plan.aggregates = append(plan.aggregates, aggregate)
	}
	if !hasGroupProjection[0] || !hasGroupProjection[1] || !hasGroupProjection[2] {
		return nativeSQLDataflowTripleGroupPlan{}, false
	}
	return plan, true
}

type nativeSQLDataflowTripleGroupState struct {
	values          [3]interface{}
	aggregateOffset int
}

func executeNativeSQLDataflowTripleGroups(ctx context.Context, query *sqlQuery, initial []SQLRow, plan nativeSQLDataflowTripleGroupPlan) ([]SQLRow, error) {
	indexes := make(map[nativeSQLDataflowTripleDistinctKey]int, len(initial))
	groups := make([]nativeSQLDataflowTripleGroupState, 0)
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
		values := [3]interface{}{}
		keys := nativeSQLDataflowTripleDistinctKey{}
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
			}
		}
		groupIndex, found := indexes[keys]
		if !found {
			groupIndex = len(groups)
			indexes[keys] = groupIndex
			groups = append(groups, nativeSQLDataflowTripleGroupState{
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

type nativeSQLDataflowTripleDistinctKey struct {
	first  nativeSQLDataflowDistinctKey
	second nativeSQLDataflowDistinctKey
	third  nativeSQLDataflowDistinctKey
}
