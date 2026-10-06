package hatSql

import (
	"context"
	"fmt"
)

type nativeSQLDataflowJoinPlan struct {
	leftQualifier string
	leftField     string
	rightField    string
}

func nativeSQLDataflowJoinPlanFor(query *sqlQuery) (nativeSQLDataflowJoinPlan, bool) {
	if query == nil || query.from == nil || len(query.joins) != 1 {
		return nativeSQLDataflowJoinPlan{}, false
	}
	join := query.joins[0]
	if join.kind != "INNER" || join.asofLeft || join.source.alias == "" || query.from.alias == "" {
		return nativeSQLDataflowJoinPlan{}, false
	}
	leftQualifier, leftField, rightField, ok := sqlHashJoinFields(join.on, []string{query.from.alias}, join.source.alias)
	if !ok {
		return nativeSQLDataflowJoinPlan{}, false
	}
	return nativeSQLDataflowJoinPlan{
		leftQualifier: leftQualifier,
		leftField:     leftField,
		rightField:    rightField,
	}, true
}

func sqlAutoNativeInnerJoinEligible(query *sqlQuery, resolver SQLSourceResolver, options SQLQueryOptions) (nativeSQLDataflowJoinPlan, bool) {
	if options.DisableNativeDataflow || query == nil || query.from == nil || resolver == nil {
		return nativeSQLDataflowJoinPlan{}, false
	}
	if query.from.kind != "CACHE" && query.from.kind != "KEYS" {
		return nativeSQLDataflowJoinPlan{}, false
	}
	if len(query.joins) != 1 || query.joins[0].source.kind != "CACHE" && query.joins[0].source.kind != "KEYS" {
		return nativeSQLDataflowJoinPlan{}, false
	}
	if query.explain || query.sample != nil || query.prewhere.kind != "" || len(query.ctes) != 0 || len(query.unions) != 0 {
		return nativeSQLDataflowJoinPlan{}, false
	}
	if query.distinct || len(query.groupBy) != 0 || len(query.orderBy) != 0 || query.having.kind != "" || query.limitBy != nil || query.limitWithTies || sqlQueryHasWithFill(query) {
		return nativeSQLDataflowJoinPlan{}, false
	}
	if sqlQueryHasAggregate(query) || sqlQueryHasWindow(query) || query.where.window != nil || sqlExprHasAggregate(query.where) || sqlExprHasCustomFunction(query.where, nil) {
		return nativeSQLDataflowJoinPlan{}, false
	}
	if options.MaxRows != 0 || options.MaxIntermediateRows != 0 || options.MaxJoinWork != 0 || options.MaxJoinBytes != 0 || options.MaxResultBytes != 0 || options.MaxSortBytes != 0 || options.MaxGroupBytes != 0 || options.MaxSetBytes != 0 || options.MaxSpillBytes != 0 || options.MaxQuerySpillBytes != 0 || options.SpillDirectory != "" {
		return nativeSQLDataflowJoinPlan{}, false
	}
	if sqlAutoNativeDataflowHasSpecializedResolver(resolver) {
		return nativeSQLDataflowJoinPlan{}, false
	}
	if query.where.kind != "" && !sqlStreamScalarExpr(query.where) {
		return nativeSQLDataflowJoinPlan{}, false
	}
	if len(query.selects) == 0 {
		return nativeSQLDataflowJoinPlan{}, false
	}
	for _, item := range query.selects {
		if item.expr.kind == "star" || !sqlStreamScalarExpr(item.expr) || sqlExprHasAggregate(item.expr) || sqlExprHasCustomFunction(item.expr, nil) {
			return nativeSQLDataflowJoinPlan{}, false
		}
	}
	plan, ok := nativeSQLDataflowJoinPlanFor(query)
	return plan, ok
}

func executeNativeSQLDataflowJoin(ctx context.Context, query *sqlQuery, plan nativeSQLDataflowJoinPlan, left, right []SQLRow) ([]SQLRow, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if query.limit == 0 {
		return []SQLRow{}, nil
	}
	matches := make(map[string][]SQLRow, len(right))
	for index, input := range right {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		row := input
		if len(query.joins[0].source.fieldTypes) != 0 {
			validated, err := validateSQLSourceFieldTypeRow(query.joins[0].source, input, index+1)
			if err != nil {
				return nil, err
			}
			row = validated
		}
		key, ok := sqlHashJoinKey(row[plan.rightField])
		if ok {
			matches[key] = append(matches[key], row)
		}
	}

	columns := sqlColumns(query.selects)
	capacity := len(left)
	if query.limit >= 0 && capacity > query.limit {
		capacity = query.limit
	}
	result := make([]SQLRow, 0, capacity)
	offset := query.offset
	for index, input := range left {
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
		key, ok := sqlHashJoinKey(row[plan.leftField])
		if !ok {
			continue
		}
		leftExec := newSQLSingleSourceExecRowAt(query.from.alias, row, index)
		for rightIndex, rightRow := range matches[key] {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			rightExec := newSQLSingleSourceExecRowAt(query.joins[0].source.alias, rightRow, rightIndex)
			joined := mergeSQLRows(leftExec, rightExec)
			if query.where.kind != "" {
				value, err := evalSQLStreamExpr(query.where, joined, nil)
				if err != nil {
					return nil, fmt.Errorf("native dataflow JOIN WHERE row %d: %w", index+1, err)
				}
				if !sqlTruthy(value) {
					continue
				}
			}
			if offset > 0 {
				offset--
				continue
			}
			projected := make(SQLRow, len(columns))
			for selectIndex, item := range query.selects {
				value, err := evalSQLStreamExpr(item.expr, joined, nil)
				if err != nil {
					return nil, fmt.Errorf("native dataflow JOIN SELECT row %d column %d: %w", index+1, selectIndex+1, err)
				}
				projected[columns[selectIndex]] = value
			}
			result = append(result, projected)
			if query.limit >= 0 && len(result) >= query.limit {
				return result, nil
			}
		}
	}
	return result, nil
}
