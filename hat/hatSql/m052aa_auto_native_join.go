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
	if join.kind != "INNER" || join.source.lateral || query.from.alias == "" || join.source.alias == "" {
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

func sqlAutoNativeJoinEligible(query *sqlQuery, resolver SQLSourceResolver, options SQLQueryOptions) bool {
	if options.DisableNativeDataflow || resolver == nil {
		return false
	}
	_, ok := nativeSQLDataflowJoinPlanFor(query)
	if !ok || query.from == nil {
		return false
	}
	join := query.joins[0]
	if (query.from.kind != "CACHE" && query.from.kind != "KEYS") || (join.source.kind != "CACHE" && join.source.kind != "KEYS") {
		return false
	}
	if query.from.final || join.source.final || query.from.query != nil || join.source.query != nil || len(query.from.fieldTypes) != 0 || len(join.source.fieldTypes) != 0 {
		return false
	}
	if query.explain || query.pipeline || query.analyze || query.sample != nil || query.prewhere.kind != "" || query.qualify.kind != "" || len(query.ctes) != 0 || len(query.unions) != 0 {
		return false
	}
	if len(query.groupBy) != 0 || len(query.groupingSets) != 0 || len(query.groupingDimensions) != 0 || query.having.kind != "" || query.distinct || len(query.orderBy) != 0 || query.limitBy != nil || query.limitWithTies || query.limit >= 0 || query.offset > 0 || sqlQueryHasWithFill(query) {
		return false
	}
	if sqlQueryHasAggregate(query) || sqlQueryHasWindow(query) || sqlQueryHasSubqueryExpression(query) {
		return false
	}
	if query.where.kind != "" && (!sqlStreamScalarExpr(query.where) || sqlExprHasCustomFunction(query.where, nil)) {
		return false
	}
	if len(query.selects) == 0 {
		return false
	}
	for _, item := range query.selects {
		if item.expr.kind == "star" || !sqlStreamScalarExpr(item.expr) || sqlExprHasAggregate(item.expr) || sqlExprHasCustomFunction(item.expr, nil) {
			return false
		}
	}
	if options.Collation != "" || options.Optimizer != nil || options.Workers != 0 || options.IndexHint.Source != "" || options.IndexHint.Field != "" || options.IndexHint.Mode != "" || options.AdaptivePlanner != nil || options.IndexAdvisor != nil || options.ProjectionAdvisor != nil || options.IndexUseRecorder != nil || options.SlowQueryRecorder != nil {
		return false
	}
	if options.MaxJoinBytes > 0 || options.JoinOverflowPolicy != "" || options.SpillBloom || options.RuntimeJoinBloomFilter || options.OperatorMemoryTracker != nil || options.RequireSourceFrontier || options.AsOfFrontier != nil || options.FinalSourceOptions != nil || options.SnapshotToken != "" || options.PlanSnapshot != nil || options.ConditionCache != nil {
		return false
	}
	if sqlAutoNativeDataflowHasSpecializedResolver(resolver) {
		return false
	}
	return true
}

func executeNativeSQLDataflowJoin(ctx context.Context, query *sqlQuery, left, right []SQLRow, plan nativeSQLDataflowJoinPlan, maxRows int, control *sqlExecutionControl) ([]SQLRow, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	join := query.joins[0]
	rightRows := wrapSQLSource(join.source, right)
	index := newSQLJoinHashIndex(len(rightRows))
	for rightIndex, row := range rightRows {
		if err := control.addJoinWork(1); err != nil {
			return nil, err
		}
		index.Add(sqlField(row, join.source.alias, plan.rightField), rightIndex)
	}

	leftRows := wrapSQLSource(*query.from, left)
	columns := sqlColumns(query.selects)
	result := make([]SQLRow, 0, len(leftRows))
	joinedRows := 0
	for leftIndex, leftRow := range leftRows {
		if err := control.check(); err != nil {
			return nil, err
		}
		candidates := index.Lookup(sqlField(leftRow, plan.leftQualifier, plan.leftField))
		for _, rightIndex := range candidates {
			if err := control.addJoinWork(1); err != nil {
				return nil, err
			}
			joinedRows++
			if maxRows > 0 && joinedRows > maxRows {
				return nil, fmt.Errorf("SQL join exceeds the %d row limit; add a more selective WHERE or ON condition", maxRows)
			}
			combined := mergeSQLRows(leftRow, rightRows[rightIndex])
			if query.where.kind != "" {
				matched, err := evalSQLStreamExpr(query.where, combined, nil)
				if err != nil {
					return nil, fmt.Errorf("native dataflow JOIN WHERE row %d: %w", leftIndex+1, err)
				}
				if !sqlTruthy(matched) {
					continue
				}
			}
			projected := make(SQLRow, len(columns))
			for selectIndex, item := range query.selects {
				value, err := evalSQLStreamExpr(item.expr, combined, nil)
				if err != nil {
					return nil, fmt.Errorf("native dataflow JOIN SELECT row %d column %d: %w", leftIndex+1, selectIndex+1, err)
				}
				projected[columns[selectIndex]] = value
			}
			result = append(result, projected)
		}
	}
	return result, nil
}
