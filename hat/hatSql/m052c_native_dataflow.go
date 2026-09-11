package hatSql

import (
	"context"
	"errors"
	"fmt"
)

// ErrSQLNativeDataflowUnsupported reports a compiled query that cannot use the
// built-in batch runtime without changing SQL semantics.
var ErrSQLNativeDataflowUnsupported = errors.New("hatSql: native dataflow shape is unsupported")

// CompileNativeDataflow creates an opt-in built-in executor for a compiled
// single-source scalar query. Execute receives an already-resolved source
// batch, then fuses the supported FILTER and PROJECT stages without invoking
// the resolver or materializing intermediate rows. The regular SQL executor
// remains the path for all other query shapes.
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
	if query.distinct || len(query.groupBy) != 0 || len(query.groupingSets) != 0 || len(query.groupingDimensions) != 0 || query.having.kind != "" || len(query.orderBy) != 0 || query.limit >= 0 || query.offset > 0 || query.limitBy != nil || query.limitWithTies || sqlQueryHasWithFill(query) {
		return fmt.Errorf("%w: query requires materialized state", ErrSQLNativeDataflowUnsupported)
	}
	if query.where.kind != "" && !sqlStreamScalarExpr(query.where) {
		return fmt.Errorf("%w: WHERE expression is not scalar", ErrSQLNativeDataflowUnsupported)
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

func nativeSQLDataflowAggregatePlan(query *sqlQuery) ([]sqlStreamAggregate, bool) {
	if query == nil || len(query.selects) == 0 || query.where.window != nil || sqlExprHasAggregate(query.where) || sqlExprHasCustomFunction(query.where, nil) {
		return nil, false
	}
	aggregates := make([]sqlStreamAggregate, len(query.selects))
	for index, item := range query.selects {
		expr := item.expr
		if expr.kind != "func" || expr.window != nil || expr.filter != nil || sqlExprHasCustomFunction(expr, nil) {
			return nil, false
		}
		aggregate := sqlStreamAggregate{name: expr.name}
		switch expr.name {
		case "COUNT":
			if len(expr.args) > 1 {
				return nil, false
			}
			if len(expr.args) == 1 && expr.args[0].kind != "star" {
				argument := expr.args[0]
				if !sqlStreamScalarExpr(argument) {
					return nil, false
				}
				aggregate.arg = &argument
			}
		case "SUM", "AVG", "MIN", "MAX":
			if len(expr.args) != 1 || expr.args[0].kind == "star" || !sqlStreamScalarExpr(expr.args[0]) {
				return nil, false
			}
			argument := expr.args[0]
			aggregate.arg = &argument
		default:
			return nil, false
		}
		aggregates[index] = aggregate
	}
	return aggregates, true
}

func executeNativeSQLDataflow(ctx context.Context, query *sqlQuery, initial []SQLRow) ([]SQLRow, error) {
	if aggregates, ok := nativeSQLDataflowAggregatePlan(query); ok {
		return executeNativeSQLDataflowAggregates(ctx, query, initial, aggregates)
	}
	columns := sqlColumns(query.selects)
	result := make([]SQLRow, 0, len(initial))
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
		projected := make(SQLRow, len(columns))
		for selectIndex, item := range query.selects {
			value, err := evalSQLStreamExpr(item.expr, execRow, nil)
			if err != nil {
				return nil, fmt.Errorf("native dataflow SELECT row %d column %d: %w", index+1, selectIndex+1, err)
			}
			projected[columns[selectIndex]] = value
		}
		result = append(result, projected)
	}
	return result, nil
}

func executeNativeSQLDataflowAggregates(ctx context.Context, query *sqlQuery, initial []SQLRow, aggregates []sqlStreamAggregate) ([]SQLRow, error) {
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
			value := evalSQLExpr(query.where, []sqlExecRow{execRow}, execRow)
			if err := sqlExpressionError(value); err != nil {
				return nil, fmt.Errorf("native dataflow WHERE row %d: %w", index+1, err)
			}
			if !sqlTruthy(value) {
				continue
			}
		}
		for aggregateIndex := range aggregates {
			if err := aggregates[aggregateIndex].add(execRow); err != nil {
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
