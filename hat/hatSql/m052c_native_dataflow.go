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

func executeNativeSQLDataflow(ctx context.Context, query *sqlQuery, initial []SQLRow) ([]SQLRow, error) {
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
