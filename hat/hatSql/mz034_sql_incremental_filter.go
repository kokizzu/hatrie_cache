package hatSql

import (
	"errors"
	"fmt"
)

var (
	// ErrSQLIncrementalFilterUnsupported identifies a query shape that cannot
	// be lowered to exact signed filter maintenance without changing SQL
	// semantics.
	ErrSQLIncrementalFilterUnsupported = errors.New("hatSql: incremental filter SQL shape is unsupported")
	// ErrSQLIncrementalFilterNil reports a method call on a nil operator.
	ErrSQLIncrementalFilterNil = errors.New("hatSql: incremental filter SQL operator is nil")
	// ErrSQLIncrementalFilterRowRequired reports a row-dependent update without
	// the source image needed to evaluate its predicate.
	ErrSQLIncrementalFilterRowRequired = errors.New("hatSql: incremental filter SQL row is required")
)

// SQLIncrementalFilter adapts a restricted SELECT * ... WHERE query to exact
// signed source-row updates. It is stateless and single-writer: callers may
// apply updates concurrently only when they provide their own synchronization.
type SQLIncrementalFilter struct {
	where sqlExpr
	alias string
}

// CompileIncrementalFilter lowers a compiled query to signed SQL filter
// maintenance. The supported shape is one CACHE or VALUES source, SELECT *,
// and an optional scalar WHERE expression without custom functions. All other
// queries retain the normal SQL executor and return an explicit error here.
func (query *CompiledSQLQuery) CompileIncrementalFilter() (*SQLIncrementalFilter, error) {
	if query == nil || query.template == nil {
		return nil, fmt.Errorf("%w: compiled SQL query is required", ErrSQLIncrementalFilterUnsupported)
	}
	if err := validateSQLIncrementalFilterQuery(query); err != nil {
		return nil, err
	}
	return &SQLIncrementalFilter{
		where: query.template.where,
		alias: query.template.from.alias,
	}, nil
}

func validateSQLIncrementalFilterQuery(query *CompiledSQLQuery) error {
	unsupported := func(reason string) error {
		return fmt.Errorf("%w: %s", ErrSQLIncrementalFilterUnsupported, reason)
	}
	if query == nil || query.template == nil {
		return unsupported("compiled SQL query is required")
	}
	template := query.template
	if query.hasParameters {
		return unsupported("parameters must be bound before incremental compilation")
	}
	if template.from == nil {
		return unsupported("one source is required")
	}
	if template.from.kind != "CACHE" && template.from.kind != "VALUES" {
		return unsupported(fmt.Sprintf("source kind %q", template.from.kind))
	}
	if template.explain || template.explainCost || template.analyze || template.pipeline || template.sample != nil || template.from.query != nil || template.from.lateral {
		return unsupported("query contains a non-row source stage")
	}
	if len(template.ctes) != 0 || len(template.joins) != 0 || len(template.unions) != 0 || len(template.groupBy) != 0 || len(template.groupingSets) != 0 || len(template.groupingDimensions) != 0 {
		return unsupported("query contains joins, grouping, CTEs, or unions")
	}
	if template.having.kind != "" || template.qualify.kind != "" || template.prewhere.kind != "" || template.limitBy != nil {
		return unsupported("query contains an unsupported filtering stage")
	}
	if template.distinct || template.limitWithTies || template.offset != 0 || template.limit >= 0 || len(template.orderBy) != 0 || len(template.windows) != 0 {
		return unsupported("query contains DISTINCT, LIMIT, ORDER BY, OFFSET, or window semantics")
	}
	if len(template.selects) != 1 || template.selects[0].expr.kind != "star" {
		return unsupported("projection must be SELECT *")
	}
	if sqlQueryHasAggregate(template) || sqlQueryHasWindow(template) {
		return unsupported("query contains aggregate or window semantics")
	}
	if template.where.kind != "" && (!sqlStreamScalarExpr(template.where) || sqlExprHasCustomFunction(template.where, nil)) {
		return unsupported("WHERE must be a scalar expression without custom functions")
	}
	return nil
}

// Apply atomically evaluates signed source-row updates. Rows rejected by the
// SQL predicate are omitted; accepted rows preserve key, timestamp, and
// signed multiplicity. The input rows are never mutated or retained.
func (operator *SQLIncrementalFilter) Apply(updates []DifferentialRow) ([]DifferentialRow, error) {
	if operator == nil {
		return nil, ErrSQLIncrementalFilterNil
	}
	if len(updates) == 0 {
		return nil, nil
	}
	changes := make([]DifferentialRow, 0, len(updates))
	for index, update := range updates {
		if update.Key == "" {
			return nil, fmt.Errorf("incremental filter SQL update %d: %w", index, ErrDifferentialRowKeyRequired)
		}
		if update.Diff == 0 {
			continue
		}
		if operator.where.kind != "" {
			if update.Row == nil {
				return nil, fmt.Errorf("incremental filter SQL update %d: %w", index, ErrSQLIncrementalFilterRowRequired)
			}
			execRow := newSQLSingleSourceExecRow(operator.alias, update.Row)
			value, err := evalSQLStreamExpr(operator.where, execRow, nil)
			if err != nil {
				return nil, fmt.Errorf("incremental filter SQL update %d: %w", index, err)
			}
			if !sqlTruthy(value) {
				continue
			}
		}
		update.Row = cloneDifferentialRow(update.Row)
		changes = append(changes, update)
	}
	if len(changes) == 0 {
		return nil, nil
	}
	return changes, nil
}
