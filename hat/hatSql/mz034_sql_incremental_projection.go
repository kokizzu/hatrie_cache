package hatSql

import (
	"errors"
	"fmt"
)

var (
	// ErrSQLIncrementalProjectionUnsupported identifies a query shape that
	// cannot be lowered to exact signed projection maintenance.
	ErrSQLIncrementalProjectionUnsupported = errors.New("hatSql: incremental projection SQL shape is unsupported")
	// ErrSQLIncrementalProjectionNil reports a method call on a nil operator.
	ErrSQLIncrementalProjectionNil = errors.New("hatSql: incremental projection SQL operator is nil")
	// ErrSQLIncrementalProjectionRowRequired reports an update without the
	// source image needed to evaluate a projection or predicate.
	ErrSQLIncrementalProjectionRowRequired = errors.New("hatSql: incremental projection SQL row is required")
)

// SQLIncrementalProjection adapts a restricted SELECT projection to exact
// signed source-row updates. Output rows retain the input DifferentialRow
// identity, timestamp, and signed multiplicity. It is stateless and
// single-writer; callers provide synchronization when applying concurrently.
type SQLIncrementalProjection struct {
	where       sqlExpr
	expressions []sqlExpr
	columns     []string
	alias       string
}

// CompileIncrementalProjection lowers a compiled query to signed SQL
// projection maintenance. The supported shape is one CACHE or VALUES source,
// one or more scalar SELECT expressions, and an optional scalar WHERE without
// custom functions. Grouping, joins, ordering, limits, and other global
// semantics retain the normal SQL executor and return an explicit error here.
func (query *CompiledSQLQuery) CompileIncrementalProjection() (*SQLIncrementalProjection, error) {
	if query == nil || query.template == nil {
		return nil, fmt.Errorf("%w: compiled SQL query is required", ErrSQLIncrementalProjectionUnsupported)
	}
	if err := validateSQLIncrementalProjectionQuery(query); err != nil {
		return nil, err
	}
	return newSQLIncrementalProjection(query), nil
}

func newSQLIncrementalProjection(query *CompiledSQLQuery) *SQLIncrementalProjection {
	selects := query.template.selects
	columns := sqlColumns(selects)
	expressions := make([]sqlExpr, len(selects))
	for index, item := range selects {
		expressions[index] = item.expr
	}
	return &SQLIncrementalProjection{
		where:       query.template.where,
		expressions: expressions,
		columns:     columns,
		alias:       query.template.from.alias,
	}
}

func validateSQLIncrementalProjectionQuery(query *CompiledSQLQuery) error {
	return validateSQLIncrementalProjectionQueryWithDistinct(query, false)
}

func validateSQLIncrementalProjectionQueryWithDistinct(query *CompiledSQLQuery, allowDistinct bool) error {
	unsupported := func(reason string) error {
		return fmt.Errorf("%w: %s", ErrSQLIncrementalProjectionUnsupported, reason)
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
	if (!allowDistinct && template.distinct) || template.limitWithTies || template.offset != 0 || template.limit >= 0 || len(template.orderBy) != 0 || len(template.windows) != 0 {
		return unsupported("query contains DISTINCT, LIMIT, ORDER BY, OFFSET, or window semantics")
	}
	if len(template.selects) == 0 {
		return unsupported("at least one projection expression is required")
	}
	columns := sqlColumns(template.selects)
	seenColumns := make(map[string]struct{}, len(columns))
	for index, item := range template.selects {
		if item.expr.kind == "star" {
			return unsupported("projection expressions must be explicit")
		}
		if !sqlStreamScalarExpr(item.expr) || sqlExprHasCustomFunction(item.expr, nil) {
			return unsupported(fmt.Sprintf("projection expression %d is not a scalar built-in expression", index))
		}
		if index >= len(columns) || columns[index] == "" {
			return unsupported(fmt.Sprintf("projection expression %d has no output column", index))
		}
		if _, exists := seenColumns[columns[index]]; exists {
			return unsupported(fmt.Sprintf("projection output column %q is duplicated", columns[index]))
		}
		seenColumns[columns[index]] = struct{}{}
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
// optional SQL predicate are omitted; accepted rows contain the projected
// columns and preserve key, timestamp, and signed multiplicity. Inputs and
// callback-visible source rows are never mutated or retained.
func (operator *SQLIncrementalProjection) Apply(updates []DifferentialRow) ([]DifferentialRow, error) {
	if operator == nil {
		return nil, ErrSQLIncrementalProjectionNil
	}
	if len(updates) == 0 {
		return nil, nil
	}
	changes := make([]DifferentialRow, 0, len(updates))
	for index, update := range updates {
		if update.Key == "" {
			return nil, fmt.Errorf("incremental projection SQL update %d: %w", index, ErrDifferentialRowKeyRequired)
		}
		if update.Diff == 0 {
			continue
		}
		if update.Row == nil {
			return nil, fmt.Errorf("incremental projection SQL update %d: %w", index, ErrSQLIncrementalProjectionRowRequired)
		}
		execRow := newSQLSingleSourceExecRow(operator.alias, update.Row)
		if operator.where.kind != "" {
			value, err := evalSQLStreamExpr(operator.where, execRow, nil)
			if err != nil {
				return nil, fmt.Errorf("incremental projection SQL update %d WHERE: %w", index, err)
			}
			if !sqlTruthy(value) {
				continue
			}
		}
		projected := make(Row, len(operator.expressions))
		for expressionIndex, expression := range operator.expressions {
			value, err := evalSQLStreamExpr(expression, execRow, nil)
			if err != nil {
				return nil, fmt.Errorf("incremental projection SQL update %d column %q: %w", index, operator.columns[expressionIndex], err)
			}
			projected[operator.columns[expressionIndex]] = value
		}
		update.Row = cloneDifferentialRow(projected)
		changes = append(changes, update)
	}
	if len(changes) == 0 {
		return nil, nil
	}
	return changes, nil
}
