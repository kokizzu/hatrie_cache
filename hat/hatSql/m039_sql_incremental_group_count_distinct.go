package hatSql

import (
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrSQLIncrementalGroupCountDistinctUnsupported identifies a grouped SQL
	// shape that cannot be lowered without changing its relational semantics.
	ErrSQLIncrementalGroupCountDistinctUnsupported = errors.New("hatSql: incremental grouped COUNT DISTINCT SQL shape is unsupported")
	// ErrSQLIncrementalGroupCountDistinctNil reports a method call on a nil
	// incremental grouped distinct-count operator.
	ErrSQLIncrementalGroupCountDistinctNil = errors.New("hatSql: incremental grouped COUNT DISTINCT SQL operator is nil")
	// ErrSQLIncrementalGroupCountDistinctRowRequired reports an update without
	// a source row needed for grouping, filtering, or distinct evaluation.
	ErrSQLIncrementalGroupCountDistinctRowRequired = errors.New("hatSql: incremental grouped COUNT DISTINCT SQL row is required")
	// ErrSQLIncrementalGroupCountDistinctValueType reports a distinct value that
	// cannot be represented exactly by the retained int64 state.
	ErrSQLIncrementalGroupCountDistinctValueType = errors.New("hatSql: incremental grouped COUNT DISTINCT requires a non-null integer value")
)

type sqlIncrementalGroupCountDistinctConfig struct {
	where       sqlExpr
	alias       string
	groupField  string
	groupColumn string
	countColumn string
	valueField  string
}

// SQLIncrementalGroupCountDistinct maintains a restricted grouped SQL result
// from signed source-row updates. Its exact per-value multiplicity state is
// retained by IncrementalGroupCountDistinctInt64; this adapter evaluates the
// SQL shape and restores the selected group and aggregate column names.
//
// The operator is single-writer. Callers sharing it between goroutines must
// provide synchronization around Apply and Snapshot.
type SQLIncrementalGroupCountDistinct struct {
	where       sqlExpr
	alias       string
	groupField  string
	groupColumn string
	countColumn string
	groupValues map[string]interface{}
	operator    *IncrementalGroupCountDistinctInt64
}

// CompileIncrementalGroupCountDistinct lowers a compiled query to exact
// signed grouped COUNT(DISTINCT int64) maintenance. The supported shape is
// one CACHE or VALUES source, one direct GROUP BY field projected in the
// result, one COUNT(DISTINCT direct_field), and an optional scalar WHERE
// without custom functions. Distinct values must be non-null integer values;
// the normal SQL executor remains available for other types and shapes.
func (query *CompiledSQLQuery) CompileIncrementalGroupCountDistinct() (*SQLIncrementalGroupCountDistinct, error) {
	config, err := validateSQLIncrementalGroupCountDistinctQuery(query)
	if err != nil {
		return nil, err
	}
	operator, err := NewIncrementalGroupCountDistinctInt64(sqlIncrementalGroupKey(config.groupField), sqlIncrementalDistinctInt64Value(config.valueField))
	if err != nil {
		return nil, err
	}
	return &SQLIncrementalGroupCountDistinct{
		where:       config.where,
		alias:       config.alias,
		groupField:  config.groupField,
		groupColumn: config.groupColumn,
		countColumn: config.countColumn,
		groupValues: make(map[string]interface{}),
		operator:    operator,
	}, nil
}

func validateSQLIncrementalGroupCountDistinctQuery(query *CompiledSQLQuery) (sqlIncrementalGroupCountDistinctConfig, error) {
	unsupported := func(reason string) (sqlIncrementalGroupCountDistinctConfig, error) {
		return sqlIncrementalGroupCountDistinctConfig{}, fmt.Errorf("%w: %s", ErrSQLIncrementalGroupCountDistinctUnsupported, reason)
	}
	if query == nil || query.template == nil {
		return unsupported("compiled SQL query is required")
	}
	if query.hasParameters {
		return unsupported("parameters must be bound before incremental compilation")
	}
	template := query.template
	if template.from == nil {
		return unsupported("one source is required")
	}
	if template.from.kind != "CACHE" && template.from.kind != "VALUES" {
		return unsupported(fmt.Sprintf("source kind %q", template.from.kind))
	}
	if template.explain || template.explainCost || template.analyze || template.pipeline || template.sample != nil || template.from.query != nil || template.from.lateral {
		return unsupported("query contains a non-row source stage")
	}
	if len(template.ctes) != 0 || len(template.joins) != 0 || len(template.unions) != 0 || len(template.groupingSets) != 0 || len(template.groupingDimensions) != 0 {
		return unsupported("query contains joins, grouping sets, CTEs, or unions")
	}
	if len(template.groupBy) != 1 || template.groupBy[0].kind != "field" {
		return unsupported("exactly one direct GROUP BY field is required")
	}
	if template.having.kind != "" || template.qualify.kind != "" || template.prewhere.kind != "" || template.limitBy != nil {
		return unsupported("query contains HAVING, QUALIFY, PREWHERE, or LIMIT BY")
	}
	if template.distinct || template.limitWithTies || template.offset != 0 || template.limit >= 0 || len(template.orderBy) != 0 || len(template.windows) != 0 {
		return unsupported("query contains DISTINCT, LIMIT, ORDER BY, OFFSET, or window semantics")
	}
	if template.where.kind != "" && (!sqlStreamScalarExpr(template.where) || sqlExprHasCustomFunction(template.where, nil)) {
		return unsupported("WHERE must be a scalar expression without custom functions")
	}

	columns := sqlColumns(template.selects)
	if len(template.selects) == 0 || len(columns) != len(template.selects) {
		return unsupported("at least one valid SELECT expression is required")
	}
	config := sqlIncrementalGroupCountDistinctConfig{
		where:      template.where,
		alias:       template.from.alias,
		groupField:  template.groupBy[0].name,
	}
	seenColumns := make(map[string]struct{}, len(columns))
	groupSeen := false
	for index, item := range template.selects {
		column := columns[index]
		if column == "" {
			return unsupported(fmt.Sprintf("SELECT expression %d has no output column", index))
		}
		if _, exists := seenColumns[column]; exists {
			return unsupported(fmt.Sprintf("SELECT output column %q is duplicated", column))
		}
		seenColumns[column] = struct{}{}

		if sqlIncrementalSameField(item.expr, template.groupBy[0]) {
			if groupSeen {
				return unsupported("group field is projected more than once")
			}
			groupSeen = true
			config.groupColumn = column
			continue
		}
		if item.expr.kind != "func" || item.expr.window != nil || item.expr.filter != nil || sqlExprHasCustomFunction(item.expr, nil) {
			return unsupported(fmt.Sprintf("SELECT expression %d is not a supported aggregate", index))
		}
		if strings.ToUpper(item.expr.name) != "COUNT" || !item.expr.distinct {
			return unsupported("exactly one COUNT(DISTINCT field) aggregate is required")
		}
		if config.countColumn != "" {
			return unsupported("only one COUNT(DISTINCT) aggregate is supported")
		}
		if len(item.expr.args) != 1 || item.expr.args[0].kind != "field" {
			return unsupported("COUNT(DISTINCT) must use one direct field")
		}
		if item.expr.args[0].qualifier != "" && item.expr.args[0].qualifier != template.from.alias {
			return unsupported("COUNT(DISTINCT) field must belong to the source")
		}
		config.countColumn = column
		config.valueField = item.expr.args[0].name
	}
	if !groupSeen {
		return unsupported("the GROUP BY field must be projected")
	}
	if config.countColumn == "" {
		return unsupported("COUNT(DISTINCT field) is required")
	}
	return config, nil
}

func sqlIncrementalDistinctInt64Value(field string) DifferentialInt64ValueFunc {
	return func(row SQLRow) (int64, error) {
		value, err := sqlIncrementalInt64(row[field])
		if err != nil {
			return 0, fmt.Errorf("%w: got %T", ErrSQLIncrementalGroupCountDistinctValueType, row[field])
		}
		return value, nil
	}
}

// Apply atomically evaluates the optional WHERE and updates retained grouped
// state. Each changed group emits a retraction of its old SQL row followed by
// an insertion of its new SQL row. Inputs are not mutated or retained.
func (operator *SQLIncrementalGroupCountDistinct) Apply(updates []DifferentialRow) ([]DifferentialRow, error) {
	if operator == nil {
		return nil, ErrSQLIncrementalGroupCountDistinctNil
	}
	if len(updates) == 0 {
		return nil, nil
	}
	accepted := make([]DifferentialRow, 0, len(updates))
	batchGroups := make(map[string]interface{}, len(updates))
	for index, update := range updates {
		if update.Key == "" {
			return nil, fmt.Errorf("incremental grouped COUNT DISTINCT SQL update %d: %w", index, ErrDifferentialRowKeyRequired)
		}
		if update.Diff == 0 {
			continue
		}
		if update.Row == nil {
			return nil, fmt.Errorf("incremental grouped COUNT DISTINCT SQL update %d: %w", index, ErrSQLIncrementalGroupCountDistinctRowRequired)
		}
		if operator.where.kind != "" {
			execRow := newSQLSingleSourceExecRow(operator.alias, update.Row)
			value, err := evalSQLStreamExpr(operator.where, execRow, nil)
			if err != nil {
				return nil, fmt.Errorf("incremental grouped COUNT DISTINCT SQL update %d WHERE: %w", index, err)
			}
			if !sqlTruthy(value) {
				continue
			}
		}
		key := sqlIncrementalGroupKey(operator.groupField)(update.Row)
		batchGroups[key] = update.Row[operator.groupField]
		accepted = append(accepted, update)
	}
	if len(accepted) == 0 {
		return nil, nil
	}
	changes, err := operator.operator.Apply(accepted)
	if err != nil {
		return nil, err
	}
	if len(changes) == 0 {
		return nil, nil
	}

	positive := make(map[string]struct{}, len(changes))
	touched := make(map[string]struct{}, len(changes))
	for index := range changes {
		change := &changes[index]
		value, ok := batchGroups[change.Key]
		if !ok {
			value, ok = operator.groupValues[change.Key]
		}
		if !ok {
			return nil, fmt.Errorf("incremental grouped COUNT DISTINCT SQL group %q: %w", change.Key, ErrSQLIncrementalGroupCountDistinctRowRequired)
		}
		change.Row = operator.sqlRow(change.Row, value)
		touched[change.Key] = struct{}{}
		if change.Diff > 0 {
			positive[change.Key] = struct{}{}
		}
	}
	for key, value := range batchGroups {
		if _, exists := positive[key]; exists {
			operator.groupValues[key] = value
			continue
		}
		if _, exists := touched[key]; exists {
			delete(operator.groupValues, key)
		}
	}
	return changes, nil
}

// Snapshot returns one positive SQL distinct-count row per retained group in
// the deterministic order of the underlying operator.
func (operator *SQLIncrementalGroupCountDistinct) Snapshot() []DifferentialRow {
	if operator == nil {
		return nil
	}
	rows := operator.operator.Snapshot()
	for index := range rows {
		value, ok := operator.groupValues[rows[index].Key]
		if !ok {
			continue
		}
		rows[index].Row = operator.sqlRow(rows[index].Row, value)
	}
	return rows
}

func (operator *SQLIncrementalGroupCountDistinct) sqlRow(aggregate SQLRow, groupValue interface{}) SQLRow {
	return Row{
		operator.groupColumn: groupValue,
		operator.countColumn: aggregate["count_distinct"],
	}
}
