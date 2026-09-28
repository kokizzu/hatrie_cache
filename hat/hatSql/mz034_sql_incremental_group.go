package hatSql

import (
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrSQLIncrementalGroupAggregateUnsupported identifies a grouped SQL
	// shape that cannot be lowered without changing its relational semantics.
	ErrSQLIncrementalGroupAggregateUnsupported = errors.New("hatSql: incremental group aggregate SQL shape is unsupported")
	// ErrSQLIncrementalGroupAggregateNil reports a method call on a nil
	// incremental grouped aggregate.
	ErrSQLIncrementalGroupAggregateNil = errors.New("hatSql: incremental group aggregate SQL operator is nil")
	// ErrSQLIncrementalGroupAggregateRowRequired reports an update without a
	// source row needed for grouping, filtering, or aggregation.
	ErrSQLIncrementalGroupAggregateRowRequired = errors.New("hatSql: incremental group aggregate SQL row is required")
	// ErrSQLIncrementalGroupAggregateValueType reports a SUM input that cannot
	// be represented exactly by the retained int64 state.
	ErrSQLIncrementalGroupAggregateValueType = errors.New("hatSql: incremental group aggregate SUM requires a non-null integer value")
)

type sqlIncrementalGroupAggregateConfig struct {
	where        sqlExpr
	alias        string
	groupField   string
	groupColumn  string
	countColumn  string
	sumField     string
	sumColumn    string
	extremaField string
	minColumn    string
	maxColumn    string
}

// SQLIncrementalGroupAggregate maintains a restricted grouped SQL result from
// signed source-row updates. Group state is retained in the existing compact
// differential COUNT and COUNT+SUM operators; this adapter only evaluates the
// SQL shape and restores the selected group column in emitted rows.
//
// The operator is single-writer. Callers sharing it between goroutines must
// provide synchronization around Apply and Snapshot.
type SQLIncrementalGroupAggregate struct {
	where       sqlExpr
	alias       string
	groupField  string
	groupColumn string
	countColumn string
	sumColumn   string
	minColumn   string
	maxColumn   string
	groupValues map[string]interface{}
	count       *IncrementalGroupCount
	countSum    *IncrementalGroupCountSumInt64
	minMax      *IncrementalGroupMinMaxInt64
}

// CompileIncrementalGroupAggregate lowers a compiled query to exact signed
// grouped maintenance. The supported shape is one CACHE or VALUES source,
// one direct GROUP BY field projected in the result, COUNT(*) and/or SUM of a
// non-null integer field, and an optional scalar WHERE without custom
// functions. Ordering, HAVING, joins, grouping sets, windows, and other global
// semantics retain the normal SQL executor and return an explicit error here.
func (query *CompiledSQLQuery) CompileIncrementalGroupAggregate() (*SQLIncrementalGroupAggregate, error) {
	config, err := validateSQLIncrementalGroupAggregateQuery(query)
	if err != nil {
		return nil, err
	}
	groupKey := sqlIncrementalGroupKey(config.groupField)
	operator := &SQLIncrementalGroupAggregate{
		where:       config.where,
		alias:       config.alias,
		groupField:  config.groupField,
		groupColumn: config.groupColumn,
		countColumn: config.countColumn,
		sumColumn:   config.sumColumn,
		minColumn:   config.minColumn,
		maxColumn:   config.maxColumn,
		groupValues: make(map[string]interface{}),
	}
	if config.extremaField != "" {
		operator.minMax, err = NewIncrementalGroupMinMaxInt64(groupKey, sqlIncrementalInt64Value(config.extremaField))
	} else if config.sumField != "" {
		operator.countSum, err = NewIncrementalGroupCountSumInt64(groupKey, sqlIncrementalInt64Value(config.sumField))
	} else {
		operator.count, err = NewIncrementalGroupCount(groupKey)
	}
	if err != nil {
		return nil, err
	}
	return operator, nil
}

func validateSQLIncrementalGroupAggregateQuery(query *CompiledSQLQuery) (sqlIncrementalGroupAggregateConfig, error) {
	unsupported := func(reason string) (sqlIncrementalGroupAggregateConfig, error) {
		return sqlIncrementalGroupAggregateConfig{}, fmt.Errorf("%w: %s", ErrSQLIncrementalGroupAggregateUnsupported, reason)
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
	config := sqlIncrementalGroupAggregateConfig{
		where:      template.where,
		alias:      template.from.alias,
		groupField: template.groupBy[0].name,
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
		switch strings.ToUpper(item.expr.name) {
		case "COUNT":
			if config.extremaField != "" {
				return unsupported("MIN/MAX cannot be combined with COUNT or SUM")
			}
			if config.countColumn != "" {
				return unsupported("only one COUNT aggregate is supported")
			}
			if len(item.expr.args) != 0 && (len(item.expr.args) != 1 || item.expr.args[0].kind != "star") {
				return unsupported("COUNT must be COUNT(*)")
			}
			config.countColumn = column
		case "SUM":
			if config.extremaField != "" {
				return unsupported("MIN/MAX cannot be combined with COUNT or SUM")
			}
			if config.sumColumn != "" {
				return unsupported("only one SUM aggregate is supported")
			}
			if len(item.expr.args) != 1 || item.expr.args[0].kind != "field" {
				return unsupported("SUM must use one direct field")
			}
			config.sumColumn = column
			config.sumField = item.expr.args[0].name
		case "MIN", "MAX":
			if config.countColumn != "" || config.sumColumn != "" {
				return unsupported("MIN/MAX cannot be combined with COUNT or SUM")
			}
			if len(item.expr.args) != 1 || item.expr.args[0].kind != "field" {
				return unsupported(fmt.Sprintf("%s must use one direct field", strings.ToUpper(item.expr.name)))
			}
			if config.extremaField != "" && config.extremaField != item.expr.args[0].name {
				return unsupported("MIN and MAX must use the same direct field")
			}
			config.extremaField = item.expr.args[0].name
			if strings.EqualFold(item.expr.name, "MIN") {
				if config.minColumn != "" {
					return unsupported("only one MIN aggregate is supported")
				}
				config.minColumn = column
			} else {
				if config.maxColumn != "" {
					return unsupported("only one MAX aggregate is supported")
				}
				config.maxColumn = column
			}
		default:
			return unsupported(fmt.Sprintf("aggregate %q is not supported", item.expr.name))
		}
	}
	if !groupSeen {
		return unsupported("the GROUP BY field must be projected")
	}
	if config.countColumn == "" && config.sumColumn == "" && config.extremaField == "" {
		return unsupported("COUNT(*), SUM(field), or MIN/MAX(field) is required")
	}
	return config, nil
}

func sqlIncrementalSameField(left, right sqlExpr) bool {
	if left.kind != "field" || right.kind != "field" || left.name != right.name {
		return false
	}
	return left.qualifier == "" || right.qualifier == "" || left.qualifier == right.qualifier
}

func sqlIncrementalGroupKey(field string) DifferentialGroupByKeyFunc {
	return func(row SQLRow) string {
		return fmt.Sprintf("%#v", row[field])
	}
}

func sqlIncrementalInt64Value(field string) DifferentialInt64ValueFunc {
	return func(row SQLRow) (int64, error) {
		return sqlIncrementalInt64(row[field])
	}
}

func sqlIncrementalInt64(value interface{}) (int64, error) {
	switch value := value.(type) {
	case int:
		return int64(value), nil
	case int8:
		return int64(value), nil
	case int16:
		return int64(value), nil
	case int32:
		return int64(value), nil
	case int64:
		return value, nil
	case uint:
		if uint64(value) > uint64(differentialMaxInt64) {
			break
		}
		return int64(value), nil
	case uint8:
		return int64(value), nil
	case uint16:
		return int64(value), nil
	case uint32:
		return int64(value), nil
	case uint64:
		if value <= uint64(differentialMaxInt64) {
			return int64(value), nil
		}
	}
	return 0, fmt.Errorf("%w: got %T", ErrSQLIncrementalGroupAggregateValueType, value)
}

// Apply atomically evaluates the optional WHERE and updates retained grouped
// state. Each changed group emits a retraction of its old SQL row followed by
// an insertion of its new SQL row. Inputs are not mutated or retained.
func (operator *SQLIncrementalGroupAggregate) Apply(updates []DifferentialRow) ([]DifferentialRow, error) {
	if operator == nil {
		return nil, ErrSQLIncrementalGroupAggregateNil
	}
	if len(updates) == 0 {
		return nil, nil
	}
	accepted := make([]DifferentialRow, 0, len(updates))
	batchGroups := make(map[string]interface{}, len(updates))
	for index, update := range updates {
		if update.Key == "" {
			return nil, fmt.Errorf("incremental group aggregate SQL update %d: %w", index, ErrDifferentialRowKeyRequired)
		}
		if update.Diff == 0 {
			continue
		}
		if update.Row == nil {
			return nil, fmt.Errorf("incremental group aggregate SQL update %d: %w", index, ErrSQLIncrementalGroupAggregateRowRequired)
		}
		if operator.where.kind != "" {
			execRow := newSQLSingleSourceExecRow(operator.alias, update.Row)
			value, err := evalSQLStreamExpr(operator.where, execRow, nil)
			if err != nil {
				return nil, fmt.Errorf("incremental group aggregate SQL update %d WHERE: %w", index, err)
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

	var (
		changes []DifferentialRow
		err     error
	)
	if operator.minMax != nil {
		changes, err = operator.minMax.Apply(accepted)
	} else if operator.countSum != nil {
		changes, err = operator.countSum.Apply(accepted)
	} else {
		changes, err = operator.count.Apply(accepted)
	}
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
			return nil, fmt.Errorf("incremental group aggregate SQL group %q: %w", change.Key, ErrSQLIncrementalGroupAggregateRowRequired)
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

// Snapshot returns one positive SQL aggregate row per retained group in the
// underlying operator's deterministic key order.
func (operator *SQLIncrementalGroupAggregate) Snapshot() []DifferentialRow {
	if operator == nil {
		return nil
	}
	var rows []DifferentialRow
	if operator.minMax != nil {
		rows = operator.minMax.Snapshot()
	} else if operator.countSum != nil {
		rows = operator.countSum.Snapshot()
	} else {
		rows = operator.count.Snapshot()
	}
	for index := range rows {
		value, ok := operator.groupValues[rows[index].Key]
		if !ok {
			continue
		}
		rows[index].Row = operator.sqlRow(rows[index].Row, value)
	}
	return rows
}

func (operator *SQLIncrementalGroupAggregate) sqlRow(aggregate SQLRow, groupValue interface{}) SQLRow {
	row := make(Row, 1)
	row[operator.groupColumn] = groupValue
	if operator.countColumn != "" {
		row[operator.countColumn] = aggregate["count"]
	}
	if operator.sumColumn != "" {
		if value, ok := aggregate["sum"].(int64); ok {
			row[operator.sumColumn] = float64(value)
		}
	}
	if operator.minMax != nil {
		if operator.minColumn != "" {
			row[operator.minColumn] = aggregate["min"]
		}
		if operator.maxColumn != "" {
			row[operator.maxColumn] = aggregate["max"]
		}
	}
	return row
}
