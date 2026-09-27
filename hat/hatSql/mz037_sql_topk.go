package hatSql

import (
	"errors"
	"fmt"
	"reflect"
)

var (
	// ErrSQLIncrementalTopKUnsupported identifies a SQL shape that cannot be
	// lowered to the exact differential Top-K operator without changing SQL
	// semantics.
	ErrSQLIncrementalTopKUnsupported = errors.New("hatSql: incremental top-k SQL shape is unsupported")
	ErrSQLIncrementalTopKNil         = errors.New("hatSql: incremental top-k SQL operator is nil")
	ErrSQLIncrementalTopKRowRequired = errors.New("hatSql: incremental top-k SQL insert requires a row")
	ErrSQLIncrementalTopKOrderNull   = errors.New("hatSql: incremental top-k SQL order value must not be null")
	ErrSQLIncrementalTopKRowConflict = errors.New("hatSql: incremental top-k SQL row conflicts with existing key")
)

const sqlIncrementalTopKOrderColumn = "\x00hatrie.sql.incremental_top_k.order"

// SQLIncrementalTopK adapts a restricted compiled SQL Top-K query to signed
// source-row updates. It is single-writer; callers sharing an operator must
// provide synchronization. The source row key is DifferentialRow.Key.
type SQLIncrementalTopK struct {
	query   *sqlQuery
	columns []string
	topK    *IncrementalTopK
	source  map[string]sqlIncrementalTopKSourceEntry
}

type sqlIncrementalTopKSourceEntry struct {
	active bool
	count  int64
	row    Row
}

// CompileIncrementalTopK lowers a compiled query to exact differential Top-K
// maintenance. The adapter accepts one finite ORDER BY/LIMIT query over a
// CACHE or VALUES source. The normal SQL compiler and executor remain
// unchanged for all other shapes.
func (query *CompiledSQLQuery) CompileIncrementalTopK() (*SQLIncrementalTopK, error) {
	if query == nil || query.template == nil {
		return nil, fmt.Errorf("%w: compiled SQL query is required", ErrSQLIncrementalTopKUnsupported)
	}
	template := query.template
	if err := validateSQLIncrementalTopKQuery(template); err != nil {
		return nil, err
	}
	topK, err := NewIncrementalTopK(IncrementalTopKDefinition{
		K:          template.limit,
		OrderKey:   sqlIncrementalTopKOrderKey,
		Descending: template.orderBy[0].desc,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSQLIncrementalTopKUnsupported, err)
	}
	return &SQLIncrementalTopK{
		query:   template,
		columns: sqlColumns(template.selects),
		topK:    topK,
		source:  make(map[string]sqlIncrementalTopKSourceEntry),
	}, nil
}

func validateSQLIncrementalTopKQuery(query *sqlQuery) error {
	unsupported := func(reason string) error {
		return fmt.Errorf("%w: %s", ErrSQLIncrementalTopKUnsupported, reason)
	}
	if query == nil || query.from == nil {
		return unsupported("one source is required")
	}
	if query.from.kind != "CACHE" && query.from.kind != "VALUES" {
		return unsupported(fmt.Sprintf("source kind %q", query.from.kind))
	}
	if query.explain || query.explainCost || query.analyze || query.pipeline || query.sample != nil || query.from.query != nil || query.from.lateral {
		return unsupported("query contains a non-row source stage")
	}
	if len(query.ctes) != 0 || len(query.joins) != 0 || len(query.unions) != 0 || len(query.groupBy) != 0 || len(query.groupingSets) != 0 || len(query.groupingDimensions) != 0 {
		return unsupported("query contains joins, grouping, CTEs, or unions")
	}
	if query.having.kind != "" || query.qualify.kind != "" || query.prewhere.kind != "" || query.limitBy != nil {
		return unsupported("query contains an unsupported filtering stage")
	}
	if query.distinct || query.limitWithTies || query.offset != 0 {
		return unsupported("DISTINCT, LIMIT WITH TIES, and OFFSET are not supported")
	}
	if query.limit < 0 {
		return unsupported("a finite LIMIT is required")
	}
	if len(query.orderBy) != 1 {
		return unsupported("exactly one ORDER BY expression is required")
	}
	order := query.orderBy[0]
	if order.fill != nil || order.collation != "" && order.collation != SQLCollationBinary || !sqlStreamScalarExpr(order.expr) {
		return unsupported("ORDER BY must be one binary-collated scalar expression")
	}
	if len(query.selects) == 0 {
		return unsupported("projection is empty")
	}
	columns := sqlColumns(query.selects)
	for index, item := range query.selects {
		if item.expr.kind == "star" || !sqlStreamScalarExpr(item.expr) {
			return unsupported(fmt.Sprintf("SELECT expression %d is not scalar", index+1))
		}
	}
	for _, column := range columns {
		if column == sqlIncrementalTopKOrderColumn {
			return unsupported("projection uses a reserved internal column")
		}
	}
	if query.where.kind != "" && (!sqlStreamScalarExpr(query.where) || sqlExprHasCustomFunction(query.where, nil)) {
		return unsupported("WHERE must be a scalar expression without custom functions")
	}
	if sqlExprHasAggregate(order.expr) || sqlExprHasWindow(order.expr) || sqlExprHasCustomFunction(order.expr, nil) {
		return unsupported("ORDER BY cannot contain aggregate, window, or custom functions")
	}
	return nil
}

func sqlIncrementalTopKOrderKey(row Row) (interface{}, error) {
	if row == nil {
		return nil, ErrSQLIncrementalTopKRowRequired
	}
	value, ok := row[sqlIncrementalTopKOrderColumn]
	if !ok {
		return nil, ErrSQLIncrementalTopKOrderNull
	}
	if value == nil {
		return nil, ErrSQLIncrementalTopKOrderNull
	}
	return value, nil
}

// Columns returns the projected SQL column names.
func (operator *SQLIncrementalTopK) Columns() []string {
	if operator == nil {
		return nil
	}
	return append([]string(nil), operator.columns...)
}

// Apply atomically applies signed source-row updates and returns only changes
// to the bounded SQL result. Positive updates for a new key must include a
// source row. Retractions may omit the row because the operator retains the
// source image until its multiplicity reaches zero.
func (operator *SQLIncrementalTopK) Apply(updates []DifferentialRow) ([]DifferentialRow, error) {
	if operator == nil || operator.topK == nil {
		return nil, ErrSQLIncrementalTopKNil
	}
	if len(updates) == 0 {
		return nil, nil
	}
	if len(updates) == 2 && updates[0].Key == updates[1].Key && updates[0].Diff < 0 && updates[1].Diff > 0 {
		if _, exists := operator.source[updates[0].Key]; exists {
			return operator.applyReplacement(updates)
		}
	}
	return operator.apply(updates)
}

func (operator *SQLIncrementalTopK) applyReplacement(updates []DifferentialRow) ([]DifferentialRow, error) {
	remove, insert := updates[0], updates[1]
	entry := operator.source[remove.Key]
	decrement := incrementalTopKMagnitude(remove.Diff)
	if decrement > uint64(entry.count) {
		return nil, fmt.Errorf("incremental top-k SQL update key %q: %w", remove.Key, ErrIncrementalTopKNegativeMultiplicity)
	}
	if decrement < uint64(entry.count) {
		return operator.applyGeneric(updates)
	}
	if remove.Row != nil && !reflect.DeepEqual(remove.Row, entry.row) {
		return nil, fmt.Errorf("incremental top-k SQL update key %q: %w", remove.Key, ErrSQLIncrementalTopKRowConflict)
	}
	newRow, err := sqlIncrementalTopKSourceRow(sqlIncrementalTopKSourceEntry{}, insert.Row)
	if err != nil {
		return nil, fmt.Errorf("incremental top-k SQL update key %q: %w", insert.Key, err)
	}
	matchedBefore, err := operator.rowMatches(entry.row)
	if err != nil {
		return nil, fmt.Errorf("incremental top-k SQL update key %q: %w", remove.Key, err)
	}
	matchedAfter, projected, orderValue, err := operator.prepareRow(newRow)
	if err != nil {
		return nil, fmt.Errorf("incremental top-k SQL update key %q: %w", insert.Key, err)
	}
	var topKUpdates [2]DifferentialRow
	count := 0
	if matchedBefore {
		topKUpdates[count] = DifferentialRow{Key: remove.Key, Time: remove.Time, Diff: remove.Diff}
		count++
	}
	if matchedAfter {
		projected[sqlIncrementalTopKOrderColumn] = orderValue
		topKUpdates[count] = DifferentialRow{Key: insert.Key, Time: insert.Time, Diff: insert.Diff, Row: projected}
		count++
	}
	changes, err := operator.topK.Apply(topKUpdates[:count])
	if err != nil {
		return nil, err
	}
	operator.source[remove.Key] = sqlIncrementalTopKSourceEntry{active: true, count: insert.Diff, row: newRow}
	for index := range changes {
		delete(changes[index].Row, sqlIncrementalTopKOrderColumn)
	}
	return changes, nil
}

func (operator *SQLIncrementalTopK) applyGeneric(updates []DifferentialRow) ([]DifferentialRow, error) {
	return operator.apply(updates)
}

func (operator *SQLIncrementalTopK) apply(updates []DifferentialRow) ([]DifferentialRow, error) {
	if len(updates) == 0 {
		return nil, nil
	}
	pending := make(map[string]sqlIncrementalTopKSourceEntry, len(updates))
	topKUpdates := make([]DifferentialRow, 0, len(updates))
	for index, update := range updates {
		if update.Key == "" {
			return nil, fmt.Errorf("incremental top-k SQL update %d: key is required", index)
		}
		if update.Diff == 0 {
			continue
		}
		entry, ok := pending[update.Key]
		if !ok {
			entry = operator.source[update.Key]
		}
		if update.Diff > 0 {
			row, err := sqlIncrementalTopKSourceRow(entry, update.Row)
			if err != nil {
				return nil, fmt.Errorf("incremental top-k SQL update %d key %q: %w", index, update.Key, err)
			}
			next, ok := addDifferentialCounts(entry.count, update.Diff)
			if !ok {
				return nil, fmt.Errorf("incremental top-k SQL update %d key %q: %w", index, update.Key, ErrIncrementalTopKOverflow)
			}
			matched, projected, orderValue, err := operator.prepareRow(row)
			if err != nil {
				return nil, fmt.Errorf("incremental top-k SQL update %d key %q: %w", index, update.Key, err)
			}
			if matched {
				projected[sqlIncrementalTopKOrderColumn] = orderValue
				topKUpdates = append(topKUpdates, DifferentialRow{Key: update.Key, Time: update.Time, Diff: update.Diff, Row: projected})
			}
			entry.active = true
			entry.count = next
			entry.row = row
			pending[update.Key] = entry
			continue
		}
		if !entry.active {
			return nil, fmt.Errorf("incremental top-k SQL update %d key %q: %w", index, update.Key, ErrIncrementalTopKNegativeMultiplicity)
		}
		if update.Row != nil && !reflect.DeepEqual(update.Row, entry.row) {
			return nil, fmt.Errorf("incremental top-k SQL update %d key %q: %w", index, update.Key, ErrSQLIncrementalTopKRowConflict)
		}
		next, ok := addDifferentialCounts(entry.count, update.Diff)
		if !ok || next < 0 {
			return nil, fmt.Errorf("incremental top-k SQL update %d key %q: %w", index, update.Key, ErrIncrementalTopKNegativeMultiplicity)
		}
		matched, err := operator.rowMatches(entry.row)
		if err != nil {
			return nil, fmt.Errorf("incremental top-k SQL update %d key %q: %w", index, update.Key, err)
		}
		if matched {
			topKUpdates = append(topKUpdates, DifferentialRow{Key: update.Key, Time: update.Time, Diff: update.Diff})
		}
		entry.count = next
		if next == 0 {
			entry = sqlIncrementalTopKSourceEntry{}
		}
		pending[update.Key] = entry
	}
	changes, err := operator.topK.Apply(topKUpdates)
	if err != nil {
		return nil, err
	}
	for key, entry := range pending {
		if !entry.active {
			delete(operator.source, key)
			continue
		}
		operator.source[key] = entry
	}
	for index := range changes {
		delete(changes[index].Row, sqlIncrementalTopKOrderColumn)
	}
	return changes, nil
}

// Snapshot returns the current projected Top-K result in SQL order.
func (operator *SQLIncrementalTopK) Snapshot() []DifferentialRow {
	if operator == nil || operator.topK == nil {
		return nil
	}
	rows := operator.topK.Snapshot()
	for index := range rows {
		delete(rows[index].Row, sqlIncrementalTopKOrderColumn)
	}
	return rows
}

func sqlIncrementalTopKSourceRow(entry sqlIncrementalTopKSourceEntry, update Row) (Row, error) {
	if entry.active {
		if update != nil && !reflect.DeepEqual(update, entry.row) {
			return nil, ErrSQLIncrementalTopKRowConflict
		}
		return entry.row, nil
	}
	if update == nil {
		return nil, ErrSQLIncrementalTopKRowRequired
	}
	return cloneDifferentialRow(update), nil
}

func (operator *SQLIncrementalTopK) prepareRow(row Row) (bool, Row, interface{}, error) {
	execRow := newSQLSingleSourceExecRow(operator.query.from.alias, row)
	matched, err := operator.rowMatches(row)
	if err != nil {
		return false, nil, nil, err
	}
	if !matched {
		return false, nil, nil, nil
	}
	orderValue, err := evalSQLStreamExpr(operator.query.orderBy[0].expr, execRow, nil)
	if err != nil {
		return false, nil, nil, err
	}
	if orderValue == nil {
		return false, nil, nil, ErrSQLIncrementalTopKOrderNull
	}
	projected := make(Row, len(operator.columns)+1)
	for index, item := range operator.query.selects {
		value, err := evalSQLStreamExpr(item.expr, execRow, nil)
		if err != nil {
			return false, nil, nil, err
		}
		projected[operator.columns[index]] = value
	}
	return true, projected, orderValue, nil
}

func (operator *SQLIncrementalTopK) rowMatches(row Row) (bool, error) {
	if operator.query.where.kind == "" {
		return true, nil
	}
	execRow := newSQLSingleSourceExecRow(operator.query.from.alias, row)
	value, err := evalSQLStreamExpr(operator.query.where, execRow, nil)
	if err != nil {
		return false, err
	}
	return sqlTruthy(value), nil
}
