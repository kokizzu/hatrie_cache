package hatSql

import (
	"fmt"
	"time"
)

type sqlColumnarArithmeticProjectionItem struct {
	field       string
	literal     interface{}
	op          string
	fieldOnLeft bool
	arithmetic  bool
}

func sqlColumnarArithmeticProjectionPlan(q *sqlQuery) ([]sqlColumnarArithmeticProjectionItem, []string, bool) {
	if q == nil || q.explain || q.from == nil || q.from.kind != "CACHE" || len(q.from.fieldTypes) != 0 || len(q.ctes) != 0 || len(q.joins) != 0 || len(q.unions) != 0 || len(q.groupBy) != 0 || len(q.groupingSets) != 0 || q.having.kind != "" || q.distinct || len(q.orderBy) != 0 || q.sample != nil || sqlQueryHasAggregate(q) || sqlQueryHasWindow(q) || sqlQueryHasSubqueryExpression(q) || q.where.kind != "" || len(q.selects) == 0 {
		return nil, nil, false
	}
	items := make([]sqlColumnarArithmeticProjectionItem, len(q.selects))
	fields := make([]string, 0, len(q.selects))
	seen := make(map[string]struct{}, len(q.selects))
	hasArithmetic := false
	addField := func(field string) {
		if _, exists := seen[field]; exists {
			return
		}
		seen[field] = struct{}{}
		fields = append(fields, field)
	}
	for index, selectItem := range q.selects {
		expr := selectItem.expr
		if expr.kind == "field" {
			if expr.name == "*" || expr.window != nil || expr.qualifier != "" && expr.qualifier != q.from.alias {
				return nil, nil, false
			}
			items[index] = sqlColumnarArithmeticProjectionItem{field: expr.name}
			addField(expr.name)
			continue
		}
		if expr.kind != "binary" || expr.window != nil || expr.op != "+" && expr.op != "-" && expr.op != "*" && expr.op != "/" && expr.op != "%" || expr.left == nil || expr.right == nil {
			return nil, nil, false
		}
		left, right := *expr.left, *expr.right
		field, literal, fieldOnLeft, ok := sqlColumnarArithmeticProjectionOperands(left, right, q.from.alias)
		if !ok {
			return nil, nil, false
		}
		if _, ok := sqlNumber(literal); !ok {
			return nil, nil, false
		}
		items[index] = sqlColumnarArithmeticProjectionItem{field: field, literal: literal, op: expr.op, fieldOnLeft: fieldOnLeft, arithmetic: true}
		addField(field)
		hasArithmetic = true
	}
	return items, fields, hasArithmetic
}

func sqlColumnarArithmeticProjectionOperands(left, right sqlExpr, alias string) (field string, literal interface{}, fieldOnLeft, ok bool) {
	if left.kind == "field" && left.window == nil && (left.qualifier == "" || left.qualifier == alias) && right.kind == "literal" {
		return left.name, right.value, true, true
	}
	if right.kind == "field" && right.window == nil && (right.qualifier == "" || right.qualifier == alias) && left.kind == "literal" {
		return right.name, left.value, false, true
	}
	return "", nil, false, false
}

func sqlColumnarArithmeticProjectionRow(items []sqlColumnarArithmeticProjectionItem, columns []string, batch ColumnarBatch, rowIndex int) SQLRow {
	row := make(SQLRow, len(items))
	for selectIndex, item := range items {
		value, _ := batch.Value(item.field, rowIndex)
		if item.arithmetic {
			if item.fieldOnLeft {
				value = sqlArithmeticValue(item.op, value, item.literal)
			} else {
				value = sqlArithmeticValue(item.op, item.literal, value)
			}
		}
		row[columns[selectIndex]] = value
	}
	return row
}

func executeSQLColumnarArithmeticProjectionQueryRows(query *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, visit func(columns []string, row SQLRow) error) (bool, error) {
	items, fields, ok := sqlColumnarArithmeticProjectionPlan(query)
	if !ok {
		return false, nil
	}
	columnar, ok := resolver.(SQLColumnarSourceResolver)
	if !ok {
		return false, nil
	}
	if query.limit == 0 {
		return true, nil
	}
	batch, _, available, err := resolveSQLColumnarSource(columnar, query.from.kind, query.from.key, fields)
	if err != nil || !available {
		return available, err
	}
	if batch.Rows < 0 {
		return true, fmt.Errorf("SQL columnar source %q returned a negative row count", query.from.key)
	}
	if control != nil && batch.Rows > control.maxRows {
		return true, fmt.Errorf("SQL source %q exceeds the %d row limit", query.from.alias, control.maxRows)
	}
	for _, field := range fields {
		if batch.FieldRows(field) != batch.Rows {
			return true, fmt.Errorf("SQL columnar source %q returned %d values for field %q, want %d", query.from.key, batch.FieldRows(field), field, batch.Rows)
		}
	}
	columns := sqlColumns(query.selects)
	emitted, resultBytes := 0, 0
	for rowIndex := 0; rowIndex < batch.Rows; rowIndex++ {
		if control != nil {
			if err := control.check(); err != nil {
				return true, err
			}
		}
		if rowIndex < query.offset {
			continue
		}
		if query.limit >= 0 && emitted >= query.limit {
			break
		}
		row := sqlColumnarArithmeticProjectionRow(items, columns, batch, rowIndex)
		if control != nil && control.options.MaxResultBytes > 0 {
			resultBytes += sqlRowBytes(row)
			if resultBytes > control.options.MaxResultBytes {
				return true, fmt.Errorf("SQL result byte budget exceeded: maximum %d bytes", control.options.MaxResultBytes)
			}
		}
		if err := visit(columns, row); err != nil {
			return true, err
		}
		emitted++
	}
	return true, nil
}

func executeSQLColumnarArithmeticProjection(q *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, metrics *sqlExecutionMetrics, outer *sqlExecRow) (SQLQueryResult, bool, error) {
	items, fields, ok := sqlColumnarArithmeticProjectionPlan(q)
	if !ok || outer != nil {
		return SQLQueryResult{}, false, nil
	}
	columnar, ok := resolver.(SQLColumnarSourceResolver)
	if !ok {
		return SQLQueryResult{}, false, nil
	}
	started := time.Now()
	batch, _, available, err := resolveSQLColumnarSource(columnar, q.from.kind, q.from.key, fields)
	if err != nil || !available {
		return SQLQueryResult{}, available, err
	}
	if batch.Rows < 0 {
		return SQLQueryResult{}, true, fmt.Errorf("SQL columnar source %q returned a negative row count", q.from.key)
	}
	if control != nil && batch.Rows > control.maxRows {
		return SQLQueryResult{}, true, fmt.Errorf("SQL source %q exceeds the %d row limit", q.from.alias, control.maxRows)
	}
	for _, field := range fields {
		if batch.FieldRows(field) != batch.Rows {
			return SQLQueryResult{}, true, fmt.Errorf("SQL columnar source %q returned %d values for field %q, want %d", q.from.key, batch.FieldRows(field), field, batch.Rows)
		}
	}
	capacity := batch.Rows
	if q.limit >= 0 && capacity > q.limit {
		capacity = q.limit
	}
	result := SQLQueryResult{Columns: sqlColumns(q.selects), Rows: make([]SQLRow, 0, capacity)}
	for rowIndex := 0; rowIndex < batch.Rows; rowIndex++ {
		if q.limit >= 0 && len(result.Rows) >= q.limit {
			break
		}
		if rowIndex < q.offset {
			continue
		}
		result.Rows = append(result.Rows, sqlColumnarArithmeticProjectionRow(items, result.Columns, batch, rowIndex))
	}
	if metrics != nil {
		metrics.record("COLUMNAR ARITHMETIC PROJECTION", "direct numeric field/literal", batch.Rows, len(result.Rows), started)
	}
	return result, true, nil
}
