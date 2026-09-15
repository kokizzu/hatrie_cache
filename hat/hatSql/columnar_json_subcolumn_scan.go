package hatSql

import (
	"fmt"
	"strings"
	"time"
)

func sqlColumnarJSONSubcolumnPath(expr sqlExpr, alias string) (ColumnarJSONSubcolumnRequest, bool) {
	if expr.kind != "func" || len(expr.args) != 2 {
		return ColumnarJSONSubcolumnRequest{}, false
	}
	switch expr.name {
	case "JSON_VALUE", "JSON_QUERY", "JSON_EXISTS":
	default:
		return ColumnarJSONSubcolumnRequest{}, false
	}
	field := expr.args[0]
	if field.kind != "field" || field.qualifier != "" && field.qualifier != alias {
		return ColumnarJSONSubcolumnRequest{}, false
	}
	path, ok := expr.args[1].value.(string)
	if expr.args[1].kind != "literal" || !ok {
		return ColumnarJSONSubcolumnRequest{}, false
	}
	segments, err := parseSQLJSONPath(path)
	if err != nil {
		return ColumnarJSONSubcolumnRequest{}, false
	}
	return ColumnarJSONSubcolumnRequest{Field: field.name, Path: formatSQLJSONPath(segments)}, true
}

func sqlColumnarJSONSubcolumnExprSupported(expr sqlExpr, alias string, add func(ColumnarJSONSubcolumnRequest)) bool {
	switch expr.kind {
	case "literal":
		return true
	case "field":
		return expr.qualifier == "" || expr.qualifier == alias
	case "func":
		path, ok := sqlColumnarJSONSubcolumnPath(expr, alias)
		if !ok {
			return false
		}
		if add != nil {
			add(path)
		}
		return true
	case "binary":
		if expr.left == nil || !sqlColumnarJSONSubcolumnExprSupported(*expr.left, alias, add) {
			return false
		}
		if expr.op == "IS NULL" || expr.op == "IS NOT NULL" {
			return true
		}
		return expr.right != nil && sqlColumnarJSONSubcolumnExprSupported(*expr.right, alias, add)
	case "in":
		if expr.left == nil || !sqlColumnarJSONSubcolumnExprSupported(*expr.left, alias, add) || len(expr.args) == 0 {
			return false
		}
		for _, argument := range expr.args {
			if argument.kind != "literal" {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func sqlColumnarJSONSubcolumnScanPlan(q *sqlQuery, outer *sqlExecRow) (fields []string, paths []ColumnarJSONSubcolumnRequest, ok bool) {
	if q == nil || outer != nil || q.from == nil || q.from.kind != "CACHE" || len(q.from.fieldTypes) != 0 || len(q.ctes) != 0 || len(q.joins) != 0 || len(q.unions) != 0 || len(q.groupBy) != 0 || len(q.groupingSets) != 0 || q.having.kind != "" || q.distinct || len(q.orderBy) != 0 || q.sample != nil || q.prewhere.kind != "" || sqlQueryHasAggregate(q) || sqlQueryHasWindow(q) || sqlQueryHasSubqueryExpression(q) || len(q.selects) == 0 {
		return nil, nil, false
	}
	seenFields := make(map[string]struct{})
	addField := func(field string) {
		if _, found := seenFields[field]; found {
			return
		}
		seenFields[field] = struct{}{}
		fields = append(fields, field)
	}
	seenPaths := make(map[string]struct{})
	addPath := func(path ColumnarJSONSubcolumnRequest) {
		key := path.Field + "\x00" + path.Path
		if _, found := seenPaths[key]; found {
			return
		}
		seenPaths[key] = struct{}{}
		paths = append(paths, path)
	}
	if q.where.kind != "" && !sqlColumnarJSONSubcolumnExprSupported(q.where, q.from.alias, addPath) {
		return nil, nil, false
	}
	for _, item := range q.selects {
		switch item.expr.kind {
		case "field":
			if item.expr.qualifier != "" && item.expr.qualifier != q.from.alias {
				return nil, nil, false
			}
			addField(item.expr.name)
		case "func":
			path, pathOK := sqlColumnarJSONSubcolumnPath(item.expr, q.from.alias)
			if !pathOK {
				return nil, nil, false
			}
			addPath(path)
		default:
			return nil, nil, false
		}
	}
	if len(paths) == 0 {
		return nil, nil, false
	}
	return fields, paths, true
}

func executeSQLColumnarJSONSubcolumnScan(q *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, metrics *sqlExecutionMetrics, outer *sqlExecRow) (SQLQueryResult, bool, error) {
	subcolumns, ok := resolver.(ColumnarJSONSubcolumnSourceResolver)
	if !ok {
		return SQLQueryResult{}, false, nil
	}
	fields, paths, ok := sqlColumnarJSONSubcolumnScanPlan(q, outer)
	if !ok {
		return SQLQueryResult{}, false, nil
	}
	started := time.Now()
	batch, _, available, err := subcolumns.ResolveSQLColumnarJSONSubcolumns(q.from.kind, q.from.key, fields, paths)
	if err != nil || !available {
		return SQLQueryResult{}, false, err
	}
	if batch.Rows < 0 {
		return SQLQueryResult{}, true, fmt.Errorf("SQL columnar JSON subcolumn source %q returned a negative row count", q.from.key)
	}
	if control != nil && batch.Rows > control.maxRows {
		return SQLQueryResult{}, true, fmt.Errorf("SQL source %q exceeds the %d row limit", q.from.alias, control.maxRows)
	}
	for _, field := range fields {
		if batch.FieldRows(field) != batch.Rows {
			return SQLQueryResult{}, true, fmt.Errorf("SQL columnar JSON subcolumn source %q returned %d values for field %q, want %d", q.from.key, batch.FieldRows(field), field, batch.Rows)
		}
	}
	for _, path := range paths {
		key := ColumnarJSONSubcolumnKey{Field: path.Field, Path: path.Path}
		column, found := batch.JSONSubcolumns[key]
		if !found {
			return SQLQueryResult{}, false, nil
		}
		if err := column.Validate(batch.Rows); err != nil {
			return SQLQueryResult{}, true, fmt.Errorf("SQL columnar JSON subcolumn source %q path %s%s: %w", q.from.key, path.Field, path.Path, err)
		}
	}
	if metrics != nil {
		pathNames := make([]string, len(paths))
		for index, path := range paths {
			pathNames[index] = path.Field + path.Path
		}
		metrics.record("COLUMNAR JSON SUBCOLUMN SCAN", strings.Join(pathNames, ",")+" fields="+strings.Join(fields, ","), 0, batch.Rows, started)
	}
	functions, _ := resolver.(SQLFunctionResolver)
	result := SQLQueryResult{Columns: sqlColumns(q.selects), Rows: []SQLRow{}}
	matched := 0
	for rowIndex := 0; rowIndex < batch.Rows; rowIndex++ {
		if control != nil {
			if err := control.check(); err != nil {
				return SQLQueryResult{}, true, err
			}
		}
		row := newSQLColumnarSourceExecRow(q.from.alias, &batch, rowIndex)
		if q.where.kind != "" {
			condition, err := evalSQLStreamExpr(q.where, row, functions)
			if err != nil {
				return SQLQueryResult{}, true, err
			}
			if !sqlTruthy(condition) {
				continue
			}
		}
		position := matched
		matched++
		if position < q.offset {
			continue
		}
		if q.limit >= 0 && len(result.Rows) >= q.limit {
			break
		}
		output := make(SQLRow, len(q.selects))
		for index, item := range q.selects {
			value, err := evalSQLStreamExpr(item.expr, row, functions)
			if err != nil {
				return SQLQueryResult{}, true, err
			}
			output[result.Columns[index]] = value
		}
		result.Rows = append(result.Rows, output)
	}
	return result, true, nil
}

func sqlColumnarJSONSubcolumnValue(expr sqlExpr, row sqlExecRow) (interface{}, bool, bool) {
	if expr.kind != "func" || len(expr.args) != 2 || expr.args[0].kind != "field" || expr.args[1].kind != "literal" {
		return nil, false, false
	}
	field := expr.args[0]
	path, pathOK := expr.args[1].value.(string)
	if !pathOK {
		return nil, false, false
	}
	for current := &row; current != nil; current = current.outer {
		if current.columnar == nil || len(current.columnar.JSONSubcolumns) == 0 {
			continue
		}
		if field.qualifier != "" && field.qualifier != current.singleAlias {
			continue
		}
		canonical := path
		if expr.jsonPath == nil || expr.jsonPath.source != path || expr.jsonPath.err != nil {
			segments, err := parseSQLJSONPath(path)
			if err != nil {
				return nil, false, false
			}
			canonical = formatSQLJSONPath(segments)
		} else if prepared := expr.jsonPath.canonicalPath(); prepared != "" {
			canonical = prepared
		}
		request := ColumnarJSONSubcolumnKey{Field: field.name, Path: canonical}
		column, found := current.columnar.JSONSubcolumns[request]
		if !found {
			continue
		}
		value, present := column.Value(current.columnarRow)
		return value, present, true
	}
	return nil, false, false
}
