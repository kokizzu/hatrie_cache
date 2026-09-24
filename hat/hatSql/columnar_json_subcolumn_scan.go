package hatSql

import (
	"container/heap"
	"fmt"
	"sort"
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
	if q == nil || outer != nil || q.from == nil || q.from.kind != "CACHE" || len(q.from.fieldTypes) != 0 || len(q.ctes) != 0 || len(q.joins) != 0 || len(q.unions) != 0 || len(q.groupBy) != 0 || len(q.groupingSets) != 0 || q.having.kind != "" || q.distinct || len(q.orderBy) > 1 || q.sample != nil || q.prewhere.kind != "" || sqlQueryHasAggregate(q) || sqlQueryHasWindow(q) || sqlQueryHasSubqueryExpression(q) || len(q.selects) == 0 {
		return nil, nil, false
	}
	if len(q.orderBy) == 1 && (q.limit < 0 || q.limitBy != nil || sqlQueryHasWithFill(q)) {
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
	if len(q.orderBy) == 1 {
		path, pathOK := sqlColumnarJSONSubcolumnPath(q.orderBy[0].expr, q.from.alias)
		if !pathOK {
			return nil, nil, false
		}
		addPath(path)
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

type sqlColumnarJSONSubcolumnGroupAggregatePlan struct {
	fields      []string
	paths       []ColumnarJSONSubcolumnRequest
	group       sqlExpr
	groupSelect []bool
	countSelect []bool
}

func sqlColumnarJSONSubcolumnExprFields(expr sqlExpr, alias string, add func(string)) bool {
	switch expr.kind {
	case "literal":
		return true
	case "field":
		if expr.qualifier != "" && expr.qualifier != alias {
			return false
		}
		add(expr.name)
		return true
	case "func":
		_, ok := sqlColumnarJSONSubcolumnPath(expr, alias)
		return ok
	case "binary":
		if expr.left == nil || !sqlColumnarJSONSubcolumnExprFields(*expr.left, alias, add) {
			return false
		}
		if expr.op == "IS NULL" || expr.op == "IS NOT NULL" {
			return true
		}
		return expr.right != nil && sqlColumnarJSONSubcolumnExprFields(*expr.right, alias, add)
	case "in":
		if expr.left == nil || len(expr.args) == 0 || !sqlColumnarJSONSubcolumnExprFields(*expr.left, alias, add) {
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

func sqlColumnarJSONSubcolumnSamePath(left, right sqlExpr, alias string) bool {
	if left.kind != "func" || right.kind != "func" || left.name != "JSON_VALUE" || right.name != "JSON_VALUE" {
		return false
	}
	leftPath, leftOK := sqlColumnarJSONSubcolumnPath(left, alias)
	rightPath, rightOK := sqlColumnarJSONSubcolumnPath(right, alias)
	return leftOK && rightOK && leftPath == rightPath
}

func sqlColumnarJSONSubcolumnCountStar(expr sqlExpr) bool {
	return expr.kind == "func" && expr.name == "COUNT" && (len(expr.args) == 0 || len(expr.args) == 1 && expr.args[0].kind == "star")
}

func sqlColumnarJSONSubcolumnGroupAggregatePlanFor(q *sqlQuery, outer *sqlExecRow) (sqlColumnarJSONSubcolumnGroupAggregatePlan, bool) {
	if q == nil || outer != nil || q.from == nil || q.from.kind != "CACHE" || len(q.from.fieldTypes) != 0 || len(q.ctes) != 0 || len(q.joins) != 0 || len(q.unions) != 0 || len(q.groupBy) != 1 || len(q.groupingSets) != 0 || q.having.kind != "" || q.distinct || len(q.orderBy) != 0 || q.limitBy != nil || q.sample != nil || q.prewhere.kind != "" || sqlQueryHasWindow(q) || sqlQueryHasSubqueryExpression(q) || len(q.selects) == 0 {
		return sqlColumnarJSONSubcolumnGroupAggregatePlan{}, false
	}
	if q.groupBy[0].name != "JSON_VALUE" {
		return sqlColumnarJSONSubcolumnGroupAggregatePlan{}, false
	}
	groupPath, ok := sqlColumnarJSONSubcolumnPath(q.groupBy[0], q.from.alias)
	if !ok {
		return sqlColumnarJSONSubcolumnGroupAggregatePlan{}, false
	}
	plan := sqlColumnarJSONSubcolumnGroupAggregatePlan{
		group:       q.groupBy[0],
		groupSelect: make([]bool, len(q.selects)),
		countSelect: make([]bool, len(q.selects)),
	}
	seenFields := make(map[string]struct{})
	addField := func(field string) {
		if _, found := seenFields[field]; found {
			return
		}
		seenFields[field] = struct{}{}
		plan.fields = append(plan.fields, field)
	}
	seenPaths := make(map[string]struct{})
	addPath := func(path ColumnarJSONSubcolumnRequest) {
		key := path.Field + "\x00" + path.Path
		if _, found := seenPaths[key]; found {
			return
		}
		seenPaths[key] = struct{}{}
		plan.paths = append(plan.paths, path)
	}
	addPath(groupPath)
	if q.where.kind != "" {
		if !sqlColumnarJSONSubcolumnExprSupported(q.where, q.from.alias, addPath) || !sqlColumnarJSONSubcolumnExprFields(q.where, q.from.alias, addField) {
			return sqlColumnarJSONSubcolumnGroupAggregatePlan{}, false
		}
	}
	countFound := false
	for index, item := range q.selects {
		switch {
		case sqlColumnarJSONSubcolumnSamePath(item.expr, q.groupBy[0], q.from.alias):
			plan.groupSelect[index] = true
		case sqlColumnarJSONSubcolumnCountStar(item.expr):
			plan.countSelect[index] = true
			countFound = true
		default:
			return sqlColumnarJSONSubcolumnGroupAggregatePlan{}, false
		}
	}
	if !countFound {
		return sqlColumnarJSONSubcolumnGroupAggregatePlan{}, false
	}
	return plan, true
}

type sqlColumnarJSONSubcolumnGroupAggregateState struct {
	key   interface{}
	count int64
}

func sqlColumnarJSONSubcolumnGroupScalar(value interface{}) (interface{}, bool) {
	switch value.(type) {
	case nil, int64, float64, string, bool:
		return value, true
	default:
		return nil, false
	}
}

func sqlColumnarJSONSubcolumnGroupMemoryBytes(groups []*sqlColumnarJSONSubcolumnGroupAggregateState) int {
	bytes := len(groups) * 64
	for _, group := range groups {
		switch value := group.key.(type) {
		case string:
			bytes += len(value)
		default:
			_ = value
			bytes += 8
		}
	}
	return bytes
}

func executeSQLColumnarJSONSubcolumnGroupAggregate(q *sqlQuery, batch ColumnarBatch, plan sqlColumnarJSONSubcolumnGroupAggregatePlan, functions SQLFunctionResolver, control *sqlExecutionControl, metrics *sqlExecutionMetrics, started time.Time) (SQLQueryResult, bool, error) {
	groups := make([]*sqlColumnarJSONSubcolumnGroupAggregateState, 0)
	byKey := make(map[interface{}]*sqlColumnarJSONSubcolumnGroupAggregateState)
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
		value, present, supported := sqlColumnarJSONSubcolumnValue(plan.group, row)
		if !supported {
			return SQLQueryResult{}, false, nil
		}
		if !present {
			value = nil
		}
		key, keySupported := sqlColumnarJSONSubcolumnGroupScalar(value)
		if !keySupported {
			return SQLQueryResult{}, false, nil
		}
		group, found := byKey[key]
		if !found {
			if control != nil && control.options.MaxGroupKeys > 0 && len(groups) >= control.options.MaxGroupKeys {
				return SQLQueryResult{}, true, fmt.Errorf("SQL group key limit exceeded: query produced %d groups, maximum %d", len(groups)+1, control.options.MaxGroupKeys)
			}
			group = &sqlColumnarJSONSubcolumnGroupAggregateState{key: key}
			byKey[key] = group
			groups = append(groups, group)
		}
		group.count++
		if control != nil && control.options.MaxGroupRowsPerKey > 0 && group.count > int64(control.options.MaxGroupRowsPerKey) {
			return SQLQueryResult{}, true, fmt.Errorf("SQL group row limit exceeded: group contains %d rows, maximum %d", group.count, control.options.MaxGroupRowsPerKey)
		}
	}
	if control != nil && (control.options.MaxGroupBytes > 0 || control.operatorMemory != nil) {
		groupBytes := sqlColumnarJSONSubcolumnGroupMemoryBytes(groups)
		if err := control.observeOperatorMemory("GROUP BY", groupBytes); err != nil {
			return SQLQueryResult{}, true, err
		}
		if control.options.MaxGroupBytes > 0 && groupBytes > control.options.MaxGroupBytes {
			return SQLQueryResult{}, true, fmt.Errorf("SQL group memory budget exceeded: maximum %d bytes", control.options.MaxGroupBytes)
		}
	}
	result := SQLQueryResult{Columns: sqlColumns(q.selects), Rows: make([]SQLRow, 0, len(groups))}
	for position, group := range groups {
		if position < q.offset || q.limit >= 0 && len(result.Rows) >= q.limit {
			continue
		}
		row := make(SQLRow, len(q.selects))
		for index := range q.selects {
			switch {
			case plan.groupSelect[index]:
				row[result.Columns[index]] = group.key
			case plan.countSelect[index]:
				row[result.Columns[index]] = group.count
			}
		}
		result.Rows = append(result.Rows, row)
	}
	if control != nil && control.options.MaxResultBytes > 0 && sqlRowsBytes(result.Rows) > control.options.MaxResultBytes {
		return SQLQueryResult{}, true, fmt.Errorf("SQL result byte budget exceeded: maximum %d bytes", control.options.MaxResultBytes)
	}
	if metrics != nil {
		pathNames := make([]string, len(plan.paths))
		for index, path := range plan.paths {
			pathNames[index] = path.Field + path.Path
		}
		metrics.record("COLUMNAR JSON SUBCOLUMN GROUP AGGREGATE", strings.Join(pathNames, ","), batch.Rows, len(result.Rows), started)
	}
	return result, true, nil
}

func sqlColumnarJSONSubcolumnTopNPath(q *sqlQuery) (ColumnarJSONSubcolumnRequest, bool) {
	if q == nil || len(q.orderBy) != 1 || q.limit < 0 || q.limitBy != nil || sqlQueryHasWithFill(q) {
		return ColumnarJSONSubcolumnRequest{}, false
	}
	if q.orderBy[0].expr.kind != "func" || q.orderBy[0].expr.name != "JSON_VALUE" {
		return ColumnarJSONSubcolumnRequest{}, false
	}
	return sqlColumnarJSONSubcolumnPath(q.orderBy[0].expr, q.from.alias)
}

func executeSQLColumnarJSONSubcolumnScan(q *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, metrics *sqlExecutionMetrics, outer *sqlExecRow) (SQLQueryResult, bool, error) {
	subcolumns, ok := resolver.(ColumnarJSONSubcolumnSourceResolver)
	if !ok {
		return SQLQueryResult{}, false, nil
	}
	groupPlan, grouped := sqlColumnarJSONSubcolumnGroupAggregatePlanFor(q, outer)
	var fields []string
	var paths []ColumnarJSONSubcolumnRequest
	if grouped {
		fields, paths = groupPlan.fields, groupPlan.paths
	} else {
		fields, paths, ok = sqlColumnarJSONSubcolumnScanPlan(q, outer)
		if !ok {
			return SQLQueryResult{}, false, nil
		}
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
	if grouped {
		functions, _ := resolver.(SQLFunctionResolver)
		return executeSQLColumnarJSONSubcolumnGroupAggregate(q, batch, groupPlan, functions, control, metrics, started)
	}
	if _, ordered := sqlColumnarJSONSubcolumnTopNPath(q); ordered {
		functions, _ := resolver.(SQLFunctionResolver)
		return executeSQLColumnarJSONSubcolumnTopN(q, batch, functions, control, metrics, started)
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

func executeSQLColumnarJSONSubcolumnTopN(q *sqlQuery, batch ColumnarBatch, functions SQLFunctionResolver, control *sqlExecutionControl, metrics *sqlExecutionMetrics, started time.Time) (SQLQueryResult, bool, error) {
	if control == nil {
		return SQLQueryResult{}, false, nil
	}
	orderPath, ok := sqlColumnarJSONSubcolumnTopNPath(q)
	if !ok {
		return SQLQueryResult{}, false, nil
	}
	capacity := sqlTopNStreamCapacity(q, control.maxRows)
	if capacity == 0 {
		return SQLQueryResult{Columns: sqlColumns(q.selects), Rows: []SQLRow{}}, true, nil
	}
	candidates := sqlTopNStreamHeap{items: make([]sqlTopNStreamItem, 0, capacity), order: q.orderBy}
	heap.Init(&candidates)
	for rowIndex := 0; rowIndex < batch.Rows; rowIndex++ {
		if err := control.check(); err != nil {
			return SQLQueryResult{}, true, err
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
		value, present, supported := sqlColumnarJSONSubcolumnValue(q.orderBy[0].expr, row)
		if !supported || orderPath.Field == "" || orderPath.Path == "" {
			return SQLQueryResult{}, false, nil
		}
		if !present {
			value = nil
		}
		candidate := sqlTopNStreamItem{ordinal: rowIndex, key: value}
		if candidates.Len() < capacity {
			heap.Push(&candidates, candidate)
			continue
		}
		if sqlTopNStreamBefore(candidate, candidates.items[0], q.orderBy) {
			candidates.items[0] = candidate
			heap.Fix(&candidates, 0)
		}
	}
	sort.SliceStable(candidates.items, func(left, right int) bool {
		return sqlTopNStreamBefore(candidates.items[left], candidates.items[right], q.orderBy)
	})
	result := SQLQueryResult{Columns: sqlColumns(q.selects), Rows: make([]SQLRow, 0, q.limit)}
	start := q.offset
	if start > len(candidates.items) {
		start = len(candidates.items)
	}
	end := start + q.limit
	if end > len(candidates.items) {
		end = len(candidates.items)
	}
	for _, candidate := range candidates.items[start:end] {
		row := newSQLColumnarSourceExecRow(q.from.alias, &batch, candidate.ordinal)
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
	if metrics != nil {
		metrics.record("COLUMNAR JSON SUBCOLUMN TOP-N", sqlExplainOrders(q.orderBy), batch.Rows, len(result.Rows), started)
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
