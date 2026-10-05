package hatSql

import "fmt"

// sqlProjectStar copies source fields into a projected row while preserving
// the existing SELECT * merge order and applying any EXCEPT list.
func sqlProjectStar(row SQLRow, source sqlExecRow, expression sqlExpr) {
	if source.singleRow != nil {
		for field, value := range source.singleRow {
			if sqlStarProjectionExcluded(expression, field) {
				continue
			}
			row[field] = value
		}
		return
	}
	for _, alias := range source.order {
		for field, value := range source.sources[alias] {
			if sqlStarProjectionExcluded(expression, field) {
				continue
			}
			row[field] = value
		}
	}
}

func sqlStarProjectionExcluded(expression sqlExpr, field string) bool {
	for _, excluded := range expression.starExcept {
		if excluded == field {
			return true
		}
	}
	return false
}

// executeSQLStarExceptFastPath keeps a simple wildcard projection on the
// source-row path. It avoids allocating execution envelopes and one-row
// groups for the common no-filter, single-source query shape.
func executeSQLStarExceptFastPath(query *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, metrics *sqlExecutionMetrics) (SQLQueryResult, bool, error) {
	if !sqlStarExceptFastPathEligible(query, resolver, control, metrics) {
		return SQLQueryResult{}, false, nil
	}
	rows, err := resolveSQLStarExceptSourceRows(*query.from, resolver)
	if err != nil {
		return SQLQueryResult{}, true, err
	}
	if len(rows) > control.maxRows {
		return SQLQueryResult{}, true, fmt.Errorf("SQL source %q exceeds the %d row limit", query.from.alias, control.maxRows)
	}
	result := SQLQueryResult{Columns: sqlColumns(query.selects), Rows: make([]SQLRow, 0, len(rows))}
	resultBytes := 0
	for _, sourceRow := range rows {
		if err := control.check(); err != nil {
			return SQLQueryResult{}, true, err
		}
		projected := make(SQLRow, len(sourceRow))
		for field, value := range sourceRow {
			if sqlStarProjectionExcluded(query.selects[0].expr, field) {
				continue
			}
			projected[field] = value
		}
		if control.options.MaxResultBytes > 0 {
			resultBytes += sqlRowBytes(projected)
			if resultBytes > control.options.MaxResultBytes {
				return SQLQueryResult{}, true, fmt.Errorf("SQL result byte budget exceeded: maximum %d bytes", control.options.MaxResultBytes)
			}
		}
		result.Rows = append(result.Rows, projected)
	}
	return result, true, nil
}

func sqlStarExceptFastPathEligible(query *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, metrics *sqlExecutionMetrics) bool {
	if query == nil || control == nil || metrics != nil || query.explain || query.pipeline || query.analyze || query.indexHint.Mode != "" || query.from == nil || len(query.selects) != 1 {
		return false
	}
	expression := query.selects[0].expr
	if expression.kind != "star" || len(expression.starExcept) == 0 {
		return false
	}
	if query.from.kind != "CACHE" && query.from.kind != "VALUES" || resolver == nil && query.from.kind != "VALUES" {
		return false
	}
	return !query.from.final && !query.from.lateral && len(query.from.fieldTypes) == 0 && len(query.ctes) == 0 && len(query.joins) == 0 && query.where.kind == "" && query.prewhere.kind == "" && len(query.groupBy) == 0 && len(query.groupingSets) == 0 && query.having.kind == "" && query.qualify.kind == "" && len(query.orderBy) == 0 && query.sample == nil && query.limitBy == nil && query.limit < 0 && query.offset == 0 && !query.distinct && len(query.unions) == 0
}

func resolveSQLStarExceptSourceRows(source sqlSource, resolver SQLSourceResolver) ([]SQLRow, error) {
	switch source.kind {
	case "VALUES":
		return valuesSQLRows(source.values, source.columns), nil
	case "CACHE":
		if resolver == nil {
			return nil, nil
		}
		return resolver.ResolveSQLSource(source.kind, source.key)
	default:
		return nil, fmt.Errorf("SQL source %q cannot use the SELECT * EXCEPT fast path", source.kind)
	}
}
