package hatSql

import (
	"fmt"
	"strings"
	"time"
)

type sqlColumnarPrewhereResult struct {
	predicateRows   int
	matches         []int
	projectionBatch ColumnarBatch
}

func resolveSQLColumnarPrewhere(q *sqlQuery, resolver SQLSourceResolver, columnar ColumnarSourceResolver, control *sqlExecutionControl, predicateFields, projectionFields []string) (sqlColumnarPrewhereResult, bool, error) {
	if q == nil || q.from == nil || q.where.kind == "" || len(predicateFields) == 0 {
		return sqlColumnarPrewhereResult{}, false, nil
	}
	prewhere, ok := columnar.(ColumnarPrewhereSourceResolver)
	if !ok {
		return sqlColumnarPrewhereResult{}, false, nil
	}
	predicateSet := make(map[string]struct{}, len(predicateFields))
	for _, field := range predicateFields {
		predicateSet[field] = struct{}{}
	}
	projectionFetchFields := make([]string, 0, len(projectionFields))
	projectionSeen := make(map[string]struct{}, len(projectionFields))
	projectionOnly := false
	for _, field := range projectionFields {
		if _, exists := predicateSet[field]; !exists {
			projectionOnly = true
		}
		if _, exists := projectionSeen[field]; exists {
			continue
		}
		projectionSeen[field] = struct{}{}
		projectionFetchFields = append(projectionFetchFields, field)
	}
	if !projectionOnly || len(projectionFetchFields) == 0 {
		return sqlColumnarPrewhereResult{}, false, nil
	}

	predicateBatch, available, err := prewhere.ResolveSQLColumnarPrewhere(q.from.kind, q.from.key, predicateFields)
	if err != nil {
		return sqlColumnarPrewhereResult{}, true, err
	}
	if !available {
		return sqlColumnarPrewhereResult{}, false, nil
	}
	if err := validateSQLColumnarPrewhereBatch(predicateBatch, predicateFields, control); err != nil {
		return sqlColumnarPrewhereResult{}, true, err
	}

	functions, _ := resolver.(SQLFunctionResolver)
	match := sqlColumnarQueryRowsMatcher(q, predicateBatch, functions)
	matches := make([]int, 0)
	for rowIndex := 0; rowIndex < predicateBatch.Rows; rowIndex++ {
		if control != nil {
			if err := control.check(); err != nil {
				return sqlColumnarPrewhereResult{}, true, err
			}
		}
		matched, err := match(rowIndex)
		if err != nil {
			return sqlColumnarPrewhereResult{}, true, err
		}
		if matched {
			matches = append(matches, rowIndex)
		}
	}
	result := sqlColumnarPrewhereResult{predicateRows: predicateBatch.Rows, matches: matches}
	if len(matches) == 0 {
		return result, true, nil
	}

	projectionBatch, available, err := prewhere.ResolveSQLColumnarProjection(q.from.kind, q.from.key, projectionFetchFields, matches)
	if err != nil {
		return sqlColumnarPrewhereResult{}, true, err
	}
	if !available {
		return sqlColumnarPrewhereResult{}, false, nil
	}
	if projectionBatch.Rows != len(matches) {
		return sqlColumnarPrewhereResult{}, true, fmt.Errorf("SQL prewhere projection returned %d rows, want %d", projectionBatch.Rows, len(matches))
	}
	if err := validateSQLColumnarPrewhereBatch(projectionBatch, projectionFetchFields, nil); err != nil {
		return sqlColumnarPrewhereResult{}, true, err
	}
	result.projectionBatch = projectionBatch
	return result, true, nil
}

func executeSQLColumnarPrewhereScan(q *sqlQuery, resolver SQLSourceResolver, columnar ColumnarSourceResolver, control *sqlExecutionControl, metrics *sqlExecutionMetrics, predicateFields, projectionFields []string) (SQLQueryResult, bool, error) {
	started := time.Now()
	prewhere, handled, err := resolveSQLColumnarPrewhere(q, resolver, columnar, control, predicateFields, projectionFields)
	if !handled {
		return SQLQueryResult{}, false, err
	}
	if err != nil {
		return SQLQueryResult{}, true, err
	}
	if metrics != nil {
		metrics.record("COLUMNAR PREWHERE", strings.Join(predicateFields, ","), prewhere.predicateRows, len(prewhere.matches), started)
	}
	if len(prewhere.matches) == 0 {
		return SQLQueryResult{Columns: sqlColumns(q.selects), Rows: []SQLRow{}}, true, nil
	}
	result := sqlColumnarMaterializeCompactedMatches(q, prewhere.projectionBatch)
	if metrics != nil {
		metrics.record("COLUMNAR PREWHERE PROJECTION", strings.Join(projectionFields, ","), len(prewhere.matches), len(result.Rows), started)
	}
	return result, true, nil
}

func executeSQLColumnarPrewhereQueryRows(q *sqlQuery, resolver SQLSourceResolver, columnar ColumnarSourceResolver, control *sqlExecutionControl, visit func(columns []string, row SQLRow) error, predicateFields, projectionFields []string) (bool, error) {
	prewhere, handled, err := resolveSQLColumnarPrewhere(q, resolver, columnar, control, predicateFields, projectionFields)
	if !handled {
		return false, err
	}
	if err != nil {
		return true, err
	}
	columns := sqlColumns(q.selects)
	emitted, resultBytes := 0, 0
	for rowIndex := 0; rowIndex < prewhere.projectionBatch.Rows; rowIndex++ {
		if control != nil {
			if err := control.check(); err != nil {
				return true, err
			}
		}
		if rowIndex < q.offset {
			continue
		}
		if q.limit >= 0 && emitted >= q.limit {
			break
		}
		row := make(SQLRow, len(q.selects))
		for selectIndex, item := range q.selects {
			row[columns[selectIndex]], _ = prewhere.projectionBatch.Value(item.expr.name, rowIndex)
		}
		if control != nil && control.options.MaxResultBytes > 0 {
			resultBytes += sqlRowBytes(row)
			if resultBytes > control.options.MaxResultBytes {
				return true, fmt.Errorf("SQL result byte budget exceeded: maximum %d bytes", control.options.MaxResultBytes)
			}
		}
		emitted++
		if err := visit(columns, row); err != nil {
			return true, err
		}
	}
	return true, nil
}

func validateSQLColumnarPrewhereBatch(batch ColumnarBatch, fields []string, control *sqlExecutionControl) error {
	if batch.Rows < 0 {
		return fmt.Errorf("SQL prewhere source returned a negative row count")
	}
	if control != nil && batch.Rows > control.maxRows {
		return fmt.Errorf("SQL prewhere source exceeds the %d row limit", control.maxRows)
	}
	for _, field := range fields {
		if batch.FieldRows(field) != batch.Rows {
			return fmt.Errorf("SQL prewhere source returned %d values for field %q, want %d", batch.FieldRows(field), field, batch.Rows)
		}
	}
	return nil
}

func sqlColumnarMaterializeCompactedMatches(q *sqlQuery, batch ColumnarBatch) SQLQueryResult {
	result := SQLQueryResult{Columns: sqlColumns(q.selects), Rows: []SQLRow{}}
	for rowIndex := 0; rowIndex < batch.Rows; rowIndex++ {
		if rowIndex < q.offset || q.limit >= 0 && len(result.Rows) >= q.limit {
			continue
		}
		row := make(SQLRow, len(q.selects))
		for selectIndex, item := range q.selects {
			row[result.Columns[selectIndex]], _ = batch.Value(item.expr.name, rowIndex)
		}
		result.Rows = append(result.Rows, row)
	}
	return result
}
