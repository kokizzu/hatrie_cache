package hatSql

import (
	"fmt"
	"time"
)

func executeSQLUnionResult(q *sqlQuery, result SQLQueryResult, resolver SQLSourceResolver, ctes map[string][]SQLRow, metrics *sqlExecutionMetrics, control *sqlExecutionControl) (SQLQueryResult, error) {
	for _, union := range q.unions {
		var right SQLQueryResult
		var err error
		nativeBranch := false
		if control != nil && control.options.SubqueryResultCache == nil && sqlAutoNativeUnionBranchEligible(union.query, resolver, control.options) {
			right, nativeBranch, err = executeSQLAutoNativeDataflow(control.ctx, union.query, resolver, control.options, control, metrics != nil)
			if nativeBranch && metrics != nil {
				metrics.steps = append(metrics.steps, right.Plan...)
			}
		}
		if !nativeBranch {
			if control == nil || control.options.SubqueryResultCache == nil {
				right, err = executeSQLQueryWithMetrics(union.query, resolver, ctes, metrics, control)
			} else {
				right, err = executeSQLCachedSubquery(union.query, resolver, ctes, metrics, control)
			}
		}
		if err != nil {
			return SQLQueryResult{}, err
		}
		if !sameSQLColumns(result.Columns, right.Columns) {
			return SQLQueryResult{}, fmt.Errorf("%s queries must project the same column names in the same order", union.kind)
		}
		started := time.Now()
		inputRows := len(result.Rows) + len(right.Rows)
		setBytes := sqlRowsBytes(result.Rows) + sqlRowsBytes(right.Rows)
		if control != nil && !union.all {
			if err := control.observeOperatorMemory("SET", setBytes); err != nil {
				return SQLQueryResult{}, err
			}
		}
		if control != nil && !union.all && control.options.MaxSetBytes > 0 && setBytes > control.options.MaxSetBytes {
			if control.options.SpillDirectory != "" && control.options.MaxSpillBytes > 0 {
				rows, spillBytes, runs, err := sqlExternalSetRows(result.Rows, right.Rows, union.kind, control.options.SpillDirectory, control.options.MaxSetBytes, control.options.MaxSpillBytes, sqlQueryCollation(q), control)
				if err != nil {
					return SQLQueryResult{}, err
				}
				result.Rows = rows
				metrics.record("EXTERNAL SET", fmt.Sprintf("%s spill_bytes=%d runs=%d", union.kind, spillBytes, runs), inputRows, len(result.Rows), started)
				continue
			}
			return SQLQueryResult{}, fmt.Errorf("SQL set memory budget exceeded: maximum %d bytes", control.options.MaxSetBytes)
		}
		switch union.kind {
		case "UNION":
			if union.all {
				result.Rows = append(result.Rows, right.Rows...)
				break
			}
			seen := make(map[string]struct{}, len(result.Rows)+len(right.Rows))
			unique := result.Rows[:0]
			for _, row := range result.Rows {
				key := sqlOutputRowKeyWithCollation(row, sqlQueryCollation(q))
				if _, exists := seen[key]; exists {
					continue
				}
				seen[key] = struct{}{}
				unique = append(unique, row)
			}
			for _, row := range right.Rows {
				key := sqlOutputRowKeyWithCollation(row, sqlQueryCollation(q))
				if _, exists := seen[key]; exists {
					continue
				}
				seen[key] = struct{}{}
				unique = append(unique, row)
			}
			result.Rows = unique
		case "INTERSECT":
			if union.all {
				available := make(map[string]int, len(right.Rows))
				for _, row := range right.Rows {
					key := sqlOutputRowKeyWithCollation(row, sqlQueryCollation(q))
					available[key]++
				}
				filtered := result.Rows[:0]
				for _, row := range result.Rows {
					key := sqlOutputRowKeyWithCollation(row, sqlQueryCollation(q))
					count := available[key]
					if count == 0 {
						continue
					}
					filtered = append(filtered, row)
					if count == 1 {
						delete(available, key)
					} else {
						available[key] = count - 1
					}
				}
				result.Rows = filtered
				break
			}
			available := make(map[string]struct{}, len(right.Rows))
			for _, row := range right.Rows {
				available[sqlOutputRowKeyWithCollation(row, sqlQueryCollation(q))] = struct{}{}
			}
			filtered := result.Rows[:0]
			for _, row := range result.Rows {
				if _, exists := available[sqlOutputRowKeyWithCollation(row, sqlQueryCollation(q))]; exists {
					filtered = append(filtered, row)
				}
			}
			result.Rows = distinctSQLQueryRows(filtered, sqlQueryCollation(q))
		case "EXCEPT":
			if union.all {
				excluded := make(map[string]int, len(right.Rows))
				for _, row := range right.Rows {
					key := sqlOutputRowKeyWithCollation(row, sqlQueryCollation(q))
					excluded[key]++
				}
				filtered := result.Rows[:0]
				for _, row := range result.Rows {
					key := sqlOutputRowKeyWithCollation(row, sqlQueryCollation(q))
					count := excluded[key]
					if count > 0 {
						if count == 1 {
							delete(excluded, key)
						} else {
							excluded[key] = count - 1
						}
						continue
					}
					filtered = append(filtered, row)
				}
				result.Rows = filtered
				break
			}
			excluded := make(map[string]struct{}, len(right.Rows))
			for _, row := range right.Rows {
				excluded[sqlOutputRowKeyWithCollation(row, sqlQueryCollation(q))] = struct{}{}
			}
			filtered := result.Rows[:0]
			for _, row := range result.Rows {
				if _, exists := excluded[sqlOutputRowKeyWithCollation(row, sqlQueryCollation(q))]; !exists {
					filtered = append(filtered, row)
				}
			}
			result.Rows = distinctSQLQueryRows(filtered, sqlQueryCollation(q))
		default:
			return SQLQueryResult{}, fmt.Errorf("unsupported SQL set operation %q", union.kind)
		}
		kind := union.kind
		if union.all {
			kind += " ALL"
		} else if union.kind == "UNION" {
			kind += " early duplicate elimination"
		}
		metrics.record("SET", kind, inputRows, len(result.Rows), started)
	}
	return result, nil
}
