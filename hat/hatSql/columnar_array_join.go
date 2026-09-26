package hatSql

import (
	"fmt"
	"time"
)

type sqlColumnarArrayJoinProjection struct {
	name   string
	field  string
	joined bool
}

type sqlColumnarArrayJoinPlan struct {
	fields      []string
	arrayField  string
	joinAlias   string
	left        bool
	projections []sqlColumnarArrayJoinProjection
}

func sqlColumnarArrayJoinPlanFor(q *sqlQuery, outer *sqlExecRow) (sqlColumnarArrayJoinPlan, bool) {
	if q == nil || outer != nil || q.from == nil || q.from.kind != "CACHE" || len(q.from.fieldTypes) != 0 || len(q.ctes) != 0 || len(q.joins) != 1 || len(q.unions) != 0 || q.where.kind != "" || q.prewhere.kind != "" || len(q.groupBy) != 0 || len(q.groupingSets) != 0 || q.having.kind != "" || q.qualify.kind != "" || len(q.orderBy) != 0 || q.sample != nil || q.limitBy != nil || q.limit >= 0 || q.offset != 0 || q.limitWithTies || q.distinct || q.explain || q.pipeline || q.analyze || len(q.selects) == 0 {
		return sqlColumnarArrayJoinPlan{}, false
	}
	join := q.joins[0]
	if join.kind != "ARRAY" && join.kind != "LEFT_ARRAY" || join.source.alias == "" || join.on.kind != "field" || join.on.qualifier != "" && join.on.qualifier != q.from.alias {
		return sqlColumnarArrayJoinPlan{}, false
	}
	plan := sqlColumnarArrayJoinPlan{
		arrayField: join.on.name,
		joinAlias:  join.source.alias,
		left:       join.kind == "LEFT_ARRAY",
	}
	seen := make(map[string]struct{}, len(q.selects)+1)
	addField := func(field string) {
		if _, ok := seen[field]; ok {
			return
		}
		seen[field] = struct{}{}
		plan.fields = append(plan.fields, field)
	}
	for _, item := range q.selects {
		expr := item.expr
		if expr.kind != "field" || expr.name == "" {
			return sqlColumnarArrayJoinPlan{}, false
		}
		name := item.alias
		if name == "" {
			name = expr.name
		}
		projection := sqlColumnarArrayJoinProjection{name: name, field: expr.name}
		if expr.qualifier == join.source.alias || expr.qualifier == "" && expr.name == join.source.alias {
			projection.joined = true
		} else if expr.qualifier == "" || expr.qualifier == q.from.alias {
			addField(expr.name)
		} else {
			return sqlColumnarArrayJoinPlan{}, false
		}
		plan.projections = append(plan.projections, projection)
	}
	addField(plan.arrayField)
	return plan, true
}

func executeSQLColumnarArrayJoin(q *sqlQuery, columnar SQLColumnarSourceResolver, control *sqlExecutionControl, metrics *sqlExecutionMetrics, outer *sqlExecRow) (SQLQueryResult, bool, error) {
	plan, ok := sqlColumnarArrayJoinPlanFor(q, outer)
	if !ok {
		return SQLQueryResult{}, false, nil
	}
	started := time.Now()
	batch, _, available, err := resolveSQLColumnarQuerySource(q, columnar, plan.fields)
	if err != nil || !available {
		return SQLQueryResult{}, available, err
	}
	if batch.Rows < 0 {
		return SQLQueryResult{}, true, fmt.Errorf("SQL columnar source %q returned a negative row count", q.from.key)
	}
	for _, field := range plan.fields {
		if batch.FieldRows(field) != batch.Rows {
			return SQLQueryResult{}, true, fmt.Errorf("SQL columnar source %q returned %d values for field %q, want %d", q.from.key, batch.FieldRows(field), field, batch.Rows)
		}
	}
	maxRows := maxSQLQueryRows
	if control != nil {
		maxRows = control.maxRows
	}
	capacity := 0
	arrayValues := batch.Columns[plan.arrayField]
	for row := 0; row < batch.Rows; row++ {
		value := arrayValues[row]
		if value == nil {
			if plan.left {
				capacity++
			}
			continue
		}
		elements, valid := sqlArrayJoinElements(value)
		if !valid {
			return SQLQueryResult{}, true, fmt.Errorf("ARRAY JOIN expression must evaluate to an array, got %T", value)
		}
		if elements.Len() == 0 {
			if plan.left {
				capacity++
			}
		} else {
			capacity += elements.Len()
		}
		if capacity > maxRows {
			return SQLQueryResult{}, true, fmt.Errorf("SQL ARRAY JOIN exceeds the %d row limit", maxRows)
		}
	}
	columns := make([]string, len(plan.projections))
	for index, projection := range plan.projections {
		columns[index] = projection.name
	}
	rows := make([]SQLRow, 0, capacity)
	for row := 0; row < batch.Rows; row++ {
		value := arrayValues[row]
		if value == nil {
			if plan.left {
				if err := appendSQLColumnarArrayJoinRow(&rows, plan, batch, row, nil, control); err != nil {
					return SQLQueryResult{}, true, err
				}
			}
			continue
		}
		elements, _ := sqlArrayJoinElements(value)
		if elements.Len() == 0 {
			if plan.left {
				if err := appendSQLColumnarArrayJoinRow(&rows, plan, batch, row, nil, control); err != nil {
					return SQLQueryResult{}, true, err
				}
			}
			continue
		}
		for index := 0; index < elements.Len(); index++ {
			if err := appendSQLColumnarArrayJoinRow(&rows, plan, batch, row, elements.Index(index).Interface(), control); err != nil {
				return SQLQueryResult{}, true, err
			}
		}
	}
	if metrics != nil {
		metrics.record("COLUMNAR ARRAY JOIN", sqlExplainExpression(q.joins[0].on)+" AS "+plan.joinAlias, batch.Rows, len(rows), started)
	}
	return SQLQueryResult{Columns: columns, Rows: rows}, true, nil
}

func appendSQLColumnarArrayJoinRow(rows *[]SQLRow, plan sqlColumnarArrayJoinPlan, batch ColumnarBatch, sourceRow int, element interface{}, control *sqlExecutionControl) error {
	if control != nil {
		if err := control.addJoinWork(1); err != nil {
			return err
		}
	}
	row := make(SQLRow, len(plan.projections))
	for _, projection := range plan.projections {
		if projection.joined {
			row[projection.name] = element
		} else {
			row[projection.name] = batch.Columns[projection.field][sourceRow]
		}
	}
	*rows = append(*rows, row)
	return nil
}
