package hatSql

import (
	"container/heap"
	"context"
	"fmt"
	"strings"
)

// sqlPartitionedOrderStreamShape keeps the partition merge limited to a
// direct single-source query whose remaining expressions can be evaluated
// row-by-row without changing SQL semantics.
func sqlPartitionedOrderStreamShape(query *sqlQuery) bool {
	if query == nil || query.explain || query.from == nil || query.from.kind == "" {
		return false
	}
	if len(query.ctes) != 0 || len(query.unions) != 0 || len(query.joins) != 0 || len(query.groupBy) != 0 || query.having.kind != "" || query.distinct || query.limitBy != nil {
		return false
	}
	if sqlQueryHasAggregate(query) || sqlQueryHasWindow(query) || query.where.window != nil || query.prewhere.window != nil {
		return false
	}
	if len(query.orderBy) != 1 || query.limitWithTies {
		return false
	}
	order := query.orderBy[0]
	if order.expr.kind != "field" || order.expr.name == "" || !sqlPartitionedOrderBinaryCollation(order.collation) || !sqlPartitionedOrderBinaryCollation(order.expr.collation) {
		return false
	}
	if order.expr.qualifier != "" && order.expr.qualifier != query.from.alias {
		return false
	}
	return true
}

func sqlPartitionedOrderBinaryCollation(collation SQLCollation) bool {
	return collation == "" || strings.EqualFold(string(collation), "BINARY")
}

// executeSQLPartitionedOrderStream merges independently ordered physical
// partitions directly into the row callback. It returns handled=false when
// the resolver does not advertise usable ordered partitions, preserving the
// existing index, top-N, and spill fallbacks.
func executeSQLPartitionedOrderStream(ctx context.Context, query *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, visit func([]string, SQLRow) error) (handled bool, err error) {
	if !sqlPartitionedOrderStreamShape(query) {
		return false, nil
	}
	partitioned, ok := resolver.(PartitionedOrderedSourceResolver)
	if !ok {
		return false, nil
	}
	if query.limit == 0 {
		return true, nil
	}
	order := query.orderBy[0]
	partitions, available, err := partitioned.ResolveSQLOrderedSourcePartitions(query.from.kind, query.from.key, order.expr.name, order.desc, order.nullsFirst, order.nullsLast)
	if err != nil {
		return true, err
	}
	if !available {
		return false, nil
	}
	if err := validateSQLOrderedPartitions(partitions); err != nil {
		return true, err
	}
	if err := validateSQLOrderedPartitionRows(partitions, order.expr.name, order.desc, order.nullsFirst, order.nullsLast); err != nil {
		return true, err
	}

	ordered := &sqlPartitionKeysetHeap{
		field:      order.expr.name,
		desc:       order.desc,
		nullsFirst: order.nullsFirst,
		nullsLast:  order.nullsLast,
	}
	heap.Init(ordered)
	pushCurrent := func(partitionIndex int) {
		rows := partitions[partitionIndex].Rows
		if len(rows) == 0 {
			return
		}
		sourceRow := rows[0]
		heap.Push(ordered, sqlPartitionKeysetHead{
			partition: partitionIndex,
			row:       0,
			value:     sourceRow[ordered.field],
			sourceRow: sourceRow,
		})
	}
	for partitionIndex := range partitions {
		pushCurrent(partitionIndex)
	}

	functions, _ := resolver.(SQLFunctionResolver)
	columns := sqlColumns(query.selects)
	emitted := 0
	skipped := 0
	inputRows := 0
	resultBytes := 0
	for ordered.Len() > 0 {
		if err := control.check(); err != nil {
			return true, err
		}
		select {
		case <-ctx.Done():
			return true, ctx.Err()
		default:
		}

		head := heap.Pop(ordered).(sqlPartitionKeysetHead)
		inputRows++
		if inputRows > control.maxRows {
			return true, fmt.Errorf("SQL source %q exceeds the %d row limit", query.from.alias, control.maxRows)
		}
		rowIndex := head.row + 1
		if rowIndex < len(partitions[head.partition].Rows) {
			next := partitions[head.partition].Rows[rowIndex]
			heap.Push(ordered, sqlPartitionKeysetHead{
				partition: head.partition,
				row:       rowIndex,
				value:     next[ordered.field],
				sourceRow: next,
			})
		}

		execRow := sqlExecRow{sources: map[string]SQLRow{query.from.alias: head.sourceRow}, order: []string{query.from.alias}}
		if query.prewhere.kind != "" {
			value, err := evalSQLStreamExpr(query.prewhere, execRow, functions)
			if err != nil {
				return true, err
			}
			if !sqlTruthy(value) {
				continue
			}
		}
		if query.where.kind != "" {
			value, err := evalSQLStreamExpr(query.where, execRow, functions)
			if err != nil {
				return true, err
			}
			if !sqlTruthy(value) {
				continue
			}
		}
		if skipped < query.offset {
			skipped++
			continue
		}

		row := Row{}
		for index, item := range query.selects {
			value, err := evalSQLStreamExpr(item.expr, execRow, functions)
			if err != nil {
				return true, err
			}
			row[columns[index]] = value
		}
		if control.options.MaxResultBytes > 0 {
			resultBytes += sqlRowBytes(row)
			if resultBytes > control.options.MaxResultBytes {
				return true, fmt.Errorf("SQL result byte budget exceeded: maximum %d bytes", control.options.MaxResultBytes)
			}
		}
		if err := visit(columns, row); err != nil {
			return true, err
		}
		emitted++
		if query.limit >= 0 && emitted >= query.limit {
			break
		}
	}
	return true, nil
}
