package hatSql

import (
	"container/heap"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const sqlPartitionedKeysetCursorPrefix = "p:"

type sqlPartitionedKeysetCursor struct {
	Fingerprint string                       `json:"f"`
	Returned    int                          `json:"r"`
	Partitions  []sqlPartitionKeysetProgress `json:"p"`
}

type sqlPartitionKeysetProgress struct {
	Name   string `json:"n"`
	Offset int    `json:"o"`
}

type sqlPartitionKeysetHead struct {
	partition int
	row       int
	value     interface{}
	sourceRow Row
}

type sqlPartitionKeysetHeap struct {
	items      []sqlPartitionKeysetHead
	field      string
	desc       bool
	nullsFirst bool
	nullsLast  bool
}

func (h sqlPartitionKeysetHeap) Len() int { return len(h.items) }

func (h sqlPartitionKeysetHeap) Less(left, right int) bool {
	leftItem, rightItem := h.items[left], h.items[right]
	leftBefore, _ := OrderLess(h.desc, h.nullsFirst, h.nullsLast, leftItem.value, rightItem.value)
	rightBefore, _ := OrderLess(h.desc, h.nullsFirst, h.nullsLast, rightItem.value, leftItem.value)
	if leftBefore != rightBefore {
		return leftBefore
	}
	if leftItem.partition != rightItem.partition {
		return leftItem.partition < rightItem.partition
	}
	return leftItem.row < rightItem.row
}

func (h *sqlPartitionKeysetHeap) Swap(left, right int) {
	h.items[left], h.items[right] = h.items[right], h.items[left]
}

func (h *sqlPartitionKeysetHeap) Push(value interface{}) {
	h.items = append(h.items, value.(sqlPartitionKeysetHead))
}

func (h *sqlPartitionKeysetHeap) Pop() interface{} {
	last := len(h.items) - 1
	value := h.items[last]
	h.items = h.items[:last]
	return value
}

func isSQLPartitionedKeysetCursor(value string) bool {
	return strings.HasPrefix(value, sqlPartitionedKeysetCursorPrefix)
}

func executeSQLPartitionedKeysetPage(ctx context.Context, query *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, fingerprint string, pageSize int, cursor string, metrics *sqlExecutionMetrics) (SQLQueryResult, bool, error) {
	partitioned, ok := resolver.(PartitionedOrderedSourceResolver)
	if !ok {
		return SQLQueryResult{}, false, nil
	}
	returned := 0
	progress := []sqlPartitionKeysetProgress(nil)
	if cursor != "" {
		decoded, err := decodeSQLPartitionedKeysetCursor(cursor)
		if err != nil {
			return SQLQueryResult{}, true, err
		}
		if decoded.Fingerprint != fingerprint {
			return SQLQueryResult{}, true, fmt.Errorf("SQL partitioned keyset cursor does not match this query and parameters")
		}
		returned = decoded.Returned
		progress = decoded.Partitions
	}

	fetch := pageSize + 1
	if query.limit >= 0 {
		remaining := query.limit - returned
		if remaining <= 0 {
			return SQLQueryResult{Columns: sqlColumns(query.selects)}, true, nil
		}
		if remaining < fetch {
			fetch = remaining
		}
	}
	if fetch == 0 {
		return SQLQueryResult{Columns: sqlColumns(query.selects)}, true, nil
	}

	partitions, available, err := partitioned.ResolveSQLOrderedSourcePartitions(query.from.kind, query.from.key, query.orderBy[0].expr.name, query.orderBy[0].desc, query.orderBy[0].nullsFirst, query.orderBy[0].nullsLast)
	if err != nil {
		return SQLQueryResult{}, true, err
	}
	if !available {
		if cursor != "" {
			return SQLQueryResult{}, true, fmt.Errorf("SQL partitioned keyset pagination source is unavailable")
		}
		return SQLQueryResult{}, false, nil
	}
	if err := validateSQLOrderedPartitions(partitions); err != nil {
		return SQLQueryResult{}, true, err
	}
	if err := validateSQLOrderedPartitionRows(partitions, query.orderBy[0].expr.name, query.orderBy[0].desc, query.orderBy[0].nullsFirst, query.orderBy[0].nullsLast); err != nil {
		return SQLQueryResult{}, true, err
	}
	if progress == nil {
		progress = make([]sqlPartitionKeysetProgress, len(partitions))
		for index, partition := range partitions {
			progress[index].Name = partition.Name
		}
	} else if err := validateSQLPartitionKeysetProgress(progress, partitions); err != nil {
		return SQLQueryResult{}, true, err
	}

	columns := sqlColumns(query.selects)
	result := SQLQueryResult{Columns: columns, Rows: make([]Row, 0, fetch)}
	if len(partitions) == 0 {
		return result, true, nil
	}
	ordered := &sqlPartitionKeysetHeap{
		field:      query.orderBy[0].expr.name,
		desc:       query.orderBy[0].desc,
		nullsFirst: query.orderBy[0].nullsFirst,
		nullsLast:  query.orderBy[0].nullsLast,
	}
	heap.Init(ordered)
	pushCurrent := func(partitionIndex int) {
		offset := progress[partitionIndex].Offset
		if offset >= len(partitions[partitionIndex].Rows) {
			return
		}
		row := partitions[partitionIndex].Rows[offset]
		heap.Push(ordered, sqlPartitionKeysetHead{
			partition: partitionIndex,
			row:       offset,
			value:     row[ordered.field],
			sourceRow: row,
		})
	}
	for partitionIndex := range partitions {
		pushCurrent(partitionIndex)
	}

	functions, _ := resolver.(SQLFunctionResolver)
	inputRows, resultBytes := 0, 0
	started := time.Now()
	var cursorProgress []sqlPartitionKeysetProgress
	for ordered.Len() > 0 && len(result.Rows) < fetch {
		if err := control.check(); err != nil {
			return SQLQueryResult{}, true, err
		}
		head := heap.Pop(ordered).(sqlPartitionKeysetHead)
		progress[head.partition].Offset = head.row + 1
		inputRows++
		if inputRows > control.maxRows {
			return SQLQueryResult{}, true, fmt.Errorf("SQL source %q exceeds the %d row limit", query.from.alias, control.maxRows)
		}
		if query.where.kind != "" {
			execRow := sqlExecRow{sources: map[string]SQLRow{query.from.alias: head.sourceRow}, order: []string{query.from.alias}}
			value, err := evalSQLStreamExpr(query.where, execRow, functions)
			if err != nil {
				return SQLQueryResult{}, true, err
			}
			if !sqlTruthy(value) {
				pushCurrent(head.partition)
				continue
			}
		}
		execRow := sqlExecRow{sources: map[string]SQLRow{query.from.alias: head.sourceRow}, order: []string{query.from.alias}}
		row := Row{}
		for index, item := range query.selects {
			value, err := evalSQLStreamExpr(item.expr, execRow, functions)
			if err != nil {
				return SQLQueryResult{}, true, err
			}
			row[columns[index]] = value
		}
		if control.options.MaxResultBytes > 0 {
			resultBytes += sqlRowBytes(row)
			if resultBytes > control.options.MaxResultBytes {
				return SQLQueryResult{}, true, fmt.Errorf("SQL result byte budget exceeded: maximum %d bytes", control.options.MaxResultBytes)
			}
		}
		result.Rows = append(result.Rows, row)
		if len(result.Rows) == pageSize {
			cursorProgress = cloneSQLPartitionKeysetProgress(progress)
		}
		pushCurrent(head.partition)
	}
	if metrics != nil {
		metrics.record("PARTITIONED KEYSET MERGE", sqlExplainSource(*query.from)+" ORDER BY "+sqlExplainOrders(query.orderBy), inputRows, len(result.Rows), started)
	}
	if len(result.Rows) > pageSize {
		result.Rows = result.Rows[:pageSize]
		result.HasMore = true
		next := sqlPartitionedKeysetCursor{Fingerprint: fingerprint, Returned: returned + pageSize, Partitions: cursorProgress}
		result.NextCursor, err = encodeSQLPartitionedKeysetCursor(next)
		if err != nil {
			return SQLQueryResult{}, true, err
		}
	}
	return result, true, nil
}

func validateSQLOrderedPartitions(partitions []SQLSourcePartition) error {
	names := make(map[string]struct{}, len(partitions))
	for _, partition := range partitions {
		name := strings.TrimSpace(partition.Name)
		if name == "" {
			return fmt.Errorf("SQL ordered partition name is required")
		}
		if _, exists := names[name]; exists {
			return fmt.Errorf("SQL ordered partition %q is duplicated", name)
		}
		names[name] = struct{}{}
	}
	return nil
}

func validateSQLOrderedPartitionRows(partitions []SQLSourcePartition, field string, desc, nullsFirst, nullsLast bool) error {
	for _, partition := range partitions {
		var previous interface{}
		seen := false
		for rowIndex, row := range partition.Rows {
			value := row[field]
			if seen {
				before, _ := OrderLess(desc, nullsFirst, nullsLast, value, previous)
				if before {
					return fmt.Errorf("SQL ordered partition %q is not sorted at row %d", partition.Name, rowIndex)
				}
			}
			previous = value
			seen = true
		}
	}
	return nil
}

func validateSQLPartitionKeysetProgress(progress []sqlPartitionKeysetProgress, partitions []SQLSourcePartition) error {
	if len(progress) != len(partitions) {
		return fmt.Errorf("SQL partitioned keyset cursor does not match the current partition layout")
	}
	for index, partition := range partitions {
		if progress[index].Name != partition.Name || progress[index].Offset < 0 || progress[index].Offset > len(partition.Rows) {
			return fmt.Errorf("SQL partitioned keyset cursor does not match partition %q", partition.Name)
		}
	}
	return nil
}

func cloneSQLPartitionKeysetProgress(progress []sqlPartitionKeysetProgress) []sqlPartitionKeysetProgress {
	clone := make([]sqlPartitionKeysetProgress, len(progress))
	copy(clone, progress)
	return clone
}

func encodeSQLPartitionedKeysetCursor(cursor sqlPartitionedKeysetCursor) (string, error) {
	value, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	return sqlPartitionedKeysetCursorPrefix + base64.RawURLEncoding.EncodeToString(value), nil
}

func decodeSQLPartitionedKeysetCursor(value string) (sqlPartitionedKeysetCursor, error) {
	if !isSQLPartitionedKeysetCursor(value) {
		return sqlPartitionedKeysetCursor{}, fmt.Errorf("invalid SQL partitioned keyset cursor")
	}
	encoded, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(value, sqlPartitionedKeysetCursorPrefix))
	if err != nil {
		return sqlPartitionedKeysetCursor{}, fmt.Errorf("invalid SQL partitioned keyset cursor")
	}
	var cursor sqlPartitionedKeysetCursor
	if json.Unmarshal(encoded, &cursor) != nil || cursor.Fingerprint == "" || cursor.Returned < 0 || cursor.Returned > maxSQLQueryRows || len(cursor.Partitions) > maxSQLQueryRows {
		return sqlPartitionedKeysetCursor{}, fmt.Errorf("invalid SQL partitioned keyset cursor")
	}
	for _, progress := range cursor.Partitions {
		if strings.TrimSpace(progress.Name) == "" || progress.Offset < 0 || progress.Offset > maxSQLQueryRows {
			return sqlPartitionedKeysetCursor{}, fmt.Errorf("invalid SQL partitioned keyset cursor")
		}
	}
	return cursor, nil
}
