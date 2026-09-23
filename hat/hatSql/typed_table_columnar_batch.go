package hatSql

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrTypedTableColumnarBatchInvalid reports malformed input that cannot be
// appended without changing the table.
var ErrTypedTableColumnarBatchInvalid = errors.New("typed table columnar batch is invalid")

// ErrTypedTableColumnarBatchKeyExists reports that an append batch contains a
// key already present in the table. Use Upsert for replacement semantics.
var ErrTypedTableColumnarBatchKeyExists = errors.New("typed table columnar batch key already exists")

// TypedTableColumnarBatch is a complete schema-ordered block for append-only
// analytical ingest. Every column must have one value for every key. Values
// are copied into the table's own primitive column slices before this method
// returns.
type TypedTableColumnarBatch struct {
	Keys    []string
	Columns [][]TypedTableValue
}

// AppendColumnarBatch validates and appends one complete columnar block. The
// operation is atomic: malformed values, duplicate keys, existing keys, and
// memory-budget failures leave the table unchanged. Existing-key replacement
// remains available through Upsert.
func (table *TypedTable) AppendColumnarBatch(batch TypedTableColumnarBatch) (int, error) {
	if table == nil {
		return 0, fmt.Errorf("typed table is nil")
	}
	rowCount, err := validateTypedTableColumnarBatchShape(batch, len(table.schema.Columns))
	if err != nil {
		return 0, err
	}
	if rowCount == 0 {
		return 0, nil
	}

	keys, err := normalizeTypedTableColumnarBatchKeys(batch.Keys)
	if err != nil {
		return 0, err
	}
	if err := validateTypedTableColumnarBatchKeys(keys); err != nil {
		return 0, err
	}

	var prepared []TypedTableValue
	if table.generated {
		prepared = make([]TypedTableValue, rowCount*len(table.columns))
		for row := 0; row < rowCount; row++ {
			values := prepared[row*len(table.columns) : (row+1)*len(table.columns)]
			copyTypedTableColumnarBatchRow(values, batch, nil, row)
			computed, err := table.applyGeneratedValues(values)
			if err != nil {
				return 0, err
			}
			if err := table.validateValues(computed); err != nil {
				return 0, fmt.Errorf("%w: row %d: %v", ErrTypedTableColumnarBatchInvalid, row, err)
			}
			copy(values, computed)
		}
	} else if err := validateTypedTableColumnarBatchValues(table, batch); err != nil {
		return 0, err
	}

	table.mu.Lock()
	defer table.mu.Unlock()
	for row, key := range keys {
		if _, exists := table.positions[key]; exists {
			return 0, fmt.Errorf("%w: %q at row %d", ErrTypedTableColumnarBatchKeyExists, key, row)
		}
	}

	var rowBytes []int64
	if table.memoryBudgetMaxBytes > 0 {
		rowBytes = make([]int64, rowCount)
		values := make([]TypedTableValue, len(table.columns))
		nextBytes := table.memoryBytes
		for row := 0; row < rowCount; row++ {
			copyTypedTableColumnarBatchRow(values, batch, prepared, row)
			rowBytes[row] = typedTableEstimatedRowBytes(keys[row], values)
			nextBytes = typedTableAddMemoryBytes(nextBytes, rowBytes[row])
			if nextBytes > table.memoryBudgetMaxBytes {
				return 0, fmt.Errorf("%w: batch would use %d bytes, max %d", ErrTypedTableMemoryBudgetExceeded, nextBytes, table.memoryBudgetMaxBytes)
			}
		}
	}

	table.clearColumnarLayoutsLocked()
	table.invalidateTypedTableDerivedCachesLocked()
	newBasePart := len(table.keys) == 0
	start := len(table.keys)
	table.keys = append(table.keys, keys...)
	if table.patchParts != nil {
		table.patchParts.deleted.ensure(start + rowCount)
	}
	for row, key := range keys {
		table.positions[key] = start + row
	}
	for column := range table.columns {
		storage := &table.columns[column]
		if prepared == nil {
			for _, value := range batch.Columns[column] {
				storage.append(value)
			}
			continue
		}
		for row := 0; row < rowCount; row++ {
			storage.append(prepared[row*len(table.columns)+column])
		}
	}
	for row := 0; row < rowCount; row++ {
		if table.memoryBudgetMaxBytes > 0 {
			table.appendTypedTableMemoryRowLocked(rowBytes[row])
		}
	}

	var ttlNow time.Time
	if table.ttl != nil && table.ttl.options.Mode == TypedTableTTLProcessingTime {
		ttlNow = table.typedTableTTLNow()
	}
	for row, key := range keys {
		index := start + row
		if table.ttl != nil {
			table.setTypedTableTTLDeadlineLocked(index, ttlNow)
		}
		table.setTypedTableColumnTTLDeadlineLocked(index)
		after := make([]TypedTableValue, len(table.columns))
		copyTypedTableColumnarBatchRow(after, batch, prepared, row)
		table.appendChangeWithoutReturnLocked(TypedTableChange{
			Operation: "INSERT",
			Key:       key,
			After:     after,
		})
	}
	if newBasePart {
		table.recordStorageEventLocked(TypedTableStorageEventBasePartCreated, 0, len(table.keys), 0, 0, 0)
	}
	return rowCount, nil
}

func validateTypedTableColumnarBatchShape(batch TypedTableColumnarBatch, columns int) (int, error) {
	rowCount := len(batch.Keys)
	if len(batch.Columns) != columns {
		return 0, fmt.Errorf("%w: batch has %d columns, want %d", ErrTypedTableColumnarBatchInvalid, len(batch.Columns), columns)
	}
	for column, values := range batch.Columns {
		if len(values) != rowCount {
			return 0, fmt.Errorf("%w: column %d has %d values, want %d", ErrTypedTableColumnarBatchInvalid, column, len(values), rowCount)
		}
	}
	return rowCount, nil
}

func normalizeTypedTableColumnarBatchKeys(keys []string) ([]string, error) {
	var normalized []string
	for row, key := range keys {
		trimmed := strings.TrimSpace(key)
		if trimmed == "" {
			return nil, fmt.Errorf("%w: key at row %d is required", ErrTypedTableColumnarBatchInvalid, row)
		}
		if trimmed == key {
			continue
		}
		if normalized == nil {
			normalized = append([]string(nil), keys...)
		}
		normalized[row] = trimmed
	}
	if normalized != nil {
		return normalized, nil
	}
	return keys, nil
}

func validateTypedTableColumnarBatchKeys(keys []string) error {
	seen := make(map[string]struct{}, len(keys))
	for row, key := range keys {
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("%w: duplicate key %q at row %d", ErrTypedTableColumnarBatchInvalid, key, row)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func validateTypedTableColumnarBatchValues(table *TypedTable, batch TypedTableColumnarBatch) error {
	for column, values := range batch.Columns {
		expected := table.columns[column].kind
		for row, value := range values {
			if !value.Valid || value.Kind == expected {
				continue
			}
			return fmt.Errorf("%w: column %q at row %d requires kind %d", ErrTypedTableColumnarBatchInvalid, table.schema.Columns[column].Name, row, expected)
		}
	}
	return nil
}

func copyTypedTableColumnarBatchRow(dst []TypedTableValue, batch TypedTableColumnarBatch, prepared []TypedTableValue, row int) {
	if prepared != nil {
		copy(dst, prepared[row*len(dst):(row+1)*len(dst)])
		return
	}
	for column := range dst {
		dst[column] = batch.Columns[column][row]
	}
}

func (table *TypedTable) appendChangeWithoutReturnLocked(change TypedTableChange) {
	table.sequence++
	change.Sequence = table.sequence
	table.changes = append(table.changes, change)
	if table.mvcc != nil {
		table.mvcc.record(change)
	}
}
