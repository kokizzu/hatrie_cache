package hatSql

import (
	"fmt"
	"strings"
	"time"
)

// AppendColumnar appends a complete batch of new rows from a columnar source.
// Keys are supplied separately because TypedTable schemas do not store the
// logical key as a regular value column. The batch must contain one scalar
// field for every schema column and is validated before any table state is
// changed. Existing or duplicate keys are rejected; use Upsert for replacement
// semantics.
func (table *TypedTable) AppendColumnar(keys []string, batch ColumnarBatch) ([]TypedTableChange, error) {
	if table == nil {
		return nil, fmt.Errorf("typed table is nil")
	}
	if batch.Rows < 0 {
		return nil, fmt.Errorf("columnar batch rows cannot be negative")
	}
	if len(keys) != batch.Rows {
		return nil, fmt.Errorf("columnar batch has %d keys for %d rows", len(keys), batch.Rows)
	}
	if batch.Rows == 0 {
		return nil, nil
	}

	normalizedKeys := keys
	keysCopied := false
	seenKeys := make(map[string]int, batch.Rows)
	for row, key := range keys {
		trimmedKey := strings.TrimSpace(key)
		if !keysCopied && trimmedKey != key {
			normalizedKeys = append([]string(nil), keys...)
			keysCopied = true
		}
		if keysCopied {
			normalizedKeys[row] = trimmedKey
		}
		key = trimmedKey
		if key == "" {
			return nil, fmt.Errorf("columnar batch key %d is required", row)
		}
		if _, found := seenKeys[key]; found {
			return nil, fmt.Errorf("columnar batch contains duplicate key %q", key)
		}
		seenKeys[key] = row
	}

	for _, definition := range table.schema.Columns {
		fieldRows := batch.FieldRows(definition.Name)
		if fieldRows != batch.Rows {
			return nil, fmt.Errorf("columnar field %q has %d rows, want %d", definition.Name, fieldRows, batch.Rows)
		}
		if definition.Kind == TypedTableNull {
			return nil, fmt.Errorf("typed table column %q cannot have null kind", definition.Name)
		}
	}

	changes := make([]TypedTableChange, batch.Rows)
	for row := 0; row < batch.Rows; row++ {
		values := make([]TypedTableValue, len(table.schema.Columns))
		for column, definition := range table.schema.Columns {
			value, err := typedTableColumnarValue(batch, definition, row)
			if err != nil {
				return nil, err
			}
			values[column] = value
		}
		values, err := table.applyGeneratedValues(values)
		if err != nil {
			return nil, fmt.Errorf("columnar row %d: %w", row, err)
		}
		if err := table.validateValues(values); err != nil {
			return nil, fmt.Errorf("columnar row %d: %w", row, err)
		}
		changes[row] = TypedTableChange{
			Operation: "INSERT",
			Key:       normalizedKeys[row],
			After:     values,
		}
	}

	table.mu.Lock()
	defer table.mu.Unlock()
	for _, key := range normalizedKeys {
		if _, exists := table.positions[key]; exists {
			return nil, fmt.Errorf("typed table key %q already exists", key)
		}
	}
	var memoryRowBytes []int64
	if table.memoryBudgetMaxBytes > 0 {
		memoryRowBytes = make([]int64, len(changes))
		var additionalBytes int64
		for index, change := range changes {
			memoryRowBytes[index] = typedTableEstimatedRowBytes(change.Key, change.After)
			additionalBytes = typedTableAddMemoryBytes(additionalBytes, memoryRowBytes[index])
		}
		if err := table.checkTypedTableMemoryBudgetLocked(0, additionalBytes); err != nil {
			return nil, err
		}
	}
	table.clearColumnarLayoutsLocked()
	table.invalidateTypedTableDerivedCachesLocked()
	newBasePart := len(table.keys) == 0
	start := len(table.keys)
	table.keys = reserveTypedTableSlice(table.keys, batch.Rows)
	table.changes = reserveTypedTableSlice(table.changes, batch.Rows)
	for column := range table.columns {
		table.columns[column].reserve(batch.Rows)
	}
	if table.patchParts != nil {
		table.patchParts.deleted.ensure(start + batch.Rows)
	}
	if newBasePart && len(table.positions) == 0 {
		table.positions = seenKeys
	}
	var ttlNow time.Time
	if table.ttl != nil && table.ttl.options.Mode == TypedTableTTLProcessingTime {
		ttlNow = table.typedTableTTLNow()
	}
	for row, change := range changes {
		index := len(table.keys)
		table.positions[change.Key] = index
		table.keys = append(table.keys, change.Key)
		for column := range table.columns {
			table.columns[column].append(change.After[column])
		}
		if table.memoryBudgetMaxBytes > 0 {
			table.appendTypedTableMemoryRowLocked(memoryRowBytes[row])
		}
		if table.ttl != nil {
			table.setTypedTableTTLDeadlineLocked(index, ttlNow)
		}
		changes[row] = table.appendChangeLocked(change)
	}
	if len(changes) > 0 {
		table.patchStateKeyLayoutGeneration++
	}
	if newBasePart {
		table.recordStorageEventLocked(TypedTableStorageEventBasePartCreated, 0, len(table.keys), 0, 0, 0)
	}
	return changes, nil
}

func typedTableColumnarValue(batch ColumnarBatch, definition TypedTableColumn, row int) (TypedTableValue, error) {
	var raw interface{}
	var valid bool
	if batch.decompressedBlockCache == nil && batch.fieldOffsets == nil &&
		batch.Dictionaries == nil && batch.PackedColumns == nil && batch.BoolColumns == nil &&
		batch.NumericColumns == nil && batch.ListColumns == nil && batch.NestedColumns == nil && batch.MapColumns == nil {
		values, found := batch.Columns[definition.Name]
		if found && row < len(values) {
			raw, valid = values[row], true
		}
	} else {
		raw, valid = batch.Value(definition.Name, row)
	}
	if !valid || raw == nil {
		return TypedNull(), nil
	}
	switch definition.Kind {
	case TypedTableString:
		value, ok := raw.(string)
		if !ok {
			return TypedTableValue{}, fmt.Errorf("columnar field %q row %d has type %T, want string", definition.Name, row, raw)
		}
		return TypedString(value), nil
	case TypedTableInt64:
		value, ok := raw.(int64)
		if !ok {
			return TypedTableValue{}, fmt.Errorf("columnar field %q row %d has type %T, want int64", definition.Name, row, raw)
		}
		return TypedInt64(value), nil
	case TypedTableFloat64:
		value, ok := raw.(float64)
		if !ok {
			return TypedTableValue{}, fmt.Errorf("columnar field %q row %d has type %T, want float64", definition.Name, row, raw)
		}
		return TypedFloat64(value), nil
	case TypedTableBool:
		value, ok := raw.(bool)
		if !ok {
			return TypedTableValue{}, fmt.Errorf("columnar field %q row %d has type %T, want bool", definition.Name, row, raw)
		}
		return TypedBool(value), nil
	default:
		return TypedTableValue{}, fmt.Errorf("columnar field %q has unsupported kind %d", definition.Name, definition.Kind)
	}
}

func reserveTypedTableSlice[T any](values []T, additional int) []T {
	if additional <= cap(values)-len(values) {
		return values
	}
	grown := make([]T, len(values), len(values)+additional)
	copy(grown, values)
	return grown
}

func (storage *typedTableColumnStorage) reserve(additional int) {
	storage.valid = reserveTypedTableSlice(storage.valid, additional)
	switch storage.kind {
	case TypedTableString:
		if storage.dictionary {
			storage.dictionaryCodes = reserveTypedTableSlice(storage.dictionaryCodes, additional)
		} else {
			storage.strings = reserveTypedTableSlice(storage.strings, additional)
		}
	case TypedTableInt64:
		storage.int64s = reserveTypedTableSlice(storage.int64s, additional)
	case TypedTableFloat64:
		storage.floats = reserveTypedTableSlice(storage.floats, additional)
	case TypedTableBool:
		storage.bools = reserveTypedTableSlice(storage.bools, additional)
	}
}
