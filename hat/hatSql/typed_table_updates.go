package hatSql

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

// ErrTypedTableUpdateInvalid reports an invalid field update request. The
// request is rejected before the row or changefeed is modified.
var ErrTypedTableUpdateInvalid = errors.New("typed table field update is invalid")

// ErrTypedTableUpdateGenerated reports that field updates are disabled for a
// table with generated columns because the operation cannot safely recompute
// the generated dependency graph in place.
var ErrTypedTableUpdateGenerated = errors.New("typed table field update does not support generated columns")

// ErrTypedTableUpdateOverflow reports arithmetic overflow in an ADD update.
var ErrTypedTableUpdateOverflow = errors.New("typed table field update overflow")

// TypedTableUpdateKind identifies the operation applied to one tuple field.
type TypedTableUpdateKind uint8

const (
	TypedTableUpdateSet TypedTableUpdateKind = iota + 1
	TypedTableUpdateAdd
)

// TypedTableUpdate is one atomic field operation. SET replaces the field and
// ADD changes an Int64 or Float64 field by Value.
type TypedTableUpdate struct {
	Column string
	Kind   TypedTableUpdateKind
	Value  TypedTableValue
}

const typedTableMaxFieldUpdates = 64

// Update applies a bounded batch of SET and numeric ADD operations to one
// existing row. All operations are validated before the row is changed.
// Generated-column tables intentionally return ErrTypedTableUpdateGenerated;
// callers should use Upsert when generated values must be recomputed.
func (table *TypedTable) Update(key string, operations []TypedTableUpdate) (TypedTableChange, error) {
	if table == nil {
		return TypedTableChange{}, fmt.Errorf("typed table is nil")
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return TypedTableChange{}, fmt.Errorf("typed table key is required")
	}
	if len(operations) == 0 || len(operations) > typedTableMaxFieldUpdates {
		return TypedTableChange{}, fmt.Errorf("%w: operation count must be between 1 and %d", ErrTypedTableUpdateInvalid, typedTableMaxFieldUpdates)
	}
	for _, operation := range operations {
		switch operation.Kind {
		case TypedTableUpdateSet, TypedTableUpdateAdd:
		default:
			return TypedTableChange{}, fmt.Errorf("%w: unknown operation kind %d", ErrTypedTableUpdateInvalid, operation.Kind)
		}
	}

	table.mu.Lock()
	defer table.mu.Unlock()
	if table.generated {
		return TypedTableChange{}, ErrTypedTableUpdateGenerated
	}
	index, exists := table.positions[key]
	if !exists || table.typedTableRowDeletedLocked(index) {
		return TypedTableChange{}, fmt.Errorf("typed table key %q does not exist", key)
	}

	var fieldIndexes [typedTableMaxFieldUpdates]int
	for operationIndex, operation := range operations {
		column := strings.TrimSpace(operation.Column)
		fieldIndex, found := table.byName[column]
		if !found {
			return TypedTableChange{}, fmt.Errorf("%w: column %q does not exist", ErrTypedTableUpdateInvalid, operation.Column)
		}
		for previous := 0; previous < operationIndex; previous++ {
			if fieldIndexes[previous] == fieldIndex {
				return TypedTableChange{}, fmt.Errorf("%w: column %q appears more than once", ErrTypedTableUpdateInvalid, operation.Column)
			}
		}
		fieldIndexes[operationIndex] = fieldIndex
	}

	before := table.rowLocked(index)
	after := cloneTypedTableValues(before)
	for operationIndex, operation := range operations {
		fieldIndex := fieldIndexes[operationIndex]
		column := table.schema.Columns[fieldIndex]
		switch operation.Kind {
		case TypedTableUpdateSet:
			if operation.Value.Valid && operation.Value.Kind != column.Kind {
				return TypedTableChange{}, fmt.Errorf("%w: SET column %q expects kind %d, got %d", ErrTypedTableUpdateInvalid, column.Name, column.Kind, operation.Value.Kind)
			}
			if !operation.Value.Valid {
				after[fieldIndex] = TypedNull()
			} else {
				after[fieldIndex] = operation.Value
			}
		case TypedTableUpdateAdd:
			if err := applyTypedTableAddUpdate(&after[fieldIndex], operation.Value, column.Name); err != nil {
				return TypedTableChange{}, err
			}
		}
	}
	if err := table.validateValues(after); err != nil {
		return TypedTableChange{}, fmt.Errorf("%w: %v", ErrTypedTableUpdateInvalid, err)
	}

	var rowBytes int64
	if table.memoryBudgetMaxBytes > 0 {
		rowBytes = typedTableEstimatedRowBytes(key, after)
		if err := table.checkTypedTableMemoryBudgetLocked(table.memoryRowBytes[index], rowBytes); err != nil {
			return TypedTableChange{}, err
		}
	}

	table.clearColumnarLayoutsLocked()
	table.invalidateTypedTableDerivedCachesLocked()
	table.appendOnly = false
	var ttlNow time.Time
	if table.ttl != nil && table.ttl.options.Mode == TypedTableTTLProcessingTime {
		ttlNow = table.typedTableTTLNow()
	}
	change := TypedTableChange{
		Operation: "UPDATE",
		Key:       key,
		Before:    before,
		After:     after,
	}
	for columnIndex := range table.columns {
		table.columns[columnIndex].set(index, after[columnIndex])
	}
	table.replaceTypedTableMemoryRowLocked(index, rowBytes)
	if table.ttl != nil {
		table.setTypedTableTTLDeadlineLocked(index, ttlNow)
	}
	table.setTypedTableColumnTTLDeadlineLocked(index)
	return table.appendChangeLocked(change), nil
}

func applyTypedTableAddUpdate(destination *TypedTableValue, delta TypedTableValue, column string) error {
	if !delta.Valid || destination == nil || !destination.Valid || destination.Kind != delta.Kind {
		return fmt.Errorf("%w: ADD column %q requires two non-null values of the same numeric kind", ErrTypedTableUpdateInvalid, column)
	}
	switch delta.Kind {
	case TypedTableInt64:
		if delta.Int64 > 0 && destination.Int64 > math.MaxInt64-delta.Int64 {
			return fmt.Errorf("%w: ADD column %q exceeds Int64", ErrTypedTableUpdateOverflow, column)
		}
		if delta.Int64 < 0 && destination.Int64 < math.MinInt64-delta.Int64 {
			return fmt.Errorf("%w: ADD column %q is below Int64", ErrTypedTableUpdateOverflow, column)
		}
		destination.Int64 += delta.Int64
	case TypedTableFloat64:
		if math.IsNaN(delta.Float64) || math.IsInf(delta.Float64, 0) || math.IsNaN(destination.Float64) || math.IsInf(destination.Float64, 0) {
			return fmt.Errorf("%w: ADD column %q requires finite Float64 values", ErrTypedTableUpdateInvalid, column)
		}
		destination.Float64 += delta.Float64
		if math.IsNaN(destination.Float64) || math.IsInf(destination.Float64, 0) {
			return fmt.Errorf("%w: ADD column %q exceeds Float64", ErrTypedTableUpdateOverflow, column)
		}
	default:
		return fmt.Errorf("%w: ADD column %q supports only Int64 and Float64", ErrTypedTableUpdateInvalid, column)
	}
	return nil
}
