package hatSql

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
)

var (
	// ErrTypedTableSortedArrangementNil reports a nil sorted arrangement.
	ErrTypedTableSortedArrangementNil = errors.New("typed table sorted arrangement is nil")
	// ErrTypedTableSortedArrangementField reports a missing or invalid field.
	ErrTypedTableSortedArrangementField = errors.New("typed table sorted arrangement field is invalid")
	// ErrTypedTableSortedArrangementChange reports an invalid source change.
	ErrTypedTableSortedArrangementChange = errors.New("typed table sorted arrangement change is invalid")
	// ErrTypedTableSortedArrangementSequenceGap reports a missing source change.
	ErrTypedTableSortedArrangementSequenceGap = errors.New("typed table sorted arrangement change sequence gap")
	// ErrTypedTableSortedArrangementTypeMismatch reports an invalid sort value.
	ErrTypedTableSortedArrangementTypeMismatch = errors.New("typed table sorted arrangement value kind differs")
)

const typedTableSortedArrangementBulkMinimumChanges = 64

// TypedTableSortedArrangementDefinition configures one ordered typed-table
// arrangement. NULL and NaN values use NullsFirst; ties are ordered by row key
// for deterministic results.
type TypedTableSortedArrangementDefinition struct {
	Field      string
	Descending bool
	NullsFirst bool
}

// TypedTableSortedArrangement maintains an ordered row-key vector while
// applying a typed table's insert, update, and delete changefeed. It is useful
// for repeated ORDER BY access when the source field is already typed.
type TypedTableSortedArrangement struct {
	mu          sync.RWMutex
	field       int
	fieldKind   TypedTableKind
	columnCount int
	definition  TypedTableSortedArrangementDefinition
	entries     map[string]TypedTableMergeJoinInput
	order       []string
	positions   map[string]int
	checkpoint  uint64
}

// NewTypedTableSortedArrangement snapshots table and creates an ordered
// arrangement beginning after the table's current change sequence.
func NewTypedTableSortedArrangement(table *TypedTable, definition TypedTableSortedArrangementDefinition) (*TypedTableSortedArrangement, error) {
	if table == nil {
		return nil, ErrTypedTableSortedArrangementNil
	}
	field, kind, found := typedTableJoinField(table, definition.Field)
	if !found {
		return nil, fmt.Errorf("%w: %q", ErrTypedTableSortedArrangementField, definition.Field)
	}
	rows, checkpoint := typedTableSortedArrangementSnapshot(table)
	arrangement := &TypedTableSortedArrangement{
		field: field, fieldKind: kind, columnCount: len(table.columns), definition: definition,
		entries:   make(map[string]TypedTableMergeJoinInput, len(rows)),
		positions: make(map[string]int, len(rows)), checkpoint: checkpoint,
	}
	for key, values := range rows {
		arrangement.entries[key] = TypedTableMergeJoinInput{Key: key, Values: values}
		arrangement.order = append(arrangement.order, key)
	}
	sort.Slice(arrangement.order, func(left, right int) bool {
		return arrangement.compareKeys(arrangement.order[left], arrangement.order[right]) < 0
	})
	for index, key := range arrangement.order {
		arrangement.positions[key] = index
	}
	return arrangement, nil
}

// Apply advances the arrangement through strictly ordered source changes.
// Replayed sequences are ignored; a gap leaves the valid prefix applied.
func (arrangement *TypedTableSortedArrangement) Apply(changes []TypedTableChange) error {
	if arrangement == nil {
		return ErrTypedTableSortedArrangementNil
	}
	arrangement.mu.Lock()
	defer arrangement.mu.Unlock()
	if arrangement.shouldBulkApply(changes) {
		arrangement.applyBulk(changes)
		return nil
	}
	for _, change := range changes {
		if change.Sequence <= arrangement.checkpoint {
			continue
		}
		if change.Sequence != arrangement.checkpoint+1 {
			return fmt.Errorf("%w: got %d after %d", ErrTypedTableSortedArrangementSequenceGap, change.Sequence, arrangement.checkpoint)
		}
		if err := arrangement.validateChange(change); err != nil {
			return err
		}
		arrangement.removeKey(change.Key)
		if change.Operation != "DELETE" {
			arrangement.entries[change.Key] = TypedTableMergeJoinInput{Key: change.Key, Values: cloneTypedTableValues(change.After)}
			arrangement.insertKey(change.Key)
		}
		arrangement.checkpoint = change.Sequence
	}
	return nil
}

func (arrangement *TypedTableSortedArrangement) shouldBulkApply(changes []TypedTableChange) bool {
	if len(changes) < typedTableSortedArrangementBulkMinimumChanges {
		return false
	}
	sequence := arrangement.checkpoint + 1
	for _, change := range changes {
		if change.Sequence != sequence {
			return false
		}
		if err := arrangement.validateChange(change); err != nil {
			return false
		}
		sequence++
	}
	return true
}

func (arrangement *TypedTableSortedArrangement) applyBulk(changes []TypedTableChange) {
	for _, change := range changes {
		delete(arrangement.entries, change.Key)
		if change.Operation != "DELETE" {
			arrangement.entries[change.Key] = TypedTableMergeJoinInput{Key: change.Key, Values: cloneTypedTableValues(change.After)}
		} else {
			delete(arrangement.positions, change.Key)
		}
		arrangement.checkpoint = change.Sequence
	}
	arrangement.rebuildOrder()
}

func (arrangement *TypedTableSortedArrangement) rebuildOrder() {
	rows := make([]TypedTableMergeJoinInput, 0, len(arrangement.entries))
	for _, key := range arrangement.order {
		if row, found := arrangement.entries[key]; found {
			rows = append(rows, row)
			arrangement.positions[key] = -1
		}
	}
	for key, row := range arrangement.entries {
		if _, found := arrangement.positions[key]; !found {
			rows = append(rows, row)
		}
	}
	sort.Slice(rows, func(left, right int) bool {
		return arrangement.compareRows(rows[left], rows[right]) < 0
	})
	arrangement.order = arrangement.order[:0]
	arrangement.positions = make(map[string]int, len(arrangement.entries))
	for index, row := range rows {
		arrangement.order = append(arrangement.order, row.Key)
		arrangement.positions[row.Key] = index
	}
}

// Checkpoint returns the latest source sequence applied to the arrangement.
func (arrangement *TypedTableSortedArrangement) Checkpoint() uint64 {
	if arrangement == nil {
		return 0
	}
	arrangement.mu.RLock()
	defer arrangement.mu.RUnlock()
	return arrangement.checkpoint
}

// Rows returns an independent snapshot in the arrangement's configured order.
func (arrangement *TypedTableSortedArrangement) Rows() []TypedTableMergeJoinInput {
	if arrangement == nil {
		return nil
	}
	arrangement.mu.RLock()
	defer arrangement.mu.RUnlock()
	rows := make([]TypedTableMergeJoinInput, 0, len(arrangement.order))
	for _, key := range arrangement.order {
		row := arrangement.entries[key]
		rows = append(rows, TypedTableMergeJoinInput{Key: row.Key, Values: cloneTypedTableValues(row.Values)})
	}
	return rows
}

func typedTableSortedArrangementSnapshot(table *TypedTable) (map[string][]TypedTableValue, uint64) {
	table.mu.RLock()
	defer table.mu.RUnlock()
	rows := make(map[string][]TypedTableValue, len(table.keys))
	for index, key := range table.keys {
		if table.typedTableRowDeletedLocked(index) {
			continue
		}
		rows[key] = table.rowLocked(index)
	}
	return rows, table.sequence
}

func (arrangement *TypedTableSortedArrangement) validateChange(change TypedTableChange) error {
	if change.Key == "" || change.Operation != "INSERT" && change.Operation != "UPDATE" && change.Operation != "DELETE" {
		return fmt.Errorf("%w: key=%q operation=%q", ErrTypedTableSortedArrangementChange, change.Key, change.Operation)
	}
	if change.Operation == "DELETE" {
		return nil
	}
	if len(change.After) != arrangement.columnCount {
		return fmt.Errorf("%w: row has %d values, want %d", ErrTypedTableSortedArrangementChange, len(change.After), arrangement.columnCount)
	}
	value := change.After[arrangement.field]
	if value.Valid && value.Kind != arrangement.fieldKind {
		return fmt.Errorf("%w: got=%d want=%d", ErrTypedTableSortedArrangementTypeMismatch, value.Kind, arrangement.fieldKind)
	}
	return nil
}

func (arrangement *TypedTableSortedArrangement) compareKeys(leftKey, rightKey string) int {
	left := arrangement.entries[leftKey]
	right := arrangement.entries[rightKey]
	return arrangement.compareRows(left, right)
}

func (arrangement *TypedTableSortedArrangement) compareRows(left, right TypedTableMergeJoinInput) int {
	leftValue, leftValid := typedTableSortedArrangementValue(left.Values, arrangement.field)
	rightValue, rightValid := typedTableSortedArrangementValue(right.Values, arrangement.field)
	if leftValid != rightValid {
		if arrangement.definition.NullsFirst == leftValid {
			return 1
		}
		return -1
	}
	if leftValid {
		comparison := compareTypedTableMergeJoinValues(leftValue, rightValue)
		if arrangement.definition.Descending {
			comparison = -comparison
		}
		if comparison != 0 {
			return comparison
		}
	}
	if left.Key < right.Key {
		return -1
	}
	if left.Key > right.Key {
		return 1
	}
	return 0
}

func typedTableSortedArrangementValue(values []TypedTableValue, field int) (TypedTableValue, bool) {
	if field < 0 || field >= len(values) || !values[field].Valid || values[field].Kind == TypedTableNull {
		return TypedTableValue{}, false
	}
	value := values[field]
	if value.Kind == TypedTableFloat64 && math.IsNaN(value.Float64) {
		return TypedTableValue{}, false
	}
	return value, true
}

func (arrangement *TypedTableSortedArrangement) removeKey(key string) {
	position, found := arrangement.positions[key]
	if !found {
		return
	}
	copy(arrangement.order[position:], arrangement.order[position+1:])
	arrangement.order = arrangement.order[:len(arrangement.order)-1]
	delete(arrangement.positions, key)
	delete(arrangement.entries, key)
	for index := position; index < len(arrangement.order); index++ {
		arrangement.positions[arrangement.order[index]] = index
	}
}

func (arrangement *TypedTableSortedArrangement) insertKey(key string) {
	position := sort.Search(len(arrangement.order), func(index int) bool {
		return arrangement.compareKeys(key, arrangement.order[index]) < 0
	})
	arrangement.order = append(arrangement.order, "")
	copy(arrangement.order[position+1:], arrangement.order[position:])
	arrangement.order[position] = key
	for index := position; index < len(arrangement.order); index++ {
		arrangement.positions[arrangement.order[index]] = index
	}
}
