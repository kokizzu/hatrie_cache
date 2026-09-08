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
	// ErrTypedTableSortedArrangementDictionaryKind reports a dictionary option
	// on a non-string sort field.
	ErrTypedTableSortedArrangementDictionaryKind = errors.New("typed table sorted arrangement dictionary field must be string")
	// ErrTypedTableSortedArrangementOrder reports an ambiguous or duplicate
	// composite order definition.
	ErrTypedTableSortedArrangementOrder = errors.New("typed table sorted arrangement order is invalid")
)

const typedTableSortedArrangementBulkMinimumChanges = 64

// TypedTableSortedArrangementDefinition configures one ordered typed-table
// arrangement. NULL and NaN values use NullsFirst; ties are ordered by row key
// for deterministic results. DictionaryEncoded interns live non-NULL string
// values in Field and is rejected for non-string fields; it is disabled by
// default to preserve the existing storage and CPU profile. OrderBy selects
// an additive composite definition; when it is non-empty, the legacy scalar
// fields must remain at their zero values.
type TypedTableSortedArrangementDefinition struct {
	Field             string
	Descending        bool
	NullsFirst        bool
	DictionaryEncoded bool
	OrderBy           []TypedTableSortedArrangementOrder
}

// TypedTableSortedArrangementOrder describes one field in a composite ordered
// arrangement. OrderBy fields are compared from first to last; ties are then
// ordered by row key for deterministic results.
type TypedTableSortedArrangementOrder struct {
	Field             string
	Descending        bool
	NullsFirst        bool
	DictionaryEncoded bool
}

type typedTableSortedArrangementOrderField struct {
	index             int
	kind              TypedTableKind
	descending        bool
	nullsFirst        bool
	dictionaryEncoded bool
}

// TypedTableSortedArrangement maintains an ordered row-key vector while
// applying a typed table's insert, update, and delete changefeed. It is useful
// for repeated ORDER BY access when the source field is already typed.
type TypedTableSortedArrangement struct {
	mu           sync.RWMutex
	field        int
	fieldKind    TypedTableKind
	columnCount  int
	definition   TypedTableSortedArrangementDefinition
	entries      map[string]TypedTableMergeJoinInput
	order        []string
	positions    map[string]int
	orderFields  []typedTableSortedArrangementOrderField
	dictionary   *typedTableSortedArrangementStringDictionary
	dictionaries []*typedTableSortedArrangementStringDictionary
	checkpoint   uint64
}

// NewTypedTableSortedArrangement snapshots table and creates an ordered
// arrangement beginning after the table's current change sequence.
func NewTypedTableSortedArrangement(table *TypedTable, definition TypedTableSortedArrangementDefinition) (*TypedTableSortedArrangement, error) {
	if table == nil {
		return nil, ErrTypedTableSortedArrangementNil
	}
	orderFields, err := typedTableSortedArrangementOrderFields(table, definition)
	if err != nil {
		return nil, err
	}
	rows, checkpoint := typedTableSortedArrangementSnapshot(table)
	arrangement := &TypedTableSortedArrangement{
		field: orderFields[0].index, fieldKind: orderFields[0].kind, columnCount: len(table.columns), definition: definition,
		entries:   make(map[string]TypedTableMergeJoinInput, len(rows)),
		positions: make(map[string]int, len(rows)), orderFields: orderFields,
		dictionaries: make([]*typedTableSortedArrangementStringDictionary, len(orderFields)), checkpoint: checkpoint,
	}
	for index, orderField := range orderFields {
		if orderField.dictionaryEncoded {
			arrangement.dictionaries[index] = &typedTableSortedArrangementStringDictionary{}
			if index == 0 {
				arrangement.dictionary = arrangement.dictionaries[index]
			}
		}
	}
	for key, values := range rows {
		arrangement.entries[key] = TypedTableMergeJoinInput{Key: key, Values: arrangement.storeValues(values)}
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

func typedTableSortedArrangementOrderFields(table *TypedTable, definition TypedTableSortedArrangementDefinition) ([]typedTableSortedArrangementOrderField, error) {
	if len(definition.OrderBy) == 0 {
		field, kind, found := typedTableJoinField(table, definition.Field)
		if !found {
			return nil, fmt.Errorf("%w: %q", ErrTypedTableSortedArrangementField, definition.Field)
		}
		if definition.DictionaryEncoded && kind != TypedTableString {
			return nil, fmt.Errorf("%w: field %q has kind %d", ErrTypedTableSortedArrangementDictionaryKind, definition.Field, kind)
		}
		return []typedTableSortedArrangementOrderField{{
			index: field, kind: kind, descending: definition.Descending, nullsFirst: definition.NullsFirst, dictionaryEncoded: definition.DictionaryEncoded,
		}}, nil
	}
	if definition.Field != "" || definition.Descending || definition.NullsFirst || definition.DictionaryEncoded {
		return nil, fmt.Errorf("%w: OrderBy cannot be combined with legacy scalar fields", ErrTypedTableSortedArrangementOrder)
	}
	fields := make([]typedTableSortedArrangementOrderField, len(definition.OrderBy))
	seen := make(map[int]struct{}, len(fields))
	for index, order := range definition.OrderBy {
		field, kind, found := typedTableJoinField(table, order.Field)
		if !found {
			return nil, fmt.Errorf("%w: %q", ErrTypedTableSortedArrangementField, order.Field)
		}
		if _, duplicate := seen[field]; duplicate {
			return nil, fmt.Errorf("%w: field %q occurs more than once", ErrTypedTableSortedArrangementOrder, order.Field)
		}
		if order.DictionaryEncoded && kind != TypedTableString {
			return nil, fmt.Errorf("%w: field %q has kind %d", ErrTypedTableSortedArrangementDictionaryKind, order.Field, kind)
		}
		seen[field] = struct{}{}
		fields[index] = typedTableSortedArrangementOrderField{
			index: field, kind: kind, descending: order.Descending, nullsFirst: order.NullsFirst, dictionaryEncoded: order.DictionaryEncoded,
		}
	}
	return fields, nil
}

// Apply advances the arrangement through strictly ordered source changes.
// Replayed sequences are ignored; a gap leaves the valid prefix applied.
func (arrangement *TypedTableSortedArrangement) Apply(changes []TypedTableChange) error {
	if arrangement == nil {
		return ErrTypedTableSortedArrangementNil
	}
	arrangement.mu.Lock()
	defer arrangement.mu.Unlock()
	if arrangement.shouldAppendBulk(changes) {
		arrangement.applyAppendBulk(changes)
		return nil
	}
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
			arrangement.entries[change.Key] = TypedTableMergeJoinInput{Key: change.Key, Values: arrangement.storeValues(change.After)}
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

func (arrangement *TypedTableSortedArrangement) shouldAppendBulk(changes []TypedTableChange) bool {
	if len(changes) < typedTableSortedArrangementBulkMinimumChanges {
		return false
	}
	sequence := arrangement.checkpoint + 1
	var seen map[string]struct{}
	var firstKey string
	var previous TypedTableMergeJoinInput
	for index, change := range changes {
		if change.Sequence != sequence || change.Operation != "INSERT" {
			return false
		}
		if err := arrangement.validateChange(change); err != nil {
			return false
		}
		if _, found := arrangement.entries[change.Key]; found {
			return false
		}
		candidate := TypedTableMergeJoinInput{Key: change.Key, Values: change.After}
		if index == 0 {
			if len(arrangement.order) > 0 && arrangement.compareRows(arrangement.entries[arrangement.order[len(arrangement.order)-1]], candidate) >= 0 {
				return false
			}
		} else if arrangement.compareRows(previous, candidate) >= 0 {
			return false
		}
		if index == 0 {
			firstKey = change.Key
		} else {
			if seen == nil {
				seen = make(map[string]struct{}, len(changes))
				seen[firstKey] = struct{}{}
			}
			if _, found := seen[change.Key]; found {
				return false
			}
			seen[change.Key] = struct{}{}
		}
		previous = candidate
		sequence++
	}
	return true
}

func (arrangement *TypedTableSortedArrangement) applyAppendBulk(changes []TypedTableChange) {
	for _, change := range changes {
		arrangement.entries[change.Key] = TypedTableMergeJoinInput{Key: change.Key, Values: arrangement.storeValues(change.After)}
		arrangement.order = append(arrangement.order, change.Key)
		arrangement.positions[change.Key] = len(arrangement.order) - 1
		arrangement.checkpoint = change.Sequence
	}
}

func (arrangement *TypedTableSortedArrangement) applyBulk(changes []TypedTableChange) {
	for _, change := range changes {
		if previous, found := arrangement.entries[change.Key]; found {
			arrangement.releaseValues(previous.Values)
		}
		delete(arrangement.entries, change.Key)
		if change.Operation != "DELETE" {
			arrangement.entries[change.Key] = TypedTableMergeJoinInput{Key: change.Key, Values: arrangement.storeValues(change.After)}
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
	return arrangement.rowsPageLocked(0, len(arrangement.order))
}

// RowsPage returns an independent snapshot of at most limit rows starting at
// offset in the arrangement's configured order. Negative offsets are treated
// as zero; non-positive limits and offsets beyond the end return an empty
// snapshot.
func (arrangement *TypedTableSortedArrangement) RowsPage(offset, limit int) []TypedTableMergeJoinInput {
	if arrangement == nil {
		return nil
	}
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		return []TypedTableMergeJoinInput{}
	}
	arrangement.mu.RLock()
	defer arrangement.mu.RUnlock()
	return arrangement.rowsPageLocked(offset, limit)
}

func (arrangement *TypedTableSortedArrangement) rowsPageLocked(offset, limit int) []TypedTableMergeJoinInput {
	if offset >= len(arrangement.order) || limit <= 0 {
		return []TypedTableMergeJoinInput{}
	}
	end := len(arrangement.order)
	if limit < end-offset {
		end = offset + limit
	}
	rows := make([]TypedTableMergeJoinInput, end-offset)
	for index, key := range arrangement.order[offset:end] {
		row := arrangement.entries[key]
		rows[index] = TypedTableMergeJoinInput{Key: row.Key, Values: cloneTypedTableValues(row.Values)}
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
	for _, orderField := range arrangement.orderFields {
		value := change.After[orderField.index]
		if value.Valid && value.Kind != orderField.kind {
			return fmt.Errorf("%w: got=%d want=%d", ErrTypedTableSortedArrangementTypeMismatch, value.Kind, orderField.kind)
		}
	}
	return nil
}

func (arrangement *TypedTableSortedArrangement) compareKeys(leftKey, rightKey string) int {
	left := arrangement.entries[leftKey]
	right := arrangement.entries[rightKey]
	return arrangement.compareRows(left, right)
}

func (arrangement *TypedTableSortedArrangement) compareRows(left, right TypedTableMergeJoinInput) int {
	for _, orderField := range arrangement.orderFields {
		leftValue, leftValid := typedTableSortedArrangementValue(left.Values, orderField.index)
		rightValue, rightValid := typedTableSortedArrangementValue(right.Values, orderField.index)
		if leftValid != rightValid {
			if orderField.nullsFirst == leftValid {
				return 1
			}
			return -1
		}
		if leftValid {
			comparison := compareTypedTableMergeJoinValues(leftValue, rightValue)
			if orderField.descending {
				comparison = -comparison
			}
			if comparison != 0 {
				return comparison
			}
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
	arrangement.releaseValues(arrangement.entries[key].Values)
	copy(arrangement.order[position:], arrangement.order[position+1:])
	arrangement.order = arrangement.order[:len(arrangement.order)-1]
	delete(arrangement.positions, key)
	delete(arrangement.entries, key)
	for index := position; index < len(arrangement.order); index++ {
		arrangement.positions[arrangement.order[index]] = index
	}
}

func (arrangement *TypedTableSortedArrangement) insertKey(key string) {
	if len(arrangement.order) == 0 || arrangement.compareKeys(arrangement.order[len(arrangement.order)-1], key) < 0 {
		arrangement.positions[key] = len(arrangement.order)
		arrangement.order = append(arrangement.order, key)
		return
	}
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
