package hatSql

import (
	"sort"
)

// TypedTableSortedArrangementCheckpointRow stores one detached row from a
// sorted arrangement checkpoint.
type TypedTableSortedArrangementCheckpointRow struct {
	Key    string
	Values []TypedTableValue
}

// TypedTableSortedArrangementCheckpoint is a detached, versioned checkpoint
// for one sorted arrangement. SourceSequence and Checkpoint must match for
// recovery without replaying the source changefeed.
type TypedTableSortedArrangementCheckpoint struct {
	Version        uint8
	TableName      string
	Definition     TypedTableSortedArrangementDefinition
	SourceSequence uint64
	Checkpoint     uint64
	Rows           []TypedTableSortedArrangementCheckpointRow
}

// CaptureCheckpoint returns the complete sorted arrangement state without
// reading or replaying the source changefeed.
func (arrangement *TypedTableSortedArrangement) CaptureCheckpoint() (TypedTableSortedArrangementCheckpoint, error) {
	if arrangement == nil || arrangement.table == nil {
		return TypedTableSortedArrangementCheckpoint{}, ErrTypedTableArrangementCheckpointInvalid
	}
	arrangement.mu.RLock()
	defer arrangement.mu.RUnlock()
	arrangement.table.mu.RLock()
	defer arrangement.table.mu.RUnlock()
	if arrangement.checkpoint != arrangement.table.sequence || len(arrangement.entries) > MaxTypedTableArrangementCheckpointRows {
		return TypedTableSortedArrangementCheckpoint{}, ErrTypedTableArrangementCheckpointInvalid
	}
	checkpoint := TypedTableSortedArrangementCheckpoint{
		Version:        TypedTableArrangementCheckpointVersion,
		TableName:      arrangement.table.schema.Name,
		Definition:     cloneTypedTableSortedArrangementDefinition(arrangement.definition),
		SourceSequence: arrangement.table.sequence,
		Checkpoint:     arrangement.checkpoint,
		Rows:           make([]TypedTableSortedArrangementCheckpointRow, 0, len(arrangement.entries)),
	}
	for key, row := range arrangement.entries {
		checkpoint.Rows = append(checkpoint.Rows, TypedTableSortedArrangementCheckpointRow{
			Key:    key,
			Values: cloneTypedTableValues(row.Values),
		})
	}
	sort.Slice(checkpoint.Rows, func(left, right int) bool {
		return checkpoint.Rows[left].Key < checkpoint.Rows[right].Key
	})
	return checkpoint, nil
}

// NewTypedTableSortedArrangementFromCheckpoint constructs a sorted
// arrangement from detached state. It validates only the target table's
// schema and source sequence; it does not scan the table rows.
func NewTypedTableSortedArrangementFromCheckpoint(table *TypedTable, checkpoint TypedTableSortedArrangementCheckpoint) (*TypedTableSortedArrangement, error) {
	if table == nil {
		return nil, ErrTypedTableSortedArrangementNil
	}
	orderFields, err := typedTableSortedArrangementOrderFields(table, checkpoint.Definition)
	if err != nil {
		return nil, err
	}
	arrangement := newTypedTableSortedArrangementShell(table, checkpoint.Definition, orderFields)
	if err := restoreTypedTableSortedArrangementCheckpointLocked(arrangement, checkpoint); err != nil {
		return nil, err
	}
	return arrangement, nil
}

// RestoreCheckpoint replaces a sorted arrangement's state after validating
// its definition, source identity, and exact source version.
func (arrangement *TypedTableSortedArrangement) RestoreCheckpoint(checkpoint TypedTableSortedArrangementCheckpoint) error {
	if arrangement == nil {
		return ErrTypedTableSortedArrangementNil
	}
	arrangement.mu.Lock()
	defer arrangement.mu.Unlock()
	return restoreTypedTableSortedArrangementCheckpointLocked(arrangement, checkpoint)
}

func newTypedTableSortedArrangementShell(table *TypedTable, definition TypedTableSortedArrangementDefinition, orderFields []typedTableSortedArrangementOrderField) *TypedTableSortedArrangement {
	arrangement := &TypedTableSortedArrangement{
		table: table, field: orderFields[0].index, fieldKind: orderFields[0].kind,
		columnCount: len(table.columns), definition: cloneTypedTableSortedArrangementDefinition(definition),
		entries: make(map[string]TypedTableMergeJoinInput), positions: make(map[string]int), orderFields: orderFields,
		dictionaries: make([]*typedTableSortedArrangementStringDictionary, len(orderFields)),
	}
	for index, orderField := range orderFields {
		if orderField.dictionaryEncoded {
			arrangement.dictionaries[index] = &typedTableSortedArrangementStringDictionary{}
			if index == 0 {
				arrangement.dictionary = arrangement.dictionaries[index]
			}
		}
	}
	return arrangement
}

func restoreTypedTableSortedArrangementCheckpointLocked(arrangement *TypedTableSortedArrangement, checkpoint TypedTableSortedArrangementCheckpoint) error {
	if arrangement == nil || arrangement.table == nil || checkpoint.Version != TypedTableArrangementCheckpointVersion || checkpoint.TableName != arrangement.table.schema.Name {
		return ErrTypedTableArrangementCheckpointInvalid
	}
	if checkpoint.SourceSequence != checkpoint.Checkpoint || len(checkpoint.Rows) > MaxTypedTableArrangementCheckpointRows {
		return ErrTypedTableArrangementCheckpointInvalid
	}
	if !typedTableSortedArrangementDefinitionEqual(arrangement.definition, checkpoint.Definition) {
		return ErrTypedTableArrangementCheckpointInvalid
	}
	arrangement.table.mu.RLock()
	currentSequence := arrangement.table.sequence
	arrangement.table.mu.RUnlock()
	if currentSequence != checkpoint.SourceSequence {
		return ErrTypedTableArrangementSourceVersionMismatch
	}

	candidate := newTypedTableSortedArrangementShell(arrangement.table, checkpoint.Definition, arrangement.orderFields)
	candidate.entries = make(map[string]TypedTableMergeJoinInput, len(checkpoint.Rows))
	candidate.order = make([]string, 0, len(checkpoint.Rows))
	candidate.positions = make(map[string]int, len(checkpoint.Rows))
	for _, row := range checkpoint.Rows {
		if row.Key == "" || len(row.Values) != arrangement.columnCount {
			return ErrTypedTableArrangementCheckpointInvalid
		}
		for index, value := range row.Values {
			if value.Valid && value.Kind != arrangement.table.columns[index].kind {
				return ErrTypedTableArrangementCheckpointInvalid
			}
		}
		if _, exists := candidate.entries[row.Key]; exists {
			return ErrTypedTableArrangementCheckpointInvalid
		}
		if err := candidate.validateChange(TypedTableChange{Key: row.Key, Operation: "INSERT", After: row.Values}); err != nil {
			return ErrTypedTableArrangementCheckpointInvalid
		}
		candidate.entries[row.Key] = TypedTableMergeJoinInput{Key: row.Key, Values: candidate.storeValues(row.Values)}
		candidate.order = append(candidate.order, row.Key)
	}
	sort.Slice(candidate.order, func(left, right int) bool {
		return candidate.compareKeys(candidate.order[left], candidate.order[right]) < 0
	})
	for index, key := range candidate.order {
		candidate.positions[key] = index
	}

	for _, row := range arrangement.entries {
		arrangement.releaseValues(row.Values)
	}
	arrangement.entries = candidate.entries
	arrangement.order = candidate.order
	arrangement.positions = candidate.positions
	arrangement.dictionaries = candidate.dictionaries
	arrangement.dictionary = candidate.dictionary
	arrangement.checkpoint = checkpoint.Checkpoint
	return nil
}

func cloneTypedTableSortedArrangementDefinition(definition TypedTableSortedArrangementDefinition) TypedTableSortedArrangementDefinition {
	definition.OrderBy = append([]TypedTableSortedArrangementOrder(nil), definition.OrderBy...)
	return definition
}

func typedTableSortedArrangementDefinitionEqual(left, right TypedTableSortedArrangementDefinition) bool {
	if left.Field != right.Field || left.Descending != right.Descending || left.NullsFirst != right.NullsFirst || left.DictionaryEncoded != right.DictionaryEncoded || len(left.OrderBy) != len(right.OrderBy) {
		return false
	}
	for index := range left.OrderBy {
		if left.OrderBy[index] != right.OrderBy[index] {
			return false
		}
	}
	return true
}
