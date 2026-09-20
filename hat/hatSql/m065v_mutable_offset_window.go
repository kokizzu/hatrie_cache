package hatSql

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

var (
	ErrIncrementalOffsetWindowMutationInvalid     = errors.New("incremental offset window mutation operation is invalid")
	ErrIncrementalOffsetWindowMutationKeyRequired = errors.New("incremental offset window mutation key is required")
	ErrIncrementalOffsetWindowMutationRowRequired = errors.New("incremental offset window mutation row is required")
	ErrIncrementalOffsetWindowMutationDuplicate   = errors.New("incremental offset window mutation key is duplicated")
	ErrIncrementalOffsetWindowMissingKey          = errors.New("incremental offset window mutation key is missing")
	ErrIncrementalOffsetWindowMutationKeyMismatch = errors.New("incremental offset window mutation key does not match row key")
	ErrIncrementalOffsetWindowMutationsDisabled   = errors.New("incremental offset window mutations are disabled")
)

// IncrementalOffsetWindowMutationOperation identifies one mutable row
// operation. INSERT and UPDATE require Row; DELETE ignores Row.
type IncrementalOffsetWindowMutationOperation uint8

const (
	IncrementalOffsetWindowInsert IncrementalOffsetWindowMutationOperation = iota + 1
	IncrementalOffsetWindowUpdate
	IncrementalOffsetWindowDelete
)

// IncrementalOffsetWindowMutation changes one stable row identity. INSERT
// may omit Key and derive it from RowKey; UPDATE and DELETE require Key.
type IncrementalOffsetWindowMutation struct {
	Operation IncrementalOffsetWindowMutationOperation
	Key       string
	Row       Row
}

type mutableIncrementalOffsetWindowEntry struct {
	key       string
	row       Row
	partition string
	order     interface{}
	value     interface{}
}

type preparedMutableIncrementalOffsetWindowMutation struct {
	operation IncrementalOffsetWindowMutationOperation
	key       string
	entry     mutableIncrementalOffsetWindowEntry
	old       mutableIncrementalOffsetWindowEntry
}

// MutableIncrementalOffsetWindow retains all base rows so arbitrary inserts,
// updates, and deletes can emit exact differential changes. The existing
// NewIncrementalOffsetWindow constructor remains append-only and does not pay
// this retention cost.
type MutableIncrementalOffsetWindow struct {
	direction    IncrementalOffsetWindowDirection
	outputColumn string
	partitionKey IncrementalWindowPartitionKeyFunc
	orderKey     IncrementalWindowOrderKeyFunc
	rowKey       IncrementalWindowRowKeyFunc
	valueKey     IncrementalOffsetWindowValueKeyFunc
	offset       int
	defaultValue interface{}
	descending   bool
	rows         map[string]mutableIncrementalOffsetWindowEntry
	partitions   map[string][]mutableIncrementalOffsetWindowEntry
	outputs      map[string]Row
}

// NewMutableIncrementalOffsetWindow creates a row-retaining LAG or LEAD
// maintainer. It is opt-in; NewIncrementalOffsetWindow remains the bounded,
// append-only default.
func NewMutableIncrementalOffsetWindow(definition IncrementalOffsetWindowDefinition) (*MutableIncrementalOffsetWindow, error) {
	appendWindow, err := NewIncrementalOffsetWindow(definition)
	if err != nil {
		return nil, err
	}
	return &MutableIncrementalOffsetWindow{
		direction:    appendWindow.direction,
		outputColumn: appendWindow.outputColumn,
		partitionKey: appendWindow.partitionKey,
		orderKey:     appendWindow.orderKey,
		rowKey:       appendWindow.rowKey,
		valueKey:     appendWindow.valueKey,
		offset:       appendWindow.offset,
		defaultValue: appendWindow.defaultValue,
		descending:   appendWindow.descending,
		rows:         make(map[string]mutableIncrementalOffsetWindowEntry),
		partitions:   make(map[string][]mutableIncrementalOffsetWindowEntry),
		outputs:      make(map[string]Row),
	}, nil
}

// Apply atomically applies a batch of row mutations. A failed validation or
// callback leaves all previously published rows and outputs unchanged.
func (window *MutableIncrementalOffsetWindow) Apply(mutations []IncrementalOffsetWindowMutation) ([]DifferentialRow, error) {
	if window == nil {
		return nil, ErrIncrementalOffsetWindowNil
	}
	if window.rows == nil {
		return nil, ErrIncrementalOffsetWindowMutationsDisabled
	}
	if len(mutations) == 0 {
		return nil, nil
	}

	prepared, affected, err := window.prepareMutableOffsetWindowMutations(mutations)
	if err != nil {
		return nil, err
	}
	if window.canApplyStableMutableOffsetUpdates(prepared) {
		return window.applyStableMutableOffsetUpdates(prepared, affected)
	}
	return window.applyMutableOffsetWindowRebuild(prepared, affected)
}

func (window *MutableIncrementalOffsetWindow) prepareMutableOffsetWindowMutations(mutations []IncrementalOffsetWindowMutation) ([]preparedMutableIncrementalOffsetWindowMutation, map[string]struct{}, error) {
	prepared := make([]preparedMutableIncrementalOffsetWindowMutation, 0, len(mutations))
	affected := make(map[string]struct{}, len(mutations))
	seen := make(map[string]struct{}, len(mutations))
	for index, mutation := range mutations {
		switch mutation.Operation {
		case IncrementalOffsetWindowInsert:
			if mutation.Row == nil {
				return nil, nil, fmt.Errorf("incremental offset window mutation %d: %w", index, ErrIncrementalOffsetWindowMutationRowRequired)
			}
			entry, err := window.prepareMutableOffsetWindowEntry(mutation.Row)
			if err != nil {
				return nil, nil, fmt.Errorf("incremental offset window mutation %d: %w", index, err)
			}
			if mutation.Key != "" && mutation.Key != entry.key {
				return nil, nil, fmt.Errorf("incremental offset window mutation %d: %w", index, ErrIncrementalOffsetWindowMutationKeyMismatch)
			}
			if _, exists := window.rows[entry.key]; exists {
				return nil, nil, fmt.Errorf("incremental offset window mutation %d key %q: %w", index, entry.key, ErrIncrementalOffsetWindowMutationDuplicate)
			}
			if _, exists := seen[entry.key]; exists {
				return nil, nil, fmt.Errorf("incremental offset window mutation %d key %q: %w", index, entry.key, ErrIncrementalOffsetWindowMutationDuplicate)
			}
			seen[entry.key] = struct{}{}
			prepared = append(prepared, preparedMutableIncrementalOffsetWindowMutation{
				operation: mutation.Operation,
				key:       entry.key,
				entry:     entry,
			})
			affected[entry.partition] = struct{}{}

		case IncrementalOffsetWindowUpdate:
			if strings.TrimSpace(mutation.Key) == "" {
				return nil, nil, fmt.Errorf("incremental offset window mutation %d: %w", index, ErrIncrementalOffsetWindowMutationKeyRequired)
			}
			old, exists := window.rows[mutation.Key]
			if !exists {
				return nil, nil, fmt.Errorf("incremental offset window mutation %d key %q: %w", index, mutation.Key, ErrIncrementalOffsetWindowMissingKey)
			}
			if mutation.Row == nil {
				return nil, nil, fmt.Errorf("incremental offset window mutation %d: %w", index, ErrIncrementalOffsetWindowMutationRowRequired)
			}
			entry, err := window.prepareMutableOffsetWindowEntry(mutation.Row)
			if err != nil {
				return nil, nil, fmt.Errorf("incremental offset window mutation %d: %w", index, err)
			}
			if entry.key != mutation.Key {
				return nil, nil, fmt.Errorf("incremental offset window mutation %d: %w", index, ErrIncrementalOffsetWindowMutationKeyMismatch)
			}
			if _, exists := seen[mutation.Key]; exists {
				return nil, nil, fmt.Errorf("incremental offset window mutation %d key %q: %w", index, mutation.Key, ErrIncrementalOffsetWindowMutationDuplicate)
			}
			seen[mutation.Key] = struct{}{}
			prepared = append(prepared, preparedMutableIncrementalOffsetWindowMutation{
				operation: mutation.Operation,
				key:       mutation.Key,
				entry:     entry,
				old:       old,
			})
			affected[old.partition] = struct{}{}
			affected[entry.partition] = struct{}{}

		case IncrementalOffsetWindowDelete:
			if strings.TrimSpace(mutation.Key) == "" {
				return nil, nil, fmt.Errorf("incremental offset window mutation %d: %w", index, ErrIncrementalOffsetWindowMutationKeyRequired)
			}
			old, exists := window.rows[mutation.Key]
			if !exists {
				return nil, nil, fmt.Errorf("incremental offset window mutation %d key %q: %w", index, mutation.Key, ErrIncrementalOffsetWindowMissingKey)
			}
			if _, exists := seen[mutation.Key]; exists {
				return nil, nil, fmt.Errorf("incremental offset window mutation %d key %q: %w", index, mutation.Key, ErrIncrementalOffsetWindowMutationDuplicate)
			}
			seen[mutation.Key] = struct{}{}
			prepared = append(prepared, preparedMutableIncrementalOffsetWindowMutation{
				operation: mutation.Operation,
				key:       mutation.Key,
				old:       old,
			})
			affected[old.partition] = struct{}{}

		default:
			return nil, nil, fmt.Errorf("incremental offset window mutation %d: %w", index, ErrIncrementalOffsetWindowMutationInvalid)
		}
	}
	return prepared, affected, nil
}

func (window *MutableIncrementalOffsetWindow) prepareMutableOffsetWindowEntry(row Row) (mutableIncrementalOffsetWindowEntry, error) {
	row = cloneIncrementalOffsetWindowRow(row)
	if _, exists := row[window.outputColumn]; exists {
		return mutableIncrementalOffsetWindowEntry{}, ErrIncrementalOffsetWindowOutputConflict
	}
	partition := ""
	if window.partitionKey != nil {
		value, err := window.partitionKey(row)
		if err != nil {
			return mutableIncrementalOffsetWindowEntry{}, fmt.Errorf("partition key: %w", err)
		}
		partition = value
	}
	order, err := window.orderKey(row)
	if err != nil {
		return mutableIncrementalOffsetWindowEntry{}, fmt.Errorf("order key: %w", err)
	}
	key, err := window.rowKey(row)
	if err != nil {
		return mutableIncrementalOffsetWindowEntry{}, fmt.Errorf("row key: %w", err)
	}
	if strings.TrimSpace(key) == "" {
		return mutableIncrementalOffsetWindowEntry{}, ErrIncrementalOffsetWindowRowKeyRequired
	}
	value, err := window.valueKey(row)
	if err != nil {
		return mutableIncrementalOffsetWindowEntry{}, fmt.Errorf("value key: %w", err)
	}
	return mutableIncrementalOffsetWindowEntry{
		key:       key,
		row:       row,
		partition: partition,
		order:     order,
		value:     value,
	}, nil
}

func (window *MutableIncrementalOffsetWindow) canApplyStableMutableOffsetUpdates(prepared []preparedMutableIncrementalOffsetWindowMutation) bool {
	if len(prepared) == 0 {
		return false
	}
	for _, mutation := range prepared {
		if mutation.operation != IncrementalOffsetWindowUpdate || mutation.old.partition != mutation.entry.partition || sqlCompare(mutation.old.order, mutation.entry.order) != 0 {
			return false
		}
	}
	return true
}

// Same-position updates avoid sorting or rebuilding unaffected output rows.
// The row slice is copied once per touched partition, while only the updated
// row and the offset-dependent row(s) are re-evaluated.
func (window *MutableIncrementalOffsetWindow) applyStableMutableOffsetUpdates(prepared []preparedMutableIncrementalOffsetWindowMutation, affected map[string]struct{}) ([]DifferentialRow, error) {
	working := make(map[string][]mutableIncrementalOffsetWindowEntry, len(affected))
	for partition := range affected {
		working[partition] = append([]mutableIncrementalOffsetWindowEntry(nil), window.partitions[partition]...)
	}
	changedKeys := make(map[string]struct{}, len(prepared)*2)
	for _, mutation := range prepared {
		entries := working[mutation.entry.partition]
		index := mutableIncrementalOffsetWindowEntryIndex(entries, mutation.key)
		if index < 0 {
			return nil, fmt.Errorf("incremental offset window stable update key %q is missing", mutation.key)
		}
		entries[index] = mutation.entry
		working[mutation.entry.partition] = entries
		changedKeys[mutation.key] = struct{}{}
		if window.offset == 0 {
			continue
		}
		dependent := index - window.offset
		if window.direction == IncrementalWindowLag {
			dependent = index + window.offset
		}
		if dependent >= 0 && dependent < len(entries) {
			changedKeys[entries[dependent].key] = struct{}{}
		}
	}

	newOutputs := make(map[string]Row, len(changedKeys))
	for key := range changedKeys {
		old, exists := window.rows[key]
		if !exists {
			return nil, fmt.Errorf("incremental offset window dependent key %q is missing", key)
		}
		entries := working[old.partition]
		index := mutableIncrementalOffsetWindowEntryIndex(entries, key)
		if index < 0 {
			return nil, fmt.Errorf("incremental offset window dependent key %q is missing", key)
		}
		newOutputs[key] = incrementalOffsetWindowOutput(entries[index].row, window.outputColumn, window.mutableOffsetValue(entries, index))
	}
	updates := diffMutableIncrementalOffsetWindowChanges(window.outputs, newOutputs, changedKeys)

	for partition, entries := range working {
		window.partitions[partition] = entries
	}
	for _, mutation := range prepared {
		window.rows[mutation.key] = mutation.entry
	}
	for key, output := range newOutputs {
		window.outputs[key] = output
	}
	return updates, nil
}

func (window *MutableIncrementalOffsetWindow) applyMutableOffsetWindowRebuild(prepared []preparedMutableIncrementalOffsetWindowMutation, affected map[string]struct{}) ([]DifferentialRow, error) {
	working := make(map[string][]mutableIncrementalOffsetWindowEntry, len(affected))
	oldKeys := make(map[string]struct{})
	for partition := range affected {
		entries := window.partitions[partition]
		working[partition] = append([]mutableIncrementalOffsetWindowEntry(nil), entries...)
		for _, entry := range entries {
			oldKeys[entry.key] = struct{}{}
		}
	}
	for _, mutation := range prepared {
		switch mutation.operation {
		case IncrementalOffsetWindowInsert:
			working[mutation.entry.partition] = append(working[mutation.entry.partition], mutation.entry)
		case IncrementalOffsetWindowUpdate:
			entries := working[mutation.old.partition]
			index := mutableIncrementalOffsetWindowEntryIndex(entries, mutation.key)
			if index < 0 {
				return nil, fmt.Errorf("incremental offset window update key %q is missing", mutation.key)
			}
			entries = append(entries[:index], entries[index+1:]...)
			working[mutation.old.partition] = entries
			working[mutation.entry.partition] = append(working[mutation.entry.partition], mutation.entry)
		case IncrementalOffsetWindowDelete:
			entries := working[mutation.old.partition]
			index := mutableIncrementalOffsetWindowEntryIndex(entries, mutation.key)
			if index < 0 {
				return nil, fmt.Errorf("incremental offset window delete key %q is missing", mutation.key)
			}
			working[mutation.old.partition] = append(entries[:index], entries[index+1:]...)
		}
	}

	newOutputs := make(map[string]Row)
	changedKeys := make(map[string]struct{}, len(oldKeys)+len(prepared))
	for key := range oldKeys {
		changedKeys[key] = struct{}{}
	}
	for _, mutation := range prepared {
		changedKeys[mutation.key] = struct{}{}
	}
	for partition, entries := range working {
		sort.SliceStable(entries, func(left, right int) bool {
			comparison := sqlCompare(entries[left].order, entries[right].order)
			if window.descending {
				comparison = -comparison
			}
			if comparison != 0 {
				return comparison < 0
			}
			return entries[left].key < entries[right].key
		})
		working[partition] = entries
		for _, entry := range entries {
			changedKeys[entry.key] = struct{}{}
		}
		for index, entry := range entries {
			newOutputs[entry.key] = incrementalOffsetWindowOutput(entry.row, window.outputColumn, window.mutableOffsetValue(entries, index))
		}
	}
	updates := diffMutableIncrementalOffsetWindowChanges(window.outputs, newOutputs, changedKeys)

	for partition, entries := range working {
		if len(entries) == 0 {
			delete(window.partitions, partition)
			continue
		}
		window.partitions[partition] = entries
	}
	for key := range oldKeys {
		delete(window.rows, key)
		delete(window.outputs, key)
	}
	for partition, entries := range working {
		for _, entry := range entries {
			window.rows[entry.key] = entry
			window.outputs[entry.key] = newOutputs[entry.key]
		}
		if len(entries) == 0 {
			delete(window.partitions, partition)
		}
	}
	return updates, nil
}

func (window *MutableIncrementalOffsetWindow) mutableOffsetValue(entries []mutableIncrementalOffsetWindowEntry, index int) interface{} {
	if window.offset == 0 {
		return entries[index].value
	}
	source := index + window.offset
	if window.direction == IncrementalWindowLag {
		source = index - window.offset
	}
	if source < 0 || source >= len(entries) {
		return window.defaultValue
	}
	return entries[source].value
}

func mutableIncrementalOffsetWindowEntryIndex(entries []mutableIncrementalOffsetWindowEntry, key string) int {
	for index, entry := range entries {
		if entry.key == key {
			return index
		}
	}
	return -1
}

func diffMutableIncrementalOffsetWindowChanges(oldOutputs, newOutputs map[string]Row, changedKeys map[string]struct{}) []DifferentialRow {
	keys := make([]string, 0, len(changedKeys))
	for key := range changedKeys {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	updates := make([]DifferentialRow, 0, len(keys)*2)
	for _, key := range keys {
		oldRow, hadOld := oldOutputs[key]
		newRow, hasNew := newOutputs[key]
		if hadOld && hasNew && reflect.DeepEqual(oldRow, newRow) {
			continue
		}
		if hadOld {
			updates = append(updates, DifferentialRow{Key: key, Diff: -1, Row: cloneIncrementalOffsetWindowRow(oldRow)})
		}
		if hasNew {
			updates = append(updates, DifferentialRow{Key: key, Diff: 1, Row: cloneIncrementalOffsetWindowRow(newRow)})
		}
	}
	return updates
}
