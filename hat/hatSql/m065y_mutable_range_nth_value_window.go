package hatSql

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

var (
	ErrMutableIncrementalRangeNthValueWindowNil             = errors.New("mutable incremental RANGE NTH_VALUE window is nil")
	ErrMutableIncrementalRangeNthValueWindowMutationInvalid = errors.New("mutable incremental RANGE NTH_VALUE window mutation is invalid")
	ErrMutableIncrementalRangeNthValueWindowKeyRequired     = errors.New("mutable incremental RANGE NTH_VALUE window mutation key is required")
	ErrMutableIncrementalRangeNthValueWindowRowRequired     = errors.New("mutable incremental RANGE NTH_VALUE window mutation row is required")
	ErrMutableIncrementalRangeNthValueWindowDuplicate       = errors.New("mutable incremental RANGE NTH_VALUE window row key already exists")
	ErrMutableIncrementalRangeNthValueWindowMissingKey      = errors.New("mutable incremental RANGE NTH_VALUE window row key does not exist")
	ErrMutableIncrementalRangeNthValueWindowKeyMismatch     = errors.New("mutable incremental RANGE NTH_VALUE window mutation key does not match row key")
)

// IncrementalRangeNthValueWindowMutationOperation identifies a mutable row operation.
type IncrementalRangeNthValueWindowMutationOperation uint8

const (
	IncrementalRangeNthValueWindowInsert IncrementalRangeNthValueWindowMutationOperation = iota + 1
	IncrementalRangeNthValueWindowUpdate
	IncrementalRangeNthValueWindowDelete
)

// IncrementalRangeNthValueWindowMutation describes one atomic NTH_VALUE mutation.
// Insert and update operations require Row; update and delete operations require Key.
type IncrementalRangeNthValueWindowMutation struct {
	Operation IncrementalRangeNthValueWindowMutationOperation
	Key       string
	Row       Row
}

type mutableIncrementalRangeNthValueWindowEntry struct {
	key       string
	partition string
	order     int64
	value     interface{}
	row       Row
}

// MutableIncrementalRangeNthValueWindow maintains exact mutable numeric RANGE
// NTH_VALUE output. It is opt-in; the append-only constructor remains the
// default path.
type MutableIncrementalRangeNthValueWindow struct {
	definition IncrementalRangeNthValueWindowDefinition
	entries    map[string]mutableIncrementalRangeNthValueWindowEntry
	outputs    map[string]Row
}

// NewMutableIncrementalRangeNthValueWindow creates an empty mutable RANGE
// NTH_VALUE maintainer.
func NewMutableIncrementalRangeNthValueWindow(definition IncrementalRangeNthValueWindowDefinition) (*MutableIncrementalRangeNthValueWindow, error) {
	if _, err := NewIncrementalRangeNthValueWindow(definition); err != nil {
		return nil, err
	}
	definition.OutputColumn = strings.TrimSpace(definition.OutputColumn)
	return &MutableIncrementalRangeNthValueWindow{
		definition: definition,
		entries:    make(map[string]mutableIncrementalRangeNthValueWindowEntry),
		outputs:    make(map[string]Row),
	}, nil
}

// Apply validates and publishes a complete mutation batch atomically. Only
// partitions touched by the batch are rebuilt through the existing exact
// append-only NTH_VALUE evaluator.
func (window *MutableIncrementalRangeNthValueWindow) Apply(mutations []IncrementalRangeNthValueWindowMutation) ([]DifferentialRow, error) {
	if window == nil {
		return nil, ErrMutableIncrementalRangeNthValueWindowNil
	}
	if len(mutations) == 0 {
		return nil, nil
	}
	if len(mutations) == 1 && mutations[0].Operation == IncrementalRangeNthValueWindowUpdate {
		changes, handled, err := window.applyStableMutableIncrementalRangeNthValueWindowUpdate(mutations[0])
		if handled || err != nil {
			return changes, err
		}
	}

	working := cloneMutableIncrementalRangeNthValueWindowEntries(window.entries)
	affected := make(map[string]struct{})
	for index, mutation := range mutations {
		switch mutation.Operation {
		case IncrementalRangeNthValueWindowInsert:
			entry, err := window.prepareMutableIncrementalRangeNthValueWindowEntry(mutation.Row)
			if err != nil {
				return nil, fmt.Errorf("mutable RANGE NTH_VALUE mutation %d: %w", index, err)
			}
			if mutation.Key != "" && mutation.Key != entry.key {
				return nil, fmt.Errorf("mutable RANGE NTH_VALUE mutation %d: %w", index, ErrMutableIncrementalRangeNthValueWindowKeyMismatch)
			}
			if _, exists := working[entry.key]; exists {
				return nil, fmt.Errorf("mutable RANGE NTH_VALUE mutation %d: %w", index, ErrMutableIncrementalRangeNthValueWindowDuplicate)
			}
			working[entry.key] = entry
			affected[entry.partition] = struct{}{}
		case IncrementalRangeNthValueWindowUpdate:
			if strings.TrimSpace(mutation.Key) == "" {
				return nil, fmt.Errorf("mutable RANGE NTH_VALUE mutation %d: %w", index, ErrMutableIncrementalRangeNthValueWindowKeyRequired)
			}
			old, exists := working[mutation.Key]
			if !exists {
				return nil, fmt.Errorf("mutable RANGE NTH_VALUE mutation %d: %w", index, ErrMutableIncrementalRangeNthValueWindowMissingKey)
			}
			entry, err := window.prepareMutableIncrementalRangeNthValueWindowEntry(mutation.Row)
			if err != nil {
				return nil, fmt.Errorf("mutable RANGE NTH_VALUE mutation %d: %w", index, err)
			}
			if entry.key != mutation.Key {
				return nil, fmt.Errorf("mutable RANGE NTH_VALUE mutation %d: %w", index, ErrMutableIncrementalRangeNthValueWindowKeyMismatch)
			}
			working[mutation.Key] = entry
			affected[old.partition] = struct{}{}
			affected[entry.partition] = struct{}{}
		case IncrementalRangeNthValueWindowDelete:
			if strings.TrimSpace(mutation.Key) == "" {
				return nil, fmt.Errorf("mutable RANGE NTH_VALUE mutation %d: %w", index, ErrMutableIncrementalRangeNthValueWindowKeyRequired)
			}
			entry, exists := working[mutation.Key]
			if !exists {
				return nil, fmt.Errorf("mutable RANGE NTH_VALUE mutation %d: %w", index, ErrMutableIncrementalRangeNthValueWindowMissingKey)
			}
			delete(working, mutation.Key)
			affected[entry.partition] = struct{}{}
		default:
			return nil, fmt.Errorf("mutable RANGE NTH_VALUE mutation %d: %w", index, ErrMutableIncrementalRangeNthValueWindowMutationInvalid)
		}
	}

	newOutputs := cloneMutableIncrementalRangeNthValueWindowOutputs(window.outputs)
	for key, entry := range window.entries {
		if _, ok := affected[entry.partition]; ok {
			delete(newOutputs, key)
		}
	}
	entriesByPartition := make(map[string][]mutableIncrementalRangeNthValueWindowEntry)
	for _, entry := range working {
		if _, ok := affected[entry.partition]; ok {
			entriesByPartition[entry.partition] = append(entriesByPartition[entry.partition], entry)
		}
	}
	partitions := make([]string, 0, len(affected))
	for partition := range affected {
		partitions = append(partitions, partition)
	}
	sort.Strings(partitions)
	for _, partition := range partitions {
		outputs, err := window.rebuildMutableIncrementalRangeNthValueWindowPartition(entriesByPartition[partition])
		if err != nil {
			return nil, err
		}
		for key, row := range outputs {
			newOutputs[key] = row
		}
	}

	changes := diffMutableIncrementalRangeNthValueWindowOutputs(window.outputs, newOutputs)
	window.entries = working
	window.outputs = newOutputs
	return changes, nil
}

func (window *MutableIncrementalRangeNthValueWindow) prepareMutableIncrementalRangeNthValueWindowEntry(row Row) (mutableIncrementalRangeNthValueWindowEntry, error) {
	if row == nil {
		return mutableIncrementalRangeNthValueWindowEntry{}, ErrMutableIncrementalRangeNthValueWindowRowRequired
	}
	partition := ""
	if window.definition.PartitionKey != nil {
		value, err := window.definition.PartitionKey(row)
		if err != nil {
			return mutableIncrementalRangeNthValueWindowEntry{}, fmt.Errorf("partition key: %w", err)
		}
		partition = value
	}
	orderValue, err := window.definition.OrderKey(row)
	if err != nil {
		return mutableIncrementalRangeNthValueWindowEntry{}, fmt.Errorf("order key: %w", err)
	}
	order, ok := orderValue.(int64)
	if !ok {
		return mutableIncrementalRangeNthValueWindowEntry{}, ErrIncrementalRangeNthValueWindowOrderInvalid
	}
	key, err := window.definition.RowKey(row)
	if err != nil {
		return mutableIncrementalRangeNthValueWindowEntry{}, fmt.Errorf("row key: %w", err)
	}
	if strings.TrimSpace(key) == "" {
		return mutableIncrementalRangeNthValueWindowEntry{}, ErrMutableIncrementalRangeNthValueWindowKeyRequired
	}
	value, err := window.definition.ValueKey(row)
	if err != nil {
		return mutableIncrementalRangeNthValueWindowEntry{}, fmt.Errorf("value key: %w", err)
	}
	return mutableIncrementalRangeNthValueWindowEntry{
		key:       key,
		partition: partition,
		order:     order,
		value:     value,
		row:       cloneMutableIncrementalRangeNthValueWindowRow(row),
	}, nil
}

func (window *MutableIncrementalRangeNthValueWindow) applyStableMutableIncrementalRangeNthValueWindowUpdate(mutation IncrementalRangeNthValueWindowMutation) ([]DifferentialRow, bool, error) {
	if strings.TrimSpace(mutation.Key) == "" {
		return nil, true, ErrMutableIncrementalRangeNthValueWindowKeyRequired
	}
	oldEntry, exists := window.entries[mutation.Key]
	if !exists {
		return nil, true, ErrMutableIncrementalRangeNthValueWindowMissingKey
	}
	newEntry, err := window.prepareMutableIncrementalRangeNthValueWindowEntry(mutation.Row)
	if err != nil {
		return nil, true, err
	}
	if newEntry.key != mutation.Key {
		return nil, true, ErrMutableIncrementalRangeNthValueWindowKeyMismatch
	}
	if oldEntry.partition != newEntry.partition || oldEntry.order != newEntry.order {
		return nil, false, nil
	}

	entries := make([]mutableIncrementalRangeNthValueWindowEntry, 0)
	for _, entry := range window.entries {
		if entry.partition != oldEntry.partition {
			continue
		}
		if entry.key == mutation.Key {
			entry = newEntry
		}
		entries = append(entries, entry)
	}
	sort.SliceStable(entries, func(left, right int) bool {
		if entries[left].order != entries[right].order {
			if window.definition.Descending {
				return entries[left].order > entries[right].order
			}
			return entries[left].order < entries[right].order
		}
		return entries[left].key < entries[right].key
	})

	updatedOutputs, changes := window.stableMutableIncrementalRangeNthValueWindowOutputs(entries, mutation.Key)
	window.entries[mutation.Key] = newEntry
	for key, row := range updatedOutputs {
		window.outputs[key] = row
	}
	return changes, true, nil
}

func (window *MutableIncrementalRangeNthValueWindow) stableMutableIncrementalRangeNthValueWindowOutputs(entries []mutableIncrementalRangeNthValueWindowEntry, updatedKey string) (map[string]Row, []DifferentialRow) {
	updatedOutputs := make(map[string]Row)
	changes := make([]DifferentialRow, 0)
	frameStart := 0
	for index := 0; index < len(entries); {
		peerEnd := index
		for peerEnd+1 < len(entries) && entries[peerEnd+1].order == entries[index].order {
			peerEnd++
		}
		order := entries[index].order
		if window.definition.Descending {
			upper := incrementalRangeNthValueWindowUpperBound(order, window.definition.FramePreceding)
			for frameStart < len(entries) && entries[frameStart].order > upper {
				frameStart++
			}
		} else {
			lower := incrementalRangeNthValueWindowLowerBound(order, window.definition.FramePreceding)
			for frameStart < len(entries) && entries[frameStart].order < lower {
				frameStart++
			}
		}
		value := interface{}(nil)
		valueIndex := frameStart + window.definition.Position - 1
		if valueIndex <= peerEnd {
			value = entries[valueIndex].value
		}
		for peerIndex := index; peerIndex <= peerEnd; peerIndex++ {
			window.appendStableMutableIncrementalRangeNthValueWindowOutput(entries[peerIndex], value, updatedKey, updatedOutputs, &changes)
		}
		index = peerEnd + 1
	}
	return updatedOutputs, changes
}

func (window *MutableIncrementalRangeNthValueWindow) appendStableMutableIncrementalRangeNthValueWindowOutput(entry mutableIncrementalRangeNthValueWindowEntry, value interface{}, updatedKey string, updatedOutputs map[string]Row, changes *[]DifferentialRow) {
	oldRow, oldOK := window.outputs[entry.key]
	if entry.key != updatedKey && oldOK {
		oldValue, valueOK := oldRow[window.definition.OutputColumn]
		if valueOK && reflect.DeepEqual(oldValue, value) {
			return
		}
	}
	newRow := cloneMutableIncrementalRangeNthValueWindowRow(entry.row)
	newRow[window.definition.OutputColumn] = value
	if oldOK && reflect.DeepEqual(oldRow, newRow) {
		return
	}
	if oldOK {
		*changes = append(*changes, DifferentialRow{Key: entry.key, Diff: -1, Row: cloneMutableIncrementalRangeNthValueWindowRow(oldRow)})
	}
	*changes = append(*changes, DifferentialRow{Key: entry.key, Diff: 1, Row: cloneMutableIncrementalRangeNthValueWindowRow(newRow)})
	updatedOutputs[entry.key] = newRow
}

func (window *MutableIncrementalRangeNthValueWindow) rebuildMutableIncrementalRangeNthValueWindowPartition(entries []mutableIncrementalRangeNthValueWindowEntry) (map[string]Row, error) {
	sort.SliceStable(entries, func(left, right int) bool {
		if entries[left].order != entries[right].order {
			if window.definition.Descending {
				return entries[left].order > entries[right].order
			}
			return entries[left].order < entries[right].order
		}
		return entries[left].key < entries[right].key
	})
	rows := make([]Row, 0, len(entries))
	for _, entry := range entries {
		rows = append(rows, cloneMutableIncrementalRangeNthValueWindowRow(entry.row))
	}
	appendWindow, err := NewIncrementalRangeNthValueWindow(window.definition)
	if err != nil {
		return nil, err
	}
	changes, err := appendWindow.Append(rows)
	if err != nil {
		return nil, err
	}
	outputs := make(map[string]Row, len(entries))
	for _, change := range changes {
		if change.Diff > 0 {
			outputs[change.Key] = cloneMutableIncrementalRangeNthValueWindowRow(change.Row)
		} else if change.Diff < 0 {
			delete(outputs, change.Key)
		}
	}
	return outputs, nil
}

func cloneMutableIncrementalRangeNthValueWindowEntries(source map[string]mutableIncrementalRangeNthValueWindowEntry) map[string]mutableIncrementalRangeNthValueWindowEntry {
	clone := make(map[string]mutableIncrementalRangeNthValueWindowEntry, len(source))
	for key, entry := range source {
		entry.row = cloneMutableIncrementalRangeNthValueWindowRow(entry.row)
		clone[key] = entry
	}
	return clone
}

func cloneMutableIncrementalRangeNthValueWindowOutputs(source map[string]Row) map[string]Row {
	clone := make(map[string]Row, len(source))
	for key, row := range source {
		clone[key] = cloneMutableIncrementalRangeNthValueWindowRow(row)
	}
	return clone
}

func cloneMutableIncrementalRangeNthValueWindowRow(row Row) Row {
	if row == nil {
		return nil
	}
	clone := make(Row, len(row))
	for key, value := range row {
		clone[key] = value
	}
	return clone
}

func diffMutableIncrementalRangeNthValueWindowOutputs(oldOutputs, newOutputs map[string]Row) []DifferentialRow {
	keys := make(map[string]struct{}, len(oldOutputs)+len(newOutputs))
	for key := range oldOutputs {
		keys[key] = struct{}{}
	}
	for key := range newOutputs {
		keys[key] = struct{}{}
	}
	orderedKeys := make([]string, 0, len(keys))
	for key := range keys {
		orderedKeys = append(orderedKeys, key)
	}
	sort.Strings(orderedKeys)
	changes := make([]DifferentialRow, 0)
	for _, key := range orderedKeys {
		oldRow, oldOK := oldOutputs[key]
		newRow, newOK := newOutputs[key]
		if oldOK && (!newOK || !reflect.DeepEqual(oldRow, newRow)) {
			changes = append(changes, DifferentialRow{Key: key, Diff: -1, Row: cloneMutableIncrementalRangeNthValueWindowRow(oldRow)})
		}
		if newOK && (!oldOK || !reflect.DeepEqual(oldRow, newRow)) {
			changes = append(changes, DifferentialRow{Key: key, Diff: 1, Row: cloneMutableIncrementalRangeNthValueWindowRow(newRow)})
		}
	}
	return changes
}
