package hatSql

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

var (
	ErrMutableIncrementalRangeBoundaryWindowNil             = errors.New("mutable incremental RANGE boundary window is nil")
	ErrMutableIncrementalRangeBoundaryWindowMutationInvalid = errors.New("mutable incremental RANGE boundary window mutation is invalid")
	ErrMutableIncrementalRangeBoundaryWindowKeyRequired     = errors.New("mutable incremental RANGE boundary window mutation key is required")
	ErrMutableIncrementalRangeBoundaryWindowRowRequired     = errors.New("mutable incremental RANGE boundary window mutation row is required")
	ErrMutableIncrementalRangeBoundaryWindowDuplicate       = errors.New("mutable incremental RANGE boundary window row key already exists")
	ErrMutableIncrementalRangeBoundaryWindowMissingKey      = errors.New("mutable incremental RANGE boundary window row key does not exist")
	ErrMutableIncrementalRangeBoundaryWindowKeyMismatch     = errors.New("mutable incremental RANGE boundary window mutation key does not match row key")
)

// IncrementalRangeBoundaryWindowMutationOperation identifies a mutable row operation.
type IncrementalRangeBoundaryWindowMutationOperation uint8

const (
	IncrementalRangeBoundaryWindowInsert IncrementalRangeBoundaryWindowMutationOperation = iota + 1
	IncrementalRangeBoundaryWindowUpdate
	IncrementalRangeBoundaryWindowDelete
)

// IncrementalRangeBoundaryWindowMutation describes one atomic boundary-window mutation.
// Insert and update operations require Row; update and delete operations require Key.
type IncrementalRangeBoundaryWindowMutation struct {
	Operation IncrementalRangeBoundaryWindowMutationOperation
	Key       string
	Row       Row
}

type mutableIncrementalRangeBoundaryWindowEntry struct {
	key       string
	partition string
	order     int64
	value     interface{}
	row       Row
}

// MutableIncrementalRangeBoundaryWindow maintains exact mutable RANGE
// FIRST_VALUE/LAST_VALUE output. It is opt-in; the append-only constructor
// remains the default path.
type MutableIncrementalRangeBoundaryWindow struct {
	definition IncrementalRangeBoundaryWindowDefinition
	entries    map[string]mutableIncrementalRangeBoundaryWindowEntry
	outputs    map[string]Row
}

// NewMutableIncrementalRangeBoundaryWindow creates an empty mutable RANGE
// FIRST_VALUE/LAST_VALUE maintainer.
func NewMutableIncrementalRangeBoundaryWindow(definition IncrementalRangeBoundaryWindowDefinition) (*MutableIncrementalRangeBoundaryWindow, error) {
	if _, err := NewIncrementalRangeBoundaryWindow(definition); err != nil {
		return nil, err
	}
	definition.OutputColumn = strings.TrimSpace(definition.OutputColumn)
	return &MutableIncrementalRangeBoundaryWindow{
		definition: definition,
		entries:    make(map[string]mutableIncrementalRangeBoundaryWindowEntry),
		outputs:    make(map[string]Row),
	}, nil
}

// Apply validates and publishes a complete mutation batch atomically. Only
// partitions touched by the batch are rebuilt through the existing exact
// append-only boundary evaluator.
func (window *MutableIncrementalRangeBoundaryWindow) Apply(mutations []IncrementalRangeBoundaryWindowMutation) ([]DifferentialRow, error) {
	if window == nil {
		return nil, ErrMutableIncrementalRangeBoundaryWindowNil
	}
	if len(mutations) == 0 {
		return nil, nil
	}
	if len(mutations) > 0 {
		changes, handled, err := window.applyStableMutableIncrementalRangeBoundaryWindowUpdates(mutations)
		if handled || err != nil {
			return changes, err
		}
	}

	working := cloneMutableIncrementalRangeBoundaryWindowEntries(window.entries)
	affected := make(map[string]struct{})
	for index, mutation := range mutations {
		switch mutation.Operation {
		case IncrementalRangeBoundaryWindowInsert:
			entry, err := window.prepareMutableIncrementalRangeBoundaryWindowEntry(mutation.Row)
			if err != nil {
				return nil, fmt.Errorf("mutable RANGE boundary mutation %d: %w", index, err)
			}
			if mutation.Key != "" && mutation.Key != entry.key {
				return nil, fmt.Errorf("mutable RANGE boundary mutation %d: %w", index, ErrMutableIncrementalRangeBoundaryWindowKeyMismatch)
			}
			if _, exists := working[entry.key]; exists {
				return nil, fmt.Errorf("mutable RANGE boundary mutation %d: %w", index, ErrMutableIncrementalRangeBoundaryWindowDuplicate)
			}
			working[entry.key] = entry
			affected[entry.partition] = struct{}{}
		case IncrementalRangeBoundaryWindowUpdate:
			if strings.TrimSpace(mutation.Key) == "" {
				return nil, fmt.Errorf("mutable RANGE boundary mutation %d: %w", index, ErrMutableIncrementalRangeBoundaryWindowKeyRequired)
			}
			old, exists := working[mutation.Key]
			if !exists {
				return nil, fmt.Errorf("mutable RANGE boundary mutation %d: %w", index, ErrMutableIncrementalRangeBoundaryWindowMissingKey)
			}
			entry, err := window.prepareMutableIncrementalRangeBoundaryWindowEntry(mutation.Row)
			if err != nil {
				return nil, fmt.Errorf("mutable RANGE boundary mutation %d: %w", index, err)
			}
			if entry.key != mutation.Key {
				return nil, fmt.Errorf("mutable RANGE boundary mutation %d: %w", index, ErrMutableIncrementalRangeBoundaryWindowKeyMismatch)
			}
			working[mutation.Key] = entry
			affected[old.partition] = struct{}{}
			affected[entry.partition] = struct{}{}
		case IncrementalRangeBoundaryWindowDelete:
			if strings.TrimSpace(mutation.Key) == "" {
				return nil, fmt.Errorf("mutable RANGE boundary mutation %d: %w", index, ErrMutableIncrementalRangeBoundaryWindowKeyRequired)
			}
			entry, exists := working[mutation.Key]
			if !exists {
				return nil, fmt.Errorf("mutable RANGE boundary mutation %d: %w", index, ErrMutableIncrementalRangeBoundaryWindowMissingKey)
			}
			delete(working, mutation.Key)
			affected[entry.partition] = struct{}{}
		default:
			return nil, fmt.Errorf("mutable RANGE boundary mutation %d: %w", index, ErrMutableIncrementalRangeBoundaryWindowMutationInvalid)
		}
	}

	newOutputs := cloneMutableIncrementalRangeBoundaryWindowOutputs(window.outputs)
	for key, entry := range window.entries {
		if _, ok := affected[entry.partition]; ok {
			delete(newOutputs, key)
		}
	}
	entriesByPartition := make(map[string][]mutableIncrementalRangeBoundaryWindowEntry)
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
		outputs, err := window.rebuildMutableIncrementalRangeBoundaryWindowPartition(entriesByPartition[partition])
		if err != nil {
			return nil, err
		}
		for key, row := range outputs {
			newOutputs[key] = row
		}
	}

	changes := diffMutableIncrementalRangeBoundaryWindowOutputs(window.outputs, newOutputs)
	window.entries = working
	window.outputs = newOutputs
	return changes, nil
}

func (window *MutableIncrementalRangeBoundaryWindow) prepareMutableIncrementalRangeBoundaryWindowEntry(row Row) (mutableIncrementalRangeBoundaryWindowEntry, error) {
	if row == nil {
		return mutableIncrementalRangeBoundaryWindowEntry{}, ErrMutableIncrementalRangeBoundaryWindowRowRequired
	}
	partition := ""
	if window.definition.PartitionKey != nil {
		value, err := window.definition.PartitionKey(row)
		if err != nil {
			return mutableIncrementalRangeBoundaryWindowEntry{}, fmt.Errorf("partition key: %w", err)
		}
		partition = value
	}
	orderValue, err := window.definition.OrderKey(row)
	if err != nil {
		return mutableIncrementalRangeBoundaryWindowEntry{}, fmt.Errorf("order key: %w", err)
	}
	order, ok := orderValue.(int64)
	if !ok {
		return mutableIncrementalRangeBoundaryWindowEntry{}, ErrIncrementalRangeBoundaryWindowOrderInvalid
	}
	key, err := window.definition.RowKey(row)
	if err != nil {
		return mutableIncrementalRangeBoundaryWindowEntry{}, fmt.Errorf("row key: %w", err)
	}
	if strings.TrimSpace(key) == "" {
		return mutableIncrementalRangeBoundaryWindowEntry{}, ErrMutableIncrementalRangeBoundaryWindowKeyRequired
	}
	value, err := window.definition.ValueKey(row)
	if err != nil {
		return mutableIncrementalRangeBoundaryWindowEntry{}, fmt.Errorf("value key: %w", err)
	}
	return mutableIncrementalRangeBoundaryWindowEntry{
		key:       key,
		partition: partition,
		order:     order,
		value:     value,
		row:       cloneMutableIncrementalRangeBoundaryWindowRow(row),
	}, nil
}

func (window *MutableIncrementalRangeBoundaryWindow) applyStableMutableIncrementalRangeBoundaryWindowUpdates(mutations []IncrementalRangeBoundaryWindowMutation) ([]DifferentialRow, bool, error) {
	if len(mutations) == 0 {
		return nil, false, nil
	}
	prepared := make(map[string]mutableIncrementalRangeBoundaryWindowEntry, len(mutations))
	partition := ""
	partitionSet := false
	for _, mutation := range mutations {
		if mutation.Operation != IncrementalRangeBoundaryWindowUpdate {
			return nil, false, nil
		}
		if strings.TrimSpace(mutation.Key) == "" {
			return nil, true, ErrMutableIncrementalRangeBoundaryWindowKeyRequired
		}
		oldEntry, exists := window.entries[mutation.Key]
		if !exists {
			return nil, true, ErrMutableIncrementalRangeBoundaryWindowMissingKey
		}
		newEntry, err := window.prepareMutableIncrementalRangeBoundaryWindowEntry(mutation.Row)
		if err != nil {
			return nil, true, err
		}
		if newEntry.key != mutation.Key {
			return nil, true, ErrMutableIncrementalRangeBoundaryWindowKeyMismatch
		}
		if oldEntry.partition != newEntry.partition || oldEntry.order != newEntry.order {
			return nil, false, nil
		}
		if partitionSet && partition != oldEntry.partition {
			return nil, false, nil
		}
		if _, exists := prepared[mutation.Key]; exists {
			return nil, false, nil
		}
		partition = oldEntry.partition
		partitionSet = true
		prepared[mutation.Key] = newEntry
	}

	entries := make([]mutableIncrementalRangeBoundaryWindowEntry, 0)
	for _, entry := range window.entries {
		if entry.partition != partition {
			continue
		}
		if replacement, ok := prepared[entry.key]; ok {
			entry = replacement
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

	updatedOutputs, changes := window.stableMutableIncrementalRangeBoundaryWindowOutputs(entries, prepared)
	for key, entry := range prepared {
		window.entries[key] = entry
	}
	for key, row := range updatedOutputs {
		window.outputs[key] = row
	}
	return changes, true, nil
}

func (window *MutableIncrementalRangeBoundaryWindow) stableMutableIncrementalRangeBoundaryWindowOutputs(entries []mutableIncrementalRangeBoundaryWindowEntry, updatedKeys map[string]mutableIncrementalRangeBoundaryWindowEntry) (map[string]Row, []DifferentialRow) {
	updatedOutputs := make(map[string]Row)
	changes := make([]DifferentialRow, 0)
	frameStart := 0
	for index := 0; index < len(entries); {
		peerEnd := index
		for peerEnd+1 < len(entries) && entries[peerEnd+1].order == entries[index].order {
			peerEnd++
		}
		if window.definition.Kind == IncrementalRangeLastValue {
			value := entries[peerEnd].value
			for peerIndex := index; peerIndex <= peerEnd; peerIndex++ {
				window.appendStableMutableIncrementalRangeBoundaryWindowOutput(entries[peerIndex], value, updatedKeys, updatedOutputs, &changes)
			}
		} else {
			for peerIndex := index; peerIndex <= peerEnd; peerIndex++ {
				order := entries[peerIndex].order
				if window.definition.Descending {
					upper := incrementalRangeBoundaryWindowUpperBound(order, window.definition.FramePreceding)
					for frameStart < len(entries) && entries[frameStart].order > upper {
						frameStart++
					}
				} else {
					lower := incrementalRangeBoundaryWindowLowerBound(order, window.definition.FramePreceding)
					for frameStart < len(entries) && entries[frameStart].order < lower {
						frameStart++
					}
				}
				window.appendStableMutableIncrementalRangeBoundaryWindowOutput(entries[peerIndex], entries[frameStart].value, updatedKeys, updatedOutputs, &changes)
			}
		}
		index = peerEnd + 1
	}
	return updatedOutputs, changes
}

func (window *MutableIncrementalRangeBoundaryWindow) appendStableMutableIncrementalRangeBoundaryWindowOutput(entry mutableIncrementalRangeBoundaryWindowEntry, value interface{}, updatedKeys map[string]mutableIncrementalRangeBoundaryWindowEntry, updatedOutputs map[string]Row, changes *[]DifferentialRow) {
	oldRow, oldOK := window.outputs[entry.key]
	if _, updated := updatedKeys[entry.key]; !updated && oldOK {
		oldValue, valueOK := oldRow[window.definition.OutputColumn]
		if valueOK && reflect.DeepEqual(oldValue, value) {
			return
		}
	}
	newRow := cloneMutableIncrementalRangeBoundaryWindowRow(entry.row)
	newRow[window.definition.OutputColumn] = value
	if oldOK && reflect.DeepEqual(oldRow, newRow) {
		return
	}
	if oldOK {
		*changes = append(*changes, DifferentialRow{Key: entry.key, Diff: -1, Row: cloneMutableIncrementalRangeBoundaryWindowRow(oldRow)})
	}
	*changes = append(*changes, DifferentialRow{Key: entry.key, Diff: 1, Row: cloneMutableIncrementalRangeBoundaryWindowRow(newRow)})
	updatedOutputs[entry.key] = newRow
}

func (window *MutableIncrementalRangeBoundaryWindow) rebuildMutableIncrementalRangeBoundaryWindowPartition(entries []mutableIncrementalRangeBoundaryWindowEntry) (map[string]Row, error) {
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
		rows = append(rows, cloneMutableIncrementalRangeBoundaryWindowRow(entry.row))
	}
	appendWindow, err := NewIncrementalRangeBoundaryWindow(window.definition)
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
			outputs[change.Key] = cloneMutableIncrementalRangeBoundaryWindowRow(change.Row)
		} else if change.Diff < 0 {
			delete(outputs, change.Key)
		}
	}
	return outputs, nil
}

func cloneMutableIncrementalRangeBoundaryWindowEntries(source map[string]mutableIncrementalRangeBoundaryWindowEntry) map[string]mutableIncrementalRangeBoundaryWindowEntry {
	clone := make(map[string]mutableIncrementalRangeBoundaryWindowEntry, len(source))
	for key, entry := range source {
		entry.row = cloneMutableIncrementalRangeBoundaryWindowRow(entry.row)
		clone[key] = entry
	}
	return clone
}

func cloneMutableIncrementalRangeBoundaryWindowOutputs(source map[string]Row) map[string]Row {
	clone := make(map[string]Row, len(source))
	for key, row := range source {
		clone[key] = cloneMutableIncrementalRangeBoundaryWindowRow(row)
	}
	return clone
}

func cloneMutableIncrementalRangeBoundaryWindowRow(row Row) Row {
	if row == nil {
		return nil
	}
	clone := make(Row, len(row))
	for key, value := range row {
		clone[key] = value
	}
	return clone
}

func diffMutableIncrementalRangeBoundaryWindowOutputs(oldOutputs, newOutputs map[string]Row) []DifferentialRow {
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
			changes = append(changes, DifferentialRow{Key: key, Diff: -1, Row: cloneMutableIncrementalRangeBoundaryWindowRow(oldRow)})
		}
		if newOK && (!oldOK || !reflect.DeepEqual(oldRow, newRow)) {
			changes = append(changes, DifferentialRow{Key: key, Diff: 1, Row: cloneMutableIncrementalRangeBoundaryWindowRow(newRow)})
		}
	}
	return changes
}
