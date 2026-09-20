package hatSql

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
)

var (
	ErrMutableIncrementalRangeWindowMutationInvalid     = errors.New("mutable incremental range window mutation operation is invalid")
	ErrMutableIncrementalRangeWindowMutationKeyRequired = errors.New("mutable incremental range window mutation key is required")
	ErrMutableIncrementalRangeWindowMutationRowRequired = errors.New("mutable incremental range window mutation row is required")
	ErrMutableIncrementalRangeWindowMutationDuplicate   = errors.New("mutable incremental range window mutation key is duplicated")
	ErrMutableIncrementalRangeWindowMissingKey          = errors.New("mutable incremental range window mutation key is missing")
	ErrMutableIncrementalRangeWindowMutationKeyMismatch = errors.New("mutable incremental range window mutation key does not match row key")
	ErrMutableIncrementalRangeWindowDisabled            = errors.New("mutable incremental range window mutations are disabled")
)

// IncrementalRangeWindowMutationOperation identifies one mutable row
// operation. INSERT and UPDATE require Row; DELETE ignores Row.
type IncrementalRangeWindowMutationOperation uint8

const (
	IncrementalRangeWindowInsert IncrementalRangeWindowMutationOperation = iota + 1
	IncrementalRangeWindowUpdate
	IncrementalRangeWindowDelete
)

// IncrementalRangeWindowMutation changes one stable row identity. INSERT may
// omit Key and derive it from RowKey; UPDATE and DELETE require Key.
type IncrementalRangeWindowMutation struct {
	Operation IncrementalRangeWindowMutationOperation
	Key       string
	Row       Row
}

type mutableIncrementalRangeWindowEntry struct {
	key       string
	row       Row
	partition string
	order     int64
}

type preparedMutableIncrementalRangeWindowMutation struct {
	operation IncrementalRangeWindowMutationOperation
	key       string
	entry     mutableIncrementalRangeWindowEntry
	old       mutableIncrementalRangeWindowEntry
}

// MutableIncrementalRangeWindow retains base rows and current outputs so it
// can apply arbitrary row changes to numeric RANGE frames. The constructor is
// opt-in; NewIncrementalRangeWindow remains the append-only default.
type MutableIncrementalRangeWindow struct {
	kind         IncrementalRangeWindowKind
	outputColumn string
	partitionKey IncrementalWindowPartitionKeyFunc
	orderKey     IncrementalWindowOrderKeyFunc
	rowKey       IncrementalWindowRowKeyFunc
	valueKey     IncrementalOffsetWindowValueKeyFunc
	preceding    int64
	descending   bool
	rows         map[string]mutableIncrementalRangeWindowEntry
	partitions   map[string][]mutableIncrementalRangeWindowEntry
	outputs      map[string]Row
}

// NewMutableIncrementalRangeWindow creates a mutable numeric RANGE
// maintainer for every kind supported by IncrementalRangeWindow. It retains
// all rows, unlike the bounded append-only constructor.
func NewMutableIncrementalRangeWindow(definition IncrementalRangeWindowDefinition) (*MutableIncrementalRangeWindow, error) {
	appendWindow, err := NewIncrementalRangeWindow(definition)
	if err != nil {
		return nil, err
	}
	return &MutableIncrementalRangeWindow{
		kind:         appendWindow.kind,
		outputColumn: appendWindow.outputColumn,
		partitionKey: appendWindow.partitionKey,
		orderKey:     appendWindow.orderKey,
		rowKey:       appendWindow.rowKey,
		valueKey:     appendWindow.valueKey,
		preceding:    appendWindow.preceding,
		descending:   appendWindow.descending,
		rows:         make(map[string]mutableIncrementalRangeWindowEntry),
		partitions:   make(map[string][]mutableIncrementalRangeWindowEntry),
		outputs:      make(map[string]Row),
	}, nil
}

// Apply atomically applies row changes and returns exact differential
// retractions and insertions for every changed RANGE result.
func (window *MutableIncrementalRangeWindow) Apply(mutations []IncrementalRangeWindowMutation) ([]DifferentialRow, error) {
	if window == nil {
		return nil, ErrIncrementalRangeWindowNil
	}
	if window.rows == nil {
		return nil, ErrMutableIncrementalRangeWindowDisabled
	}
	if len(mutations) == 0 {
		return nil, nil
	}
	prepared, affected, err := window.prepareMutableIncrementalRangeWindowMutations(mutations)
	if err != nil {
		return nil, err
	}
	if window.kind == IncrementalRangeWindowCount && window.canApplyStableCountUpdates(prepared) {
		return window.applyStableCountUpdates(prepared, affected)
	}
	if (window.kind == IncrementalRangeWindowMinInt64 || window.kind == IncrementalRangeWindowMaxInt64) && window.canApplyStableExtremaUpdates(prepared) {
		return window.applyStableExtremaUpdates(prepared, affected)
	}
	if window.kind == IncrementalRangeWindowSumInt64 && window.canApplyStableSumUpdates(prepared) {
		return window.applyStableSumUpdates(prepared, affected)
	}
	return window.applyMutableIncrementalRangeWindowRebuild(prepared, affected)
}

func (window *MutableIncrementalRangeWindow) prepareMutableIncrementalRangeWindowMutations(mutations []IncrementalRangeWindowMutation) ([]preparedMutableIncrementalRangeWindowMutation, map[string]struct{}, error) {
	prepared := make([]preparedMutableIncrementalRangeWindowMutation, 0, len(mutations))
	affected := make(map[string]struct{}, len(mutations))
	seen := make(map[string]struct{}, len(mutations))
	for index, mutation := range mutations {
		switch mutation.Operation {
		case IncrementalRangeWindowInsert:
			if mutation.Row == nil {
				return nil, nil, fmt.Errorf("mutable incremental range window mutation %d: %w", index, ErrMutableIncrementalRangeWindowMutationRowRequired)
			}
			entry, err := window.prepareMutableIncrementalRangeWindowEntry(mutation.Row)
			if err != nil {
				return nil, nil, fmt.Errorf("mutable incremental range window mutation %d: %w", index, err)
			}
			if mutation.Key != "" && mutation.Key != entry.key {
				return nil, nil, fmt.Errorf("mutable incremental range window mutation %d: %w", index, ErrMutableIncrementalRangeWindowMutationKeyMismatch)
			}
			if _, exists := window.rows[entry.key]; exists {
				return nil, nil, fmt.Errorf("mutable incremental range window mutation %d key %q: %w", index, entry.key, ErrMutableIncrementalRangeWindowMutationDuplicate)
			}
			if _, exists := seen[entry.key]; exists {
				return nil, nil, fmt.Errorf("mutable incremental range window mutation %d key %q: %w", index, entry.key, ErrMutableIncrementalRangeWindowMutationDuplicate)
			}
			seen[entry.key] = struct{}{}
			prepared = append(prepared, preparedMutableIncrementalRangeWindowMutation{operation: mutation.Operation, key: entry.key, entry: entry})
			affected[entry.partition] = struct{}{}

		case IncrementalRangeWindowUpdate:
			if strings.TrimSpace(mutation.Key) == "" {
				return nil, nil, fmt.Errorf("mutable incremental range window mutation %d: %w", index, ErrMutableIncrementalRangeWindowMutationKeyRequired)
			}
			old, exists := window.rows[mutation.Key]
			if !exists {
				return nil, nil, fmt.Errorf("mutable incremental range window mutation %d key %q: %w", index, mutation.Key, ErrMutableIncrementalRangeWindowMissingKey)
			}
			if mutation.Row == nil {
				return nil, nil, fmt.Errorf("mutable incremental range window mutation %d: %w", index, ErrMutableIncrementalRangeWindowMutationRowRequired)
			}
			entry, err := window.prepareMutableIncrementalRangeWindowEntry(mutation.Row)
			if err != nil {
				return nil, nil, fmt.Errorf("mutable incremental range window mutation %d: %w", index, err)
			}
			if entry.key != mutation.Key {
				return nil, nil, fmt.Errorf("mutable incremental range window mutation %d: %w", index, ErrMutableIncrementalRangeWindowMutationKeyMismatch)
			}
			if _, exists := seen[mutation.Key]; exists {
				return nil, nil, fmt.Errorf("mutable incremental range window mutation %d key %q: %w", index, mutation.Key, ErrMutableIncrementalRangeWindowMutationDuplicate)
			}
			seen[mutation.Key] = struct{}{}
			prepared = append(prepared, preparedMutableIncrementalRangeWindowMutation{operation: mutation.Operation, key: mutation.Key, entry: entry, old: old})
			affected[old.partition] = struct{}{}
			affected[entry.partition] = struct{}{}

		case IncrementalRangeWindowDelete:
			if strings.TrimSpace(mutation.Key) == "" {
				return nil, nil, fmt.Errorf("mutable incremental range window mutation %d: %w", index, ErrMutableIncrementalRangeWindowMutationKeyRequired)
			}
			old, exists := window.rows[mutation.Key]
			if !exists {
				return nil, nil, fmt.Errorf("mutable incremental range window mutation %d key %q: %w", index, mutation.Key, ErrMutableIncrementalRangeWindowMissingKey)
			}
			if _, exists := seen[mutation.Key]; exists {
				return nil, nil, fmt.Errorf("mutable incremental range window mutation %d key %q: %w", index, mutation.Key, ErrMutableIncrementalRangeWindowMutationDuplicate)
			}
			seen[mutation.Key] = struct{}{}
			prepared = append(prepared, preparedMutableIncrementalRangeWindowMutation{operation: mutation.Operation, key: mutation.Key, old: old})
			affected[old.partition] = struct{}{}

		default:
			return nil, nil, fmt.Errorf("mutable incremental range window mutation %d: %w", index, ErrMutableIncrementalRangeWindowMutationInvalid)
		}
	}
	return prepared, affected, nil
}

func (window *MutableIncrementalRangeWindow) prepareMutableIncrementalRangeWindowEntry(row Row) (mutableIncrementalRangeWindowEntry, error) {
	row = cloneIncrementalRangeWindowRow(row)
	if _, exists := row[window.outputColumn]; exists {
		return mutableIncrementalRangeWindowEntry{}, ErrIncrementalRangeWindowOutputConflict
	}
	partition := ""
	if window.partitionKey != nil {
		value, err := window.partitionKey(row)
		if err != nil {
			return mutableIncrementalRangeWindowEntry{}, fmt.Errorf("partition key: %w", err)
		}
		partition = value
	}
	orderValue, err := window.orderKey(row)
	if err != nil {
		return mutableIncrementalRangeWindowEntry{}, fmt.Errorf("order key: %w", err)
	}
	order, ok := orderValue.(int64)
	if !ok {
		return mutableIncrementalRangeWindowEntry{}, ErrIncrementalRangeWindowOrderInvalid
	}
	key, err := window.rowKey(row)
	if err != nil {
		return mutableIncrementalRangeWindowEntry{}, fmt.Errorf("row key: %w", err)
	}
	if strings.TrimSpace(key) == "" {
		return mutableIncrementalRangeWindowEntry{}, ErrIncrementalRangeWindowRowKeyRequired
	}
	return mutableIncrementalRangeWindowEntry{key: key, row: row, partition: partition, order: order}, nil
}

func (window *MutableIncrementalRangeWindow) canApplyStableCountUpdates(prepared []preparedMutableIncrementalRangeWindowMutation) bool {
	if len(prepared) == 0 {
		return false
	}
	for _, mutation := range prepared {
		if mutation.operation != IncrementalRangeWindowUpdate || mutation.old.partition != mutation.entry.partition || mutation.old.order != mutation.entry.order {
			return false
		}
	}
	return true
}

func (window *MutableIncrementalRangeWindow) applyStableCountUpdates(prepared []preparedMutableIncrementalRangeWindowMutation, affected map[string]struct{}) ([]DifferentialRow, error) {
	working := make(map[string][]mutableIncrementalRangeWindowEntry, len(affected))
	changedKeys := make(map[string]struct{}, len(prepared))
	for partition := range affected {
		working[partition] = append([]mutableIncrementalRangeWindowEntry(nil), window.partitions[partition]...)
	}
	for _, mutation := range prepared {
		entries := working[mutation.entry.partition]
		index := mutableIncrementalRangeWindowEntryIndex(entries, mutation.key)
		if index < 0 {
			return nil, fmt.Errorf("mutable incremental range window stable update key %q is missing", mutation.key)
		}
		entries[index] = mutation.entry
		working[mutation.entry.partition] = entries
		changedKeys[mutation.key] = struct{}{}
	}
	newOutputs := make(map[string]Row, len(changedKeys))
	for key := range changedKeys {
		old, exists := window.rows[key]
		if !exists {
			return nil, fmt.Errorf("mutable incremental range window stable update key %q is missing", key)
		}
		entries := working[old.partition]
		index := mutableIncrementalRangeWindowEntryIndex(entries, key)
		if index < 0 {
			return nil, fmt.Errorf("mutable incremental range window stable update key %q is missing", key)
		}
		oldOutput, exists := window.outputs[key]
		if !exists {
			return nil, fmt.Errorf("mutable incremental range window output %q is missing", key)
		}
		newOutputs[key] = incrementalRangeWindowOutput(entries[index].row, window.outputColumn, oldOutput[window.outputColumn])
	}
	updates := diffMutableIncrementalRangeWindowChanges(window.outputs, newOutputs, changedKeys)
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

func (window *MutableIncrementalRangeWindow) canApplyStableExtremaUpdates(prepared []preparedMutableIncrementalRangeWindowMutation) bool {
	if len(prepared) == 0 {
		return false
	}
	partition := ""
	partitionSet := false
	for _, mutation := range prepared {
		if mutation.operation != IncrementalRangeWindowUpdate || mutation.old.partition != mutation.entry.partition || mutation.old.order != mutation.entry.order {
			return false
		}
		if partitionSet && partition != mutation.old.partition {
			return false
		}
		partition = mutation.old.partition
		partitionSet = true
	}
	return true
}

func (window *MutableIncrementalRangeWindow) applyStableExtremaUpdates(prepared []preparedMutableIncrementalRangeWindowMutation, affected map[string]struct{}) ([]DifferentialRow, error) {
	partition := prepared[0].old.partition
	original := window.partitions[partition]
	entries := append([]mutableIncrementalRangeWindowEntry(nil), original...)
	updatedKeys := make(map[string]struct{}, len(prepared))
	for _, mutation := range prepared {
		index := mutableIncrementalRangeWindowEntryIndex(entries, mutation.key)
		if index < 0 {
			return nil, fmt.Errorf("mutable incremental range window stable update key %q is missing", mutation.key)
		}
		entries[index] = mutation.entry
		updatedKeys[mutation.key] = struct{}{}
	}

	values := make([]int64, len(entries))
	valid := make([]bool, len(entries))
	for index, entry := range entries {
		value, ok, err := mutableIncrementalRangeWindowExtremaValue(window.valueKey, entry.row)
		if err != nil {
			return nil, err
		}
		values[index] = value
		valid[index] = ok
	}

	minimum := window.kind == IncrementalRangeWindowMinInt64
	monotonic := make([]int, 0, len(entries))
	monotonicHead := 0
	frameStart := 0
	changedKeys := make(map[string]struct{})
	newOutputs := make(map[string]Row)
	for index := 0; index < len(entries); {
		peerEnd := index
		for peerEnd+1 < len(entries) && entries[peerEnd+1].order == entries[index].order {
			peerEnd++
		}
		order := entries[index].order
		if window.descending {
			upper := incrementalRangeWindowUpperBound(order, window.preceding)
			for frameStart < len(entries) && entries[frameStart].order > upper {
				frameStart++
			}
		} else {
			lower := incrementalRangeWindowLowerBound(order, window.preceding)
			for frameStart < len(entries) && entries[frameStart].order < lower {
				frameStart++
			}
		}
		for monotonicHead < len(monotonic) && monotonic[monotonicHead] < frameStart {
			monotonicHead++
		}
		for peerIndex := index; peerIndex <= peerEnd; peerIndex++ {
			if !valid[peerIndex] {
				continue
			}
			value := values[peerIndex]
			for len(monotonic) > monotonicHead {
				last := monotonic[len(monotonic)-1]
				if (minimum && values[last] < value) || (!minimum && values[last] > value) {
					break
				}
				monotonic = monotonic[:len(monotonic)-1]
			}
			monotonic = append(monotonic, peerIndex)
		}
		var output interface{}
		if monotonicHead < len(monotonic) {
			output = values[monotonic[monotonicHead]]
		}
		for peerIndex := index; peerIndex <= peerEnd; peerIndex++ {
			entry := entries[peerIndex]
			oldRow, oldOK := window.outputs[entry.key]
			if !oldOK {
				return nil, fmt.Errorf("mutable incremental range window output %q is missing", entry.key)
			}
			if _, updated := updatedKeys[entry.key]; !updated {
				oldValue, valueOK := oldRow[window.outputColumn]
				if valueOK && reflect.DeepEqual(oldValue, output) {
					continue
				}
			}
			newRow := incrementalRangeWindowOutput(entry.row, window.outputColumn, output)
			if reflect.DeepEqual(oldRow, newRow) {
				continue
			}
			changedKeys[entry.key] = struct{}{}
			newOutputs[entry.key] = newRow
		}
		index = peerEnd + 1
	}

	updates := diffMutableIncrementalRangeWindowChanges(window.outputs, newOutputs, changedKeys)
	window.partitions[partition] = entries
	for _, mutation := range prepared {
		window.rows[mutation.key] = mutation.entry
	}
	for key, output := range newOutputs {
		window.outputs[key] = output
	}
	return updates, nil
}

func mutableIncrementalRangeWindowExtremaValue(valueKey IncrementalOffsetWindowValueKeyFunc, row Row) (int64, bool, error) {
	value, err := valueKey(row)
	if err != nil {
		return 0, false, err
	}
	if value == nil {
		return 0, false, nil
	}
	intValue, ok := value.(int64)
	if !ok {
		return 0, false, ErrIncrementalRangeWindowExtremaValueInvalid
	}
	return intValue, true, nil
}

func (window *MutableIncrementalRangeWindow) canApplyStableSumUpdates(prepared []preparedMutableIncrementalRangeWindowMutation) bool {
	if len(prepared) == 0 {
		return false
	}
	for _, mutation := range prepared {
		if mutation.operation != IncrementalRangeWindowUpdate || mutation.old.partition != mutation.entry.partition || mutation.old.order != mutation.entry.order {
			return false
		}
	}
	return true
}

// A same-position SUM batch changes only rows whose RANGE contains one of the
// changed orders. Reusing each current sum avoids rebuilding peer snapshots or
// materializing unaffected rows.
func (window *MutableIncrementalRangeWindow) applyStableSumUpdates(prepared []preparedMutableIncrementalRangeWindowMutation, affected map[string]struct{}) ([]DifferentialRow, error) {
	type stableSumUpdate struct {
		mutation preparedMutableIncrementalRangeWindowMutation
		oldValue int64
		newValue int64
		valueSet bool
	}
	stableUpdates := make([]stableSumUpdate, len(prepared))
	for index, mutation := range prepared {
		oldValue, oldValid, err := mutableIncrementalRangeWindowInt64Value(window.valueKey, mutation.old.row)
		if err != nil {
			return nil, err
		}
		newValue, newValid, err := mutableIncrementalRangeWindowInt64Value(window.valueKey, mutation.entry.row)
		if err != nil {
			return nil, err
		}
		if oldValid != newValid {
			return window.applyMutableIncrementalRangeWindowRebuild(prepared, affected)
		}
		stableUpdates[index] = stableSumUpdate{
			mutation: mutation,
			oldValue: oldValue,
			newValue: newValue,
			valueSet: oldValid,
		}
	}
	working := make(map[string][]mutableIncrementalRangeWindowEntry, len(affected))
	for partition := range affected {
		working[partition] = append([]mutableIncrementalRangeWindowEntry(nil), window.partitions[partition]...)
	}
	for _, update := range stableUpdates {
		entries := working[update.mutation.entry.partition]
		index := mutableIncrementalRangeWindowEntryIndex(entries, update.mutation.key)
		if index < 0 {
			return nil, fmt.Errorf("mutable incremental range window stable update key %q is missing", update.mutation.key)
		}
		entries[index] = update.mutation.entry
		working[update.mutation.entry.partition] = entries
	}

	changedKeys := make(map[string]struct{}, len(prepared)*2)
	newOutputs := make(map[string]Row)
	for partition, entries := range working {
		for _, entry := range entries {
			var derived interface{}
			applies := false
			for _, update := range stableUpdates {
				mutation := update.mutation
				if mutation.entry.partition != partition || !mutableIncrementalRangeWindowFrameContains(window, entry.order, mutation.entry.order) {
					continue
				}
				if !applies {
					oldOutput, exists := window.outputs[entry.key]
					if !exists {
						return nil, fmt.Errorf("mutable incremental range window output %q is missing", entry.key)
					}
					derived = oldOutput[window.outputColumn]
					applies = true
				}
				if !update.valueSet {
					continue
				}
				oldSum, ok := derived.(int64)
				if !ok {
					return window.applyMutableIncrementalRangeWindowRebuild(prepared, affected)
				}
				withoutOld, err := subtractIncrementalRangeWindowSum(oldSum, update.oldValue)
				if err != nil {
					return window.applyMutableIncrementalRangeWindowRebuild(prepared, affected)
				}
				derived, err = addIncrementalRangeWindowSum(withoutOld, update.newValue)
				if err != nil {
					return window.applyMutableIncrementalRangeWindowRebuild(prepared, affected)
				}
			}
			if !applies {
				continue
			}
			changedKeys[entry.key] = struct{}{}
			newOutputs[entry.key] = incrementalRangeWindowOutput(entry.row, window.outputColumn, derived)
		}
	}
	updates := diffMutableIncrementalRangeWindowChanges(window.outputs, newOutputs, changedKeys)
	for partition, entries := range working {
		window.partitions[partition] = entries
	}
	for _, update := range stableUpdates {
		window.rows[update.mutation.key] = update.mutation.entry
	}
	for key, output := range newOutputs {
		window.outputs[key] = output
	}
	return updates, nil
}

func mutableIncrementalRangeWindowInt64Value(valueKey IncrementalOffsetWindowValueKeyFunc, row Row) (int64, bool, error) {
	value, err := valueKey(row)
	if err != nil {
		return 0, false, err
	}
	if value == nil {
		return 0, false, nil
	}
	intValue, ok := value.(int64)
	if !ok {
		return 0, false, ErrIncrementalRangeWindowSumValueInvalid
	}
	return intValue, true, nil
}

func mutableIncrementalRangeWindowFrameContains(window *MutableIncrementalRangeWindow, rowOrder, changedOrder int64) bool {
	if window.descending {
		lower := changedOrder - window.preceding
		if window.preceding > 0 && changedOrder < math.MinInt64+window.preceding {
			lower = math.MinInt64
		}
		return rowOrder <= changedOrder && rowOrder >= lower
	}
	upper := changedOrder + window.preceding
	if window.preceding > 0 && changedOrder > math.MaxInt64-window.preceding {
		upper = math.MaxInt64
	}
	return rowOrder >= changedOrder && rowOrder <= upper
}

func (window *MutableIncrementalRangeWindow) applyMutableIncrementalRangeWindowRebuild(prepared []preparedMutableIncrementalRangeWindowMutation, affected map[string]struct{}) ([]DifferentialRow, error) {
	working := make(map[string][]mutableIncrementalRangeWindowEntry, len(affected))
	oldKeys := make(map[string]struct{})
	for partition := range affected {
		entries := append([]mutableIncrementalRangeWindowEntry(nil), window.partitions[partition]...)
		working[partition] = entries
		for _, entry := range entries {
			oldKeys[entry.key] = struct{}{}
		}
	}
	for _, mutation := range prepared {
		switch mutation.operation {
		case IncrementalRangeWindowInsert:
			working[mutation.entry.partition] = append(working[mutation.entry.partition], mutation.entry)
		case IncrementalRangeWindowUpdate:
			entries := working[mutation.old.partition]
			index := mutableIncrementalRangeWindowEntryIndex(entries, mutation.key)
			if index < 0 {
				return nil, fmt.Errorf("mutable incremental range window update key %q is missing", mutation.key)
			}
			working[mutation.old.partition] = append(entries[:index], entries[index+1:]...)
			working[mutation.entry.partition] = append(working[mutation.entry.partition], mutation.entry)
		case IncrementalRangeWindowDelete:
			entries := working[mutation.old.partition]
			index := mutableIncrementalRangeWindowEntryIndex(entries, mutation.key)
			if index < 0 {
				return nil, fmt.Errorf("mutable incremental range window delete key %q is missing", mutation.key)
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
			if entries[left].order != entries[right].order {
				if window.descending {
					return entries[left].order > entries[right].order
				}
				return entries[left].order < entries[right].order
			}
			return entries[left].key < entries[right].key
		})
		working[partition] = entries
		for _, entry := range entries {
			changedKeys[entry.key] = struct{}{}
		}
		outputs, err := window.rebuildMutableIncrementalRangeWindowPartition(entries)
		if err != nil {
			return nil, fmt.Errorf("mutable incremental range window partition %q: %w", partition, err)
		}
		for key, output := range outputs {
			newOutputs[key] = output
		}
	}
	updates := diffMutableIncrementalRangeWindowChanges(window.outputs, newOutputs, changedKeys)

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

func (window *MutableIncrementalRangeWindow) rebuildMutableIncrementalRangeWindowPartition(entries []mutableIncrementalRangeWindowEntry) (map[string]Row, error) {
	rows := make([]Row, len(entries))
	for index, entry := range entries {
		rows[index] = entry.row
	}
	appendWindow, err := NewIncrementalRangeWindow(IncrementalRangeWindowDefinition{
		Kind:           window.kind,
		OutputColumn:   window.outputColumn,
		PartitionKey:   window.partitionKey,
		OrderKey:       window.orderKey,
		RowKey:         window.rowKey,
		ValueKey:       window.valueKey,
		FramePreceding: window.preceding,
		Descending:     window.descending,
	})
	if err != nil {
		return nil, err
	}
	updates, err := appendWindow.Append(rows)
	if err != nil {
		return nil, err
	}
	outputs := make(map[string]Row, len(entries))
	for _, update := range updates {
		if update.Diff < 0 {
			delete(outputs, update.Key)
			continue
		}
		if update.Diff > 0 {
			outputs[update.Key] = cloneIncrementalRangeWindowRow(update.Row)
		}
	}
	if len(outputs) != len(entries) {
		return nil, fmt.Errorf("rebuild produced %d outputs for %d rows", len(outputs), len(entries))
	}
	return outputs, nil
}

func mutableIncrementalRangeWindowEntryIndex(entries []mutableIncrementalRangeWindowEntry, key string) int {
	for index, entry := range entries {
		if entry.key == key {
			return index
		}
	}
	return -1
}

func diffMutableIncrementalRangeWindowChanges(oldOutputs, newOutputs map[string]Row, changedKeys map[string]struct{}) []DifferentialRow {
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
			updates = append(updates, DifferentialRow{Key: key, Diff: -1, Row: cloneIncrementalRangeWindowRow(oldRow)})
		}
		if hasNew {
			updates = append(updates, DifferentialRow{Key: key, Diff: 1, Row: cloneIncrementalRangeWindowRow(newRow)})
		}
	}
	return updates
}
