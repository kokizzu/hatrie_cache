package hatSql

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

var (
	ErrIncrementalFrameWindowMutationsDisabled   = errors.New("incremental frame window mutations are disabled")
	ErrIncrementalFrameWindowMutationInvalid     = errors.New("incremental frame window mutation kind is invalid")
	ErrIncrementalFrameWindowMutationKeyRequired = errors.New("incremental frame window mutation key is required")
	ErrIncrementalFrameWindowMutationDuplicate   = errors.New("incremental frame window mutation key is duplicated")
	ErrIncrementalFrameWindowMutationMissing     = errors.New("incremental frame window mutation key is missing")
	ErrIncrementalFrameWindowMutationKeyMismatch = errors.New("incremental frame window mutation key does not match row key")
)

// IncrementalFrameWindowMutationKind identifies a mutable row operation.
type IncrementalFrameWindowMutationKind string

const (
	IncrementalFrameWindowInsert IncrementalFrameWindowMutationKind = "INSERT"
	IncrementalFrameWindowUpdate IncrementalFrameWindowMutationKind = "UPDATE"
	IncrementalFrameWindowDelete IncrementalFrameWindowMutationKind = "DELETE"
)

// IncrementalFrameWindowMutation changes one stable row identity. INSERT and
// UPDATE require Row without the derived output column; DELETE ignores Row.
// A batch must not mention the same key more than once.
type IncrementalFrameWindowMutation struct {
	Kind IncrementalFrameWindowMutationKind
	Key  string
	Row  Row
}

// NewMutableIncrementalFrameWindow creates a frame maintainer that retains
// base rows so Apply can emit exact retractions and insertions for arbitrary
// inserts, updates, and deletes. The existing constructor remains
// append-only and does not pay this storage cost.
func NewMutableIncrementalFrameWindow(definition IncrementalFrameWindowDefinition) (*IncrementalFrameWindow, error) {
	window, err := NewIncrementalFrameWindow(definition)
	if err != nil {
		return nil, err
	}
	window.mutableRows = make(map[string]Row)
	window.mutableOutputs = make(map[string]Row)
	window.mutablePartitions = make(map[string]string)
	window.mutableRowsByPartition = make(map[string]map[string]Row)
	return window, nil
}

// Apply atomically applies mutable row changes and returns the differential
// retractions and insertions needed to replace affected frame outputs. Ordered
// insert-only batches use the append fast path; other batches rebuild only the
// affected partitions in frame order before publishing new state.
func (window *IncrementalFrameWindow) Apply(mutations []IncrementalFrameWindowMutation) ([]DifferentialRow, error) {
	if window == nil {
		return nil, ErrIncrementalFrameWindowNil
	}
	if window.mutableRows == nil {
		return nil, ErrIncrementalFrameWindowMutationsDisabled
	}
	if len(mutations) == 0 {
		return nil, nil
	}

	normalized := make([]IncrementalFrameWindowMutation, len(mutations))
	seen := make(map[string]struct{}, len(mutations))
	allInserts := true
	for index, mutation := range mutations {
		kind := IncrementalFrameWindowMutationKind(strings.ToUpper(strings.TrimSpace(string(mutation.Kind))))
		if kind != IncrementalFrameWindowInsert && kind != IncrementalFrameWindowUpdate && kind != IncrementalFrameWindowDelete {
			return nil, fmt.Errorf("incremental frame window mutation %d: %w", index, ErrIncrementalFrameWindowMutationInvalid)
		}
		key := mutation.Key
		if strings.TrimSpace(key) == "" {
			return nil, fmt.Errorf("incremental frame window mutation %d: %w", index, ErrIncrementalFrameWindowMutationKeyRequired)
		}
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("incremental frame window mutation %d key %q: %w", index, key, ErrIncrementalFrameWindowMutationDuplicate)
		}
		seen[key] = struct{}{}
		_, exists := window.mutableRows[key]
		switch kind {
		case IncrementalFrameWindowInsert:
			if exists {
				return nil, fmt.Errorf("incremental frame window mutation %d key %q: %w", index, key, ErrIncrementalFrameWindowMutationDuplicate)
			}
			if err := window.validateMutableFrameWindowRow(index, key, mutation.Row); err != nil {
				return nil, err
			}
		case IncrementalFrameWindowUpdate:
			allInserts = false
			if !exists {
				return nil, fmt.Errorf("incremental frame window mutation %d key %q: %w", index, key, ErrIncrementalFrameWindowMutationMissing)
			}
			if err := window.validateMutableFrameWindowRow(index, key, mutation.Row); err != nil {
				return nil, err
			}
		case IncrementalFrameWindowDelete:
			allInserts = false
			if !exists {
				return nil, fmt.Errorf("incremental frame window mutation %d key %q: %w", index, key, ErrIncrementalFrameWindowMutationMissing)
			}
		}
		normalized[index] = IncrementalFrameWindowMutation{Kind: kind, Key: key, Row: mutation.Row}
	}

	if allInserts {
		rows := make([]Row, len(normalized))
		for index, mutation := range normalized {
			rows[index] = mutation.Row
		}
		updates, err := window.Append(rows)
		if err == nil {
			return updates, nil
		}
		if !errors.Is(err, ErrIncrementalFrameWindowOutOfOrder) {
			return nil, err
		}
	}

	affectedPartitions := make(map[string]struct{}, len(normalized)*2)
	mutationPartitions := make([]string, len(normalized))
	for index, mutation := range normalized {
		switch mutation.Kind {
		case IncrementalFrameWindowInsert, IncrementalFrameWindowUpdate:
			partition, err := window.mutableFrameWindowPartitionForRow(index, mutation.Row)
			if err != nil {
				return nil, err
			}
			mutationPartitions[index] = partition
			if mutation.Kind == IncrementalFrameWindowUpdate {
				affectedPartitions[window.mutablePartitions[mutation.Key]] = struct{}{}
			}
			affectedPartitions[partition] = struct{}{}
		case IncrementalFrameWindowDelete:
			affectedPartitions[window.mutablePartitions[mutation.Key]] = struct{}{}
		}
	}

	candidates := make(map[string]map[string]Row, len(affectedPartitions))
	for partition := range affectedPartitions {
		current := window.mutableRowsByPartition[partition]
		candidate := make(map[string]Row, len(current))
		for key, row := range current {
			candidate[key] = row
		}
		candidates[partition] = candidate
	}
	for index, mutation := range normalized {
		switch mutation.Kind {
		case IncrementalFrameWindowInsert, IncrementalFrameWindowUpdate:
			partition := mutationPartitions[index]
			if mutation.Kind == IncrementalFrameWindowUpdate {
				oldPartition := window.mutablePartitions[mutation.Key]
				if oldPartition != partition {
					delete(candidates[oldPartition], mutation.Key)
				}
			}
			candidates[partition][mutation.Key] = cloneIncrementalFrameWindowRow(mutation.Row)
		case IncrementalFrameWindowDelete:
			delete(candidates[window.mutablePartitions[mutation.Key]], mutation.Key)
		}
	}
	return window.applyMutableFrameWindowRebuild(candidates, affectedPartitions)
}

func (window *IncrementalFrameWindow) validateMutableFrameWindowRow(index int, key string, row Row) error {
	actual, err := window.rowKey(row)
	if err != nil {
		return fmt.Errorf("incremental frame window mutation %d row key: %w", index, err)
	}
	if strings.TrimSpace(actual) == "" {
		return fmt.Errorf("incremental frame window mutation %d: %w", index, ErrIncrementalFrameWindowRowKeyRequired)
	}
	if actual != key {
		return fmt.Errorf("incremental frame window mutation %d key %q: got %q: %w", index, key, actual, ErrIncrementalFrameWindowMutationKeyMismatch)
	}
	if _, exists := row[window.outputColumn]; exists {
		return fmt.Errorf("incremental frame window mutation %d: %w", index, ErrIncrementalFrameWindowOutputConflict)
	}
	return nil
}

func (window *IncrementalFrameWindow) mutableFrameWindowPartitionForRow(index int, row Row) (string, error) {
	if window.partitionKey == nil {
		return "", nil
	}
	partition, err := window.partitionKey(row)
	if err != nil {
		return "", fmt.Errorf("incremental frame window mutation %d partition key: %w", index, err)
	}
	return partition, nil
}

type incrementalFrameWindowMutablePreparedRow struct {
	key       string
	row       Row
	partition string
	order     interface{}
}

func (window *IncrementalFrameWindow) applyMutableFrameWindowRebuild(rowsByPartition map[string]map[string]Row, affectedPartitions map[string]struct{}) ([]DifferentialRow, error) {
	preparedByPartition := make(map[string][]incrementalFrameWindowMutablePreparedRow, len(affectedPartitions))
	for partition, rows := range rowsByPartition {
		for key, row := range rows {
			order, err := window.orderKey(row)
			if err != nil {
				return nil, fmt.Errorf("incremental frame window rebuild key %q order key: %w", key, err)
			}
			if err := window.validateMutableFrameWindowRow(0, key, row); err != nil {
				return nil, fmt.Errorf("incremental frame window rebuild key %q: %w", key, err)
			}
			preparedByPartition[partition] = append(preparedByPartition[partition], incrementalFrameWindowMutablePreparedRow{
				key: key, row: row, partition: partition, order: order,
			})
		}
	}

	changedKeys := make(map[string]struct{})
	newOutputs := make(map[string]Row)
	newPartitions := make(map[string]incrementalFrameWindowPartition, len(window.partitions))
	for partition, state := range window.partitions {
		newPartitions[partition] = state
	}
	for key, partition := range window.mutablePartitions {
		if _, affected := affectedPartitions[partition]; affected {
			changedKeys[key] = struct{}{}
		}
	}
	for partition := range affectedPartitions {
		delete(newPartitions, partition)
		prepared := preparedByPartition[partition]
		sort.SliceStable(prepared, func(left, right int) bool {
			comparison := sqlCompare(prepared[left].order, prepared[right].order)
			if window.descending {
				comparison = -comparison
			}
			if comparison != 0 {
				return comparison < 0
			}
			return prepared[left].key < prepared[right].key
		})
		if len(prepared) == 0 {
			continue
		}
		rebuilt := &IncrementalFrameWindow{
			kind:           window.kind,
			outputColumn:   window.outputColumn,
			partitionKey:   window.partitionKey,
			orderKey:       window.orderKey,
			rowKey:         window.rowKey,
			valueKey:       window.valueKey,
			framePreceding: window.framePreceding,
			descending:     window.descending,
			partitions:     make(map[string]incrementalFrameWindowPartition),
			keys:           make(map[string]struct{}, len(prepared)),
		}
		sortedRows := make([]Row, len(prepared))
		for index, item := range prepared {
			sortedRows[index] = item.row
		}
		rebuiltUpdates, err := rebuilt.Append(sortedRows)
		if err != nil {
			return nil, fmt.Errorf("incremental frame window rebuild partition %q: %w", partition, err)
		}
		state, exists := rebuilt.partitions[partition]
		if !exists {
			return nil, fmt.Errorf("incremental frame window rebuild partition %q: state is missing", partition)
		}
		newPartitions[partition] = state
		for _, update := range rebuiltUpdates {
			changedKeys[update.Key] = struct{}{}
			newOutputs[update.Key] = update.Row
		}
	}
	updates := diffIncrementalFrameWindowChanges(window.mutableOutputs, newOutputs, changedKeys)

	for key := range changedKeys {
		delete(window.mutableOutputs, key)
	}
	for key, row := range newOutputs {
		window.mutableOutputs[key] = cloneIncrementalFrameWindowRow(row)
	}
	window.partitions = newPartitions
	if window.mutableRowsByPartition == nil {
		window.mutableRowsByPartition = make(map[string]map[string]Row)
	}
	for partition := range affectedPartitions {
		for key := range window.mutableRowsByPartition[partition] {
			delete(window.mutableRows, key)
			delete(window.mutablePartitions, key)
			delete(window.keys, key)
		}
		candidate := rowsByPartition[partition]
		if len(candidate) == 0 {
			delete(window.mutableRowsByPartition, partition)
			continue
		}
		window.mutableRowsByPartition[partition] = candidate
		for key, row := range candidate {
			window.mutableRows[key] = row
			window.mutablePartitions[key] = partition
			window.keys[key] = struct{}{}
		}
	}
	return updates, nil
}

func diffIncrementalFrameWindowChanges(oldOutputs, newOutputs map[string]Row, changedKeys map[string]struct{}) []DifferentialRow {
	orderedKeys := make([]string, 0, len(changedKeys))
	for key := range changedKeys {
		orderedKeys = append(orderedKeys, key)
	}
	sort.Strings(orderedKeys)

	updates := make([]DifferentialRow, 0, len(orderedKeys)*2)
	for _, key := range orderedKeys {
		oldRow, hadOld := oldOutputs[key]
		newRow, hasNew := newOutputs[key]
		if hadOld && hasNew && reflect.DeepEqual(oldRow, newRow) {
			continue
		}
		if hadOld {
			updates = append(updates, DifferentialRow{Key: key, Diff: -1, Row: cloneIncrementalFrameWindowRow(oldRow)})
		}
		if hasNew {
			updates = append(updates, DifferentialRow{Key: key, Diff: 1, Row: cloneIncrementalFrameWindowRow(newRow)})
		}
	}
	return updates
}

func cloneIncrementalFrameWindowRow(row Row) Row {
	if row == nil {
		return nil
	}
	clone := make(Row, len(row))
	for key, value := range row {
		clone[key] = value
	}
	return clone
}
