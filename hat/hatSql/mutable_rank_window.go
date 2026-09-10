package hatSql

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

var (
	// ErrIncrementalWindowMutationsDisabled reports use of Apply on an
	// append-only window created by NewIncrementalRankWindow.
	ErrIncrementalWindowMutationsDisabled = errors.New("incremental rank window mutations are disabled")
	// ErrIncrementalWindowMutationInvalid reports an unsupported mutation kind.
	ErrIncrementalWindowMutationInvalid = errors.New("incremental rank window mutation kind is invalid")
	// ErrIncrementalWindowMutationKeyRequired reports a missing mutation key.
	ErrIncrementalWindowMutationKeyRequired = errors.New("incremental rank window mutation key is required")
	// ErrIncrementalWindowMutationDuplicate reports a repeated or already
	// existing key in an insert mutation.
	ErrIncrementalWindowMutationDuplicate = errors.New("incremental rank window mutation key is duplicated")
	// ErrIncrementalWindowMutationMissing reports an update or delete for an
	// identity that is not present.
	ErrIncrementalWindowMutationMissing = errors.New("incremental rank window mutation key is missing")
	// ErrIncrementalWindowMutationKeyMismatch reports a row identity that does
	// not match the mutation key.
	ErrIncrementalWindowMutationKeyMismatch = errors.New("incremental rank window mutation key does not match row key")
)

// IncrementalRankWindowMutationKind identifies a mutable row operation.
type IncrementalRankWindowMutationKind string

const (
	IncrementalRankWindowInsert IncrementalRankWindowMutationKind = "INSERT"
	IncrementalRankWindowUpdate IncrementalRankWindowMutationKind = "UPDATE"
	IncrementalRankWindowDelete IncrementalRankWindowMutationKind = "DELETE"
)

// IncrementalRankWindowMutation changes one stable row identity. INSERT and
// UPDATE require Row without the derived output column; DELETE ignores Row.
// A batch must not mention the same key more than once.
type IncrementalRankWindowMutation struct {
	Kind IncrementalRankWindowMutationKind
	Key  string
	Row  Row
}

// NewMutableIncrementalRankWindow creates a rank maintainer that retains base
// rows so Apply can emit exact retractions and insertions for arbitrary
// inserts, updates, and deletes. The existing constructor remains
// append-only and does not pay this storage cost.
func NewMutableIncrementalRankWindow(definition IncrementalRankWindowDefinition) (*IncrementalRankWindow, error) {
	window, err := NewIncrementalRankWindow(definition)
	if err != nil {
		return nil, err
	}
	window.mutableRows = make(map[string]Row)
	window.mutableOutputs = make(map[string]Row)
	window.mutablePartitions = make(map[string]string)
	return window, nil
}

// Apply atomically applies mutable row changes and returns the differential
// retractions and insertions needed to replace affected rank outputs. Ordered
// insert-only batches use the append fast path; other batches rebuild the
// retained rows in rank order before publishing new state.
func (window *IncrementalRankWindow) Apply(mutations []IncrementalRankWindowMutation) ([]DifferentialRow, error) {
	if window == nil {
		return nil, ErrIncrementalRankWindowNil
	}
	if window.mutableRows == nil {
		return nil, ErrIncrementalWindowMutationsDisabled
	}
	if len(mutations) == 0 {
		return nil, nil
	}

	normalized := make([]IncrementalRankWindowMutation, len(mutations))
	seen := make(map[string]struct{}, len(mutations))
	allInserts := true
	for index, mutation := range mutations {
		kind := IncrementalRankWindowMutationKind(strings.ToUpper(strings.TrimSpace(string(mutation.Kind))))
		if kind != IncrementalRankWindowInsert && kind != IncrementalRankWindowUpdate && kind != IncrementalRankWindowDelete {
			return nil, fmt.Errorf("incremental rank window mutation %d: %w", index, ErrIncrementalWindowMutationInvalid)
		}
		key := mutation.Key
		if strings.TrimSpace(key) == "" {
			return nil, fmt.Errorf("incremental rank window mutation %d: %w", index, ErrIncrementalWindowMutationKeyRequired)
		}
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("incremental rank window mutation %d key %q: %w", index, key, ErrIncrementalWindowMutationDuplicate)
		}
		seen[key] = struct{}{}
		_, exists := window.mutableRows[key]
		switch kind {
		case IncrementalRankWindowInsert:
			if exists {
				return nil, fmt.Errorf("incremental rank window mutation %d key %q: %w", index, key, ErrIncrementalWindowMutationDuplicate)
			}
			if err := window.validateMutableMutationRow(index, key, mutation.Row); err != nil {
				return nil, err
			}
		case IncrementalRankWindowUpdate:
			allInserts = false
			if !exists {
				return nil, fmt.Errorf("incremental rank window mutation %d key %q: %w", index, key, ErrIncrementalWindowMutationMissing)
			}
			if err := window.validateMutableMutationRow(index, key, mutation.Row); err != nil {
				return nil, err
			}
		case IncrementalRankWindowDelete:
			allInserts = false
			if !exists {
				return nil, fmt.Errorf("incremental rank window mutation %d key %q: %w", index, key, ErrIncrementalWindowMutationMissing)
			}
		}
		normalized[index] = IncrementalRankWindowMutation{Kind: kind, Key: key, Row: mutation.Row}
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
		if !errors.Is(err, ErrIncrementalWindowOutOfOrder) {
			return nil, err
		}
	}

	candidate := make(map[string]Row, len(window.mutableRows)+len(normalized))
	for key, row := range window.mutableRows {
		candidate[key] = row
	}
	candidatePartitions := make(map[string]string, len(window.mutablePartitions)+len(normalized))
	for key, partition := range window.mutablePartitions {
		candidatePartitions[key] = partition
	}
	affectedPartitions := make(map[string]struct{}, len(normalized)*2)
	for index, mutation := range normalized {
		switch mutation.Kind {
		case IncrementalRankWindowInsert, IncrementalRankWindowUpdate:
			partition, err := window.mutablePartitionForRow(index, mutation.Row)
			if err != nil {
				return nil, err
			}
			if mutation.Kind == IncrementalRankWindowUpdate {
				affectedPartitions[candidatePartitions[mutation.Key]] = struct{}{}
			}
			candidate[mutation.Key] = cloneIncrementalRankWindowRow(mutation.Row)
			candidatePartitions[mutation.Key] = partition
			affectedPartitions[partition] = struct{}{}
		case IncrementalRankWindowDelete:
			delete(candidate, mutation.Key)
			affectedPartitions[candidatePartitions[mutation.Key]] = struct{}{}
			delete(candidatePartitions, mutation.Key)
		}
	}
	return window.applyMutableRankWindowRebuild(candidate, candidatePartitions, affectedPartitions)
}

func (window *IncrementalRankWindow) validateMutableMutationRow(index int, key string, row Row) error {
	actual, err := window.rowKey(row)
	if err != nil {
		return fmt.Errorf("incremental rank window mutation %d row key: %w", index, err)
	}
	if strings.TrimSpace(actual) == "" {
		return fmt.Errorf("incremental rank window mutation %d: %w", index, ErrIncrementalWindowRowKeyRequired)
	}
	if actual != key {
		return fmt.Errorf("incremental rank window mutation %d key %q: got %q: %w", index, key, actual, ErrIncrementalWindowMutationKeyMismatch)
	}
	if _, exists := row[window.outputColumn]; exists {
		return fmt.Errorf("incremental rank window mutation %d: %w", index, ErrIncrementalWindowOutputConflict)
	}
	return nil
}

func (window *IncrementalRankWindow) mutablePartitionForRow(index int, row Row) (string, error) {
	if window.partitionKey == nil {
		return "", nil
	}
	partition, err := window.partitionKey(row)
	if err != nil {
		return "", fmt.Errorf("incremental rank window mutation %d partition key: %w", index, err)
	}
	return partition, nil
}

type incrementalRankWindowPreparedRow struct {
	key       string
	row       Row
	partition string
	order     interface{}
}

func (window *IncrementalRankWindow) applyMutableRankWindowRebuild(rows map[string]Row, partitions map[string]string, affectedPartitions map[string]struct{}) ([]DifferentialRow, error) {
	preparedByPartition := make(map[string][]incrementalRankWindowPreparedRow, len(affectedPartitions))
	for key, row := range rows {
		partition := partitions[key]
		_, affected := affectedPartitions[partition]
		if !affected {
			continue
		}
		order, err := window.orderKey(row)
		if err != nil {
			return nil, fmt.Errorf("incremental rank window rebuild key %q order key: %w", key, err)
		}
		actual, err := window.rowKey(row)
		if err != nil {
			return nil, fmt.Errorf("incremental rank window rebuild key %q row key: %w", key, err)
		}
		if strings.TrimSpace(actual) == "" {
			return nil, fmt.Errorf("incremental rank window rebuild key %q: %w", key, ErrIncrementalWindowRowKeyRequired)
		}
		if actual != key {
			return nil, fmt.Errorf("incremental rank window rebuild key %q: got %q: %w", key, actual, ErrIncrementalWindowMutationKeyMismatch)
		}
		if _, exists := row[window.outputColumn]; exists {
			return nil, fmt.Errorf("incremental rank window rebuild key %q: %w", key, ErrIncrementalWindowOutputConflict)
		}
		preparedByPartition[partition] = append(preparedByPartition[partition], incrementalRankWindowPreparedRow{key: key, row: row, partition: partition, order: order})
	}

	changedKeys := make(map[string]struct{})
	newOutputs := make(map[string]Row)
	newPartitions := make(map[string]incrementalRankWindowPartition, len(window.partitions))
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
		rebuilt := &IncrementalRankWindow{
			kind:         window.kind,
			outputColumn: window.outputColumn,
			partitionKey: window.partitionKey,
			orderKey:     window.orderKey,
			rowKey:       window.rowKey,
			descending:   window.descending,
			partitions:   make(map[string]incrementalRankWindowPartition),
			keys:         make(map[string]struct{}, len(prepared)),
		}
		sortedRows := make([]Row, len(prepared))
		for index, item := range prepared {
			sortedRows[index] = item.row
		}
		rebuiltUpdates, err := rebuilt.Append(sortedRows)
		if err != nil {
			return nil, fmt.Errorf("incremental rank window rebuild partition %q: %w", partition, err)
		}
		state, exists := rebuilt.partitions[partition]
		if !exists {
			return nil, fmt.Errorf("incremental rank window rebuild partition %q: state is missing", partition)
		}
		newPartitions[partition] = state
		for _, update := range rebuiltUpdates {
			changedKeys[update.Key] = struct{}{}
			newOutputs[update.Key] = update.Row
		}
	}
	updates := diffIncrementalRankWindowChanges(window.mutableOutputs, newOutputs, changedKeys)

	for key := range changedKeys {
		delete(window.mutableOutputs, key)
	}
	for key, row := range newOutputs {
		window.mutableOutputs[key] = row
	}
	window.partitions = newPartitions
	window.keys = make(map[string]struct{}, len(rows))
	for key := range rows {
		window.keys[key] = struct{}{}
	}
	window.mutableRows = rows
	window.mutablePartitions = partitions
	return updates, nil
}

func diffIncrementalRankWindowChanges(oldOutputs, newOutputs map[string]Row, changedKeys map[string]struct{}) []DifferentialRow {
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
			updates = append(updates, DifferentialRow{Key: key, Diff: -1, Row: cloneIncrementalRankWindowRow(oldRow)})
		}
		if hasNew {
			updates = append(updates, DifferentialRow{Key: key, Diff: 1, Row: cloneIncrementalRankWindowRow(newRow)})
		}
	}
	return updates
}

func cloneIncrementalRankWindowRow(row Row) Row {
	if row == nil {
		return nil
	}
	clone := make(Row, len(row))
	for key, value := range row {
		clone[key] = value
	}
	return clone
}
