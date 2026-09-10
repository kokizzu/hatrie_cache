package hatSql

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

var (
	ErrIncrementalOffsetWindowNil              = errors.New("incremental offset window is nil")
	ErrIncrementalOffsetWindowInvalidDirection = errors.New("incremental offset window direction is invalid")
	ErrIncrementalOffsetWindowOutputRequired   = errors.New("incremental offset window output column is required")
	ErrIncrementalOffsetWindowOrderRequired    = errors.New("incremental offset window order key is required")
	ErrIncrementalOffsetWindowRowKeyRequired   = errors.New("incremental offset window row key is required")
	ErrIncrementalOffsetWindowValueRequired    = errors.New("incremental offset window value key is required")
	ErrIncrementalOffsetWindowNegativeOffset   = errors.New("incremental offset window offset must be non-negative")
	ErrIncrementalOffsetWindowDuplicateKey     = errors.New("incremental offset window row key already exists")
	ErrIncrementalOffsetWindowOutputConflict   = errors.New("incremental offset window output column conflicts with input")
	ErrIncrementalOffsetWindowOutOfOrder       = errors.New("incremental offset window row is out of order")
)

// IncrementalOffsetWindowDirection selects the SQL LAG or LEAD operation.
type IncrementalOffsetWindowDirection uint8

const (
	IncrementalWindowLag IncrementalOffsetWindowDirection = iota + 1
	IncrementalWindowLead
)

// IncrementalOffsetWindowValueKeyFunc extracts the value returned by LAG or
// LEAD. A nil value represents SQL NULL.
type IncrementalOffsetWindowValueKeyFunc func(Row) (interface{}, error)

// IncrementalOffsetWindowDefinition configures an append-only LAG or LEAD
// maintainer. Rows must arrive in non-decreasing order within each partition,
// unless Descending is enabled.
type IncrementalOffsetWindowDefinition struct {
	Direction    IncrementalOffsetWindowDirection
	OutputColumn string
	PartitionKey IncrementalWindowPartitionKeyFunc
	OrderKey     IncrementalWindowOrderKeyFunc
	RowKey       IncrementalWindowRowKeyFunc
	ValueKey     IncrementalOffsetWindowValueKeyFunc
	Offset       int
	DefaultValue interface{}
	Descending   bool
}

// IncrementalOffsetWindow maintains append-only LAG or LEAD values. LAG
// retains only offset values per partition. LEAD retains only rows that do not
// yet have enough following rows, so its retained state is also bounded by the
// configured offset.
type IncrementalOffsetWindow struct {
	direction    IncrementalOffsetWindowDirection
	outputColumn string
	partitionKey IncrementalWindowPartitionKeyFunc
	orderKey     IncrementalWindowOrderKeyFunc
	rowKey       IncrementalWindowRowKeyFunc
	valueKey     IncrementalOffsetWindowValueKeyFunc
	offset       int
	defaultValue interface{}
	descending   bool
	partitions   map[string]incrementalOffsetWindowPartition
	keys         map[string]struct{}
}

type incrementalOffsetWindowPartition struct {
	lastOrder interface{}
	hasOrder  bool
	history   []interface{}
	pending   []incrementalOffsetWindowPending
}

type incrementalOffsetWindowPending struct {
	key   string
	row   Row
	value interface{}
}

type incrementalOffsetWindowPreparedRow struct {
	key       string
	row       Row
	partition string
	order     interface{}
	value     interface{}
}

// NewIncrementalOffsetWindow creates an empty append-only LAG or LEAD
// maintainer. The default value is emitted until a row at the requested offset
// exists for LEAD, or while insufficient history exists for LAG.
func NewIncrementalOffsetWindow(definition IncrementalOffsetWindowDefinition) (*IncrementalOffsetWindow, error) {
	if definition.Direction != IncrementalWindowLag && definition.Direction != IncrementalWindowLead {
		return nil, ErrIncrementalOffsetWindowInvalidDirection
	}
	outputColumn := strings.TrimSpace(definition.OutputColumn)
	if outputColumn == "" {
		return nil, ErrIncrementalOffsetWindowOutputRequired
	}
	if definition.OrderKey == nil {
		return nil, ErrIncrementalOffsetWindowOrderRequired
	}
	if definition.RowKey == nil {
		return nil, ErrIncrementalOffsetWindowRowKeyRequired
	}
	if definition.ValueKey == nil {
		return nil, ErrIncrementalOffsetWindowValueRequired
	}
	if definition.Offset < 0 {
		return nil, ErrIncrementalOffsetWindowNegativeOffset
	}
	return &IncrementalOffsetWindow{
		direction:    definition.Direction,
		outputColumn: outputColumn,
		partitionKey: definition.PartitionKey,
		orderKey:     definition.OrderKey,
		rowKey:       definition.RowKey,
		valueKey:     definition.ValueKey,
		offset:       definition.Offset,
		defaultValue: definition.DefaultValue,
		descending:   definition.Descending,
		partitions:   make(map[string]incrementalOffsetWindowPartition),
		keys:         make(map[string]struct{}),
	}, nil
}

// Append applies an ordered insert batch and returns the current derived value
// for each new row plus any LEAD rows whose future value became known. Input
// rows are cloned before the output column is added.
func (window *IncrementalOffsetWindow) Append(rows []Row) ([]DifferentialRow, error) {
	if window == nil {
		return nil, ErrIncrementalOffsetWindowNil
	}
	if len(rows) == 0 {
		return nil, nil
	}

	prepared := make([]incrementalOffsetWindowPreparedRow, 0, len(rows))
	states := make(map[string]incrementalOffsetWindowPartition)
	pendingKeys := make(map[string]struct{}, len(rows))
	for index, row := range rows {
		partition := ""
		if window.partitionKey != nil {
			value, err := window.partitionKey(row)
			if err != nil {
				return nil, fmt.Errorf("incremental offset window row %d partition key: %w", index, err)
			}
			partition = value
		}
		order, err := window.orderKey(row)
		if err != nil {
			return nil, fmt.Errorf("incremental offset window row %d order key: %w", index, err)
		}
		key, err := window.rowKey(row)
		if err != nil {
			return nil, fmt.Errorf("incremental offset window row %d row key: %w", index, err)
		}
		if strings.TrimSpace(key) == "" {
			return nil, fmt.Errorf("incremental offset window row %d: %w", index, ErrIncrementalOffsetWindowRowKeyRequired)
		}
		if _, exists := window.keys[key]; exists {
			return nil, fmt.Errorf("incremental offset window row %d key %q: %w", index, key, ErrIncrementalOffsetWindowDuplicateKey)
		}
		if _, exists := pendingKeys[key]; exists {
			return nil, fmt.Errorf("incremental offset window row %d key %q: %w", index, key, ErrIncrementalOffsetWindowDuplicateKey)
		}
		if _, exists := row[window.outputColumn]; exists {
			return nil, fmt.Errorf("incremental offset window row %d: %w", index, ErrIncrementalOffsetWindowOutputConflict)
		}
		value, err := window.valueKey(row)
		if err != nil {
			return nil, fmt.Errorf("incremental offset window row %d value key: %w", index, err)
		}

		state, exists := states[partition]
		if !exists {
			state = cloneIncrementalOffsetWindowPartition(window.partitions[partition])
		}
		if state.hasOrder {
			comparison := sqlCompare(state.lastOrder, order)
			if window.descending {
				comparison = -comparison
			}
			if comparison > 0 {
				return nil, fmt.Errorf("incremental offset window row %d partition %q: %w", index, partition, ErrIncrementalOffsetWindowOutOfOrder)
			}
		}
		state.lastOrder = order
		state.hasOrder = true
		states[partition] = state
		pendingKeys[key] = struct{}{}
		prepared = append(prepared, incrementalOffsetWindowPreparedRow{
			key:       key,
			row:       row,
			partition: partition,
			order:     order,
			value:     value,
		})
	}

	byPartition := make(map[string][]incrementalOffsetWindowPreparedRow, len(states))
	partitionOrder := make([]string, 0, len(states))
	for _, row := range prepared {
		if _, exists := byPartition[row.partition]; !exists {
			partitionOrder = append(partitionOrder, row.partition)
		}
		byPartition[row.partition] = append(byPartition[row.partition], row)
	}
	updates := make([]DifferentialRow, 0, len(rows)*2)
	for _, partition := range partitionOrder {
		state := states[partition]
		partitionRows := byPartition[partition]
		if window.direction == IncrementalWindowLag {
			updates = append(updates, window.appendLagPartition(&state, partitionRows)...)
		} else {
			updates = append(updates, window.appendLeadPartition(&state, partitionRows)...)
		}
		states[partition] = state
	}

	for _, partition := range partitionOrder {
		window.partitions[partition] = states[partition]
	}
	for key := range pendingKeys {
		window.keys[key] = struct{}{}
	}
	return updates, nil
}

func (window *IncrementalOffsetWindow) appendLagPartition(state *incrementalOffsetWindowPartition, rows []incrementalOffsetWindowPreparedRow) []DifferentialRow {
	updates := make([]DifferentialRow, 0, len(rows))
	for _, row := range rows {
		value := window.defaultValue
		if window.offset == 0 {
			value = row.value
		} else if len(state.history) == window.offset {
			value = state.history[0]
		}
		updates = append(updates, DifferentialRow{
			Key:  row.key,
			Diff: 1,
			Row:  incrementalOffsetWindowOutput(row.row, window.outputColumn, value),
		})
		if window.offset > 0 {
			state.history = append(state.history, row.value)
			if len(state.history) > window.offset {
				state.history = state.history[len(state.history)-window.offset:]
			}
		}
	}
	return updates
}

func (window *IncrementalOffsetWindow) appendLeadPartition(state *incrementalOffsetWindowPartition, rows []incrementalOffsetWindowPreparedRow) []DifferentialRow {
	if window.offset == 0 {
		updates := make([]DifferentialRow, 0, len(rows))
		for _, row := range rows {
			updates = append(updates, DifferentialRow{
				Key:  row.key,
				Diff: 1,
				Row:  incrementalOffsetWindowOutput(row.row, window.outputColumn, row.value),
			})
		}
		state.pending = nil
		return updates
	}

	oldPendingCount := len(state.pending)
	combined := make([]incrementalOffsetWindowPending, 0, oldPendingCount+len(rows))
	combined = append(combined, state.pending...)
	for _, row := range rows {
		combined = append(combined, incrementalOffsetWindowPending{
			key:   row.key,
			row:   cloneIncrementalOffsetWindowRow(row.row),
			value: row.value,
		})
	}
	updates := make([]DifferentialRow, 0, len(rows)*2)
	for index, pending := range combined {
		value := window.defaultValue
		future := index + window.offset
		if future < len(combined) {
			value = combined[future].value
		}
		if index < oldPendingCount {
			if reflect.DeepEqual(value, window.defaultValue) {
				continue
			}
			updates = append(updates,
				DifferentialRow{
					Key:  pending.key,
					Diff: -1,
					Row:  incrementalOffsetWindowOutput(pending.row, window.outputColumn, window.defaultValue),
				},
				DifferentialRow{
					Key:  pending.key,
					Diff: 1,
					Row:  incrementalOffsetWindowOutput(pending.row, window.outputColumn, value),
				},
			)
			continue
		}
		updates = append(updates, DifferentialRow{
			Key:  pending.key,
			Diff: 1,
			Row:  incrementalOffsetWindowOutput(pending.row, window.outputColumn, value),
		})
	}
	start := len(combined) - window.offset
	if start < 0 {
		start = 0
	}
	state.pending = append(state.pending[:0], combined[start:]...)
	return updates
}

func cloneIncrementalOffsetWindowPartition(state incrementalOffsetWindowPartition) incrementalOffsetWindowPartition {
	state.history = append([]interface{}(nil), state.history...)
	state.pending = append([]incrementalOffsetWindowPending(nil), state.pending...)
	return state
}

func incrementalOffsetWindowOutput(row Row, outputColumn string, value interface{}) Row {
	output := make(Row, len(row)+1)
	for column, fieldValue := range row {
		output[column] = fieldValue
	}
	output[outputColumn] = value
	return output
}

func cloneIncrementalOffsetWindowRow(row Row) Row {
	if row == nil {
		return nil
	}
	clone := make(Row, len(row))
	for column, value := range row {
		clone[column] = value
	}
	return clone
}
