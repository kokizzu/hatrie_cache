package hatSql

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrIncrementalBoundaryWindowNil            = errors.New("incremental boundary window is nil")
	ErrIncrementalBoundaryWindowInvalidKind    = errors.New("incremental boundary window kind is invalid")
	ErrIncrementalBoundaryWindowOutputRequired = errors.New("incremental boundary window output column is required")
	ErrIncrementalBoundaryWindowOrderRequired  = errors.New("incremental boundary window order key is required")
	ErrIncrementalBoundaryWindowRowKeyRequired = errors.New("incremental boundary window row key is required")
	ErrIncrementalBoundaryWindowValueRequired  = errors.New("incremental boundary window value key is required")
	ErrIncrementalBoundaryWindowDuplicateKey   = errors.New("incremental boundary window row key already exists")
	ErrIncrementalBoundaryWindowOutputConflict = errors.New("incremental boundary window output column conflicts with input")
	ErrIncrementalBoundaryWindowOutOfOrder     = errors.New("incremental boundary window row is out of order")
)

// IncrementalBoundaryWindowKind selects the value maintained for an
// unbounded-preceding, current-row SQL window frame.
type IncrementalBoundaryWindowKind uint8

const (
	IncrementalWindowFirstValue IncrementalBoundaryWindowKind = iota + 1
	IncrementalWindowLastValue
)

// IncrementalBoundaryWindowDefinition configures an append-only FIRST_VALUE
// or LAST_VALUE maintainer. Rows must arrive in non-decreasing order within
// each partition, unless Descending is enabled.
type IncrementalBoundaryWindowDefinition struct {
	Kind         IncrementalBoundaryWindowKind
	OutputColumn string
	PartitionKey IncrementalWindowPartitionKeyFunc
	OrderKey     IncrementalWindowOrderKeyFunc
	RowKey       IncrementalWindowRowKeyFunc
	ValueKey     IncrementalOffsetWindowValueKeyFunc
	Descending   bool
}

// IncrementalBoundaryWindow maintains append-only FIRST_VALUE or LAST_VALUE
// results for an unbounded-preceding, current-row frame. It retains one value
// per partition for FIRST_VALUE and no input rows.
type IncrementalBoundaryWindow struct {
	kind         IncrementalBoundaryWindowKind
	outputColumn string
	partitionKey IncrementalWindowPartitionKeyFunc
	orderKey     IncrementalWindowOrderKeyFunc
	rowKey       IncrementalWindowRowKeyFunc
	valueKey     IncrementalOffsetWindowValueKeyFunc
	descending   bool
	partitions   map[string]incrementalBoundaryWindowPartition
	keys         map[string]struct{}
}

type incrementalBoundaryWindowPartition struct {
	lastOrder     interface{}
	hasOrder      bool
	firstValue    interface{}
	hasFirstValue bool
}

type incrementalBoundaryWindowPreparedRow struct {
	key       string
	row       Row
	partition string
	value     interface{}
}

// NewIncrementalBoundaryWindow creates an empty append-only FIRST_VALUE or
// LAST_VALUE maintainer.
func NewIncrementalBoundaryWindow(definition IncrementalBoundaryWindowDefinition) (*IncrementalBoundaryWindow, error) {
	if definition.Kind != IncrementalWindowFirstValue && definition.Kind != IncrementalWindowLastValue {
		return nil, ErrIncrementalBoundaryWindowInvalidKind
	}
	outputColumn := strings.TrimSpace(definition.OutputColumn)
	if outputColumn == "" {
		return nil, ErrIncrementalBoundaryWindowOutputRequired
	}
	if definition.OrderKey == nil {
		return nil, ErrIncrementalBoundaryWindowOrderRequired
	}
	if definition.RowKey == nil {
		return nil, ErrIncrementalBoundaryWindowRowKeyRequired
	}
	if definition.ValueKey == nil {
		return nil, ErrIncrementalBoundaryWindowValueRequired
	}
	return &IncrementalBoundaryWindow{
		kind:         definition.Kind,
		outputColumn: outputColumn,
		partitionKey: definition.PartitionKey,
		orderKey:     definition.OrderKey,
		rowKey:       definition.RowKey,
		valueKey:     definition.ValueKey,
		descending:   definition.Descending,
		partitions:   make(map[string]incrementalBoundaryWindowPartition),
		keys:         make(map[string]struct{}),
	}, nil
}

// Append applies an ordered insert batch and returns the derived value for
// each new row. Validation and callback execution are atomic: an error leaves
// the window unchanged.
func (window *IncrementalBoundaryWindow) Append(rows []Row) ([]DifferentialRow, error) {
	if window == nil {
		return nil, ErrIncrementalBoundaryWindowNil
	}
	if len(rows) == 0 {
		return nil, nil
	}

	prepared := make([]incrementalBoundaryWindowPreparedRow, 0, len(rows))
	states := make(map[string]incrementalBoundaryWindowPartition)
	pendingKeys := make(map[string]struct{}, len(rows))
	for index, row := range rows {
		partition := ""
		if window.partitionKey != nil {
			value, err := window.partitionKey(row)
			if err != nil {
				return nil, fmt.Errorf("incremental boundary window row %d partition key: %w", index, err)
			}
			partition = value
		}
		order, err := window.orderKey(row)
		if err != nil {
			return nil, fmt.Errorf("incremental boundary window row %d order key: %w", index, err)
		}
		key, err := window.rowKey(row)
		if err != nil {
			return nil, fmt.Errorf("incremental boundary window row %d row key: %w", index, err)
		}
		if strings.TrimSpace(key) == "" {
			return nil, fmt.Errorf("incremental boundary window row %d: %w", index, ErrIncrementalBoundaryWindowRowKeyRequired)
		}
		if _, exists := window.keys[key]; exists {
			return nil, fmt.Errorf("incremental boundary window row %d key %q: %w", index, key, ErrIncrementalBoundaryWindowDuplicateKey)
		}
		if _, exists := pendingKeys[key]; exists {
			return nil, fmt.Errorf("incremental boundary window row %d key %q: %w", index, key, ErrIncrementalBoundaryWindowDuplicateKey)
		}
		if _, exists := row[window.outputColumn]; exists {
			return nil, fmt.Errorf("incremental boundary window row %d: %w", index, ErrIncrementalBoundaryWindowOutputConflict)
		}
		value, err := window.valueKey(row)
		if err != nil {
			return nil, fmt.Errorf("incremental boundary window row %d value key: %w", index, err)
		}

		state, exists := states[partition]
		if !exists {
			state = window.partitions[partition]
		}
		if state.hasOrder {
			comparison := sqlCompare(state.lastOrder, order)
			if window.descending {
				comparison = -comparison
			}
			if comparison > 0 {
				return nil, fmt.Errorf("incremental boundary window row %d partition %q: %w", index, partition, ErrIncrementalBoundaryWindowOutOfOrder)
			}
		}
		state.lastOrder = order
		state.hasOrder = true
		states[partition] = state
		pendingKeys[key] = struct{}{}
		prepared = append(prepared, incrementalBoundaryWindowPreparedRow{
			key:       key,
			row:       row,
			partition: partition,
			value:     value,
		})
	}

	updates := make([]DifferentialRow, 0, len(prepared))
	for _, row := range prepared {
		state := states[row.partition]
		value := row.value
		if window.kind == IncrementalWindowFirstValue {
			if !state.hasFirstValue {
				state.firstValue = row.value
				state.hasFirstValue = true
			}
			value = state.firstValue
		}
		updates = append(updates, DifferentialRow{
			Key:  row.key,
			Diff: 1,
			Row:  incrementalFrameWindowOutput(row.row, window.outputColumn, value),
		})
		states[row.partition] = state
	}

	for partition, state := range states {
		window.partitions[partition] = state
	}
	for key := range pendingKeys {
		window.keys[key] = struct{}{}
	}
	return updates, nil
}
