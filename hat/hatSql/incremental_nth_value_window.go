package hatSql

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrIncrementalNthValueWindowNil             = errors.New("incremental NTH_VALUE window is nil")
	ErrIncrementalNthValueWindowOutputRequired  = errors.New("incremental NTH_VALUE window output column is required")
	ErrIncrementalNthValueWindowOrderRequired   = errors.New("incremental NTH_VALUE window order key is required")
	ErrIncrementalNthValueWindowRowKeyRequired  = errors.New("incremental NTH_VALUE window row key is required")
	ErrIncrementalNthValueWindowValueRequired   = errors.New("incremental NTH_VALUE window value key is required")
	ErrIncrementalNthValueWindowInvalidPosition = errors.New("incremental NTH_VALUE window position must be positive")
	ErrIncrementalNthValueWindowDuplicateKey    = errors.New("incremental NTH_VALUE window row key already exists")
	ErrIncrementalNthValueWindowOutputConflict  = errors.New("incremental NTH_VALUE window output column conflicts with input")
	ErrIncrementalNthValueWindowOutOfOrder      = errors.New("incremental NTH_VALUE window row is out of order")
)

// IncrementalNthValueWindowDefinition configures an append-only NTH_VALUE
// maintainer for ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW. Position is
// one-based, as in SQL NTH_VALUE(expr, position). Rows must arrive in
// non-decreasing order within each partition, unless Descending is enabled.
type IncrementalNthValueWindowDefinition struct {
	Position     int
	OutputColumn string
	PartitionKey IncrementalWindowPartitionKeyFunc
	OrderKey     IncrementalWindowOrderKeyFunc
	RowKey       IncrementalWindowRowKeyFunc
	ValueKey     IncrementalOffsetWindowValueKeyFunc
	Descending   bool
}

// IncrementalNthValueWindow maintains an append-only fixed-position
// NTH_VALUE result. It retains one value and a row count per partition, and
// emits one positive differential row for each appended row.
type IncrementalNthValueWindow struct {
	position     int
	outputColumn string
	partitionKey IncrementalWindowPartitionKeyFunc
	orderKey     IncrementalWindowOrderKeyFunc
	rowKey       IncrementalWindowRowKeyFunc
	valueKey     IncrementalOffsetWindowValueKeyFunc
	descending   bool
	partitions   map[string]incrementalNthValueWindowPartition
	keys         map[string]struct{}
}

type incrementalNthValueWindowPartition struct {
	lastOrder   interface{}
	hasOrder    bool
	seen        int
	nthValue    interface{}
	hasNthValue bool
}

type incrementalNthValueWindowPreparedRow struct {
	key       string
	row       Row
	partition string
	value     interface{}
}

// NewIncrementalNthValueWindow creates an empty append-only NTH_VALUE
// maintainer.
func NewIncrementalNthValueWindow(definition IncrementalNthValueWindowDefinition) (*IncrementalNthValueWindow, error) {
	if definition.Position <= 0 {
		return nil, ErrIncrementalNthValueWindowInvalidPosition
	}
	outputColumn := strings.TrimSpace(definition.OutputColumn)
	if outputColumn == "" {
		return nil, ErrIncrementalNthValueWindowOutputRequired
	}
	if definition.OrderKey == nil {
		return nil, ErrIncrementalNthValueWindowOrderRequired
	}
	if definition.RowKey == nil {
		return nil, ErrIncrementalNthValueWindowRowKeyRequired
	}
	if definition.ValueKey == nil {
		return nil, ErrIncrementalNthValueWindowValueRequired
	}
	return &IncrementalNthValueWindow{
		position:     definition.Position,
		outputColumn: outputColumn,
		partitionKey: definition.PartitionKey,
		orderKey:     definition.OrderKey,
		rowKey:       definition.RowKey,
		valueKey:     definition.ValueKey,
		descending:   definition.Descending,
		partitions:   make(map[string]incrementalNthValueWindowPartition),
		keys:         make(map[string]struct{}),
	}, nil
}

// Append applies an ordered insert batch and returns the derived NTH_VALUE
// for each new row. Rows before the requested position receive SQL NULL.
// Validation and callback execution are atomic: an error leaves the window
// unchanged.
func (window *IncrementalNthValueWindow) Append(rows []Row) ([]DifferentialRow, error) {
	if window == nil {
		return nil, ErrIncrementalNthValueWindowNil
	}
	if len(rows) == 0 {
		return nil, nil
	}

	prepared := make([]incrementalNthValueWindowPreparedRow, 0, len(rows))
	states := make(map[string]incrementalNthValueWindowPartition)
	pendingKeys := make(map[string]struct{}, len(rows))
	for index, row := range rows {
		partition := ""
		if window.partitionKey != nil {
			value, err := window.partitionKey(row)
			if err != nil {
				return nil, fmt.Errorf("incremental NTH_VALUE window row %d partition key: %w", index, err)
			}
			partition = value
		}
		order, err := window.orderKey(row)
		if err != nil {
			return nil, fmt.Errorf("incremental NTH_VALUE window row %d order key: %w", index, err)
		}
		key, err := window.rowKey(row)
		if err != nil {
			return nil, fmt.Errorf("incremental NTH_VALUE window row %d row key: %w", index, err)
		}
		if strings.TrimSpace(key) == "" {
			return nil, fmt.Errorf("incremental NTH_VALUE window row %d: %w", index, ErrIncrementalNthValueWindowRowKeyRequired)
		}
		if _, exists := window.keys[key]; exists {
			return nil, fmt.Errorf("incremental NTH_VALUE window row %d key %q: %w", index, key, ErrIncrementalNthValueWindowDuplicateKey)
		}
		if _, exists := pendingKeys[key]; exists {
			return nil, fmt.Errorf("incremental NTH_VALUE window row %d key %q: %w", index, key, ErrIncrementalNthValueWindowDuplicateKey)
		}
		if _, exists := row[window.outputColumn]; exists {
			return nil, fmt.Errorf("incremental NTH_VALUE window row %d: %w", index, ErrIncrementalNthValueWindowOutputConflict)
		}
		value, err := window.valueKey(row)
		if err != nil {
			return nil, fmt.Errorf("incremental NTH_VALUE window row %d value key: %w", index, err)
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
				return nil, fmt.Errorf("incremental NTH_VALUE window row %d partition %q: %w", index, partition, ErrIncrementalNthValueWindowOutOfOrder)
			}
		}
		state.lastOrder = order
		state.hasOrder = true
		states[partition] = state
		pendingKeys[key] = struct{}{}
		prepared = append(prepared, incrementalNthValueWindowPreparedRow{
			key:       key,
			row:       row,
			partition: partition,
			value:     value,
		})
	}

	updates := make([]DifferentialRow, 0, len(prepared))
	for _, row := range prepared {
		state := states[row.partition]
		state.seen++
		if !state.hasNthValue && state.seen == window.position {
			state.nthValue = row.value
			state.hasNthValue = true
		}
		value := interface{}(nil)
		if state.hasNthValue {
			value = state.nthValue
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
