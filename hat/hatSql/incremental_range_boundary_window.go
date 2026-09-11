package hatSql

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrIncrementalRangeBoundaryWindowNil            = errors.New("incremental range boundary window is nil")
	ErrIncrementalRangeBoundaryWindowInvalidKind    = errors.New("incremental range boundary window kind is invalid")
	ErrIncrementalRangeBoundaryWindowOutputRequired = errors.New("incremental range boundary window output column is required")
	ErrIncrementalRangeBoundaryWindowOrderRequired  = errors.New("incremental range boundary window order key is required")
	ErrIncrementalRangeBoundaryWindowOrderInvalid   = errors.New("incremental range boundary window order key must be int64")
	ErrIncrementalRangeBoundaryWindowRowKeyRequired = errors.New("incremental range boundary window row key is required")
	ErrIncrementalRangeBoundaryWindowValueRequired  = errors.New("incremental range boundary window value key is required")
	ErrIncrementalRangeBoundaryWindowNegativeFrame  = errors.New("incremental range boundary window preceding bound must be non-negative")
	ErrIncrementalRangeBoundaryWindowDuplicateKey   = errors.New("incremental range boundary window row key already exists")
	ErrIncrementalRangeBoundaryWindowOutputConflict = errors.New("incremental range boundary window output column conflicts with input")
	ErrIncrementalRangeBoundaryWindowOutOfOrder     = errors.New("incremental range boundary window row is out of order")
)

// IncrementalRangeBoundaryWindowKind selects the value maintained for an
// inclusive numeric RANGE BETWEEN N PRECEDING AND CURRENT ROW frame.
type IncrementalRangeBoundaryWindowKind uint8

const (
	IncrementalRangeFirstValue IncrementalRangeBoundaryWindowKind = iota + 1
	IncrementalRangeLastValue
)

// IncrementalRangeBoundaryWindowDefinition configures an append-only
// FIRST_VALUE or LAST_VALUE maintainer. OrderKey must return int64 values.
// Rows must arrive in non-decreasing order within each partition, unless
// Descending is enabled.
type IncrementalRangeBoundaryWindowDefinition struct {
	Kind           IncrementalRangeBoundaryWindowKind
	OutputColumn   string
	PartitionKey   IncrementalWindowPartitionKeyFunc
	OrderKey       IncrementalWindowOrderKeyFunc
	RowKey         IncrementalWindowRowKeyFunc
	ValueKey       IncrementalOffsetWindowValueKeyFunc
	FramePreceding int64
	Descending     bool
}

// IncrementalRangeBoundaryWindow maintains append-only FIRST_VALUE or
// LAST_VALUE results for a numeric RANGE frame. FIRST_VALUE retains active
// frame values; LAST_VALUE retains only the current peer group because later
// non-peer rows cannot change an earlier row's RANGE frame.
type IncrementalRangeBoundaryWindow struct {
	kind         IncrementalRangeBoundaryWindowKind
	outputColumn string
	partitionKey IncrementalWindowPartitionKeyFunc
	orderKey     IncrementalWindowOrderKeyFunc
	rowKey       IncrementalWindowRowKeyFunc
	valueKey     IncrementalOffsetWindowValueKeyFunc
	preceding    int64
	descending   bool
	partitions   map[string]incrementalRangeBoundaryWindowPartition
	keys         map[string]struct{}
}

type incrementalRangeBoundaryWindowPartition struct {
	lastOrder  int64
	hasOrder   bool
	active     []incrementalRangeBoundaryWindowContribution
	activeHead int
	peerOrder  int64
	hasPeer    bool
	peers      []incrementalRangeBoundaryWindowPeer
}

type incrementalRangeBoundaryWindowContribution struct {
	order int64
	value interface{}
}

type incrementalRangeBoundaryWindowPeer struct {
	key   string
	row   Row
	value interface{}
}

type incrementalRangeBoundaryWindowPreparedRow struct {
	key       string
	row       Row
	partition string
	order     int64
	value     interface{}
}

// NewIncrementalRangeBoundaryWindow creates an empty append-only numeric
// RANGE FIRST_VALUE or LAST_VALUE maintainer.
func NewIncrementalRangeBoundaryWindow(definition IncrementalRangeBoundaryWindowDefinition) (*IncrementalRangeBoundaryWindow, error) {
	if definition.Kind != IncrementalRangeFirstValue && definition.Kind != IncrementalRangeLastValue {
		return nil, ErrIncrementalRangeBoundaryWindowInvalidKind
	}
	outputColumn := strings.TrimSpace(definition.OutputColumn)
	if outputColumn == "" {
		return nil, ErrIncrementalRangeBoundaryWindowOutputRequired
	}
	if definition.OrderKey == nil {
		return nil, ErrIncrementalRangeBoundaryWindowOrderRequired
	}
	if definition.RowKey == nil {
		return nil, ErrIncrementalRangeBoundaryWindowRowKeyRequired
	}
	if definition.ValueKey == nil {
		return nil, ErrIncrementalRangeBoundaryWindowValueRequired
	}
	if definition.FramePreceding < 0 {
		return nil, ErrIncrementalRangeBoundaryWindowNegativeFrame
	}
	return &IncrementalRangeBoundaryWindow{
		kind:         definition.Kind,
		outputColumn: outputColumn,
		partitionKey: definition.PartitionKey,
		orderKey:     definition.OrderKey,
		rowKey:       definition.RowKey,
		valueKey:     definition.ValueKey,
		preceding:    definition.FramePreceding,
		descending:   definition.Descending,
		partitions:   make(map[string]incrementalRangeBoundaryWindowPartition),
		keys:         make(map[string]struct{}),
	}, nil
}

// Append applies an ordered insert batch and returns the derived value for
// each new row. Peer replacements for LAST_VALUE are emitted as exact
// retractions and insertions. Validation and callbacks are atomic.
func (window *IncrementalRangeBoundaryWindow) Append(rows []Row) ([]DifferentialRow, error) {
	if window == nil {
		return nil, ErrIncrementalRangeBoundaryWindowNil
	}
	if len(rows) == 0 {
		return nil, nil
	}

	prepared := make([]incrementalRangeBoundaryWindowPreparedRow, 0, len(rows))
	states := make(map[string]incrementalRangeBoundaryWindowPartition)
	pendingKeys := make(map[string]struct{}, len(rows))
	for index, row := range rows {
		partition := ""
		if window.partitionKey != nil {
			value, err := window.partitionKey(row)
			if err != nil {
				return nil, fmt.Errorf("incremental range boundary window row %d partition key: %w", index, err)
			}
			partition = value
		}
		orderValue, err := window.orderKey(row)
		if err != nil {
			return nil, fmt.Errorf("incremental range boundary window row %d order key: %w", index, err)
		}
		order, ok := orderValue.(int64)
		if !ok {
			return nil, fmt.Errorf("incremental range boundary window row %d: %w", index, ErrIncrementalRangeBoundaryWindowOrderInvalid)
		}
		key, err := window.rowKey(row)
		if err != nil {
			return nil, fmt.Errorf("incremental range boundary window row %d row key: %w", index, err)
		}
		if strings.TrimSpace(key) == "" {
			return nil, fmt.Errorf("incremental range boundary window row %d: %w", index, ErrIncrementalRangeBoundaryWindowRowKeyRequired)
		}
		if _, exists := window.keys[key]; exists {
			return nil, fmt.Errorf("incremental range boundary window row %d key %q: %w", index, key, ErrIncrementalRangeBoundaryWindowDuplicateKey)
		}
		if _, exists := pendingKeys[key]; exists {
			return nil, fmt.Errorf("incremental range boundary window row %d key %q: %w", index, key, ErrIncrementalRangeBoundaryWindowDuplicateKey)
		}
		if _, exists := row[window.outputColumn]; exists {
			return nil, fmt.Errorf("incremental range boundary window row %d: %w", index, ErrIncrementalRangeBoundaryWindowOutputConflict)
		}
		value, err := window.valueKey(row)
		if err != nil {
			return nil, fmt.Errorf("incremental range boundary window row %d value key: %w", index, err)
		}

		state, exists := states[partition]
		if !exists {
			state = cloneIncrementalRangeBoundaryWindowPartition(window.partitions[partition])
		}
		if state.hasOrder {
			comparison := 0
			if state.lastOrder < order {
				comparison = -1
			} else if state.lastOrder > order {
				comparison = 1
			} else {
				comparison = 0
			}
			if window.descending {
				comparison = -comparison
			}
			if comparison > 0 {
				return nil, fmt.Errorf("incremental range boundary window row %d partition %q: %w", index, partition, ErrIncrementalRangeBoundaryWindowOutOfOrder)
			}
		}
		state.lastOrder = order
		state.hasOrder = true
		states[partition] = state
		pendingKeys[key] = struct{}{}
		prepared = append(prepared, incrementalRangeBoundaryWindowPreparedRow{
			key:       key,
			row:       row,
			partition: partition,
			order:     order,
			value:     value,
		})
	}

	updates := make([]DifferentialRow, 0, len(prepared))
	for _, row := range prepared {
		state := states[row.partition]
		if window.kind == IncrementalRangeFirstValue {
			removeIncrementalRangeBoundaryWindowExpired(&state, row.order, window.preceding, window.descending)
		}
		samePeer := state.hasPeer && state.peerOrder == row.order
		if !samePeer {
			state.peerOrder = row.order
			state.hasPeer = true
			state.peers = state.peers[:0]
		}

		value := row.value
		if window.kind == IncrementalRangeFirstValue {
			state.active = append(state.active, incrementalRangeBoundaryWindowContribution{order: row.order, value: row.value})
			value = state.active[state.activeHead].value
		} else if samePeer {
			for _, peer := range state.peers {
				updates = append(updates, DifferentialRow{
					Key:  peer.key,
					Diff: -1,
					Row:  incrementalRangeWindowOutput(peer.row, window.outputColumn, peer.value),
				})
				updates = append(updates, DifferentialRow{
					Key:  peer.key,
					Diff: 1,
					Row:  incrementalRangeWindowOutput(peer.row, window.outputColumn, row.value),
				})
			}
		}
		updates = append(updates, DifferentialRow{
			Key:  row.key,
			Diff: 1,
			Row:  incrementalRangeWindowOutput(row.row, window.outputColumn, value),
		})
		state.peers = append(state.peers, incrementalRangeBoundaryWindowPeer{key: row.key, row: row.row, value: value})
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

func cloneIncrementalRangeBoundaryWindowPartition(source incrementalRangeBoundaryWindowPartition) incrementalRangeBoundaryWindowPartition {
	clone := source
	clone.active = append([]incrementalRangeBoundaryWindowContribution(nil), source.active...)
	clone.peers = append([]incrementalRangeBoundaryWindowPeer(nil), source.peers...)
	return clone
}

func removeIncrementalRangeBoundaryWindowExpired(state *incrementalRangeBoundaryWindowPartition, order, preceding int64, descending bool) {
	if state == nil {
		return
	}
	if descending {
		upper := incrementalRangeBoundaryWindowUpperBound(order, preceding)
		for state.activeHead < len(state.active) && state.active[state.activeHead].order > upper {
			state.activeHead++
		}
	} else {
		lower := incrementalRangeBoundaryWindowLowerBound(order, preceding)
		for state.activeHead < len(state.active) && state.active[state.activeHead].order < lower {
			state.activeHead++
		}
	}
	if state.activeHead >= len(state.active) {
		state.active = state.active[:0]
		state.activeHead = 0
		return
	}
	if state.activeHead > 0 && (state.activeHead >= 64 || state.activeHead*2 >= len(state.active)) {
		copy(state.active, state.active[state.activeHead:])
		state.active = state.active[:len(state.active)-state.activeHead]
		state.activeHead = 0
	}
}

func incrementalRangeBoundaryWindowLowerBound(order, preceding int64) int64 {
	if preceding > 0 && order < minIncrementalRangeBoundaryWindowInt64+preceding {
		return minIncrementalRangeBoundaryWindowInt64
	}
	return order - preceding
}

func incrementalRangeBoundaryWindowUpperBound(order, preceding int64) int64 {
	if preceding > 0 && order > maxIncrementalRangeBoundaryWindowInt64-preceding {
		return maxIncrementalRangeBoundaryWindowInt64
	}
	return order + preceding
}

const (
	maxIncrementalRangeBoundaryWindowInt64 int64 = 1<<63 - 1
	minIncrementalRangeBoundaryWindowInt64 int64 = -1 << 63
)
