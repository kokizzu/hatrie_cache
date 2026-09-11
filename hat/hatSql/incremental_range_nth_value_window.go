package hatSql

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

var (
	ErrIncrementalRangeNthValueWindowNil             = errors.New("incremental RANGE NTH_VALUE window is nil")
	ErrIncrementalRangeNthValueWindowOutputRequired  = errors.New("incremental RANGE NTH_VALUE window output column is required")
	ErrIncrementalRangeNthValueWindowOrderRequired   = errors.New("incremental RANGE NTH_VALUE window order key is required")
	ErrIncrementalRangeNthValueWindowOrderInvalid    = errors.New("incremental RANGE NTH_VALUE window order key must be int64")
	ErrIncrementalRangeNthValueWindowRowKeyRequired  = errors.New("incremental RANGE NTH_VALUE window row key is required")
	ErrIncrementalRangeNthValueWindowValueRequired   = errors.New("incremental RANGE NTH_VALUE window value key is required")
	ErrIncrementalRangeNthValueWindowInvalidPosition = errors.New("incremental RANGE NTH_VALUE window position must be positive")
	ErrIncrementalRangeNthValueWindowNegativeFrame   = errors.New("incremental RANGE NTH_VALUE window preceding bound must be non-negative")
	ErrIncrementalRangeNthValueWindowDuplicateKey    = errors.New("incremental RANGE NTH_VALUE window row key already exists")
	ErrIncrementalRangeNthValueWindowOutputConflict  = errors.New("incremental RANGE NTH_VALUE window output column conflicts with input")
	ErrIncrementalRangeNthValueWindowOutOfOrder      = errors.New("incremental RANGE NTH_VALUE window row is out of order")
)

// IncrementalRangeNthValueWindowDefinition configures an append-only,
// peer-aware NTH_VALUE maintainer for RANGE BETWEEN FramePreceding PRECEDING
// AND CURRENT ROW. Position is one-based.
// Order keys are int64 values and rows must arrive monotonically within each
// partition, unless Descending is enabled.
type IncrementalRangeNthValueWindowDefinition struct {
	Position       int
	OutputColumn   string
	PartitionKey   IncrementalWindowPartitionKeyFunc
	OrderKey       IncrementalWindowOrderKeyFunc
	RowKey         IncrementalWindowRowKeyFunc
	ValueKey       IncrementalOffsetWindowValueKeyFunc
	FramePreceding int64
	Descending     bool
}

// IncrementalRangeNthValueWindow maintains append-only NTH_VALUE results for
// an inclusive numeric RANGE frame. It retains only the active frame rows and
// the current peer group per partition; no result history is retained.
type IncrementalRangeNthValueWindow struct {
	position     int
	outputColumn string
	partitionKey IncrementalWindowPartitionKeyFunc
	orderKey     IncrementalWindowOrderKeyFunc
	rowKey       IncrementalWindowRowKeyFunc
	valueKey     IncrementalOffsetWindowValueKeyFunc
	preceding    int64
	descending   bool
	partitions   map[string]incrementalRangeNthValueWindowPartition
	keys         map[string]struct{}
}

type incrementalRangeNthValueWindowPartition struct {
	lastOrder  int64
	hasOrder   bool
	active     []incrementalRangeNthValueWindowContribution
	activeHead int
	peerOrder  int64
	hasPeer    bool
	peers      []incrementalRangeNthValueWindowPeer
}

type incrementalRangeNthValueWindowContribution struct {
	order int64
	value interface{}
}

type incrementalRangeNthValueWindowPeer struct {
	key    string
	row    []incrementalRangeNthValueWindowField
	output interface{}
}

type incrementalRangeNthValueWindowField struct {
	key   string
	value interface{}
}

type incrementalRangeNthValueWindowPreparedRow struct {
	key       string
	row       Row
	partition string
	order     int64
	value     interface{}
}

// NewIncrementalRangeNthValueWindow creates an empty append-only numeric
// RANGE NTH_VALUE maintainer.
func NewIncrementalRangeNthValueWindow(definition IncrementalRangeNthValueWindowDefinition) (*IncrementalRangeNthValueWindow, error) {
	if definition.Position <= 0 {
		return nil, ErrIncrementalRangeNthValueWindowInvalidPosition
	}
	outputColumn := strings.TrimSpace(definition.OutputColumn)
	if outputColumn == "" {
		return nil, ErrIncrementalRangeNthValueWindowOutputRequired
	}
	if definition.OrderKey == nil {
		return nil, ErrIncrementalRangeNthValueWindowOrderRequired
	}
	if definition.RowKey == nil {
		return nil, ErrIncrementalRangeNthValueWindowRowKeyRequired
	}
	if definition.ValueKey == nil {
		return nil, ErrIncrementalRangeNthValueWindowValueRequired
	}
	if definition.FramePreceding < 0 {
		return nil, ErrIncrementalRangeNthValueWindowNegativeFrame
	}
	return &IncrementalRangeNthValueWindow{
		position:     definition.Position,
		outputColumn: outputColumn,
		partitionKey: definition.PartitionKey,
		orderKey:     definition.OrderKey,
		rowKey:       definition.RowKey,
		valueKey:     definition.ValueKey,
		preceding:    definition.FramePreceding,
		descending:   definition.Descending,
		partitions:   make(map[string]incrementalRangeNthValueWindowPartition),
		keys:         make(map[string]struct{}),
	}, nil
}

// Append applies an ordered insert batch and returns differential rows for
// the new rows plus any earlier rows in the same peer group whose NTH_VALUE
// changed. Callback validation and state changes are atomic on error.
func (window *IncrementalRangeNthValueWindow) Append(rows []Row) ([]DifferentialRow, error) {
	if window == nil {
		return nil, ErrIncrementalRangeNthValueWindowNil
	}
	if len(rows) == 0 {
		return nil, nil
	}

	prepared := make([]incrementalRangeNthValueWindowPreparedRow, 0, len(rows))
	states := make(map[string]incrementalRangeNthValueWindowPartition)
	pendingKeys := make(map[string]struct{}, len(rows))
	for index, row := range rows {
		partition := ""
		if window.partitionKey != nil {
			value, err := window.partitionKey(row)
			if err != nil {
				return nil, fmt.Errorf("incremental RANGE NTH_VALUE window row %d partition key: %w", index, err)
			}
			partition = value
		}
		orderValue, err := window.orderKey(row)
		if err != nil {
			return nil, fmt.Errorf("incremental RANGE NTH_VALUE window row %d order key: %w", index, err)
		}
		order, ok := orderValue.(int64)
		if !ok {
			return nil, fmt.Errorf("incremental RANGE NTH_VALUE window row %d: %w", index, ErrIncrementalRangeNthValueWindowOrderInvalid)
		}
		key, err := window.rowKey(row)
		if err != nil {
			return nil, fmt.Errorf("incremental RANGE NTH_VALUE window row %d row key: %w", index, err)
		}
		if strings.TrimSpace(key) == "" {
			return nil, fmt.Errorf("incremental RANGE NTH_VALUE window row %d: %w", index, ErrIncrementalRangeNthValueWindowRowKeyRequired)
		}
		if _, exists := window.keys[key]; exists {
			return nil, fmt.Errorf("incremental RANGE NTH_VALUE window row %d key %q: %w", index, key, ErrIncrementalRangeNthValueWindowDuplicateKey)
		}
		if _, exists := pendingKeys[key]; exists {
			return nil, fmt.Errorf("incremental RANGE NTH_VALUE window row %d key %q: %w", index, key, ErrIncrementalRangeNthValueWindowDuplicateKey)
		}
		if _, exists := row[window.outputColumn]; exists {
			return nil, fmt.Errorf("incremental RANGE NTH_VALUE window row %d: %w", index, ErrIncrementalRangeNthValueWindowOutputConflict)
		}
		value, err := window.valueKey(row)
		if err != nil {
			return nil, fmt.Errorf("incremental RANGE NTH_VALUE window row %d value key: %w", index, err)
		}

		state, exists := states[partition]
		if !exists {
			state = cloneIncrementalRangeNthValueWindowPartition(window.partitions[partition])
		}
		if state.hasOrder && ((window.descending && order > state.lastOrder) || (!window.descending && order < state.lastOrder)) {
			return nil, fmt.Errorf("incremental RANGE NTH_VALUE window row %d partition %q: %w", index, partition, ErrIncrementalRangeNthValueWindowOutOfOrder)
		}
		state.lastOrder = order
		state.hasOrder = true
		states[partition] = state
		pendingKeys[key] = struct{}{}
		prepared = append(prepared, incrementalRangeNthValueWindowPreparedRow{
			key:       key,
			row:       row,
			partition: partition,
			order:     order,
			value:     value,
		})
	}

	updates := make([]DifferentialRow, 0, len(prepared))
	for _, preparedRow := range prepared {
		state := states[preparedRow.partition]
		expireIncrementalRangeNthValueWindow(&state, preparedRow.order, window.preceding, window.descending)
		samePeer := state.hasPeer && state.peerOrder == preparedRow.order
		if !samePeer {
			state.peers = state.peers[:0]
			state.peerOrder = preparedRow.order
			state.hasPeer = true
		}

		state.active = append(state.active, incrementalRangeNthValueWindowContribution{
			order: preparedRow.order,
			value: preparedRow.value,
		})
		value := incrementalRangeNthValueWindowValue(state, window.position)
		if samePeer {
			for index := range state.peers {
				peer := &state.peers[index]
				if reflect.DeepEqual(peer.output, value) {
					continue
				}
				updates = append(updates,
					DifferentialRow{Key: peer.key, Diff: -1, Row: incrementalRangeNthValueWindowOutputFromSnapshot(peer.row, window.outputColumn, peer.output)},
					DifferentialRow{Key: peer.key, Diff: 1, Row: incrementalRangeNthValueWindowOutputFromSnapshot(peer.row, window.outputColumn, value)},
				)
				peer.output = value
			}
		}
		updates = append(updates, DifferentialRow{
			Key:  preparedRow.key,
			Diff: 1,
			Row:  incrementalRangeNthValueWindowOutput(preparedRow.row, window.outputColumn, value),
		})
		state.peers = append(state.peers, incrementalRangeNthValueWindowPeer{
			key:    preparedRow.key,
			row:    snapshotIncrementalRangeNthValueWindowRow(preparedRow.row),
			output: value,
		})
		states[preparedRow.partition] = state
	}

	for partition, state := range states {
		window.partitions[partition] = state
	}
	for key := range pendingKeys {
		window.keys[key] = struct{}{}
	}
	return updates, nil
}

func cloneIncrementalRangeNthValueWindowPartition(source incrementalRangeNthValueWindowPartition) incrementalRangeNthValueWindowPartition {
	clone := source
	clone.active = append([]incrementalRangeNthValueWindowContribution(nil), source.active...)
	clone.peers = make([]incrementalRangeNthValueWindowPeer, len(source.peers))
	for index, peer := range source.peers {
		clone.peers[index] = incrementalRangeNthValueWindowPeer{
			key:    peer.key,
			row:    append([]incrementalRangeNthValueWindowField(nil), peer.row...),
			output: peer.output,
		}
	}
	return clone
}

func expireIncrementalRangeNthValueWindow(state *incrementalRangeNthValueWindowPartition, order, preceding int64, descending bool) {
	if state == nil {
		return
	}
	if descending {
		upper := incrementalRangeNthValueWindowUpperBound(order, preceding)
		for state.activeHead < len(state.active) && state.active[state.activeHead].order > upper {
			state.activeHead++
		}
	} else {
		lower := incrementalRangeNthValueWindowLowerBound(order, preceding)
		for state.activeHead < len(state.active) && state.active[state.activeHead].order < lower {
			state.activeHead++
		}
	}
	if state.activeHead > 0 && (state.activeHead >= 64 || state.activeHead*2 >= len(state.active)) {
		copy(state.active, state.active[state.activeHead:])
		state.active = state.active[:len(state.active)-state.activeHead]
		state.activeHead = 0
	}
}

func incrementalRangeNthValueWindowValue(state incrementalRangeNthValueWindowPartition, position int) interface{} {
	index := state.activeHead + position - 1
	if index < state.activeHead || index >= len(state.active) {
		return nil
	}
	return state.active[index].value
}

func incrementalRangeNthValueWindowLowerBound(order, preceding int64) int64 {
	if preceding > 0 && order < (-1<<63)+preceding {
		return -1 << 63
	}
	return order - preceding
}

func incrementalRangeNthValueWindowUpperBound(order, preceding int64) int64 {
	if preceding > 0 && order > (1<<63-1)-preceding {
		return 1<<63 - 1
	}
	return order + preceding
}

func incrementalRangeNthValueWindowOutput(row Row, outputColumn string, value interface{}) Row {
	output := make(Row, len(row)+1)
	for key, item := range row {
		output[key] = item
	}
	output[outputColumn] = value
	return output
}

func snapshotIncrementalRangeNthValueWindowRow(row Row) []incrementalRangeNthValueWindowField {
	snapshot := make([]incrementalRangeNthValueWindowField, 0, len(row))
	for key, value := range row {
		snapshot = append(snapshot, incrementalRangeNthValueWindowField{key: key, value: value})
	}
	return snapshot
}

func incrementalRangeNthValueWindowOutputFromSnapshot(snapshot []incrementalRangeNthValueWindowField, outputColumn string, value interface{}) Row {
	output := make(Row, len(snapshot)+1)
	for _, field := range snapshot {
		output[field.key] = field.value
	}
	output[outputColumn] = value
	return output
}
