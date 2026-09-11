package hatSql

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrIncrementalRangeWindowNil                  = errors.New("incremental range window is nil")
	ErrIncrementalRangeWindowInvalidKind          = errors.New("incremental range window kind is invalid")
	ErrIncrementalRangeWindowOutputRequired       = errors.New("incremental range window output column is required")
	ErrIncrementalRangeWindowOrderRequired        = errors.New("incremental range window order key is required")
	ErrIncrementalRangeWindowOrderInvalid         = errors.New("incremental range window order key must be int64")
	ErrIncrementalRangeWindowRowKeyRequired       = errors.New("incremental range window row key is required")
	ErrIncrementalRangeWindowValueRequired        = errors.New("incremental range window value key is required")
	ErrIncrementalRangeWindowNegativeFrame        = errors.New("incremental range window preceding bound must be non-negative")
	ErrIncrementalRangeWindowDuplicateKey         = errors.New("incremental range window row key already exists")
	ErrIncrementalRangeWindowOutputConflict       = errors.New("incremental range window output column conflicts with input")
	ErrIncrementalRangeWindowOutOfOrder           = errors.New("incremental range window row is out of order")
	ErrIncrementalRangeWindowSumValueInvalid      = errors.New("incremental range window SUM value must be int64 or nil")
	ErrIncrementalRangeWindowExtremaValueInvalid  = errors.New("incremental range window MIN/MAX value must be int64 or nil")
	ErrIncrementalRangeWindowDistinctValueInvalid = errors.New("incremental range window COUNT DISTINCT value must be int64 or nil")
	ErrIncrementalRangeWindowSumOverflow          = errors.New("incremental range window SUM overflows int64")
)

// IncrementalRangeWindowKind selects the aggregate maintained for an
// append-only RANGE BETWEEN N PRECEDING AND CURRENT ROW frame. Order keys are
// int64 values so the distance bound is exact and allocation-free.
type IncrementalRangeWindowKind uint8

const (
	IncrementalRangeWindowCount IncrementalRangeWindowKind = iota + 1
	IncrementalRangeWindowSumInt64
	IncrementalRangeWindowMinInt64
	IncrementalRangeWindowMaxInt64
	IncrementalRangeWindowCountDistinctInt64
)

// IncrementalRangeWindowDefinition configures an append-only, peer-aware
// RANGE frame. Rows must arrive in non-decreasing order within each partition,
// unless Descending is enabled. FramePreceding is the inclusive int64 distance
// in the ordering direction.
type IncrementalRangeWindowDefinition struct {
	Kind           IncrementalRangeWindowKind
	OutputColumn   string
	PartitionKey   IncrementalWindowPartitionKeyFunc
	OrderKey       IncrementalWindowOrderKeyFunc
	RowKey         IncrementalWindowRowKeyFunc
	ValueKey       IncrementalOffsetWindowValueKeyFunc
	FramePreceding int64
	Descending     bool
}

// IncrementalRangeWindow maintains append-only COUNT(*), COUNT(DISTINCT int64),
// SUM(int64), MIN(int64), or MAX(int64) values for an inclusive numeric RANGE
// frame. When a peer row arrives, prior rows in that peer group receive exact
// differential replacement events because SQL RANGE frames include all rows
// with the same order key.
type IncrementalRangeWindow struct {
	kind         IncrementalRangeWindowKind
	outputColumn string
	partitionKey IncrementalWindowPartitionKeyFunc
	orderKey     IncrementalWindowOrderKeyFunc
	rowKey       IncrementalWindowRowKeyFunc
	valueKey     IncrementalOffsetWindowValueKeyFunc
	preceding    int64
	descending   bool
	partitions   map[string]incrementalRangeWindowPartition
	keys         map[string]struct{}
}

type incrementalRangeWindowPartition struct {
	lastOrder      int64
	hasOrder       bool
	active         []incrementalRangeWindowContribution
	activeHead     int
	sum            int64
	validCount     int
	peerOrder      int64
	hasPeer        bool
	peers          []incrementalRangeWindowPeer
	monotonic      []incrementalRangeWindowExtremaEntry
	monotonicHead  int
	nextSequence   uint64
	distinctCounts map[int64]int
}

type incrementalRangeWindowContribution struct {
	sequence uint64
	order    int64
	value    int64
	valid    bool
}

type incrementalRangeWindowExtremaEntry struct {
	sequence uint64
	value    int64
}

type incrementalRangeWindowPeer struct {
	key    string
	row    []incrementalRangeWindowField
	output interface{}
}

type incrementalRangeWindowField struct {
	key   string
	value interface{}
}

type incrementalRangeWindowPreparedRow struct {
	key          string
	row          Row
	partition    string
	order        int64
	contribution incrementalRangeWindowContribution
}

// NewIncrementalRangeWindow creates an empty append-only numeric RANGE
// maintainer. The existing ROWS frame maintainer remains the lower-retention
// choice when peer-aware value-distance semantics are not needed.
func NewIncrementalRangeWindow(definition IncrementalRangeWindowDefinition) (*IncrementalRangeWindow, error) {
	if definition.Kind != IncrementalRangeWindowCount && definition.Kind != IncrementalRangeWindowSumInt64 && definition.Kind != IncrementalRangeWindowMinInt64 && definition.Kind != IncrementalRangeWindowMaxInt64 && definition.Kind != IncrementalRangeWindowCountDistinctInt64 {
		return nil, ErrIncrementalRangeWindowInvalidKind
	}
	outputColumn := strings.TrimSpace(definition.OutputColumn)
	if outputColumn == "" {
		return nil, ErrIncrementalRangeWindowOutputRequired
	}
	if definition.OrderKey == nil {
		return nil, ErrIncrementalRangeWindowOrderRequired
	}
	if definition.RowKey == nil {
		return nil, ErrIncrementalRangeWindowRowKeyRequired
	}
	if definition.Kind != IncrementalRangeWindowCount && definition.ValueKey == nil {
		return nil, ErrIncrementalRangeWindowValueRequired
	}
	if definition.FramePreceding < 0 {
		return nil, ErrIncrementalRangeWindowNegativeFrame
	}
	return &IncrementalRangeWindow{
		kind:         definition.Kind,
		outputColumn: outputColumn,
		partitionKey: definition.PartitionKey,
		orderKey:     definition.OrderKey,
		rowKey:       definition.RowKey,
		valueKey:     definition.ValueKey,
		preceding:    definition.FramePreceding,
		descending:   definition.Descending,
		partitions:   make(map[string]incrementalRangeWindowPartition),
		keys:         make(map[string]struct{}),
	}, nil
}

// Append applies an ordered insert batch and returns derived differential
// rows. Callback validation, peer replacements, and aggregate arithmetic are
// atomic: an error leaves the window unchanged.
func (window *IncrementalRangeWindow) Append(rows []Row) ([]DifferentialRow, error) {
	if window == nil {
		return nil, ErrIncrementalRangeWindowNil
	}
	if len(rows) == 0 {
		return nil, nil
	}

	prepared := make([]incrementalRangeWindowPreparedRow, 0, len(rows))
	states := make(map[string]incrementalRangeWindowPartition)
	pendingKeys := make(map[string]struct{}, len(rows))
	for index, row := range rows {
		partition := ""
		if window.partitionKey != nil {
			value, err := window.partitionKey(row)
			if err != nil {
				return nil, fmt.Errorf("incremental range window row %d partition key: %w", index, err)
			}
			partition = value
		}
		orderValue, err := window.orderKey(row)
		if err != nil {
			return nil, fmt.Errorf("incremental range window row %d order key: %w", index, err)
		}
		order, ok := orderValue.(int64)
		if !ok {
			return nil, fmt.Errorf("incremental range window row %d: %w", index, ErrIncrementalRangeWindowOrderInvalid)
		}
		key, err := window.rowKey(row)
		if err != nil {
			return nil, fmt.Errorf("incremental range window row %d row key: %w", index, err)
		}
		if strings.TrimSpace(key) == "" {
			return nil, fmt.Errorf("incremental range window row %d: %w", index, ErrIncrementalRangeWindowRowKeyRequired)
		}
		if _, exists := window.keys[key]; exists {
			return nil, fmt.Errorf("incremental range window row %d key %q: %w", index, key, ErrIncrementalRangeWindowDuplicateKey)
		}
		if _, exists := pendingKeys[key]; exists {
			return nil, fmt.Errorf("incremental range window row %d key %q: %w", index, key, ErrIncrementalRangeWindowDuplicateKey)
		}
		if _, exists := row[window.outputColumn]; exists {
			return nil, fmt.Errorf("incremental range window row %d: %w", index, ErrIncrementalRangeWindowOutputConflict)
		}

		contribution := incrementalRangeWindowContribution{order: order, valid: true}
		if window.kind != IncrementalRangeWindowCount {
			value, err := window.valueKey(row)
			if err != nil {
				return nil, fmt.Errorf("incremental range window row %d value key: %w", index, err)
			}
			if value == nil {
				contribution.valid = false
			} else {
				intValue, ok := value.(int64)
				if !ok {
					err := ErrIncrementalRangeWindowSumValueInvalid
					if window.kind == IncrementalRangeWindowMinInt64 || window.kind == IncrementalRangeWindowMaxInt64 {
						err = ErrIncrementalRangeWindowExtremaValueInvalid
					} else if window.kind == IncrementalRangeWindowCountDistinctInt64 {
						err = ErrIncrementalRangeWindowDistinctValueInvalid
					}
					return nil, fmt.Errorf("incremental range window row %d: %w", index, err)
				}
				contribution.value = intValue
			}
		}

		state, exists := states[partition]
		if !exists {
			state = cloneIncrementalRangeWindowPartition(window.partitions[partition])
		}
		if state.hasOrder && ((window.descending && order > state.lastOrder) || (!window.descending && order < state.lastOrder)) {
			return nil, fmt.Errorf("incremental range window row %d partition %q: %w", index, partition, ErrIncrementalRangeWindowOutOfOrder)
		}
		state.lastOrder = order
		state.hasOrder = true
		states[partition] = state
		pendingKeys[key] = struct{}{}
		prepared = append(prepared, incrementalRangeWindowPreparedRow{
			key:          key,
			row:          row,
			partition:    partition,
			order:        order,
			contribution: contribution,
		})
	}

	updates := make([]DifferentialRow, 0, len(prepared))
	for _, preparedRow := range prepared {
		state := states[preparedRow.partition]
		if err := removeIncrementalRangeWindowExpired(&state, preparedRow.order, window.preceding, window.descending, window.kind); err != nil {
			return nil, fmt.Errorf("incremental range window partition %q: %w", preparedRow.partition, err)
		}
		samePeer := state.hasPeer && state.peerOrder == preparedRow.order
		if !samePeer {
			state.peers = state.peers[:0]
			state.peerOrder = preparedRow.order
			state.hasPeer = true
		}

		preparedRow.contribution.sequence = state.nextSequence
		state.nextSequence++
		state.active = append(state.active, preparedRow.contribution)
		if window.kind == IncrementalRangeWindowSumInt64 && preparedRow.contribution.valid {
			newSum, err := addIncrementalRangeWindowSum(state.sum, preparedRow.contribution.value)
			if err != nil {
				return nil, fmt.Errorf("incremental range window partition %q: %w", preparedRow.partition, err)
			}
			state.sum = newSum
			state.validCount++
		} else if (window.kind == IncrementalRangeWindowMinInt64 || window.kind == IncrementalRangeWindowMaxInt64) && preparedRow.contribution.valid {
			state.validCount++
			appendIncrementalRangeWindowExtrema(&state, preparedRow.contribution, window.kind)
		} else if window.kind == IncrementalRangeWindowCountDistinctInt64 {
			if preparedRow.contribution.valid {
				if state.distinctCounts == nil {
					state.distinctCounts = make(map[int64]int)
				}
				state.distinctCounts[preparedRow.contribution.value]++
			}
		}

		value := interface{}(int64(len(state.active) - state.activeHead))
		if window.kind == IncrementalRangeWindowSumInt64 {
			if state.validCount == 0 {
				value = nil
			} else {
				value = state.sum
			}
		} else if window.kind == IncrementalRangeWindowMinInt64 || window.kind == IncrementalRangeWindowMaxInt64 {
			if state.validCount == 0 {
				value = nil
			} else {
				value = state.monotonic[state.monotonicHead].value
			}
		} else if window.kind == IncrementalRangeWindowCountDistinctInt64 {
			value = int64(len(state.distinctCounts))
		}
		for index := range state.peers {
			peer := &state.peers[index]
			if samePeer {
				updates = append(updates,
					DifferentialRow{Key: peer.key, Diff: -1, Row: incrementalRangeWindowOutputFromSnapshot(peer.row, window.outputColumn, peer.output)},
					DifferentialRow{Key: peer.key, Diff: 1, Row: incrementalRangeWindowOutputFromSnapshot(peer.row, window.outputColumn, value)},
				)
				peer.output = value
			}
		}
		updates = append(updates, DifferentialRow{
			Key:  preparedRow.key,
			Diff: 1,
			Row:  incrementalRangeWindowOutput(preparedRow.row, window.outputColumn, value),
		})
		state.peers = append(state.peers, incrementalRangeWindowPeer{
			key: preparedRow.key, row: snapshotIncrementalRangeWindowRow(preparedRow.row), output: value,
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

func cloneIncrementalRangeWindowPartition(source incrementalRangeWindowPartition) incrementalRangeWindowPartition {
	clone := source
	clone.active = append([]incrementalRangeWindowContribution(nil), source.active...)
	clone.monotonic = append([]incrementalRangeWindowExtremaEntry(nil), source.monotonic...)
	if source.distinctCounts != nil {
		clone.distinctCounts = make(map[int64]int, len(source.distinctCounts))
		for value, count := range source.distinctCounts {
			clone.distinctCounts[value] = count
		}
	}
	clone.peers = make([]incrementalRangeWindowPeer, len(source.peers))
	for index, peer := range source.peers {
		clone.peers[index] = incrementalRangeWindowPeer{
			key: peer.key, row: append([]incrementalRangeWindowField(nil), peer.row...), output: peer.output,
		}
	}
	return clone
}

func cloneIncrementalRangeWindowRow(row Row) Row {
	clone := make(Row, len(row))
	for key, value := range row {
		clone[key] = value
	}
	return clone
}

func incrementalRangeWindowOutput(row Row, outputColumn string, value interface{}) Row {
	output := make(Row, len(row)+1)
	for key, item := range row {
		output[key] = item
	}
	output[outputColumn] = value
	return output
}

func snapshotIncrementalRangeWindowRow(row Row) []incrementalRangeWindowField {
	snapshot := make([]incrementalRangeWindowField, 0, len(row))
	for key, value := range row {
		snapshot = append(snapshot, incrementalRangeWindowField{key: key, value: value})
	}
	return snapshot
}

func incrementalRangeWindowOutputFromSnapshot(snapshot []incrementalRangeWindowField, outputColumn string, value interface{}) Row {
	output := make(Row, len(snapshot)+1)
	for _, field := range snapshot {
		output[field.key] = field.value
	}
	output[outputColumn] = value
	return output
}

func appendIncrementalRangeWindowExtrema(state *incrementalRangeWindowPartition, contribution incrementalRangeWindowContribution, kind IncrementalRangeWindowKind) {
	if state == nil || !contribution.valid {
		return
	}
	minimum := kind == IncrementalRangeWindowMinInt64
	for len(state.monotonic) > state.monotonicHead {
		last := state.monotonic[len(state.monotonic)-1].value
		if (minimum && last < contribution.value) || (!minimum && last > contribution.value) {
			break
		}
		state.monotonic = state.monotonic[:len(state.monotonic)-1]
	}
	state.monotonic = append(state.monotonic, incrementalRangeWindowExtremaEntry{
		sequence: contribution.sequence,
		value:    contribution.value,
	})
}

func removeIncrementalRangeWindowExpired(state *incrementalRangeWindowPartition, order, preceding int64, descending bool, kind IncrementalRangeWindowKind) error {
	if state == nil {
		return nil
	}
	sumEnabled := kind == IncrementalRangeWindowSumInt64
	extremaEnabled := kind == IncrementalRangeWindowMinInt64 || kind == IncrementalRangeWindowMaxInt64
	distinctEnabled := kind == IncrementalRangeWindowCountDistinctInt64
	if descending {
		upper := incrementalRangeWindowUpperBound(order, preceding)
		for state.activeHead < len(state.active) && state.active[state.activeHead].order > upper {
			contribution := state.active[state.activeHead]
			state.activeHead++
			if sumEnabled && contribution.valid {
				newSum, err := subtractIncrementalRangeWindowSum(state.sum, contribution.value)
				if err != nil {
					return err
				}
				state.sum = newSum
				state.validCount--
			} else if extremaEnabled && contribution.valid {
				state.validCount--
			} else if distinctEnabled && contribution.valid {
				count := state.distinctCounts[contribution.value]
				if count <= 1 {
					delete(state.distinctCounts, contribution.value)
				} else {
					state.distinctCounts[contribution.value] = count - 1
				}
			}
		}
	} else {
		lower := incrementalRangeWindowLowerBound(order, preceding)
		for state.activeHead < len(state.active) && state.active[state.activeHead].order < lower {
			contribution := state.active[state.activeHead]
			state.activeHead++
			if sumEnabled && contribution.valid {
				newSum, err := subtractIncrementalRangeWindowSum(state.sum, contribution.value)
				if err != nil {
					return err
				}
				state.sum = newSum
				state.validCount--
			} else if extremaEnabled && contribution.valid {
				state.validCount--
			} else if distinctEnabled && contribution.valid {
				count := state.distinctCounts[contribution.value]
				if count <= 1 {
					delete(state.distinctCounts, contribution.value)
				} else {
					state.distinctCounts[contribution.value] = count - 1
				}
			}
		}
	}
	if extremaEnabled {
		if state.activeHead >= len(state.active) {
			state.monotonic = state.monotonic[:0]
			state.monotonicHead = 0
		} else {
			firstSequence := state.active[state.activeHead].sequence
			for state.monotonicHead < len(state.monotonic) && state.monotonic[state.monotonicHead].sequence < firstSequence {
				state.monotonicHead++
			}
			if state.monotonicHead > 0 && (state.monotonicHead >= 64 || state.monotonicHead*2 >= len(state.monotonic)) {
				copy(state.monotonic, state.monotonic[state.monotonicHead:])
				state.monotonic = state.monotonic[:len(state.monotonic)-state.monotonicHead]
				state.monotonicHead = 0
			}
		}
	}
	if state.activeHead > 0 && (state.activeHead >= 64 || state.activeHead*2 >= len(state.active)) {
		copy(state.active, state.active[state.activeHead:])
		state.active = state.active[:len(state.active)-state.activeHead]
		state.activeHead = 0
	}
	return nil
}

func incrementalRangeWindowLowerBound(order, preceding int64) int64 {
	minimum := int64(-1 << 63)
	if preceding > 0 && order < minimum+preceding {
		return minimum
	}
	return order - preceding
}

func incrementalRangeWindowUpperBound(order, preceding int64) int64 {
	maximum := int64(1<<63 - 1)
	if preceding > 0 && order > maximum-preceding {
		return maximum
	}
	return order + preceding
}

func addIncrementalRangeWindowSum(left, right int64) (int64, error) {
	maximum := int64(1<<63 - 1)
	minimum := int64(-1 << 63)
	if (right > 0 && left > maximum-right) || (right < 0 && left < minimum-right) {
		return 0, ErrIncrementalRangeWindowSumOverflow
	}
	return left + right, nil
}

func subtractIncrementalRangeWindowSum(left, right int64) (int64, error) {
	minimum := int64(-1 << 63)
	maximum := int64(1<<63 - 1)
	if (right > 0 && left < minimum+right) || (right < 0 && left > maximum+right) {
		return 0, ErrIncrementalRangeWindowSumOverflow
	}
	return left - right, nil
}
