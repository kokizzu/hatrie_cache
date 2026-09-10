package hatSql

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrIncrementalFrameWindowNil                 = errors.New("incremental frame window is nil")
	ErrIncrementalFrameWindowInvalidKind         = errors.New("incremental frame window kind is invalid")
	ErrIncrementalFrameWindowOutputRequired      = errors.New("incremental frame window output column is required")
	ErrIncrementalFrameWindowOrderRequired       = errors.New("incremental frame window order key is required")
	ErrIncrementalFrameWindowRowKeyRequired      = errors.New("incremental frame window row key is required")
	ErrIncrementalFrameWindowValueRequired       = errors.New("incremental frame window value key is required")
	ErrIncrementalFrameWindowNegativeFrame       = errors.New("incremental frame window preceding bound must be non-negative")
	ErrIncrementalFrameWindowDuplicateKey        = errors.New("incremental frame window row key already exists")
	ErrIncrementalFrameWindowOutputConflict      = errors.New("incremental frame window output column conflicts with input")
	ErrIncrementalFrameWindowOutOfOrder          = errors.New("incremental frame window row is out of order")
	ErrIncrementalFrameWindowSumValueInvalid     = errors.New("incremental frame window SUM value must be int64 or nil")
	ErrIncrementalFrameWindowExtremaValueInvalid = errors.New("incremental frame window MIN/MAX value must be int64 or nil")
	ErrIncrementalFrameWindowAvgValueInvalid     = errors.New("incremental frame window AVG value must be int64 or nil")
	ErrIncrementalFrameWindowSumOverflow         = errors.New("incremental frame window SUM overflows int64")
)

// IncrementalFrameWindowKind selects the aggregate maintained for a bounded
// ROWS BETWEEN N PRECEDING AND CURRENT ROW frame.
type IncrementalFrameWindowKind uint8

const (
	IncrementalWindowFrameCount IncrementalFrameWindowKind = iota + 1
	IncrementalWindowFrameSumInt64
	IncrementalWindowFrameMinInt64
	IncrementalWindowFrameMaxInt64
	IncrementalWindowFrameAvgInt64
)

// IncrementalFrameWindowDefinition configures an append-only bounded frame
// maintainer. Rows must arrive in non-decreasing order within each partition,
// unless Descending is enabled. FramePreceding is the N in ROWS BETWEEN N
// PRECEDING AND CURRENT ROW.
type IncrementalFrameWindowDefinition struct {
	Kind           IncrementalFrameWindowKind
	OutputColumn   string
	PartitionKey   IncrementalWindowPartitionKeyFunc
	OrderKey       IncrementalWindowOrderKeyFunc
	RowKey         IncrementalWindowRowKeyFunc
	ValueKey       IncrementalOffsetWindowValueKeyFunc
	FramePreceding int
	Descending     bool
}

// IncrementalFrameWindow maintains an append-only bounded COUNT(*), SUM(int64),
// AVG(int64), MIN(int64), or MAX(int64) frame. It retains at most
// FramePreceding+1 contributions per partition and emits one positive
// differential row for each appended row.
type IncrementalFrameWindow struct {
	kind           IncrementalFrameWindowKind
	outputColumn   string
	partitionKey   IncrementalWindowPartitionKeyFunc
	orderKey       IncrementalWindowOrderKeyFunc
	rowKey         IncrementalWindowRowKeyFunc
	valueKey       IncrementalOffsetWindowValueKeyFunc
	framePreceding int
	descending     bool
	partitions     map[string]incrementalFrameWindowPartition
	keys           map[string]struct{}
}

type incrementalFrameWindowPartition struct {
	lastOrder      interface{}
	hasOrder       bool
	contributions  []incrementalFrameWindowContribution
	sum            int64
	validSumCount  int
	monotonic      []incrementalFrameWindowExtremaEntry
	monotonicHead  int
	monotonicCount int
	nextSequence   uint64
}

type incrementalFrameWindowContribution struct {
	sequence uint64
	value    int64
	valid    bool
}

type incrementalFrameWindowExtremaEntry struct {
	sequence uint64
	value    int64
}

type incrementalFrameWindowPreparedRow struct {
	key          string
	row          Row
	partition    string
	contribution incrementalFrameWindowContribution
}

// NewIncrementalFrameWindow creates an empty append-only bounded frame
// maintainer.
func NewIncrementalFrameWindow(definition IncrementalFrameWindowDefinition) (*IncrementalFrameWindow, error) {
	if definition.Kind != IncrementalWindowFrameCount &&
		definition.Kind != IncrementalWindowFrameSumInt64 &&
		definition.Kind != IncrementalWindowFrameMinInt64 &&
		definition.Kind != IncrementalWindowFrameMaxInt64 &&
		definition.Kind != IncrementalWindowFrameAvgInt64 {
		return nil, ErrIncrementalFrameWindowInvalidKind
	}
	outputColumn := strings.TrimSpace(definition.OutputColumn)
	if outputColumn == "" {
		return nil, ErrIncrementalFrameWindowOutputRequired
	}
	if definition.OrderKey == nil {
		return nil, ErrIncrementalFrameWindowOrderRequired
	}
	if definition.RowKey == nil {
		return nil, ErrIncrementalFrameWindowRowKeyRequired
	}
	if definition.Kind != IncrementalWindowFrameCount && definition.ValueKey == nil {
		return nil, ErrIncrementalFrameWindowValueRequired
	}
	if definition.FramePreceding < 0 {
		return nil, ErrIncrementalFrameWindowNegativeFrame
	}
	return &IncrementalFrameWindow{
		kind:           definition.Kind,
		outputColumn:   outputColumn,
		partitionKey:   definition.PartitionKey,
		orderKey:       definition.OrderKey,
		rowKey:         definition.RowKey,
		valueKey:       definition.ValueKey,
		framePreceding: definition.FramePreceding,
		descending:     definition.Descending,
		partitions:     make(map[string]incrementalFrameWindowPartition),
		keys:           make(map[string]struct{}),
	}, nil
}

// Append applies an ordered insert batch and returns the derived value for
// each new row. Validation, callback execution, and aggregate arithmetic are
// atomic: an error leaves the window unchanged.
func (window *IncrementalFrameWindow) Append(rows []Row) ([]DifferentialRow, error) {
	if window == nil {
		return nil, ErrIncrementalFrameWindowNil
	}
	if len(rows) == 0 {
		return nil, nil
	}

	prepared := make([]incrementalFrameWindowPreparedRow, 0, len(rows))
	states := make(map[string]incrementalFrameWindowPartition)
	pendingKeys := make(map[string]struct{}, len(rows))
	for index, row := range rows {
		partition := ""
		if window.partitionKey != nil {
			value, err := window.partitionKey(row)
			if err != nil {
				return nil, fmt.Errorf("incremental frame window row %d partition key: %w", index, err)
			}
			partition = value
		}
		order, err := window.orderKey(row)
		if err != nil {
			return nil, fmt.Errorf("incremental frame window row %d order key: %w", index, err)
		}
		key, err := window.rowKey(row)
		if err != nil {
			return nil, fmt.Errorf("incremental frame window row %d row key: %w", index, err)
		}
		if strings.TrimSpace(key) == "" {
			return nil, fmt.Errorf("incremental frame window row %d: %w", index, ErrIncrementalFrameWindowRowKeyRequired)
		}
		if _, exists := window.keys[key]; exists {
			return nil, fmt.Errorf("incremental frame window row %d key %q: %w", index, key, ErrIncrementalFrameWindowDuplicateKey)
		}
		if _, exists := pendingKeys[key]; exists {
			return nil, fmt.Errorf("incremental frame window row %d key %q: %w", index, key, ErrIncrementalFrameWindowDuplicateKey)
		}
		if _, exists := row[window.outputColumn]; exists {
			return nil, fmt.Errorf("incremental frame window row %d: %w", index, ErrIncrementalFrameWindowOutputConflict)
		}

		contribution := incrementalFrameWindowContribution{valid: true}
		if window.kind != IncrementalWindowFrameCount {
			value, err := window.valueKey(row)
			if err != nil {
				return nil, fmt.Errorf("incremental frame window row %d value key: %w", index, err)
			}
			if value == nil {
				contribution.valid = false
			} else {
				intValue, ok := value.(int64)
				if !ok {
					err := ErrIncrementalFrameWindowSumValueInvalid
					if window.kind == IncrementalWindowFrameMinInt64 || window.kind == IncrementalWindowFrameMaxInt64 {
						err = ErrIncrementalFrameWindowExtremaValueInvalid
					} else if window.kind == IncrementalWindowFrameAvgInt64 {
						err = ErrIncrementalFrameWindowAvgValueInvalid
					}
					return nil, fmt.Errorf("incremental frame window row %d: %w", index, err)
				}
				contribution.value = intValue
			}
		}

		state, exists := states[partition]
		if !exists {
			state = cloneIncrementalFrameWindowPartition(window.partitions[partition])
		}
		if state.hasOrder {
			comparison := sqlCompare(state.lastOrder, order)
			if window.descending {
				comparison = -comparison
			}
			if comparison > 0 {
				return nil, fmt.Errorf("incremental frame window row %d partition %q: %w", index, partition, ErrIncrementalFrameWindowOutOfOrder)
			}
		}
		state.lastOrder = order
		state.hasOrder = true
		states[partition] = state
		pendingKeys[key] = struct{}{}
		prepared = append(prepared, incrementalFrameWindowPreparedRow{
			key:          key,
			row:          row,
			partition:    partition,
			contribution: contribution,
		})
	}

	updates := make([]DifferentialRow, 0, len(prepared))
	for _, row := range prepared {
		state := states[row.partition]
		sequence := state.nextSequence
		state.nextSequence++
		row.contribution.sequence = sequence
		if len(state.contributions) > window.framePreceding {
			outgoing := state.contributions[0]
			if window.framePreceding == 0 {
				state.contributions = state.contributions[:0]
			} else {
				state.contributions = state.contributions[1:]
			}
			if (window.kind == IncrementalWindowFrameSumInt64 || window.kind == IncrementalWindowFrameAvgInt64) && outgoing.valid {
				if state.validSumCount == 1 {
					state.sum = 0
				} else {
					newSum, err := subtractIncrementalFrameWindowSum(state.sum, outgoing.value)
					if err != nil {
						return nil, fmt.Errorf("incremental frame window partition %q: %w", row.partition, err)
					}
					state.sum = newSum
				}
				state.validSumCount--
			}
		}
		state.contributions = append(state.contributions, row.contribution)
		value := interface{}(int64(len(state.contributions)))
		if window.kind == IncrementalWindowFrameSumInt64 || window.kind == IncrementalWindowFrameAvgInt64 {
			if row.contribution.valid {
				newSum, err := addIncrementalFrameWindowSum(state.sum, row.contribution.value)
				if err != nil {
					return nil, fmt.Errorf("incremental frame window partition %q: %w", row.partition, err)
				}
				state.sum = newSum
				state.validSumCount++
			}
			if state.validSumCount == 0 {
				value = nil
			} else if window.kind == IncrementalWindowFrameSumInt64 {
				value = state.sum
			} else {
				value = float64(state.sum) / float64(state.validSumCount)
			}
		} else if window.kind == IncrementalWindowFrameMinInt64 || window.kind == IncrementalWindowFrameMaxInt64 {
			value = window.appendIncrementalFrameWindowExtrema(&state, row.contribution)
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

func cloneIncrementalFrameWindowPartition(state incrementalFrameWindowPartition) incrementalFrameWindowPartition {
	state.contributions = append([]incrementalFrameWindowContribution(nil), state.contributions...)
	state.monotonic = append([]incrementalFrameWindowExtremaEntry(nil), state.monotonic...)
	return state
}

func (window *IncrementalFrameWindow) appendIncrementalFrameWindowExtrema(state *incrementalFrameWindowPartition, contribution incrementalFrameWindowContribution) interface{} {
	if len(state.contributions) > 0 && state.monotonicCount > 0 {
		oldest := state.contributions[0].sequence
		for state.monotonicCount > 0 && state.monotonic[state.monotonicHead].sequence < oldest {
			state.monotonicHead++
			if state.monotonicHead == len(state.monotonic) {
				state.monotonicHead = 0
			}
			state.monotonicCount--
		}
		if state.monotonicCount == 0 {
			state.monotonicHead = 0
		}
	}
	if contribution.valid {
		if len(state.monotonic) == 0 {
			state.monotonic = make([]incrementalFrameWindowExtremaEntry, window.framePreceding+1)
		}
		for state.monotonicCount > 0 {
			lastIndex := state.monotonicHead + state.monotonicCount - 1
			if lastIndex >= len(state.monotonic) {
				lastIndex -= len(state.monotonic)
			}
			last := state.monotonic[lastIndex]
			remove := last.value >= contribution.value
			if window.kind == IncrementalWindowFrameMaxInt64 {
				remove = last.value <= contribution.value
			}
			if !remove {
				break
			}
			state.monotonicCount--
		}
		if state.monotonicCount == 0 {
			state.monotonicHead = 0
		}
		tailIndex := state.monotonicHead + state.monotonicCount
		if tailIndex >= len(state.monotonic) {
			tailIndex -= len(state.monotonic)
		}
		state.monotonic[tailIndex] = incrementalFrameWindowExtremaEntry{
			sequence: contribution.sequence,
			value:    contribution.value,
		}
		state.monotonicCount++
	}
	if state.monotonicCount > 0 {
		return state.monotonic[state.monotonicHead].value
	}
	return nil
}

func incrementalFrameWindowOutput(row Row, outputColumn string, value interface{}) Row {
	output := make(Row, len(row)+1)
	for column, fieldValue := range row {
		output[column] = fieldValue
	}
	output[outputColumn] = value
	return output
}

func addIncrementalFrameWindowSum(left, right int64) (int64, error) {
	if right > 0 && left > maxIncrementalFrameWindowInt64-right {
		return 0, ErrIncrementalFrameWindowSumOverflow
	}
	if right < 0 && left < minIncrementalFrameWindowInt64-right {
		return 0, ErrIncrementalFrameWindowSumOverflow
	}
	return left + right, nil
}

func subtractIncrementalFrameWindowSum(left, right int64) (int64, error) {
	if right > 0 && left < minIncrementalFrameWindowInt64+right {
		return 0, ErrIncrementalFrameWindowSumOverflow
	}
	if right < 0 && left > maxIncrementalFrameWindowInt64+right {
		return 0, ErrIncrementalFrameWindowSumOverflow
	}
	return left - right, nil
}

const (
	maxIncrementalFrameWindowInt64 int64 = 1<<63 - 1
	minIncrementalFrameWindowInt64 int64 = -1 << 63
)
