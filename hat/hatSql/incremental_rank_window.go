package hatSql

import (
	"errors"
	"fmt"
	"math"
	"strings"
)

var (
	// ErrIncrementalRankWindowNil reports a method call on a nil window.
	ErrIncrementalRankWindowNil = errors.New("incremental rank window is nil")
	// ErrIncrementalWindowInvalidKind reports an unsupported rank function.
	ErrIncrementalWindowInvalidKind = errors.New("incremental rank window kind is invalid")
	// ErrIncrementalWindowOutputColumnRequired reports a missing output field.
	ErrIncrementalWindowOutputColumnRequired = errors.New("incremental rank window output column is required")
	// ErrIncrementalWindowOrderKeyRequired reports a missing order callback.
	ErrIncrementalWindowOrderKeyRequired = errors.New("incremental rank window order key is required")
	// ErrIncrementalWindowRowKeyRequired reports a missing row identity callback.
	ErrIncrementalWindowRowKeyRequired = errors.New("incremental rank window row key is required")
	// ErrIncrementalWindowOutOfOrder reports a row that would invalidate the
	// append-only order within its partition.
	ErrIncrementalWindowOutOfOrder = errors.New("incremental rank window row is out of order")
	// ErrIncrementalWindowDuplicateKey reports a repeated append identity.
	ErrIncrementalWindowDuplicateKey = errors.New("incremental rank window row key is duplicated")
	// ErrIncrementalWindowOutputConflict reports an input field collision.
	ErrIncrementalWindowOutputConflict = errors.New("incremental rank window output column conflicts with input")
	// ErrIncrementalWindowCountOverflow reports exhausted int64 rank space.
	ErrIncrementalWindowCountOverflow = errors.New("incremental rank window count overflowed")
)

// IncrementalRankWindowKind selects the rank value written to each emitted
// row. Rows must arrive in non-decreasing order within each partition, unless
// Descending is enabled.
type IncrementalRankWindowKind uint8

const (
	IncrementalWindowRowNumber IncrementalRankWindowKind = iota + 1
	IncrementalWindowRank
	IncrementalWindowDenseRank
)

// IncrementalWindowPartitionKeyFunc identifies a logical window partition.
// A nil callback puts every row in one partition.
type IncrementalWindowPartitionKeyFunc func(Row) (string, error)

// IncrementalWindowOrderKeyFunc returns the SQL-comparable order value.
type IncrementalWindowOrderKeyFunc func(Row) (interface{}, error)

// IncrementalWindowRowKeyFunc returns a stable unique identity for an
// append-only row. The identity is also used as DifferentialRow.Key.
type IncrementalWindowRowKeyFunc func(Row) (string, error)

// IncrementalRankWindowDefinition configures an append-only rank maintainer.
// The output column must not already exist in an input row because the
// maintainer preserves the input row and adds exactly one derived value.
type IncrementalRankWindowDefinition struct {
	Kind         IncrementalRankWindowKind
	OutputColumn string
	PartitionKey IncrementalWindowPartitionKeyFunc
	OrderKey     IncrementalWindowOrderKeyFunc
	RowKey       IncrementalWindowRowKeyFunc
	Descending   bool
}

// IncrementalRankWindow maintains one ROW_NUMBER, RANK, or DENSE_RANK value
// per append-only partition. Append validates the entire batch before
// publishing any state, so callback failures, duplicate identities, and
// out-of-order rows are atomic failures.
type IncrementalRankWindow struct {
	kind         IncrementalRankWindowKind
	outputColumn string
	partitionKey IncrementalWindowPartitionKeyFunc
	orderKey     IncrementalWindowOrderKeyFunc
	rowKey       IncrementalWindowRowKeyFunc
	descending   bool
	partitions   map[string]incrementalRankWindowPartition
	keys         map[string]struct{}
}

type incrementalRankWindowPartition struct {
	count     int64
	rank      int64
	denseRank int64
	order     interface{}
	hasOrder  bool
}

// NewIncrementalRankWindow creates an empty append-only rank maintainer.
func NewIncrementalRankWindow(definition IncrementalRankWindowDefinition) (*IncrementalRankWindow, error) {
	if definition.Kind != IncrementalWindowRowNumber && definition.Kind != IncrementalWindowRank && definition.Kind != IncrementalWindowDenseRank {
		return nil, ErrIncrementalWindowInvalidKind
	}
	outputColumn := strings.TrimSpace(definition.OutputColumn)
	if outputColumn == "" {
		return nil, ErrIncrementalWindowOutputColumnRequired
	}
	if definition.OrderKey == nil {
		return nil, ErrIncrementalWindowOrderKeyRequired
	}
	if definition.RowKey == nil {
		return nil, ErrIncrementalWindowRowKeyRequired
	}
	return &IncrementalRankWindow{
		kind:         definition.Kind,
		outputColumn: outputColumn,
		partitionKey: definition.PartitionKey,
		orderKey:     definition.OrderKey,
		rowKey:       definition.RowKey,
		descending:   definition.Descending,
		partitions:   make(map[string]incrementalRankWindowPartition),
		keys:         make(map[string]struct{}),
	}, nil
}

// Append applies an ordered insert batch and returns one positive differential
// update per row. It does not mutate input row maps.
func (window *IncrementalRankWindow) Append(rows []Row) ([]DifferentialRow, error) {
	if window == nil {
		return nil, ErrIncrementalRankWindowNil
	}
	if len(rows) == 0 {
		return nil, nil
	}

	states := make(map[string]incrementalRankWindowPartition, len(rows))
	pendingKeys := make(map[string]struct{}, len(rows))
	updates := make([]DifferentialRow, 0, len(rows))
	for index, row := range rows {
		partition := ""
		if window.partitionKey != nil {
			value, err := window.partitionKey(row)
			if err != nil {
				return nil, fmt.Errorf("incremental rank window row %d partition key: %w", index, err)
			}
			partition = value
		}
		order, err := window.orderKey(row)
		if err != nil {
			return nil, fmt.Errorf("incremental rank window row %d order key: %w", index, err)
		}
		key, err := window.rowKey(row)
		if err != nil {
			return nil, fmt.Errorf("incremental rank window row %d row key: %w", index, err)
		}
		if strings.TrimSpace(key) == "" {
			return nil, fmt.Errorf("incremental rank window row %d: %w", index, ErrIncrementalWindowRowKeyRequired)
		}
		if _, exists := window.keys[key]; exists {
			return nil, fmt.Errorf("incremental rank window row %d key %q: %w", index, key, ErrIncrementalWindowDuplicateKey)
		}
		if _, exists := pendingKeys[key]; exists {
			return nil, fmt.Errorf("incremental rank window row %d key %q: %w", index, key, ErrIncrementalWindowDuplicateKey)
		}
		if _, exists := row[window.outputColumn]; exists {
			return nil, fmt.Errorf("incremental rank window row %d: %w", index, ErrIncrementalWindowOutputConflict)
		}

		state, exists := states[partition]
		if !exists {
			state = window.partitions[partition]
		}
		if state.count == math.MaxInt64 {
			return nil, ErrIncrementalWindowCountOverflow
		}
		nextCount := state.count + 1
		nextRank, nextDenseRank := int64(1), int64(1)
		if state.hasOrder {
			comparison := sqlCompare(state.order, order)
			if window.descending {
				comparison = -comparison
			}
			if comparison > 0 {
				return nil, fmt.Errorf("incremental rank window row %d partition %q: %w", index, partition, ErrIncrementalWindowOutOfOrder)
			}
			nextRank = state.rank
			nextDenseRank = state.denseRank
			if comparison != 0 {
				nextRank = nextCount
				if nextDenseRank == math.MaxInt64 {
					return nil, ErrIncrementalWindowCountOverflow
				}
				nextDenseRank++
			}
		}
		state = incrementalRankWindowPartition{
			count:     nextCount,
			rank:      nextRank,
			denseRank: nextDenseRank,
			order:     order,
			hasOrder:  true,
		}
		states[partition] = state
		pendingKeys[key] = struct{}{}
		value := nextRank
		switch window.kind {
		case IncrementalWindowRowNumber:
			value = nextCount
		case IncrementalWindowDenseRank:
			value = nextDenseRank
		}
		output := make(Row, len(row)+1)
		for column, fieldValue := range row {
			output[column] = fieldValue
		}
		output[window.outputColumn] = value
		updates = append(updates, DifferentialRow{Key: key, Diff: 1, Row: output})
	}

	for partition, state := range states {
		window.partitions[partition] = state
	}
	for key := range pendingKeys {
		window.keys[key] = struct{}{}
	}
	return updates, nil
}
