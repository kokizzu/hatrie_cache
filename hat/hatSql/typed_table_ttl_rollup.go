package hatSql

import (
	"fmt"
	"strings"
	"sync"
)

// TypedTableTTLRollup summarizes rows at the moment row TTL removes them.
// It is intentionally separate from a live-table aggregate: only expired row
// images are added, so the summary can retain coarse history after details are
// physically deleted. The zero/default scheduler does not create rollups.
type TypedTableTTLRollup struct {
	mu         sync.Mutex
	table      *TypedTable
	aggregate  *TypedTableAggregate
	checkpoint uint64
}

// NewTypedTableTTLRollup validates an aggregate definition against the source
// table and creates an empty expired-row summary.
func NewTypedTableTTLRollup(table *TypedTable, definition TypedTableAggregateDefinition) (*TypedTableTTLRollup, error) {
	if table == nil {
		return nil, fmt.Errorf("typed table TTL rollup table is nil")
	}
	table.mu.RLock()
	hasColumnTTL := table.columnTTLs != nil
	table.mu.RUnlock()
	if hasColumnTTL {
		return nil, fmt.Errorf("typed table TTL rollup cannot combine with column TTL")
	}
	aggregate, err := NewTypedTableAggregate(table, definition)
	if err != nil {
		return nil, err
	}
	return &TypedTableTTLRollup{table: table, aggregate: aggregate}, nil
}

// ApplyExpired adds new row-TTL DELETE images to the rollup. Sequences are a
// monotone subset of the source changefeed: unrelated inserts and updates are
// allowed between expiry batches, while replayed sequences are ignored.
func (rollup *TypedTableTTLRollup) ApplyExpired(changes []TypedTableChange) error {
	if rollup == nil {
		return fmt.Errorf("typed table TTL rollup is nil")
	}
	_, err := rollup.applyExpired(changes)
	return err
}

func (rollup *TypedTableTTLRollup) applyExpired(changes []TypedTableChange) (int, error) {
	rollup.mu.Lock()
	defer rollup.mu.Unlock()

	last := rollup.checkpoint
	for _, change := range changes {
		if change.Sequence <= rollup.checkpoint {
			continue
		}
		if change.Sequence <= last {
			return 0, fmt.Errorf("typed table TTL rollup change sequence %d is not increasing after %d", change.Sequence, last)
		}
		if !strings.EqualFold(change.Operation, "DELETE") {
			return 0, fmt.Errorf("typed table TTL rollup requires DELETE changes, got %q", change.Operation)
		}
		if len(change.Before) == 0 || len(change.After) != 0 {
			return 0, fmt.Errorf("typed table TTL rollup DELETE change %d must have only a before image", change.Sequence)
		}
		if len(change.Before) != len(rollup.table.columns) {
			return 0, fmt.Errorf("typed table TTL rollup change %d has %d values, want %d", change.Sequence, len(change.Before), len(rollup.table.columns))
		}
		last = change.Sequence
	}

	applied := 0
	for _, change := range changes {
		if change.Sequence <= rollup.checkpoint {
			continue
		}
		if err := rollup.aggregate.applyRow(change.Before, 1); err != nil {
			return applied, err
		}
		rollup.checkpoint = change.Sequence
		applied++
	}
	return applied, nil
}

// Rows returns the current coarse summary, ordered like TypedTableAggregate.Rows.
func (rollup *TypedTableTTLRollup) Rows() []Row {
	if rollup == nil {
		return nil
	}
	rollup.mu.Lock()
	defer rollup.mu.Unlock()
	return rollup.aggregate.Rows()
}

// Checkpoint returns the highest expired-row change sequence applied.
func (rollup *TypedTableTTLRollup) Checkpoint() uint64 {
	if rollup == nil {
		return 0
	}
	rollup.mu.Lock()
	defer rollup.mu.Unlock()
	return rollup.checkpoint
}
