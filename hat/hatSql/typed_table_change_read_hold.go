package hatSql

import (
	"errors"
	"fmt"

	"hatrie_cache/hat/hatDataStructure"
)

var (
	// ErrTypedTableChangeReadHoldActive reports compaction blocked by a reader.
	ErrTypedTableChangeReadHoldActive = errors.New("typed table change read hold blocks compaction")
	// ErrTypedTableChangeReadHoldRange reports a sequence outside a hold snapshot.
	ErrTypedTableChangeReadHoldRange = errors.New("typed table change read hold sequence is outside its snapshot")
	// ErrTypedTableChangeReadHoldReleased reports use after Release.
	ErrTypedTableChangeReadHoldReleased = errors.New("typed table change read hold is released")
)

type typedTableChangeReadHoldSet = hatDataStructure.FrontierReadHoldSet

// TypedTableChangeReadHold pins the table changefeed history needed by a
// multi-call reader. The upper frontier is fixed at acquisition until Advance
// is called. Release it as soon as the reader has finished.
type TypedTableChangeReadHold struct {
	table *TypedTable
	hold  *hatDataStructure.FrontierReadHold
}

// AcquireChangeReadHold pins changes from since through the current table
// tail. A since below the retained boundary cannot be repaired by a hold.
func (table *TypedTable) AcquireChangeReadHold(since uint64) (*TypedTableChangeReadHold, error) {
	if table == nil {
		return nil, errors.New("typed table is nil")
	}
	table.mu.Lock()
	defer table.mu.Unlock()
	if since < table.compactedThrough {
		return nil, ErrTypedTableChangesCompacted
	}
	if since > table.sequence {
		return nil, fmt.Errorf("%w: sequence %d is ahead of table tail %d", ErrTypedTableChangeReadHoldRange, since, table.sequence)
	}
	if table.changeReadHolds == nil {
		table.changeReadHolds = hatDataStructure.NewFrontierReadHoldSet()
	}
	hold, err := table.changeReadHolds.Acquire(since, table.sequence)
	if err != nil {
		return nil, err
	}
	return &TypedTableChangeReadHold{table: table, hold: hold}, nil
}

// ID returns the stable hold identifier, or zero for a nil hold.
func (hold *TypedTableChangeReadHold) ID() uint64 {
	if hold == nil || hold.hold == nil {
		return 0
	}
	return hold.hold.ID()
}

// Since returns the inclusive oldest sequence the reader may still request.
func (hold *TypedTableChangeReadHold) Since() uint64 {
	if hold == nil || hold.hold == nil {
		return 0
	}
	return hold.hold.Since()
}

// Upper returns the inclusive tail frontier pinned by the hold.
func (hold *TypedTableChangeReadHold) Upper() uint64 {
	if hold == nil || hold.hold == nil {
		return 0
	}
	return hold.hold.Upper()
}

// Advance moves the lower frontier and extends the upper frontier to the
// current table tail. The lower frontier must not regress.
func (hold *TypedTableChangeReadHold) Advance(since uint64) error {
	if hold == nil || hold.hold == nil || hold.table == nil {
		return ErrTypedTableChangeReadHoldReleased
	}
	hold.table.mu.RLock()
	compactedThrough := hold.table.compactedThrough
	upper := hold.table.sequence
	hold.table.mu.RUnlock()
	if since < compactedThrough || since > upper {
		return fmt.Errorf("%w: sequence %d is outside retained table range %d..%d", ErrTypedTableChangeReadHoldRange, since, compactedThrough, upper)
	}
	if err := hold.hold.Advance(since, upper); err != nil {
		if errors.Is(err, hatDataStructure.ErrFrontierReadHoldReleased) {
			return ErrTypedTableChangeReadHoldReleased
		}
		return err
	}
	return nil
}

// ChangesAfter reads a bounded page inside the hold's fixed snapshot. New
// changes become visible only after Advance extends the upper frontier.
func (hold *TypedTableChangeReadHold) ChangesAfter(sequence uint64, limit int) ([]TypedTableChange, uint64, error) {
	if hold == nil || hold.hold == nil || hold.table == nil {
		return nil, 0, ErrTypedTableChangeReadHoldReleased
	}
	readSince := hold.hold.Since()
	upper := hold.hold.Upper()
	if !hold.hold.Allows(readSince) {
		return nil, upper, ErrTypedTableChangeReadHoldReleased
	}
	if sequence < readSince || sequence > upper {
		return nil, upper, fmt.Errorf("%w: sequence %d is outside hold range %d..%d", ErrTypedTableChangeReadHoldRange, sequence, readSince, upper)
	}
	if limit > 0 {
		available := upper - sequence
		if uint64(limit) > available {
			if available == 0 {
				limit = 0
			} else if available <= uint64(^uint(0)>>1) {
				limit = int(available)
			}
		}
	}
	hold.table.mu.RLock()
	defer hold.table.mu.RUnlock()
	changes, tail, err := hold.table.changesAfterLocked(sequence, limit)
	if tail > upper {
		tail = upper
	}
	return changes, tail, err
}

// Release removes the hold. It is idempotent.
func (hold *TypedTableChangeReadHold) Release() error {
	if hold == nil || hold.hold == nil {
		return ErrTypedTableChangeReadHoldReleased
	}
	if err := hold.hold.Release(); err != nil {
		return err
	}
	return nil
}
