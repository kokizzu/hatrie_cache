package hatSql

import (
	"fmt"
	"sync"
)

// TypedTableJoinArrangementFreshness reports both input checkpoints against
// their current typed-table changefeed tails.
type TypedTableJoinArrangementFreshness struct {
	LeftCheckpoint      uint64 `json:"left_checkpoint"`
	LeftSourceSequence  uint64 `json:"left_source_sequence"`
	RightCheckpoint     uint64 `json:"right_checkpoint"`
	RightSourceSequence uint64 `json:"right_source_sequence"`
	Stale               bool   `json:"stale"`
}

// TypedTableJoinArrangementHydration reports one bounded replay for both join
// inputs. Complete is false when another Hydrate call is required.
type TypedTableJoinArrangementHydration struct {
	LeftBefore          uint64 `json:"left_before"`
	LeftAfter           uint64 `json:"left_after"`
	LeftSourceSequence  uint64 `json:"left_source_sequence"`
	LeftApplied         int    `json:"left_applied"`
	RightBefore         uint64 `json:"right_before"`
	RightAfter          uint64 `json:"right_after"`
	RightSourceSequence uint64 `json:"right_source_sequence"`
	RightApplied        int    `json:"right_applied"`
	Complete            bool   `json:"complete"`
}

// TypedTableJoinArrangements shares one exact join state among consumers with
// the same input tables and join definition.
type TypedTableJoinArrangements struct {
	mu      sync.Mutex
	left    *TypedTable
	right   *TypedTable
	entries map[string]*typedTableJoinArrangementEntry
}

type typedTableJoinArrangementEntry struct {
	mu   sync.Mutex
	join *TypedTableJoin
	refs int
}

// TypedTableJoinArrangement is a reference-counted lease on a shared join.
type TypedTableJoinArrangement struct {
	mu    sync.Mutex
	owner *TypedTableJoinArrangements
	key   string
	entry *typedTableJoinArrangementEntry
}

func NewTypedTableJoinArrangements(left, right *TypedTable) (*TypedTableJoinArrangements, error) {
	if left == nil || right == nil || left == right {
		return nil, fmt.Errorf("typed table join arrangements require two distinct tables")
	}
	return &TypedTableJoinArrangements{left: left, right: right, entries: map[string]*typedTableJoinArrangementEntry{}}, nil
}

func (arrangements *TypedTableJoinArrangements) Acquire(definition TypedTableJoinDefinition) (*TypedTableJoinArrangement, error) {
	if arrangements == nil {
		return nil, fmt.Errorf("typed table join arrangements are nil")
	}
	key := definition.LeftField + "\x00" + definition.RightField
	arrangements.mu.Lock()
	defer arrangements.mu.Unlock()
	entry := arrangements.entries[key]
	if entry == nil {
		join, err := NewTypedTableJoin(arrangements.left, arrangements.right, definition)
		if err != nil {
			return nil, err
		}
		entry = &typedTableJoinArrangementEntry{join: join}
		arrangements.entries[key] = entry
	}
	entry.refs++
	return &TypedTableJoinArrangement{owner: arrangements, key: key, entry: entry}, nil
}

func (arrangements *TypedTableJoinArrangements) Active() int {
	if arrangements == nil {
		return 0
	}
	arrangements.mu.Lock()
	defer arrangements.mu.Unlock()
	return len(arrangements.entries)
}

func (arrangement *TypedTableJoinArrangement) ApplyLeft(changes []TypedTableChange) error {
	entry, err := arrangement.activeEntry()
	if err != nil {
		return err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return entry.join.ApplyLeft(changes)
}
func (arrangement *TypedTableJoinArrangement) ApplyRight(changes []TypedTableChange) error {
	entry, err := arrangement.activeEntry()
	if err != nil {
		return err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return entry.join.ApplyRight(changes)
}
func (arrangement *TypedTableJoinArrangement) Freshness() (TypedTableJoinArrangementFreshness, error) {
	entry, err := arrangement.activeEntry()
	if err != nil {
		return TypedTableJoinArrangementFreshness{}, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	leftCheckpoint := entry.join.LeftCheckpoint()
	rightCheckpoint := entry.join.RightCheckpoint()
	entry.join.left.mu.RLock()
	leftSourceSequence := entry.join.left.sequence
	entry.join.left.mu.RUnlock()
	entry.join.right.mu.RLock()
	rightSourceSequence := entry.join.right.sequence
	entry.join.right.mu.RUnlock()
	return TypedTableJoinArrangementFreshness{
		LeftCheckpoint: leftCheckpoint, LeftSourceSequence: leftSourceSequence,
		RightCheckpoint: rightCheckpoint, RightSourceSequence: rightSourceSequence,
		Stale: leftCheckpoint < leftSourceSequence || rightCheckpoint < rightSourceSequence,
	}, nil
}
func (arrangement *TypedTableJoinArrangement) Hydrate(limit int) (TypedTableJoinArrangementHydration, error) {
	if limit < 0 {
		return TypedTableJoinArrangementHydration{}, fmt.Errorf("typed table join arrangement hydration batch cannot be negative")
	}
	if limit == 0 {
		limit = DefaultTypedTableArrangementHydrationBatch
	}
	entry, err := arrangement.activeEntry()
	if err != nil {
		return TypedTableJoinArrangementHydration{}, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	leftBefore := entry.join.LeftCheckpoint()
	rightBefore := entry.join.RightCheckpoint()
	leftChanges, leftSourceSequence, err := entry.join.left.ChangesAfter(leftBefore, limit)
	if err != nil {
		return TypedTableJoinArrangementHydration{}, err
	}
	rightChanges, rightSourceSequence, err := entry.join.right.ChangesAfter(rightBefore, limit)
	if err != nil {
		return TypedTableJoinArrangementHydration{}, err
	}
	if err := entry.join.ApplyLeft(leftChanges); err != nil {
		return TypedTableJoinArrangementHydration{}, err
	}
	if err := entry.join.ApplyRight(rightChanges); err != nil {
		return TypedTableJoinArrangementHydration{}, err
	}
	leftAfter := entry.join.LeftCheckpoint()
	rightAfter := entry.join.RightCheckpoint()
	return TypedTableJoinArrangementHydration{
		LeftBefore: leftBefore, LeftAfter: leftAfter, LeftSourceSequence: leftSourceSequence, LeftApplied: len(leftChanges),
		RightBefore: rightBefore, RightAfter: rightAfter, RightSourceSequence: rightSourceSequence, RightApplied: len(rightChanges),
		Complete: leftAfter >= leftSourceSequence && rightAfter >= rightSourceSequence,
	}, nil
}
func (arrangement *TypedTableJoinArrangement) Rows() []TypedTableJoinRow {
	entry, err := arrangement.activeEntry()
	if err != nil {
		return nil
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return entry.join.Rows()
}
func (arrangement *TypedTableJoinArrangement) Release() bool {
	if arrangement == nil {
		return false
	}
	arrangement.mu.Lock()
	defer arrangement.mu.Unlock()
	if arrangement.owner == nil || arrangement.entry == nil {
		return false
	}
	arrangement.owner.mu.Lock()
	defer arrangement.owner.mu.Unlock()
	entry := arrangement.owner.entries[arrangement.key]
	if entry != arrangement.entry {
		return false
	}
	entry.refs--
	if entry.refs == 0 {
		delete(arrangement.owner.entries, arrangement.key)
	}
	arrangement.owner, arrangement.entry = nil, nil
	return true
}
func (arrangement *TypedTableJoinArrangement) activeEntry() (*typedTableJoinArrangementEntry, error) {
	if arrangement == nil {
		return nil, fmt.Errorf("typed table join arrangement is released")
	}
	arrangement.mu.Lock()
	defer arrangement.mu.Unlock()
	if arrangement.owner == nil || arrangement.entry == nil {
		return nil, fmt.Errorf("typed table join arrangement is released")
	}
	return arrangement.entry, nil
}
