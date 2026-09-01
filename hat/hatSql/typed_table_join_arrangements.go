package hatSql

import (
	"fmt"
	"sync"
)

// TypedTableJoinArrangements shares one exact join state among consumers with
// the same input tables and join definition.
type TypedTableJoinArrangements struct {
	mu      sync.Mutex
	left    *TypedTable
	right   *TypedTable
	entries map[string]*typedTableJoinArrangementEntry
}

type typedTableJoinArrangementEntry struct {
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
		if err != nil { return nil, err }
		entry = &typedTableJoinArrangementEntry{join: join}
		arrangements.entries[key] = entry
	}
	entry.refs++
	return &TypedTableJoinArrangement{owner: arrangements, key: key, entry: entry}, nil
}

func (arrangements *TypedTableJoinArrangements) Active() int {
	if arrangements == nil { return 0 }
	arrangements.mu.Lock(); defer arrangements.mu.Unlock()
	return len(arrangements.entries)
}

func (arrangement *TypedTableJoinArrangement) ApplyLeft(changes []TypedTableChange) error {
	entry, err := arrangement.activeEntry(); if err != nil { return err }; return entry.join.ApplyLeft(changes)
}
func (arrangement *TypedTableJoinArrangement) ApplyRight(changes []TypedTableChange) error {
	entry, err := arrangement.activeEntry(); if err != nil { return err }; return entry.join.ApplyRight(changes)
}
func (arrangement *TypedTableJoinArrangement) Rows() []TypedTableJoinRow {
	entry, err := arrangement.activeEntry(); if err != nil { return nil }; return entry.join.Rows()
}
func (arrangement *TypedTableJoinArrangement) Release() bool {
	if arrangement == nil { return false }
	arrangement.mu.Lock(); defer arrangement.mu.Unlock()
	if arrangement.owner == nil || arrangement.entry == nil { return false }
	arrangement.owner.mu.Lock(); defer arrangement.owner.mu.Unlock()
	entry := arrangement.owner.entries[arrangement.key]
	if entry != arrangement.entry { return false }
	entry.refs--
	if entry.refs == 0 { delete(arrangement.owner.entries, arrangement.key) }
	arrangement.owner, arrangement.entry = nil, nil
	return true
}
func (arrangement *TypedTableJoinArrangement) activeEntry() (*typedTableJoinArrangementEntry, error) {
	if arrangement == nil { return nil, fmt.Errorf("typed table join arrangement is released") }
	arrangement.mu.Lock(); defer arrangement.mu.Unlock()
	if arrangement.owner == nil || arrangement.entry == nil { return nil, fmt.Errorf("typed table join arrangement is released") }
	return arrangement.entry, nil
}
