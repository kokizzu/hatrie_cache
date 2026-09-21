package hatDataStructure

import (
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrMutablePackedRTreeNil reports a method call on a nil mutable tree.
	ErrMutablePackedRTreeNil = errors.New("hatriecache: mutable packed R-tree is nil")
	// ErrMutablePackedRTreeDuplicateID reports duplicate IDs during construction.
	ErrMutablePackedRTreeDuplicateID = errors.New("hatriecache: mutable packed R-tree ID is duplicated")
)

// MutableSpatialEntry associates a stable ID and value with an indexed box.
// IDs identify later updates or deletes.
type MutableSpatialEntry[T any] struct {
	ID     uint64
	Bounds SpatialBox
	Value  T
}

type mutablePackedValue[T any] struct {
	ID    uint64
	Value T
}

// MutablePackedRTree keeps an immutable packed base plus a small mutable R-tree
// overlay. Upserts and deletes affect the overlay immediately; Compact folds
// the current state back into the packed base. This keeps steady-state reads
// compact while avoiding a full rebuild for every mutation.
type MutablePackedRTree[T any] struct {
	mu      sync.RWMutex
	options PackedRTreeOptions
	base    *PackedRTree[mutablePackedValue[T]]
	delta   *RTree
	known   map[uint64]struct{}
	updates map[uint64]MutableSpatialEntry[T]
	dirty   map[uint64]struct{}
	length  int
}

// NewMutablePackedRTree builds an updateable packed spatial index. The input
// entries are copied, and duplicate IDs are rejected.
func NewMutablePackedRTree[T any](entries []MutableSpatialEntry[T], options PackedRTreeOptions) (*MutablePackedRTree[T], error) {
	known := make(map[uint64]struct{}, len(entries))
	packedEntries := make([]SpatialEntry[mutablePackedValue[T]], 0, len(entries))
	for index, entry := range entries {
		if !entry.Bounds.Valid() {
			return nil, fmt.Errorf("%w: entry %d", ErrSpatialBoxInvalid, index)
		}
		if _, exists := known[entry.ID]; exists {
			return nil, fmt.Errorf("%w: %d", ErrMutablePackedRTreeDuplicateID, entry.ID)
		}
		known[entry.ID] = struct{}{}
		packedEntries = append(packedEntries, SpatialEntry[mutablePackedValue[T]]{
			Bounds: entry.Bounds,
			Value:  mutablePackedValue[T]{ID: entry.ID, Value: entry.Value},
		})
	}
	base, err := NewPackedRTree(packedEntries, options)
	if err != nil {
		return nil, err
	}
	delta, err := NewRTree(0)
	if err != nil {
		return nil, err
	}
	return &MutablePackedRTree[T]{
		options: options,
		base:    base,
		delta:   delta,
		known:   known,
		updates: make(map[uint64]MutableSpatialEntry[T]),
		dirty:   make(map[uint64]struct{}),
		length:  len(entries),
	}, nil
}

// Upsert inserts or replaces one mutable entry.
func (tree *MutablePackedRTree[T]) Upsert(id uint64, bounds SpatialBox, value T) error {
	if tree == nil {
		return ErrMutablePackedRTreeNil
	}
	if !bounds.Valid() {
		return ErrSpatialBoxInvalid
	}
	tree.mu.Lock()
	defer tree.mu.Unlock()
	if err := tree.ensureInitializedLocked(); err != nil {
		return err
	}
	if err := tree.delta.Upsert(id, RTreeBounds{
		MinX: bounds.MinX,
		MinY: bounds.MinY,
		MaxX: bounds.MaxX,
		MaxY: bounds.MaxY,
	}); err != nil {
		return err
	}
	if _, exists := tree.known[id]; !exists {
		tree.length++
		tree.known[id] = struct{}{}
	}
	tree.updates[id] = MutableSpatialEntry[T]{ID: id, Bounds: bounds, Value: value}
	tree.dirty[id] = struct{}{}
	return nil
}

// Delete removes an entry and reports whether it existed.
func (tree *MutablePackedRTree[T]) Delete(id uint64) bool {
	if tree == nil {
		return false
	}
	tree.mu.Lock()
	defer tree.mu.Unlock()
	if tree.known == nil {
		return false
	}
	if _, exists := tree.known[id]; !exists {
		return false
	}
	delete(tree.known, id)
	delete(tree.updates, id)
	if tree.delta != nil {
		tree.delta.Delete(id)
	}
	tree.dirty[id] = struct{}{}
	tree.length--
	return true
}

// Query returns values whose boxes intersect query. Results follow packed-base
// traversal order, followed by currently updated entries in R-tree ID order.
// Callers must not rely on insertion order; this matches PackedRTree's spatial
// traversal contract while avoiding a per-query result sort.
func (tree *MutablePackedRTree[T]) Query(query SpatialBox) ([]T, error) {
	return tree.QueryInto(query, nil)
}

// QueryInto appends matching values into a reused destination after clearing
// its length. The destination is reused when it has enough capacity.
func (tree *MutablePackedRTree[T]) QueryInto(query SpatialBox, destination []T) ([]T, error) {
	if tree == nil {
		return nil, ErrMutablePackedRTreeNil
	}
	if !query.Valid() {
		return nil, ErrSpatialBoxInvalid
	}
	tree.mu.RLock()
	defer tree.mu.RUnlock()
	destination = destination[:0]
	if tree.base == nil || tree.delta == nil {
		return destination, nil
	}
	if _, err := tree.base.Visit(query, func(entry mutablePackedValue[T]) bool {
		if _, dirty := tree.dirty[entry.ID]; !dirty {
			destination = append(destination, entry.Value)
		}
		return true
	}); err != nil {
		return nil, err
	}
	if len(tree.dirty) == 0 {
		return destination, nil
	}
	var deltaStorage [64]uint64
	deltaIDs, err := tree.delta.SearchInto(deltaStorage[:0], RTreeBounds{
		MinX: query.MinX,
		MinY: query.MinY,
		MaxX: query.MaxX,
		MaxY: query.MaxY,
	})
	if err != nil {
		return nil, err
	}
	for _, id := range deltaIDs {
		if entry, exists := tree.updates[id]; exists {
			destination = append(destination, entry.Value)
		}
	}
	return destination, nil
}

// Compact merges all pending updates into a new immutable packed base. It is
// safe to call when no updates are pending and is intentionally explicit so
// callers can choose a write/read pause boundary.
func (tree *MutablePackedRTree[T]) Compact() error {
	if tree == nil {
		return ErrMutablePackedRTreeNil
	}
	tree.mu.Lock()
	defer tree.mu.Unlock()
	if err := tree.ensureInitializedLocked(); err != nil {
		return err
	}
	if len(tree.dirty) == 0 {
		return nil
	}
	packedEntries := make([]SpatialEntry[mutablePackedValue[T]], 0, len(tree.known))
	if tree.base != nil {
		for _, entry := range tree.base.entries {
			id := entry.Value.ID
			if _, dirty := tree.dirty[id]; dirty {
				continue
			}
			if _, exists := tree.known[id]; exists {
				packedEntries = append(packedEntries, entry)
			}
		}
	}
	for id, entry := range tree.updates {
		if _, exists := tree.known[id]; exists {
			packedEntries = append(packedEntries, SpatialEntry[mutablePackedValue[T]]{
				Bounds: entry.Bounds,
				Value:  mutablePackedValue[T]{ID: id, Value: entry.Value},
			})
		}
	}
	base, err := newPackedRTreeOwned(packedEntries, tree.options)
	if err != nil {
		return err
	}
	delta, err := NewRTree(0)
	if err != nil {
		return err
	}
	tree.base = base
	tree.delta = delta
	tree.updates = make(map[uint64]MutableSpatialEntry[T])
	tree.dirty = make(map[uint64]struct{})
	return nil
}

// Len returns the number of live entries.
func (tree *MutablePackedRTree[T]) Len() int {
	if tree == nil {
		return 0
	}
	tree.mu.RLock()
	defer tree.mu.RUnlock()
	return tree.length
}

// PendingUpdates returns the number of IDs that will be reconciled by the next
// compaction. A delete and a later reinsert of the same ID count once.
func (tree *MutablePackedRTree[T]) PendingUpdates() int {
	if tree == nil {
		return 0
	}
	tree.mu.RLock()
	defer tree.mu.RUnlock()
	return len(tree.dirty)
}

func (tree *MutablePackedRTree[T]) ensureInitializedLocked() error {
	if tree.known == nil {
		tree.known = make(map[uint64]struct{})
	}
	if tree.updates == nil {
		tree.updates = make(map[uint64]MutableSpatialEntry[T])
	}
	if tree.dirty == nil {
		tree.dirty = make(map[uint64]struct{})
	}
	if tree.base == nil {
		base, err := NewPackedRTree[mutablePackedValue[T]](nil, tree.options)
		if err != nil {
			return err
		}
		tree.base = base
	}
	if tree.delta == nil {
		delta, err := NewRTree(0)
		if err != nil {
			return err
		}
		tree.delta = delta
	}
	return nil
}
