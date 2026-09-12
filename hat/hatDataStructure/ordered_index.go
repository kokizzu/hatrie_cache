package hatDataStructure

import (
	"errors"
	"sort"
	"sync"
	"sync/atomic"
)

var (
	// ErrOrderedIndexNil indicates that an operation was attempted on a nil index.
	ErrOrderedIndexNil = errors.New("hatDataStructure: ordered index is nil")
	// ErrOrderedIndexExtractorRequired indicates that no key extractor was supplied.
	ErrOrderedIndexExtractorRequired = errors.New("hatDataStructure: ordered index extractor is required")
	// ErrOrderedIndexComparatorRequired indicates that no key comparator was supplied.
	ErrOrderedIndexComparatorRequired = errors.New("hatDataStructure: ordered index comparator is required")
	// ErrOrderedIndexIteratorNil indicates that Next was called on an empty iterator value.
	ErrOrderedIndexIteratorNil = errors.New("hatDataStructure: ordered index iterator is nil")
	// ErrOrderedIndexIteratorInvalidated indicates that the index changed after
	// the iterator was created.
	ErrOrderedIndexIteratorInvalidated = errors.New("hatDataStructure: ordered index iterator is invalidated")
	// ErrOrderedIndexIteratorClosed indicates that Next was called after Close.
	ErrOrderedIndexIteratorClosed = errors.New("hatDataStructure: ordered index iterator is closed")
)

// OrderedIndexEntry is one stable-ID value in key order. Entries with equal
// keys are ordered by ID.
type OrderedIndexEntry[T any, K any] struct {
	ID    uint64
	Key   K
	Value T
}

// OrderedIndex is a concurrent sorted-vector secondary index. It favors
// compact read-heavy ordered scans: First, Seek, SeekAfter, and iterator Next
// do not allocate. A mutation invalidates existing iterators instead of
// exposing a partially reordered view.
type OrderedIndex[T any, K any] struct {
	mu         sync.RWMutex
	extractor  func(T) K
	compare    func(K, K) int
	entries    []OrderedIndexEntry[T, K]
	positions  map[uint64]int
	generation atomic.Uint64
	active     atomic.Int64
}

// OrderedIndexIterator walks one stable view of an OrderedIndex. Iterators
// must not be used concurrently with themselves; the underlying index is safe
// for concurrent mutation, which invalidates the iterator deterministically.
type OrderedIndexIterator[T any, K any] struct {
	index      *OrderedIndex[T, K]
	entries    []OrderedIndexEntry[T, K]
	generation uint64
	position   int
	closed     bool
	released   bool
}

// NewOrderedIndex creates an ordered index using extractor and compare. The
// capacity is an initial entry hint and may be zero.
func NewOrderedIndex[T any, K any](extractor func(T) K, compare func(K, K) int, capacity int) (*OrderedIndex[T, K], error) {
	if extractor == nil {
		return nil, ErrOrderedIndexExtractorRequired
	}
	if compare == nil {
		return nil, ErrOrderedIndexComparatorRequired
	}
	if capacity < 0 {
		capacity = 0
	}
	index := &OrderedIndex[T, K]{extractor: extractor, compare: compare}
	if capacity > 0 {
		index.entries = make([]OrderedIndexEntry[T, K], 0, capacity)
		index.positions = make(map[uint64]int, capacity)
	}
	return index, nil
}

// Upsert inserts value under id or replaces the existing value and reorders it
// when its extracted key changes. The sorted order is key ascending, then ID.
func (index *OrderedIndex[T, K]) Upsert(id uint64, value T) error {
	if index == nil {
		return ErrOrderedIndexNil
	}
	key := index.extractor(value)
	index.mu.Lock()
	defer index.mu.Unlock()
	index.ensureInitializedLocked()
	if index.active.Load() == 0 {
		if position, exists := index.positions[id]; exists {
			index.removeAtLocked(position)
		}
		position := index.searchInsertLocked(index.entries, key, id)
		index.entries = append(index.entries, OrderedIndexEntry[T, K]{})
		copy(index.entries[position+1:], index.entries[position:])
		index.entries[position] = OrderedIndexEntry[T, K]{ID: id, Key: key, Value: value}
		for current := position; current < len(index.entries); current++ {
			index.positions[index.entries[current].ID] = current
		}
	} else {
		index.upsertCopyOnWriteLocked(id, key, value)
	}
	index.generation.Add(1)
	return nil
}

// Delete removes id and reports whether it was present.
func (index *OrderedIndex[T, K]) Delete(id uint64) bool {
	if index == nil {
		return false
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	position, exists := index.positions[id]
	if !exists {
		return false
	}
	if index.active.Load() == 0 {
		index.removeAtLocked(position)
	} else {
		updated := make([]OrderedIndexEntry[T, K], len(index.entries)-1)
		copy(updated, index.entries[:position])
		copy(updated[position:], index.entries[position+1:])
		index.entries = updated
		delete(index.positions, id)
		for current := position; current < len(updated); current++ {
			index.positions[updated[current].ID] = current
		}
	}
	index.generation.Add(1)
	return true
}

// Clear removes all entries while retaining the configured extractor and
// comparator. Existing iterators are invalidated.
func (index *OrderedIndex[T, K]) Clear() {
	if index == nil {
		return
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	index.entries = nil
	index.positions = nil
	index.generation.Add(1)
}

// Len returns the number of indexed IDs.
func (index *OrderedIndex[T, K]) Len() int {
	if index == nil {
		return 0
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	return len(index.entries)
}

// First returns an iterator positioned at the first entry.
func (index *OrderedIndex[T, K]) First() (OrderedIndexIterator[T, K], bool) {
	if index == nil {
		return OrderedIndexIterator[T, K]{}, false
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	if len(index.entries) == 0 {
		return OrderedIndexIterator[T, K]{}, false
	}
	index.active.Add(1)
	return OrderedIndexIterator[T, K]{index: index, entries: index.entries, generation: index.generation.Load()}, true
}

// Seek returns an iterator at the first entry whose key is greater than or
// equal to key.
func (index *OrderedIndex[T, K]) Seek(key K) (OrderedIndexIterator[T, K], bool) {
	return index.seek(key, false)
}

// SeekAfter returns an iterator at the first entry whose key is strictly
// greater than key.
func (index *OrderedIndex[T, K]) SeekAfter(key K) (OrderedIndexIterator[T, K], bool) {
	return index.seek(key, true)
}

// SnapshotInto copies entries in sorted order into dst, reusing its backing
// array when possible. It is the allocation-bearing alternative for callers
// that need a stable collection after mutations.
func (index *OrderedIndex[T, K]) SnapshotInto(dst []OrderedIndexEntry[T, K]) []OrderedIndexEntry[T, K] {
	dst = dst[:0]
	if index == nil {
		return dst
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	if cap(dst) < len(index.entries) {
		dst = make([]OrderedIndexEntry[T, K], 0, len(index.entries))
	}
	return append(dst, index.entries...)
}

// Next returns the current entry and advances the iterator. A mutation after
// iterator creation returns ErrOrderedIndexIteratorInvalidated.
func (iterator *OrderedIndexIterator[T, K]) Next() (OrderedIndexEntry[T, K], bool, error) {
	if iterator == nil || iterator.index == nil {
		return OrderedIndexEntry[T, K]{}, false, ErrOrderedIndexIteratorNil
	}
	if iterator.closed {
		return OrderedIndexEntry[T, K]{}, false, ErrOrderedIndexIteratorClosed
	}
	index := iterator.index
	if iterator.generation != index.generation.Load() {
		iterator.release()
		return OrderedIndexEntry[T, K]{}, false, ErrOrderedIndexIteratorInvalidated
	}
	if iterator.released {
		return OrderedIndexEntry[T, K]{}, false, nil
	}
	if iterator.position >= len(iterator.entries) {
		iterator.release()
		return OrderedIndexEntry[T, K]{}, false, nil
	}
	entry := iterator.entries[iterator.position]
	iterator.position++
	return entry, true, nil
}

// Close releases the iterator's live-view protection. It is useful when a
// caller stops before reaching the end; a closed iterator cannot be resumed.
func (iterator *OrderedIndexIterator[T, K]) Close() {
	if iterator == nil || iterator.index == nil || iterator.closed {
		return
	}
	iterator.closed = true
	iterator.release()
}

func (index *OrderedIndex[T, K]) seek(key K, strict bool) (OrderedIndexIterator[T, K], bool) {
	if index == nil {
		return OrderedIndexIterator[T, K]{}, false
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	position := sort.Search(len(index.entries), func(position int) bool {
		comparison := index.compare(index.entries[position].Key, key)
		if strict {
			return comparison > 0
		}
		return comparison >= 0
	})
	if position >= len(index.entries) {
		return OrderedIndexIterator[T, K]{}, false
	}
	index.active.Add(1)
	return OrderedIndexIterator[T, K]{index: index, entries: index.entries, generation: index.generation.Load(), position: position}, true
}

func (index *OrderedIndex[T, K]) ensureInitializedLocked() {
	if index.positions == nil {
		index.positions = make(map[uint64]int)
	}
}

func (index *OrderedIndex[T, K]) searchInsertLocked(entries []OrderedIndexEntry[T, K], key K, id uint64) int {
	return sort.Search(len(entries), func(position int) bool {
		comparison := index.compare(entries[position].Key, key)
		return comparison > 0 || comparison == 0 && entries[position].ID >= id
	})
}

func (index *OrderedIndex[T, K]) searchInsertExcludingLocked(entries []OrderedIndexEntry[T, K], excluded int, key K, id uint64) int {
	return sort.Search(len(entries)-1, func(position int) bool {
		if position >= excluded {
			position++
		}
		comparison := index.compare(entries[position].Key, key)
		return comparison > 0 || comparison == 0 && entries[position].ID >= id
	})
}

func (index *OrderedIndex[T, K]) upsertCopyOnWriteLocked(id uint64, key K, value T) {
	entries := index.entries
	oldPosition, exists := index.positions[id]
	position := len(entries)
	if exists {
		position = index.searchInsertExcludingLocked(entries, oldPosition, key, id)
	}
	updatedLength := len(entries)
	if !exists {
		updatedLength++
	}
	updated := make([]OrderedIndexEntry[T, K], updatedLength)
	write := 0
	for current := 0; current < len(entries); current++ {
		if exists && current == oldPosition {
			continue
		}
		if write == position {
			updated[write] = OrderedIndexEntry[T, K]{ID: id, Key: key, Value: value}
			write++
		}
		updated[write] = entries[current]
		write++
	}
	if write == position {
		updated[write] = OrderedIndexEntry[T, K]{ID: id, Key: key, Value: value}
	}
	index.entries = updated
	for current := 0; current < len(updated); current++ {
		index.positions[updated[current].ID] = current
	}
}

func (index *OrderedIndex[T, K]) removeAtLocked(position int) {
	removed := index.entries[position].ID
	copy(index.entries[position:], index.entries[position+1:])
	index.entries = index.entries[:len(index.entries)-1]
	delete(index.positions, removed)
	for current := position; current < len(index.entries); current++ {
		index.positions[index.entries[current].ID] = current
	}
}

func (iterator *OrderedIndexIterator[T, K]) release() {
	if iterator.released {
		return
	}
	iterator.index.active.Add(-1)
	iterator.released = true
}
