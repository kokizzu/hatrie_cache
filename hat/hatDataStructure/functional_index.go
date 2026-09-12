package hatDataStructure

import (
	"errors"
	"sync"
)

var (
	// ErrFunctionalIndexNil indicates that an operation was attempted on a nil index.
	ErrFunctionalIndexNil = errors.New("hatDataStructure: functional index is nil")
	// ErrFunctionalIndexExtractorRequired indicates that no key function was supplied.
	ErrFunctionalIndexExtractorRequired = errors.New("hatDataStructure: functional index extractor is required")
)

type functionalIndexEntry[T any, K comparable] struct {
	key   K
	value T
}

// FunctionalIndex maps a caller-defined key derived from each value to stable
// IDs. Duplicate keys retain insertion order. The index is safe for concurrent
// readers and writers; LookupInto can reuse caller-owned scratch space to avoid
// per-query allocations.
type FunctionalIndex[T any, K comparable] struct {
	mu        sync.RWMutex
	extractor func(T) K
	entries   map[uint64]functionalIndexEntry[T, K]
	postings  map[K][]uint64
}

// NewFunctionalIndex creates an index using extractor to derive each key.
// Capacity is only an initial sizing hint and may be zero.
func NewFunctionalIndex[T any, K comparable](extractor func(T) K, capacity int) (*FunctionalIndex[T, K], error) {
	if extractor == nil {
		return nil, ErrFunctionalIndexExtractorRequired
	}
	if capacity < 0 {
		capacity = 0
	}
	return &FunctionalIndex[T, K]{
		extractor: extractor,
		entries:   make(map[uint64]functionalIndexEntry[T, K], capacity),
		postings:  make(map[K][]uint64, capacity),
	}, nil
}

// Upsert inserts or replaces a value for id. Replacing a value also moves its
// posting when the derived key changes.
func (index *FunctionalIndex[T, K]) Upsert(id uint64, value T) error {
	if index == nil {
		return ErrFunctionalIndexNil
	}
	key := index.extractor(value)
	index.mu.Lock()
	defer index.mu.Unlock()
	if index.entries == nil {
		index.entries = make(map[uint64]functionalIndexEntry[T, K])
	}
	if index.postings == nil {
		index.postings = make(map[K][]uint64)
	}
	if current, ok := index.entries[id]; ok {
		if current.key != key {
			index.removeFunctionalPostingLocked(current.key, id)
			index.postings[key] = append(index.postings[key], id)
		}
		index.entries[id] = functionalIndexEntry[T, K]{key: key, value: value}
		return nil
	}
	index.entries[id] = functionalIndexEntry[T, K]{key: key, value: value}
	index.postings[key] = append(index.postings[key], id)
	return nil
}

// Delete removes id and returns whether it was present.
func (index *FunctionalIndex[T, K]) Delete(id uint64) bool {
	if index == nil {
		return false
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	entry, ok := index.entries[id]
	if !ok {
		return false
	}
	delete(index.entries, id)
	index.removeFunctionalPostingLocked(entry.key, id)
	return true
}

// Lookup returns values whose derived key equals key, in posting order.
func (index *FunctionalIndex[T, K]) Lookup(key K) []T {
	return index.LookupInto(key, nil)
}

// LookupInto replaces dst with values whose derived key equals key. It reuses
// dst's backing array when it has enough capacity.
func (index *FunctionalIndex[T, K]) LookupInto(key K, dst []T) []T {
	dst = dst[:0]
	if index == nil {
		return dst
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	ids := index.postings[key]
	if cap(dst) < len(ids) {
		dst = make([]T, 0, len(ids))
	}
	for _, id := range ids {
		if entry, ok := index.entries[id]; ok {
			dst = append(dst, entry.value)
		}
	}
	return dst
}

// LookupIDs returns stable IDs whose derived key equals key, in posting order.
func (index *FunctionalIndex[T, K]) LookupIDs(key K) []uint64 {
	return index.LookupIDsInto(key, nil)
}

// LookupIDsInto replaces dst with stable IDs whose derived key equals key.
// It reuses dst's backing array when it has enough capacity.
func (index *FunctionalIndex[T, K]) LookupIDsInto(key K, dst []uint64) []uint64 {
	dst = dst[:0]
	if index == nil {
		return dst
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	ids := index.postings[key]
	if cap(dst) < len(ids) {
		dst = make([]uint64, 0, len(ids))
	}
	return append(dst, ids...)
}

// Len returns the number of indexed IDs.
func (index *FunctionalIndex[T, K]) Len() int {
	if index == nil {
		return 0
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	return len(index.entries)
}

// DistinctKeys returns the number of derived keys with at least one ID.
func (index *FunctionalIndex[T, K]) DistinctKeys() int {
	if index == nil {
		return 0
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	return len(index.postings)
}

// Clear removes all entries while retaining the extractor for reuse.
func (index *FunctionalIndex[T, K]) Clear() {
	if index == nil {
		return
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	index.entries = nil
	index.postings = nil
}

func (index *FunctionalIndex[T, K]) removeFunctionalPostingLocked(key K, id uint64) {
	ids := index.postings[key]
	for position, candidate := range ids {
		if candidate != id {
			continue
		}
		copy(ids[position:], ids[position+1:])
		ids[len(ids)-1] = 0
		ids = ids[:len(ids)-1]
		if len(ids) == 0 {
			delete(index.postings, key)
		} else {
			index.postings[key] = ids
		}
		return
	}
}
