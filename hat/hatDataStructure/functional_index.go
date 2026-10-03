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

const functionalIndexSmallVectorThreshold = 16

type functionalIndexEntry[T any, K comparable] struct {
	key   K
	value T
}

type functionalIndexSmallEntry[T any, K comparable] struct {
	id    uint64
	key   K
	value T
}

// FunctionalIndex maps a caller-defined key derived from each value to stable
// IDs. Duplicate keys retain insertion order. The index is safe for concurrent
// readers and writers; LookupInto can reuse caller-owned scratch space to avoid
// per-query allocations.
type FunctionalIndex[T any, K comparable] struct {
	mu           sync.RWMutex
	extractor    func(T) K
	capacityHint int
	entries      map[uint64]functionalIndexEntry[T, K]
	postings     map[K]u64PostingList
	smallEntries []functionalIndexSmallEntry[T, K]
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
	index := &FunctionalIndex[T, K]{extractor: extractor, capacityHint: capacity}
	if capacity > 0 {
		smallCapacity := capacity
		if smallCapacity > functionalIndexSmallVectorThreshold {
			smallCapacity = functionalIndexSmallVectorThreshold
		}
		index.smallEntries = make([]functionalIndexSmallEntry[T, K], 0, smallCapacity)
	}
	return index, nil
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
		index.upsertFunctionalSmallLocked(id, key, value)
		return nil
	}
	if current, ok := index.entries[id]; ok {
		if current.key != key {
			index.removeFunctionalPostingLocked(current.key, id)
			index.appendFunctionalPostingLocked(key, id)
		}
		index.entries[id] = functionalIndexEntry[T, K]{key: key, value: value}
		return nil
	}
	index.entries[id] = functionalIndexEntry[T, K]{key: key, value: value}
	index.appendFunctionalPostingLocked(key, id)
	return nil
}

// Delete removes id and returns whether it was present.
func (index *FunctionalIndex[T, K]) Delete(id uint64) bool {
	if index == nil {
		return false
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	if index.entries == nil {
		for position, entry := range index.smallEntries {
			if entry.id != id {
				continue
			}
			copy(index.smallEntries[position:], index.smallEntries[position+1:])
			index.smallEntries[len(index.smallEntries)-1] = functionalIndexSmallEntry[T, K]{}
			index.smallEntries = index.smallEntries[:len(index.smallEntries)-1]
			return true
		}
		return false
	}
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
	if index.entries == nil {
		for _, entry := range index.smallEntries {
			if entry.key == key {
				dst = append(dst, entry.value)
			}
		}
		return dst
	}
	posting, ok := index.postings[key]
	if !ok {
		return dst
	}
	if posting.rest == nil {
		if entry, ok := index.entries[posting.first]; ok {
			dst = append(dst, entry.value)
		}
		return dst
	}
	if cap(dst) < len(posting.rest.rest)+2 {
		dst = make([]T, 0, len(posting.rest.rest)+2)
	}
	if entry, ok := index.entries[posting.first]; ok {
		dst = append(dst, entry.value)
	}
	if entry, ok := index.entries[posting.rest.first]; ok {
		dst = append(dst, entry.value)
	}
	for _, id := range posting.rest.rest {
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
	if index.entries == nil {
		for _, entry := range index.smallEntries {
			if entry.key == key {
				dst = append(dst, entry.id)
			}
		}
		return dst
	}
	posting, ok := index.postings[key]
	if !ok {
		return dst
	}
	return posting.values(dst)
}

// Len returns the number of indexed IDs.
func (index *FunctionalIndex[T, K]) Len() int {
	if index == nil {
		return 0
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	if index.entries == nil {
		return len(index.smallEntries)
	}
	return len(index.entries)
}

// DistinctKeys returns the number of derived keys with at least one ID.
func (index *FunctionalIndex[T, K]) DistinctKeys() int {
	if index == nil {
		return 0
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	if index.entries == nil {
		return functionalIndexSmallDistinctKeys(index.smallEntries)
	}
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
	index.smallEntries = nil
}

func (index *FunctionalIndex[T, K]) upsertFunctionalSmallLocked(id uint64, key K, value T) {
	for position, entry := range index.smallEntries {
		if entry.id != id {
			continue
		}
		if entry.key == key {
			index.smallEntries[position] = functionalIndexSmallEntry[T, K]{id: id, key: key, value: value}
			return
		}
		copy(index.smallEntries[position:], index.smallEntries[position+1:])
		index.smallEntries[len(index.smallEntries)-1] = functionalIndexSmallEntry[T, K]{}
		index.smallEntries = index.smallEntries[:len(index.smallEntries)-1]
		break
	}
	index.smallEntries = append(index.smallEntries, functionalIndexSmallEntry[T, K]{id: id, key: key, value: value})
	if len(index.smallEntries) > functionalIndexSmallVectorThreshold {
		index.promoteFunctionalSmallLocked()
	}
}

func (index *FunctionalIndex[T, K]) promoteFunctionalSmallLocked() {
	capacity := index.capacityHint
	if capacity < len(index.smallEntries) {
		capacity = len(index.smallEntries)
	}
	index.entries = make(map[uint64]functionalIndexEntry[T, K], capacity)
	index.postings = make(map[K]u64PostingList, capacity)
	for _, entry := range index.smallEntries {
		index.entries[entry.id] = functionalIndexEntry[T, K]{key: entry.key, value: entry.value}
		index.appendFunctionalPostingLocked(entry.key, entry.id)
	}
	index.smallEntries = nil
}

func functionalIndexSmallDistinctKeys[T any, K comparable](entries []functionalIndexSmallEntry[T, K]) int {
	distinct := 0
	for position, entry := range entries {
		seen := false
		for previous := 0; previous < position; previous++ {
			if entries[previous].key == entry.key {
				seen = true
				break
			}
		}
		if !seen {
			distinct++
		}
	}
	return distinct
}

func (index *FunctionalIndex[T, K]) containsFunctionalID(key K, id uint64) bool {
	if index == nil {
		return false
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	if index.entries == nil {
		for _, entry := range index.smallEntries {
			if entry.key == key && entry.id == id {
				return true
			}
		}
		return false
	}
	posting, ok := index.postings[key]
	if !ok {
		return false
	}
	if posting.first == id {
		return true
	}
	if posting.rest == nil {
		return false
	}
	if posting.rest.first == id {
		return true
	}
	for _, candidate := range posting.rest.rest {
		if candidate == id {
			return true
		}
	}
	return false
}

func (index *FunctionalIndex[T, K]) removeFunctionalPostingLocked(key K, id uint64) {
	posting, ok := index.postings[key]
	if !ok {
		return
	}
	next, removed, empty := posting.removeInOrder(id)
	if !removed {
		return
	}
	if empty {
		delete(index.postings, key)
		return
	}
	index.postings[key] = next
}

func (index *FunctionalIndex[T, K]) appendFunctionalPostingLocked(key K, id uint64) {
	if posting, ok := index.postings[key]; ok {
		index.postings[key] = posting.appendInOrder(id)
		return
	}
	index.postings[key] = newU64PostingList(id)
}
