package hatDataStructure

import (
	"errors"
	"sort"
	"sync"
)

var (
	ErrOrderedIndexRangeCacheIndexRequired   = errors.New("hatDataStructure: ordered index range cache index is required")
	ErrOrderedIndexRangeCacheCapacityInvalid = errors.New("hatDataStructure: ordered index range cache capacity is invalid")
)

// OrderedIndexRangeCacheStats reports cache activity and retained tuple count.
type OrderedIndexRangeCacheStats struct {
	Hits    uint64
	Misses  uint64
	Entries int
	Values  int
}

// OrderedIndexRangeCache is an opt-in bounded cache for repeated inclusive
// ranges over an OrderedIndex. Entries are invalidated as a group whenever the
// index generation changes. The cache stores stable range snapshots and copies
// them into caller-provided destination capacity on a hit.
type OrderedIndexRangeCache[T any, K any] struct {
	mu         sync.Mutex
	index      *OrderedIndex[T, K]
	capacity   int
	generation uint64
	entries    []orderedIndexRangeCacheEntry[T, K]
	next       int
	hits       uint64
	misses     uint64
}

type orderedIndexRangeCacheEntry[T any, K any] struct {
	start  K
	end    K
	values []OrderedIndexEntry[T, K]
}

// NewOrderedIndexRangeCache creates a bounded range cache. A positive
// capacity is required; zero capacity is rejected so accidental construction
// cannot silently add miss-only work to a read path.
func NewOrderedIndexRangeCache[T any, K any](index *OrderedIndex[T, K], capacity int) (*OrderedIndexRangeCache[T, K], error) {
	if index == nil {
		return nil, ErrOrderedIndexRangeCacheIndexRequired
	}
	if capacity <= 0 {
		return nil, ErrOrderedIndexRangeCacheCapacityInvalid
	}
	return &OrderedIndexRangeCache[T, K]{
		index:      index,
		capacity:   capacity,
		generation: index.generation.Load(),
		entries:    make([]orderedIndexRangeCacheEntry[T, K], 0, capacity),
	}, nil
}

// Range returns an independent result slice for the inclusive range. Use
// RangeInto in a hot loop to reuse destination capacity and avoid the result
// allocation on cache hits.
func (cache *OrderedIndexRangeCache[T, K]) Range(start, end K) ([]OrderedIndexEntry[T, K], bool) {
	return cache.RangeInto(start, end, nil)
}

// RangeInto copies the inclusive range into destination and reports whether
// the range contains entries. The returned slice aliases destination only.
// Cache hits reuse destination capacity and do not allocate when it is large
// enough for the cached tuple set.
func (cache *OrderedIndexRangeCache[T, K]) RangeInto(start, end K, destination []OrderedIndexEntry[T, K]) ([]OrderedIndexEntry[T, K], bool) {
	destination = destination[:0]
	if cache == nil || cache.index == nil {
		return destination, false
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.index.compare(start, end) > 0 {
		return destination, false
	}
	generation := cache.index.generation.Load()
	if cache.generation != generation {
		cache.entries = cache.entries[:0]
		cache.next = 0
		cache.generation = generation
	}
	for index := range cache.entries {
		entry := &cache.entries[index]
		if cache.index.compare(entry.start, start) == 0 && cache.index.compare(entry.end, end) == 0 {
			cache.hits++
			return append(destination, entry.values...), true
		}
	}
	values, found := orderedIndexRangeSnapshot(cache.index, start, end)
	if !found {
		return destination, false
	}
	cache.misses++
	entry := orderedIndexRangeCacheEntry[T, K]{start: start, end: end, values: values}
	if len(cache.entries) < cache.capacity {
		cache.entries = append(cache.entries, entry)
	} else {
		cache.entries[cache.next] = entry
		cache.next++
		if cache.next == cache.capacity {
			cache.next = 0
		}
	}
	return append(destination, values...), true
}

// Stats returns cache activity and the number of retained ordered-index
// entries. Values counts tuples, not their byte size.
func (cache *OrderedIndexRangeCache[T, K]) Stats() OrderedIndexRangeCacheStats {
	if cache == nil {
		return OrderedIndexRangeCacheStats{}
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	values := 0
	for _, entry := range cache.entries {
		values += len(entry.values)
	}
	return OrderedIndexRangeCacheStats{Hits: cache.hits, Misses: cache.misses, Entries: len(cache.entries), Values: values}
}

func orderedIndexRangeSnapshot[T any, K any](index *OrderedIndex[T, K], start, end K) ([]OrderedIndexEntry[T, K], bool) {
	index.mu.RLock()
	defer index.mu.RUnlock()
	if len(index.entries) == 0 || index.compare(start, end) > 0 {
		return nil, false
	}
	first := sort.Search(len(index.entries), func(position int) bool {
		return index.compare(index.entries[position].Key, start) >= 0
	})
	if first >= len(index.entries) {
		return nil, false
	}
	limit := sort.Search(len(index.entries), func(position int) bool {
		return index.compare(index.entries[position].Key, end) > 0
	})
	if first >= limit {
		return nil, false
	}
	values := make([]OrderedIndexEntry[T, K], limit-first)
	copy(values, index.entries[first:limit])
	return values, true
}
