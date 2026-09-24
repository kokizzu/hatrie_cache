package hatDataStructure

import (
	"errors"
	"sync"
)

var (
	// ErrRangeTupleCacheNil reports an operation on a nil cache.
	ErrRangeTupleCacheNil = errors.New("hatDataStructure: range tuple cache is nil")
	// ErrRangeTupleCacheCapacityInvalid reports a non-positive cache capacity.
	ErrRangeTupleCacheCapacityInvalid = errors.New("hatDataStructure: range tuple cache capacity must be positive")
)

type rangeTupleCacheKey[K comparable] struct {
	start K
	end   K
}

type rangeTupleCacheEntry[K comparable, V any] struct {
	key     rangeTupleCacheKey[K]
	version uint64
	values  []V
	prev    *rangeTupleCacheEntry[K, V]
	next    *rangeTupleCacheEntry[K, V]
}

// RangeTupleCacheStats reports bounded cache activity. Entries is the current
// number of retained ranges; values returned by Get are owned by the cache and
// must be treated as read-only by callers.
type RangeTupleCacheStats struct {
	Hits      uint64
	Misses    uint64
	Evictions uint64
	Entries   int
}

// RangeTupleCache is a bounded, version-aware LRU cache for immutable range
// results. K must be comparable so a composite range key can be looked up
// without serializing or allocating it. The caller supplies a source version;
// a version mismatch is a miss and removes the stale range.
//
// Put clones values on admission. Get does not allocate, but its returned
// slice is read-only and remains valid even when the same range is replaced.
// The cache is disabled unless a caller explicitly creates it with a positive
// capacity.
type RangeTupleCache[K comparable, V any] struct {
	mu        sync.Mutex
	capacity  int
	entries   map[rangeTupleCacheKey[K]]*rangeTupleCacheEntry[K, V]
	head      *rangeTupleCacheEntry[K, V]
	tail      *rangeTupleCacheEntry[K, V]
	hits      uint64
	misses    uint64
	evictions uint64
}

// NewRangeTupleCache creates a bounded range-result cache.
func NewRangeTupleCache[K comparable, V any](capacity int) (*RangeTupleCache[K, V], error) {
	if capacity <= 0 {
		return nil, ErrRangeTupleCacheCapacityInvalid
	}
	return &RangeTupleCache[K, V]{
		capacity: capacity,
		entries:  make(map[rangeTupleCacheKey[K]]*rangeTupleCacheEntry[K, V], capacity),
	}, nil
}

// Get returns a cached result for the exact range and source version. The
// returned slice must not be modified.
func (cache *RangeTupleCache[K, V]) Get(start, end K, version uint64) ([]V, bool) {
	if cache == nil {
		return nil, false
	}
	key := rangeTupleCacheKey[K]{start: start, end: end}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	entry, ok := cache.entries[key]
	if !ok {
		cache.misses++
		return nil, false
	}
	if entry.version != version {
		cache.removeLocked(entry)
		cache.misses++
		return nil, false
	}
	cache.moveToFrontLocked(entry)
	cache.hits++
	return entry.values, true
}

// Put publishes one range result for a source version. The values are cloned
// before publication so the caller can safely reuse its input buffer.
func (cache *RangeTupleCache[K, V]) Put(start, end K, version uint64, values []V) error {
	if cache == nil {
		return ErrRangeTupleCacheNil
	}
	if cache.capacity <= 0 {
		return ErrRangeTupleCacheCapacityInvalid
	}
	stored := make([]V, len(values))
	copy(stored, values)
	key := rangeTupleCacheKey[K]{start: start, end: end}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if entry, exists := cache.entries[key]; exists {
		entry.version = version
		entry.values = stored
		cache.moveToFrontLocked(entry)
		return nil
	}
	entry := &rangeTupleCacheEntry[K, V]{key: key, version: version, values: stored}
	cache.entries[key] = entry
	cache.pushFrontLocked(entry)
	if len(cache.entries) > cache.capacity {
		cache.removeLocked(cache.tail)
		cache.evictions++
	}
	return nil
}

// Invalidate removes one exact range and reports whether it was cached.
func (cache *RangeTupleCache[K, V]) Invalidate(start, end K) bool {
	if cache == nil {
		return false
	}
	key := rangeTupleCacheKey[K]{start: start, end: end}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	entry, ok := cache.entries[key]
	if !ok {
		return false
	}
	cache.removeLocked(entry)
	return true
}

// Clear drops all retained ranges while preserving the configured capacity.
func (cache *RangeTupleCache[K, V]) Clear() {
	if cache == nil {
		return
	}
	cache.mu.Lock()
	cache.entries = make(map[rangeTupleCacheKey[K]]*rangeTupleCacheEntry[K, V], cache.capacity)
	cache.head = nil
	cache.tail = nil
	cache.mu.Unlock()
}

// Stats returns a point-in-time activity snapshot.
func (cache *RangeTupleCache[K, V]) Stats() RangeTupleCacheStats {
	if cache == nil {
		return RangeTupleCacheStats{}
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	return RangeTupleCacheStats{
		Hits:      cache.hits,
		Misses:    cache.misses,
		Evictions: cache.evictions,
		Entries:   len(cache.entries),
	}
}

func (cache *RangeTupleCache[K, V]) pushFrontLocked(entry *rangeTupleCacheEntry[K, V]) {
	entry.prev = nil
	entry.next = cache.head
	if cache.head != nil {
		cache.head.prev = entry
	} else {
		cache.tail = entry
	}
	cache.head = entry
}

func (cache *RangeTupleCache[K, V]) moveToFrontLocked(entry *rangeTupleCacheEntry[K, V]) {
	if entry == cache.head {
		return
	}
	if entry.prev != nil {
		entry.prev.next = entry.next
	}
	if entry.next != nil {
		entry.next.prev = entry.prev
	} else {
		cache.tail = entry.prev
	}
	cache.pushFrontLocked(entry)
}

func (cache *RangeTupleCache[K, V]) removeLocked(entry *rangeTupleCacheEntry[K, V]) {
	if entry == nil {
		return
	}
	if entry.prev != nil {
		entry.prev.next = entry.next
	} else {
		cache.head = entry.next
	}
	if entry.next != nil {
		entry.next.prev = entry.prev
	} else {
		cache.tail = entry.prev
	}
	delete(cache.entries, entry.key)
	entry.prev = nil
	entry.next = nil
}
