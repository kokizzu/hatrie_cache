package hatDataStructure

import (
	"errors"
	"sync"
)

const (
	// DefaultPageIndexResidencyMaxEntries bounds the number of resident page
	// indexes when MaxEntries is zero.
	DefaultPageIndexResidencyMaxEntries = 1024
	maxPageIndexResidencyEntries        = 1 << 20
	maxInitialPageIndexResidencyEntries = 1024
)

var (
	// ErrPageIndexResidencyDisabled reports a zero byte budget. A zero budget
	// keeps this optional policy disabled instead of retaining page indexes.
	ErrPageIndexResidencyDisabled = errors.New("hatriecache: page-index residency is disabled")
	// ErrPageIndexResidencyOptions reports an invalid entry limit.
	ErrPageIndexResidencyOptions = errors.New("hatriecache: page-index residency options are invalid")
)

// PageIndexResidencyOptions configures a bounded immutable page-index cache.
// Callers provide the retained byte size for each page index on Put.
type PageIndexResidencyOptions struct {
	// MaxBytes is required and bounds the total admitted page-index bytes.
	MaxBytes uint64
	// MaxEntries bounds the number of page indexes. Zero selects
	// DefaultPageIndexResidencyMaxEntries.
	MaxEntries int
}

// PageIndexResidencyStats is a point-in-time residency and admission snapshot.
type PageIndexResidencyStats struct {
	MaxBytes     uint64
	MaxEntries   int
	Bytes        uint64
	Entries      int
	Hits         uint64
	Misses       uint64
	Admissions   uint64
	Replacements uint64
	Evictions    uint64
	Rejections   uint64
}

type pageIndexResidencyEntry[K comparable, V any] struct {
	key   K
	value V
	bytes uint64
	prev  int32
	next  int32
}

const noPageIndexResidencySlot int32 = -1

// PageIndexResidency retains caller-owned immutable page indexes behind a
// byte- and entry-bounded LRU policy. Put never copies the value; callers must
// not mutate a value after admission. Get moves a resident page to the MRU
// position and performs no heap allocation.
type PageIndexResidency[K comparable, V any] struct {
	mu         sync.Mutex
	maxBytes   uint64
	maxEntries int
	bytes      uint64
	entries    map[K]uint32
	nodes      []pageIndexResidencyEntry[K, V]
	free       []uint32
	head       int32
	tail       int32
	stats      PageIndexResidencyStats
}

// NewPageIndexResidency creates a bounded page-index policy. MaxBytes must be
// positive; zero intentionally disables this optional cache policy.
func NewPageIndexResidency[K comparable, V any](options PageIndexResidencyOptions) (*PageIndexResidency[K, V], error) {
	if options.MaxBytes == 0 {
		return nil, ErrPageIndexResidencyDisabled
	}
	if options.MaxEntries == 0 {
		options.MaxEntries = DefaultPageIndexResidencyMaxEntries
	}
	if options.MaxEntries < 1 || options.MaxEntries > maxPageIndexResidencyEntries {
		return nil, ErrPageIndexResidencyOptions
	}
	return &PageIndexResidency[K, V]{
		maxBytes:   options.MaxBytes,
		maxEntries: options.MaxEntries,
		head:       noPageIndexResidencySlot,
		tail:       noPageIndexResidencySlot,
		stats: PageIndexResidencyStats{
			MaxBytes:   options.MaxBytes,
			MaxEntries: options.MaxEntries,
		},
	}, nil
}

// Get returns a resident page index and marks it most recently used.
func (cache *PageIndexResidency[K, V]) Get(key K) (V, bool) {
	if cache == nil {
		var zero V
		return zero, false
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	slot, ok := cache.entries[key]
	if !ok {
		cache.stats.Misses++
		var zero V
		return zero, false
	}
	cache.stats.Hits++
	index := int32(slot - 1)
	value := cache.nodes[index].value
	cache.moveToFrontLocked(index)
	return value, true
}

// Put admits or replaces one immutable page index. An entry larger than the
// byte budget is rejected without evicting an existing resident page.
func (cache *PageIndexResidency[K, V]) Put(key K, value V, sizeBytes uint64) bool {
	if cache == nil {
		return false
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if sizeBytes > cache.maxBytes {
		cache.stats.Rejections++
		return false
	}
	if slot, ok := cache.entries[key]; ok {
		entry := &cache.nodes[slot-1]
		cache.bytes -= entry.bytes
		entry.value = value
		entry.bytes = sizeBytes
		cache.bytes += sizeBytes
		cache.stats.Replacements++
		cache.moveToFrontLocked(int32(slot - 1))
		cache.evictToBudgetLocked()
		return true
	}
	for cache.bytes > cache.maxBytes-sizeBytes || len(cache.entries) >= cache.maxEntries {
		if !cache.evictOldestLocked() {
			return false
		}
	}
	cache.prepareStorageLocked()
	var index int32
	if free := len(cache.free); free > 0 {
		index = int32(cache.free[free-1])
		cache.free = cache.free[:free-1]
	} else {
		index = int32(len(cache.nodes))
		cache.nodes = append(cache.nodes, pageIndexResidencyEntry[K, V]{})
	}
	entry := &cache.nodes[index]
	*entry = pageIndexResidencyEntry[K, V]{
		key:   key,
		value: value,
		bytes: sizeBytes,
		prev:  noPageIndexResidencySlot,
		next:  noPageIndexResidencySlot,
	}
	cache.entries[key] = uint32(index) + 1
	cache.bytes += sizeBytes
	cache.pushFrontLocked(index)
	cache.stats.Admissions++
	return true
}

// Delete removes one resident page index and reports whether it was present.
func (cache *PageIndexResidency[K, V]) Delete(key K) bool {
	if cache == nil {
		return false
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	slot, ok := cache.entries[key]
	if !ok {
		return false
	}
	cache.removeLocked(int32(slot - 1))
	return true
}

// Clear removes all resident page indexes and returns the number removed.
// Cumulative hit, miss, and eviction counters are retained.
func (cache *PageIndexResidency[K, V]) Clear() int {
	if cache == nil {
		return 0
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	removed := len(cache.entries)
	clear(cache.entries)
	var zero pageIndexResidencyEntry[K, V]
	for index := range cache.nodes {
		cache.nodes[index] = zero
	}
	cache.nodes = cache.nodes[:0]
	cache.free = cache.free[:0]
	cache.head = noPageIndexResidencySlot
	cache.tail = noPageIndexResidencySlot
	cache.bytes = 0
	return removed
}

// Len returns the number of resident page indexes.
func (cache *PageIndexResidency[K, V]) Len() int {
	if cache == nil {
		return 0
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	return len(cache.entries)
}

// Stats returns a point-in-time copy of residency, budget, and admission
// counters.
func (cache *PageIndexResidency[K, V]) Stats() PageIndexResidencyStats {
	if cache == nil {
		return PageIndexResidencyStats{}
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	stats := cache.stats
	stats.Bytes = cache.bytes
	stats.Entries = len(cache.entries)
	return stats
}

func (cache *PageIndexResidency[K, V]) evictToBudgetLocked() {
	for cache.bytes > cache.maxBytes || len(cache.entries) > cache.maxEntries {
		if !cache.evictOldestLocked() {
			return
		}
	}
}

func (cache *PageIndexResidency[K, V]) prepareStorageLocked() {
	if cache.nodes != nil || cache.entries != nil {
		return
	}
	capacity := cache.maxEntries
	if capacity > maxInitialPageIndexResidencyEntries {
		capacity = maxInitialPageIndexResidencyEntries
	}
	cache.entries = make(map[K]uint32, capacity)
	cache.nodes = make([]pageIndexResidencyEntry[K, V], 0, capacity)
}

func (cache *PageIndexResidency[K, V]) evictOldestLocked() bool {
	if cache.tail == noPageIndexResidencySlot {
		return false
	}
	cache.removeLocked(cache.tail)
	cache.stats.Evictions++
	return true
}

func (cache *PageIndexResidency[K, V]) removeLocked(index int32) {
	entry := &cache.nodes[index]
	if entry.prev != noPageIndexResidencySlot {
		cache.nodes[entry.prev].next = entry.next
	} else {
		cache.head = entry.next
	}
	if entry.next != noPageIndexResidencySlot {
		cache.nodes[entry.next].prev = entry.prev
	} else {
		cache.tail = entry.prev
	}
	delete(cache.entries, entry.key)
	cache.bytes -= entry.bytes
	*entry = pageIndexResidencyEntry[K, V]{}
	cache.free = append(cache.free, uint32(index))
}

func (cache *PageIndexResidency[K, V]) pushFrontLocked(index int32) {
	entry := &cache.nodes[index]
	entry.prev = noPageIndexResidencySlot
	entry.next = cache.head
	if cache.head != noPageIndexResidencySlot {
		cache.nodes[cache.head].prev = index
	} else {
		cache.tail = index
	}
	cache.head = index
}

func (cache *PageIndexResidency[K, V]) moveToFrontLocked(index int32) {
	if cache.head == index {
		return
	}
	entry := &cache.nodes[index]
	if entry.prev != noPageIndexResidencySlot {
		cache.nodes[entry.prev].next = entry.next
	}
	if entry.next != noPageIndexResidencySlot {
		cache.nodes[entry.next].prev = entry.prev
	} else {
		cache.tail = entry.prev
	}
	cache.pushFrontLocked(index)
}
