package hatDataStructure

import "errors"

const (
	// MaxFrequencyAdmissionCacheCapacity prevents accidental unbounded cache state.
	MaxFrequencyAdmissionCacheCapacity = 1 << 20
	// MaxFrequencyAdmissionCacheCounterCount bounds the compact frequency sketch.
	MaxFrequencyAdmissionCacheCounterCount = 1 << 24
	frequencyAdmissionMinimumCounterCount  = 64
	frequencyAdmissionProbes               = 4
	frequencyAdmissionNibbleMask           = uint64(0x7777777777777777)
)

var (
	// ErrFrequencyAdmissionCacheCapacityInvalid indicates an unsupported cache capacity.
	ErrFrequencyAdmissionCacheCapacityInvalid = errors.New("hatriecache: frequency admission cache capacity is invalid")
	// ErrFrequencyAdmissionCacheHashRequired indicates that no key hash was supplied.
	ErrFrequencyAdmissionCacheHashRequired = errors.New("hatriecache: frequency admission cache hash is required")
	// ErrFrequencyAdmissionCacheCounterCountInvalid indicates an unsupported sketch size.
	ErrFrequencyAdmissionCacheCounterCountInvalid = errors.New("hatriecache: frequency admission cache counter count is invalid")
)

// FrequencyAdmissionCacheOptions configures a bounded frequency-admission cache.
// Hash must be stable for the lifetime of the cache and should distribute keys
// uniformly. The cache is intentionally not synchronized; callers that share a
// cache between goroutines should provide their own synchronization.
type FrequencyAdmissionCacheOptions[K comparable] struct {
	Capacity     int
	Hash         func(K) uint64
	CounterCount int
	SampleWindow uint64
}

// FrequencyAdmissionCacheStats is a point-in-time cache and admission snapshot.
type FrequencyAdmissionCacheStats struct {
	Capacity     int
	Entries      int
	CounterCount int
	CounterBytes int
	Hits         uint64
	Misses       uint64
	Admitted     uint64
	Rejected     uint64
	Evictions    uint64
	Aged         uint64
}

type frequencyAdmissionCacheEntry[K comparable, V any] struct {
	key       K
	value     V
	frequency uint8
	previous  int32
	next      int32
}

// FrequencyAdmissionCache is a bounded cache that admits a new key only when
// its approximate frequency is at least that of the least-recently-used
// resident key. A packed four-bit count-min sketch makes scans less likely to
// displace frequently reused values while keeping the memory bound explicit.
//
// Get misses are observed by the sketch. This lets a caller warm a candidate
// before Set without storing an unbounded stream of cold values.
type FrequencyAdmissionCache[K comparable, V any] struct {
	capacity     int
	hash         func(K) uint64
	counterCount int
	counterMask  uint64
	counters     []uint64
	sampleWindow uint64
	observations uint64
	entries      []frequencyAdmissionCacheEntry[K, V]
	indexes      map[K]int32
	freeHead     int32
	front        int32
	back         int32
	length       int
	stats        FrequencyAdmissionCacheStats
}

// NewFrequencyAdmissionCache creates a bounded, non-thread-safe cache.
func NewFrequencyAdmissionCache[K comparable, V any](options FrequencyAdmissionCacheOptions[K]) (*FrequencyAdmissionCache[K, V], error) {
	if options.Capacity <= 0 || options.Capacity > MaxFrequencyAdmissionCacheCapacity {
		return nil, ErrFrequencyAdmissionCacheCapacityInvalid
	}
	if options.Hash == nil {
		return nil, ErrFrequencyAdmissionCacheHashRequired
	}
	counterCount := options.CounterCount
	if counterCount == 0 {
		counterCount = frequencyAdmissionDefaultCounterCount(options.Capacity)
	}
	if counterCount < frequencyAdmissionMinimumCounterCount || counterCount > MaxFrequencyAdmissionCacheCounterCount || counterCount&(counterCount-1) != 0 {
		return nil, ErrFrequencyAdmissionCacheCounterCountInvalid
	}
	sampleWindow := options.SampleWindow
	if sampleWindow == 0 {
		sampleWindow = uint64(options.Capacity) * 10
		if sampleWindow < 64 {
			sampleWindow = 64
		}
	}
	cache := &FrequencyAdmissionCache[K, V]{
		capacity:     options.Capacity,
		hash:         options.Hash,
		counterCount: counterCount,
		counterMask:  uint64(counterCount - 1),
		counters:     make([]uint64, (counterCount+15)/16),
		sampleWindow: sampleWindow,
		entries:      make([]frequencyAdmissionCacheEntry[K, V], options.Capacity),
		indexes:      make(map[K]int32, options.Capacity),
		freeHead:     -1,
		front:        -1,
		back:         -1,
	}
	for index := range cache.entries {
		cache.entries[index].previous = -1
		cache.entries[index].next = cache.freeHead
		cache.freeHead = int32(index)
	}
	cache.stats.Capacity = options.Capacity
	cache.stats.CounterCount = counterCount
	cache.stats.CounterBytes = len(cache.counters) * 8
	return cache, nil
}

// Get returns a resident value and records either a hit or a miss.
func (cache *FrequencyAdmissionCache[K, V]) Get(key K) (V, bool) {
	var zero V
	if cache == nil {
		return zero, false
	}
	cache.recordObservation()
	index, ok := cache.indexes[key]
	if !ok {
		cache.stats.Misses++
		cache.increment(key)
		return zero, false
	}
	cache.stats.Hits++
	cache.incrementResident(index)
	cache.moveFront(index)
	return cache.entries[index].value, true
}

// Set stores value when key is already resident or when the admission policy
// accepts it. It returns false when a cold candidate is rejected.
func (cache *FrequencyAdmissionCache[K, V]) Set(key K, value V) bool {
	if cache == nil {
		return false
	}
	cache.recordObservation()
	if index, ok := cache.indexes[key]; ok {
		cache.entries[index].value = value
		cache.incrementResident(index)
		cache.moveFront(index)
		return true
	}
	cache.increment(key)
	candidateFrequency := cache.estimate(key)
	index := int32(-1)
	if cache.length == cache.capacity {
		victim := cache.back
		if victim < 0 || cache.entries[victim].frequency >= candidateFrequency {
			cache.stats.Rejected++
			return false
		}
		cache.removeResident(victim)
		index = victim
		cache.stats.Evictions++
	} else {
		index = cache.takeFree()
	}
	if index < 0 {
		return false
	}
	cache.entries[index] = frequencyAdmissionCacheEntry[K, V]{key: key, value: value, frequency: candidateFrequency, previous: -1, next: -1}
	cache.indexes[key] = index
	cache.length++
	cache.linkFront(index)
	cache.stats.Admitted++
	return true
}

// Delete removes key and reports whether it was resident.
func (cache *FrequencyAdmissionCache[K, V]) Delete(key K) bool {
	if cache == nil {
		return false
	}
	index, ok := cache.indexes[key]
	if !ok {
		return false
	}
	cache.removeResident(index)
	cache.entries[index].next = cache.freeHead
	cache.freeHead = index
	return true
}

// Clear removes all resident values without resetting cumulative statistics.
func (cache *FrequencyAdmissionCache[K, V]) Clear() {
	if cache == nil {
		return
	}
	clear(cache.indexes)
	cache.freeHead = -1
	cache.front = -1
	cache.back = -1
	cache.length = 0
	for index := range cache.entries {
		cache.entries[index] = frequencyAdmissionCacheEntry[K, V]{previous: -1, next: cache.freeHead}
		cache.freeHead = int32(index)
	}
}

// Len returns the number of resident values.
func (cache *FrequencyAdmissionCache[K, V]) Len() int {
	if cache == nil {
		return 0
	}
	return cache.length
}

// Stats returns cumulative counters and current bounded memory metadata.
func (cache *FrequencyAdmissionCache[K, V]) Stats() FrequencyAdmissionCacheStats {
	if cache == nil {
		return FrequencyAdmissionCacheStats{}
	}
	stats := cache.stats
	stats.Entries = cache.length
	return stats
}

func (cache *FrequencyAdmissionCache[K, V]) takeFree() int32 {
	index := cache.freeHead
	if index >= 0 {
		cache.freeHead = cache.entries[index].next
	}
	return index
}

func (cache *FrequencyAdmissionCache[K, V]) removeResident(index int32) {
	var zeroKey K
	var zeroValue V
	delete(cache.indexes, cache.entries[index].key)
	cache.unlink(index)
	cache.entries[index].key = zeroKey
	cache.entries[index].value = zeroValue
	cache.entries[index].frequency = 0
	cache.length--
}

func (cache *FrequencyAdmissionCache[K, V]) moveFront(index int32) {
	if cache.front == index {
		return
	}
	cache.unlink(index)
	cache.linkFront(index)
}

func (cache *FrequencyAdmissionCache[K, V]) unlink(index int32) {
	entry := &cache.entries[index]
	if entry.previous >= 0 {
		cache.entries[entry.previous].next = entry.next
	} else if cache.front == index {
		cache.front = entry.next
	}
	if entry.next >= 0 {
		cache.entries[entry.next].previous = entry.previous
	} else if cache.back == index {
		cache.back = entry.previous
	}
	entry.previous = -1
	entry.next = -1
}

func (cache *FrequencyAdmissionCache[K, V]) linkFront(index int32) {
	entry := &cache.entries[index]
	entry.previous = -1
	entry.next = cache.front
	if cache.front >= 0 {
		cache.entries[cache.front].previous = index
	} else {
		cache.back = index
	}
	cache.front = index
}

func (cache *FrequencyAdmissionCache[K, V]) recordObservation() {
	cache.observations++
	if cache.observations >= cache.sampleWindow {
		for index := range cache.counters {
			cache.counters[index] = (cache.counters[index] >> 1) & frequencyAdmissionNibbleMask
		}
		for index := cache.front; index >= 0; index = cache.entries[index].next {
			cache.entries[index].frequency >>= 1
		}
		cache.observations = 0
		cache.stats.Aged++
	}
}

func (cache *FrequencyAdmissionCache[K, V]) incrementResident(index int32) {
	if cache.entries[index].frequency < 15 {
		cache.entries[index].frequency++
	}
}

func (cache *FrequencyAdmissionCache[K, V]) increment(key K) {
	hash := cache.hash(key)
	for probe := uint64(0); probe < frequencyAdmissionProbes; probe++ {
		position := frequencyAdmissionMix(hash, probe) & cache.counterMask
		word := position >> 4
		shift := (position & 15) * 4
		value := (cache.counters[word] >> shift) & 15
		if value < 15 {
			cache.counters[word] += uint64(1) << shift
		}
	}
}

func (cache *FrequencyAdmissionCache[K, V]) estimate(key K) uint8 {
	hash := cache.hash(key)
	minimum := uint8(15)
	for probe := uint64(0); probe < frequencyAdmissionProbes; probe++ {
		position := frequencyAdmissionMix(hash, probe) & cache.counterMask
		word := position >> 4
		shift := (position & 15) * 4
		value := uint8((cache.counters[word] >> shift) & 15)
		if value < minimum {
			minimum = value
		}
	}
	return minimum
}

func frequencyAdmissionDefaultCounterCount(capacity int) int {
	target := capacity * 4
	if target < frequencyAdmissionMinimumCounterCount {
		target = frequencyAdmissionMinimumCounterCount
	}
	counterCount := 1
	for counterCount < target {
		counterCount <<= 1
	}
	return counterCount
}

func frequencyAdmissionMix(hash, probe uint64) uint64 {
	hash += 0x9e3779b97f4a7c15 * (probe + 1)
	hash = (hash ^ (hash >> 30)) * 0xbf58476d1ce4e5b9
	hash = (hash ^ (hash >> 27)) * 0x94d049bb133111eb
	return hash ^ (hash >> 31)
}
