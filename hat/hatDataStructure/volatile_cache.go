package hatDataStructure

import (
	"errors"
	"sync"
	"time"
)

var (
	// ErrVolatileCacheNil reports an operation on a nil cache.
	ErrVolatileCacheNil = errors.New("volatile cache is nil")
	// ErrVolatileCacheInvalidCapacity reports a non-positive cache capacity.
	ErrVolatileCacheInvalidCapacity = errors.New("volatile cache capacity must be positive")
	// ErrVolatileCacheInvalidMaxBytes reports a negative byte limit.
	ErrVolatileCacheInvalidMaxBytes = errors.New("volatile cache max bytes must not be negative")
	// ErrVolatileCacheMissingSizer reports a byte limit without a size function.
	ErrVolatileCacheMissingSizer = errors.New("volatile cache max bytes requires a size function")
	// ErrVolatileCacheNegativeTTL reports a negative item TTL.
	ErrVolatileCacheNegativeTTL = errors.New("volatile cache ttl must not be negative")
	// ErrVolatileCacheNegativeItemSize reports a negative size function result.
	ErrVolatileCacheNegativeItemSize = errors.New("volatile cache item size must not be negative")
	// ErrVolatileCacheItemTooLarge reports an item larger than the byte limit.
	ErrVolatileCacheItemTooLarge = errors.New("volatile cache item exceeds max bytes")
)

// VolatileCacheOptions configures a bounded, memory-only VolatileCache.
// Capacity is required. MaxBytes is optional; zero means unlimited. SizeOf is
// used for byte accounting and is required when MaxBytes is non-zero. Now is
// injectable for deterministic TTL tests and defaults to time.Now.
type VolatileCacheOptions[K comparable, V any] struct {
	Capacity int
	MaxBytes int
	SizeOf   func(K, V) int
	Now      func() time.Time
}

// VolatileCacheStats contains cumulative counters and the current footprint.
// Hits and Misses include both Get and Peek calls. Items and Bytes are current
// values, while all other fields are cumulative since construction. Clear
// removes entries but does not reset cumulative counters.
type VolatileCacheStats struct {
	Hits        uint64
	Misses      uint64
	Sets        uint64
	Deletes     uint64
	Evictions   uint64
	Expirations uint64
	Items       int
	Bytes       int
}

type volatileCacheEntry[K comparable, V any] struct {
	key       K
	value     V
	size      int
	hasExpiry bool
	expiresAt time.Time
	previous  *volatileCacheEntry[K, V]
	next      *volatileCacheEntry[K, V]
}

// VolatileCache is a thread-safe bounded LRU cache intended for ephemeral
// state. It has no persistence or serialization behavior. Values are stored as
// provided; callers that mutate reference values must provide their own
// synchronization or copying.
type VolatileCache[K comparable, V any] struct {
	mu       sync.Mutex
	capacity int
	maxBytes int
	sizeOf   func(K, V) int
	now      func() time.Time
	items    map[K]*volatileCacheEntry[K, V]
	head     *volatileCacheEntry[K, V]
	tail     *volatileCacheEntry[K, V]
	bytes    int
	stats    VolatileCacheStats
}

// NewVolatileCache creates a bounded memory-only cache.
func NewVolatileCache[K comparable, V any](options VolatileCacheOptions[K, V]) (*VolatileCache[K, V], error) {
	if options.Capacity <= 0 {
		return nil, ErrVolatileCacheInvalidCapacity
	}
	if options.MaxBytes < 0 {
		return nil, ErrVolatileCacheInvalidMaxBytes
	}
	if options.MaxBytes > 0 && options.SizeOf == nil {
		return nil, ErrVolatileCacheMissingSizer
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	return &VolatileCache[K, V]{
		capacity: options.Capacity,
		maxBytes: options.MaxBytes,
		sizeOf:   options.SizeOf,
		now:      now,
		items:    make(map[K]*volatileCacheEntry[K, V], options.Capacity),
	}, nil
}

// Set stores value under key. A zero TTL means no expiration. A positive TTL
// is measured from the call and replaces the existing value atomically. An
// invalid or oversized update leaves an existing value unchanged.
func (c *VolatileCache[K, V]) Set(key K, value V, ttl time.Duration) error {
	if c == nil {
		return ErrVolatileCacheNil
	}
	if ttl < 0 {
		return ErrVolatileCacheNegativeTTL
	}
	size, err := c.itemSize(key, value)
	if err != nil {
		return err
	}
	if c.maxBytes > 0 && size > c.maxBytes {
		return ErrVolatileCacheItemTooLarge
	}

	var now time.Time
	if ttl > 0 {
		now = c.now()
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if existing, ok := c.items[key]; ok {
		if existing.hasExpiry {
			if now.IsZero() {
				now = c.now()
			}
			if !now.Before(existing.expiresAt) {
				c.removeEntry(existing)
				c.stats.Expirations++
				existing = nil
			}
		}
		if existing != nil {
			c.bytes += size - existing.size
			existing.value = value
			existing.size = size
			existing.hasExpiry = ttl > 0
			if existing.hasExpiry {
				existing.expiresAt = now.Add(ttl)
			} else {
				existing.expiresAt = time.Time{}
			}
			c.moveToFront(existing)
			c.stats.Sets++
			c.evictIfNeeded()
			return nil
		}
	}

	entry := &volatileCacheEntry[K, V]{
		key:       key,
		value:     value,
		size:      size,
		hasExpiry: ttl > 0,
		expiresAt: now.Add(ttl),
	}
	c.items[key] = entry
	c.bytes += size
	c.insertFront(entry)
	c.stats.Sets++
	c.evictIfNeeded()
	return nil
}

// Get returns the value for key and promotes a hit to the LRU front.
func (c *VolatileCache[K, V]) Get(key K) (V, bool) {
	return c.lookup(key, true)
}

// Peek returns the value for key without changing its LRU position.
func (c *VolatileCache[K, V]) Peek(key K) (V, bool) {
	return c.lookup(key, false)
}

func (c *VolatileCache[K, V]) lookup(key K, promote bool) (V, bool) {
	var zero V
	if c == nil {
		return zero, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.items[key]
	if !ok {
		c.stats.Misses++
		return zero, false
	}
	if entry.hasExpiry && !c.now().Before(entry.expiresAt) {
		c.removeEntry(entry)
		c.stats.Expirations++
		c.stats.Misses++
		return zero, false
	}
	c.stats.Hits++
	if promote {
		c.moveToFront(entry)
	}
	return entry.value, true
}

// Delete removes key and reports whether an entry was present.
func (c *VolatileCache[K, V]) Delete(key K) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.items[key]
	if !ok {
		return false
	}
	c.removeEntry(entry)
	c.stats.Deletes++
	return true
}

// PurgeExpired removes all entries expired at or before now and returns the
// number removed. It is the explicit O(n) cleanup operation for idle caches.
func (c *VolatileCache[K, V]) PurgeExpired(now time.Time) int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	removed := 0
	for _, entry := range c.items {
		if entry.hasExpiry && !now.Before(entry.expiresAt) {
			c.removeEntry(entry)
			c.stats.Expirations++
			removed++
		}
	}
	return removed
}

// Clear removes all entries while retaining cumulative operation counters.
func (c *VolatileCache[K, V]) Clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = make(map[K]*volatileCacheEntry[K, V], c.capacity)
	c.head = nil
	c.tail = nil
	c.bytes = 0
}

// Len returns the number of live entries currently held by the cache. Expired
// entries are removed lazily when accessed or explicitly purged.
func (c *VolatileCache[K, V]) Len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

// Stats returns cumulative counters and the current item/byte footprint.
func (c *VolatileCache[K, V]) Stats() VolatileCacheStats {
	if c == nil {
		return VolatileCacheStats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	stats := c.stats
	stats.Items = len(c.items)
	stats.Bytes = c.bytes
	return stats
}

func (c *VolatileCache[K, V]) itemSize(key K, value V) (int, error) {
	if c.sizeOf == nil {
		return 0, nil
	}
	size := c.sizeOf(key, value)
	if size < 0 {
		return 0, ErrVolatileCacheNegativeItemSize
	}
	return size, nil
}

func (c *VolatileCache[K, V]) insertFront(entry *volatileCacheEntry[K, V]) {
	entry.previous = nil
	entry.next = c.head
	if c.head != nil {
		c.head.previous = entry
	} else {
		c.tail = entry
	}
	c.head = entry
}

func (c *VolatileCache[K, V]) moveToFront(entry *volatileCacheEntry[K, V]) {
	if c.head == entry {
		return
	}
	if entry.previous != nil {
		entry.previous.next = entry.next
	} else {
		c.head = entry.next
	}
	if entry.next != nil {
		entry.next.previous = entry.previous
	} else {
		c.tail = entry.previous
	}
	c.insertFront(entry)
}

func (c *VolatileCache[K, V]) removeEntry(entry *volatileCacheEntry[K, V]) {
	if entry.previous != nil {
		entry.previous.next = entry.next
	} else {
		c.head = entry.next
	}
	if entry.next != nil {
		entry.next.previous = entry.previous
	} else {
		c.tail = entry.previous
	}
	delete(c.items, entry.key)
	c.bytes -= entry.size
	entry.previous = nil
	entry.next = nil
}

func (c *VolatileCache[K, V]) evictIfNeeded() {
	for len(c.items) > c.capacity || (c.maxBytes > 0 && c.bytes > c.maxBytes) {
		if c.tail == nil {
			return
		}
		c.removeEntry(c.tail)
		c.stats.Evictions++
	}
}
