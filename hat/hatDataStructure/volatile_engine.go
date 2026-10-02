package hatDataStructure

import (
	"container/list"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultVolatileEngineMaxEntries bounds the number of entries when the
	// caller does not provide an explicit limit.
	DefaultVolatileEngineMaxEntries = 100000
	// DefaultVolatileEngineMaxBytes bounds key and value bytes together.
	DefaultVolatileEngineMaxBytes uint64 = 64 << 20
	// DefaultVolatileEngineMaxKeyBytes bounds one key.
	DefaultVolatileEngineMaxKeyBytes = 256
	// DefaultVolatileEngineMaxValueBytes bounds one value.
	DefaultVolatileEngineMaxValueBytes uint64 = 16 << 20
)

var (
	ErrVolatileEngineOptionsInvalid = errors.New("volatile engine options invalid")
	ErrVolatileEngineKeyEmpty       = errors.New("volatile engine key is empty")
	ErrVolatileEngineKeyTooLong     = errors.New("volatile engine key is too long")
	ErrVolatileEngineValueTooLarge  = errors.New("volatile engine value is too large")
	ErrVolatileEngineCapacity       = errors.New("volatile engine capacity exceeded")
	ErrVolatileEngineTTLInvalid     = errors.New("volatile engine ttl is invalid")
	ErrVolatileEngineInvalid        = errors.New("volatile engine is nil")
)

// VolatileEvictionPolicy controls what happens when a write would exceed a
// configured bound.
type VolatileEvictionPolicy uint8

const (
	// VolatileEvictionReject preserves existing entries and rejects the write.
	VolatileEvictionReject VolatileEvictionPolicy = iota
	// VolatileEvictionOldest removes oldest entries until the write fits.
	VolatileEvictionOldest
)

// VolatileEngineOptions configures a bounded, memory-only key/value engine.
// A zero limit selects the corresponding documented default. A zero TTL means
// no expiry; a positive TTL is measured from the engine's clock.
type VolatileEngineOptions struct {
	MaxEntries     int
	MaxBytes       uint64
	MaxKeyBytes    int
	MaxValueBytes  uint64
	EvictionPolicy VolatileEvictionPolicy
	Now            func() time.Time
}

// VolatileEngineStats contains cumulative operation counters and current
// occupancy. Entries and Bytes are point-in-time values.
type VolatileEngineStats struct {
	Entries    uint64
	Bytes      uint64
	Gets       uint64
	Hits       uint64
	Misses     uint64
	Sets       uint64
	SetRejects uint64
	Deletes    uint64
	Expired    uint64
	Evictions  uint64
}

type volatileEngineEntry struct {
	key       string
	value     []byte
	bytes     uint64
	expiresAt time.Time
	element   *list.Element
}

// VolatileEngine is an explicit bounded, in-memory cache. It never writes a
// journal or creates storage files, so callers must treat its contents as
// disposable and choose a durable data structure separately when needed.
type VolatileEngine struct {
	mu sync.RWMutex

	entries map[string]*volatileEngineEntry
	order   *list.List

	maxEntries     int
	maxBytes       uint64
	maxKeyBytes    int
	maxValueBytes  uint64
	evictionPolicy VolatileEvictionPolicy
	now            func() time.Time
	bytes          uint64

	gets       atomic.Uint64
	hits       atomic.Uint64
	misses     atomic.Uint64
	sets       atomic.Uint64
	setRejects atomic.Uint64
	deletes    atomic.Uint64
	expired    atomic.Uint64
	evictions  atomic.Uint64
}

// NewVolatileEngine creates a bounded memory-only engine.
func NewVolatileEngine(options VolatileEngineOptions) (*VolatileEngine, error) {
	if options.MaxEntries < 0 || options.MaxKeyBytes < 0 {
		return nil, ErrVolatileEngineOptionsInvalid
	}
	if options.EvictionPolicy != VolatileEvictionReject && options.EvictionPolicy != VolatileEvictionOldest {
		return nil, ErrVolatileEngineOptionsInvalid
	}

	maxEntries := options.MaxEntries
	if maxEntries == 0 {
		maxEntries = DefaultVolatileEngineMaxEntries
	}
	maxBytes := options.MaxBytes
	if maxBytes == 0 {
		maxBytes = DefaultVolatileEngineMaxBytes
	}
	maxKeyBytes := options.MaxKeyBytes
	if maxKeyBytes == 0 {
		maxKeyBytes = DefaultVolatileEngineMaxKeyBytes
	}
	maxValueBytes := options.MaxValueBytes
	if maxValueBytes == 0 {
		maxValueBytes = DefaultVolatileEngineMaxValueBytes
	}
	if maxEntries < 1 || maxBytes == 0 || maxKeyBytes < 1 || maxValueBytes == 0 {
		return nil, ErrVolatileEngineOptionsInvalid
	}

	now := options.Now
	if now == nil {
		now = time.Now
	}
	return &VolatileEngine{
		entries:        make(map[string]*volatileEngineEntry),
		order:          list.New(),
		maxEntries:     maxEntries,
		maxBytes:       maxBytes,
		maxKeyBytes:    maxKeyBytes,
		maxValueBytes:  maxValueBytes,
		evictionPolicy: options.EvictionPolicy,
		now:            now,
	}, nil
}

// Set stores a copy of value. Existing entries are replaced atomically. ttl
// must be zero or positive; positive values expire after that duration.
func (engine *VolatileEngine) Set(key string, value []byte, ttl time.Duration) error {
	if engine == nil {
		return ErrVolatileEngineInvalid
	}
	if err := engine.validateKey(key); err != nil {
		return err
	}
	if uint64(len(value)) > engine.maxValueBytes {
		return ErrVolatileEngineValueTooLarge
	}
	if ttl < 0 {
		return ErrVolatileEngineTTLInvalid
	}

	keyBytes := uint64(len(key))
	valueBytes := uint64(len(value))
	if keyBytes > ^uint64(0)-valueBytes {
		return ErrVolatileEngineCapacity
	}
	entryBytes := keyBytes + valueBytes
	if entryBytes > engine.maxBytes {
		return ErrVolatileEngineCapacity
	}
	now := engine.now()
	expiresAt := time.Time{}
	if ttl > 0 {
		expiresAt = now.Add(ttl)
	}

	engine.mu.Lock()
	defer engine.mu.Unlock()
	engine.purgeExpiredLocked(now)

	old := engine.entries[key]
	if engine.wouldExceedLocked(old, entryBytes) {
		engine.evictUntilFitsLocked(old, entryBytes)
	}
	if engine.wouldExceedLocked(old, entryBytes) {
		engine.setRejects.Add(1)
		return ErrVolatileEngineCapacity
	}

	copied := append([]byte(nil), value...)
	if old != nil {
		engine.bytes -= old.bytes
		old.value = copied
		old.bytes = entryBytes
		old.expiresAt = expiresAt
		engine.bytes += entryBytes
	} else {
		entry := &volatileEngineEntry{
			key:       key,
			value:     copied,
			bytes:     entryBytes,
			expiresAt: expiresAt,
		}
		entry.element = engine.order.PushBack(entry)
		engine.entries[key] = entry
		engine.bytes += entryBytes
	}
	engine.sets.Add(1)
	return nil
}

// Get returns a copy of the stored value.
func (engine *VolatileEngine) Get(key string) ([]byte, bool) {
	return engine.GetInto(key, nil)
}

// GetInto copies a stored value into destination, reusing its capacity when
// possible. The returned slice aliases destination and is safe for the caller
// to retain or modify.
func (engine *VolatileEngine) GetInto(key string, destination []byte) ([]byte, bool) {
	if engine == nil || engine.validateKey(key) != nil {
		return nil, false
	}
	engine.gets.Add(1)
	now := engine.now()

	engine.mu.RLock()
	entry := engine.entries[key]
	if entry != nil && !volatileEntryExpired(entry, now) {
		destination = append(destination[:0], entry.value...)
		engine.mu.RUnlock()
		engine.hits.Add(1)
		return destination, true
	}
	engine.mu.RUnlock()

	if entry == nil {
		engine.misses.Add(1)
		return nil, false
	}

	engine.mu.Lock()
	entry = engine.entries[key]
	if entry == nil {
		engine.mu.Unlock()
		engine.misses.Add(1)
		return nil, false
	}
	if volatileEntryExpired(entry, now) {
		engine.removeEntryLocked(entry, false)
		engine.expired.Add(1)
		engine.mu.Unlock()
		engine.misses.Add(1)
		return nil, false
	}
	destination = append(destination[:0], entry.value...)
	engine.mu.Unlock()
	engine.hits.Add(1)
	return destination, true
}

// Delete removes key and reports whether an entry was present.
func (engine *VolatileEngine) Delete(key string) bool {
	if engine == nil || engine.validateKey(key) != nil {
		return false
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	entry := engine.entries[key]
	if entry == nil {
		return false
	}
	engine.removeEntryLocked(entry, false)
	engine.deletes.Add(1)
	return true
}

// PurgeExpired removes all expired entries and returns the number removed.
func (engine *VolatileEngine) PurgeExpired() int {
	if engine == nil {
		return 0
	}
	engine.mu.Lock()
	removed := engine.purgeExpiredLocked(engine.now())
	engine.mu.Unlock()
	return removed
}

// Clear removes all entries and returns the number removed.
func (engine *VolatileEngine) Clear() int {
	if engine == nil {
		return 0
	}
	engine.mu.Lock()
	removed := len(engine.entries)
	engine.entries = make(map[string]*volatileEngineEntry)
	engine.order.Init()
	engine.bytes = 0
	engine.mu.Unlock()
	return removed
}

// Len returns the current number of entries after removing expired entries.
func (engine *VolatileEngine) Len() int {
	if engine == nil {
		return 0
	}
	engine.mu.Lock()
	engine.purgeExpiredLocked(engine.now())
	length := len(engine.entries)
	engine.mu.Unlock()
	return length
}

// Stats returns current occupancy and cumulative operation counters.
func (engine *VolatileEngine) Stats() VolatileEngineStats {
	if engine == nil {
		return VolatileEngineStats{}
	}
	engine.mu.RLock()
	entries := uint64(len(engine.entries))
	bytes := engine.bytes
	engine.mu.RUnlock()
	return VolatileEngineStats{
		Entries:    entries,
		Bytes:      bytes,
		Gets:       engine.gets.Load(),
		Hits:       engine.hits.Load(),
		Misses:     engine.misses.Load(),
		Sets:       engine.sets.Load(),
		SetRejects: engine.setRejects.Load(),
		Deletes:    engine.deletes.Load(),
		Expired:    engine.expired.Load(),
		Evictions:  engine.evictions.Load(),
	}
}

func (engine *VolatileEngine) validateKey(key string) error {
	if key == "" {
		return ErrVolatileEngineKeyEmpty
	}
	if len(key) > engine.maxKeyBytes {
		return ErrVolatileEngineKeyTooLong
	}
	return nil
}

func (engine *VolatileEngine) wouldExceedLocked(old *volatileEngineEntry, newBytes uint64) bool {
	entryCount := len(engine.entries)
	oldBytes := uint64(0)
	if old == nil {
		entryCount++
	} else {
		oldBytes = old.bytes
	}
	return entryCount > engine.maxEntries || engine.bytes-oldBytes+newBytes > engine.maxBytes
}

func (engine *VolatileEngine) evictUntilFitsLocked(old *volatileEngineEntry, newBytes uint64) {
	if engine.evictionPolicy != VolatileEvictionOldest {
		return
	}
	for engine.wouldExceedLocked(old, newBytes) {
		front := engine.order.Front()
		if front == nil {
			return
		}
		entry := front.Value.(*volatileEngineEntry)
		if entry == old {
			front = front.Next()
			if front == nil {
				return
			}
			entry = front.Value.(*volatileEngineEntry)
		}
		engine.removeEntryLocked(entry, true)
	}
}

func (engine *VolatileEngine) purgeExpiredLocked(now time.Time) int {
	removed := 0
	for element := engine.order.Front(); element != nil; {
		next := element.Next()
		entry := element.Value.(*volatileEngineEntry)
		if volatileEntryExpired(entry, now) {
			engine.removeEntryLocked(entry, false)
			engine.expired.Add(1)
			removed++
		}
		element = next
	}
	return removed
}

func (engine *VolatileEngine) removeEntryLocked(entry *volatileEngineEntry, eviction bool) {
	delete(engine.entries, entry.key)
	engine.order.Remove(entry.element)
	engine.bytes -= entry.bytes
	if eviction {
		engine.evictions.Add(1)
	}
}

func volatileEntryExpired(entry *volatileEngineEntry, now time.Time) bool {
	return !entry.expiresAt.IsZero() && !now.Before(entry.expiresAt)
}
