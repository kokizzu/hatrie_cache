package hatCache

import (
	"container/heap"
	"errors"
	"fmt"
	"sync"
	"time"
)

// VolatileEngineBackend identifies the explicit memory-only cache engine.
const VolatileEngineBackend = "volatile"

const volatileEntryOverheadBytes uint64 = 64

var (
	// ErrVolatileEngineInvalidOptions reports a missing or invalid memory bound.
	ErrVolatileEngineInvalidOptions = errors.New("hatriecache: volatile engine requires a positive memory or entry bound")
	// ErrVolatileEngineClosed reports an operation on a closed engine.
	ErrVolatileEngineClosed = errors.New("hatriecache: volatile engine is closed")
	// ErrVolatileEntryTooLarge reports an item that cannot fit in the byte budget.
	ErrVolatileEntryTooLarge = errors.New("hatriecache: volatile entry exceeds the byte budget")
	// ErrVolatileCapacityExceeded reports a capacity that cannot admit an item.
	ErrVolatileCapacityExceeded = errors.New("hatriecache: volatile engine capacity cannot admit the entry")
	// ErrVolatileTTLInvalid reports a negative TTL.
	ErrVolatileTTLInvalid = errors.New("hatriecache: volatile TTL must be non-negative")
)

// VolatileEngineOptions bounds an explicit memory-only engine. A zero member
// disables that bound; at least one bound must be positive. Now is injectable
// for deterministic expiry tests and should be omitted in production.
type VolatileEngineOptions struct {
	MaxBytes   uint64
	MaxEntries int
	Now        func() time.Time
}

// VolatileEngineStats is an operator-facing snapshot. ResidentBytes is a
// logical estimate that includes key/value bytes and a fixed per-entry
// metadata allowance; it is not a process RSS measurement.
type VolatileEngineStats struct {
	Backend       string `json:"backend"`
	Durable       bool   `json:"durable"`
	Closed        bool   `json:"closed"`
	MaxBytes      uint64 `json:"max_bytes"`
	MaxEntries    int    `json:"max_entries"`
	Entries       int    `json:"entries"`
	ResidentBytes uint64 `json:"resident_bytes"`
	Evictions     uint64 `json:"evictions"`
	Expirations   uint64 `json:"expirations"`
	Hits          uint64 `json:"hits"`
	Misses        uint64 `json:"misses"`
}

// VolatileEngine stores byte values in the in-memory HAT-trie without a path,
// journal, checkpoint, or backup surface. It is deliberately separate from
// PersistentStore so callers cannot accidentally treat it as durable.
type VolatileEngine struct {
	mu          sync.Mutex
	trie        *HatTrie
	maxBytes    uint64
	maxEntries  int
	now         func() time.Time
	entries     map[string]volatileEngineEntry
	queue       []volatileEngineQueueItem
	queueHead   int
	expirations volatileEngineExpirationHeap
	nextVersion uint64
	resident    uint64
	evictions   uint64
	expired     uint64
	hits        uint64
	misses      uint64
	closed      bool
}

type volatileEngineEntry struct {
	bytes      uint64
	version    uint64
	queueIndex int
	expiresAt  time.Time
}

type volatileEngineQueueItem struct {
	key     string
	version uint64
}

type volatileEngineExpiration struct {
	key       string
	version   uint64
	expiresAt time.Time
}

type volatileEngineExpirationHeap []volatileEngineExpiration

func (h volatileEngineExpirationHeap) Len() int { return len(h) }

func (h volatileEngineExpirationHeap) Less(i, j int) bool {
	return h[i].expiresAt.Before(h[j].expiresAt)
}

func (h volatileEngineExpirationHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *volatileEngineExpirationHeap) Push(value interface{}) {
	*h = append(*h, value.(volatileEngineExpiration))
}

func (h *volatileEngineExpirationHeap) Pop() interface{} {
	old := *h
	last := len(old) - 1
	value := old[last]
	*h = old[:last]
	return value
}

// NewVolatileEngine constructs an explicitly bounded, non-durable engine.
func NewVolatileEngine(options VolatileEngineOptions) (*VolatileEngine, error) {
	if options.MaxBytes == 0 && options.MaxEntries <= 0 {
		return nil, ErrVolatileEngineInvalidOptions
	}
	if options.MaxEntries < 0 {
		return nil, fmt.Errorf("%w: max entries must be non-negative", ErrVolatileEngineInvalidOptions)
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	engine := &VolatileEngine{
		trie:       CreateHatTrie(),
		maxBytes:   options.MaxBytes,
		maxEntries: options.MaxEntries,
		now:        now,
		entries:    make(map[string]volatileEngineEntry),
	}
	heap.Init(&engine.expirations)
	return engine, nil
}

// Backend reports the explicit memory-only backend name.
func (engine *VolatileEngine) Backend() string {
	return VolatileEngineBackend
}

// Durable is always false for a VolatileEngine.
func (engine *VolatileEngine) Durable() bool { return false }

// Path is always empty because the engine has no filesystem state.
func (engine *VolatileEngine) Path() string { return "" }

// SetBytes stores a copied byte value and optionally expires it after ttl.
// A zero TTL means no expiration. Values that do not fit the configured byte
// budget are rejected before any eviction or mutation occurs.
func (engine *VolatileEngine) SetBytes(key string, value []byte, ttl time.Duration) error {
	if engine == nil {
		return ErrVolatileEngineClosed
	}
	if ttl < 0 {
		return ErrVolatileTTLInvalid
	}
	if err := validateKey(key); err != nil {
		return err
	}
	bytes := volatileEntryBytes(key, len(value))
	if engine.maxBytes > 0 && bytes > engine.maxBytes {
		return fmt.Errorf("%w: key=%q estimated=%d max=%d", ErrVolatileEntryTooLarge, key, bytes, engine.maxBytes)
	}

	engine.mu.Lock()
	defer engine.mu.Unlock()
	if engine.closed {
		return ErrVolatileEngineClosed
	}
	var now time.Time
	if ttl > 0 || len(engine.expirations) > 0 {
		now = engine.now()
		engine.expireLocked(now)
	}
	old, replacing := engine.entries[key]
	if err := engine.makeRoomLocked(key, bytes, old.bytes, replacing); err != nil {
		return err
	}
	if err := engine.trie.UpsertBytesChecked(key, value); err != nil {
		return err
	}
	engine.nextVersion++
	entry := volatileEngineEntry{bytes: bytes, version: engine.nextVersion, queueIndex: -1}
	if ttl > 0 {
		entry.expiresAt = now.Add(ttl)
		heap.Push(&engine.expirations, volatileEngineExpiration{key: key, version: entry.version, expiresAt: entry.expiresAt})
	}
	if replacing {
		engine.resident -= old.bytes
	}
	engine.entries[key] = entry
	engine.resident += bytes
	if replacing && old.queueIndex >= engine.queueHead && old.queueIndex < len(engine.queue) && engine.queue[old.queueIndex].key == key && engine.queue[old.queueIndex].version == old.version {
		engine.queue[old.queueIndex].version = entry.version
		entry.queueIndex = old.queueIndex
		engine.entries[key] = entry
	} else {
		entry.queueIndex = len(engine.queue)
		engine.entries[key] = entry
		engine.queue = append(engine.queue, volatileEngineQueueItem{key: key, version: entry.version})
	}
	engine.compactQueueLocked()
	engine.compactExpirationsLocked()
	return nil
}

// GetBytes returns a copy of a live value and whether it was present.
func (engine *VolatileEngine) GetBytes(key string) ([]byte, bool, error) {
	if engine == nil {
		return nil, false, ErrVolatileEngineClosed
	}
	if err := validateKey(key); err != nil {
		return nil, false, err
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if engine.closed {
		return nil, false, ErrVolatileEngineClosed
	}
	if len(engine.expirations) > 0 {
		engine.expireLocked(engine.now())
	}
	if _, ok := engine.entries[key]; !ok {
		engine.misses++
		return nil, false, nil
	}
	value, err := engine.trie.GetBytesChecked(key)
	if err != nil {
		return nil, false, err
	}
	engine.hits++
	return value, true, nil
}

// Delete removes a value and returns whether it was present.
func (engine *VolatileEngine) Delete(key string) (bool, error) {
	if engine == nil {
		return false, ErrVolatileEngineClosed
	}
	if err := validateKey(key); err != nil {
		return false, err
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if engine.closed {
		return false, ErrVolatileEngineClosed
	}
	if len(engine.expirations) > 0 {
		engine.expireLocked(engine.now())
	}
	if _, ok := engine.entries[key]; !ok {
		return false, nil
	}
	deleted, err := engine.trie.DeleteChecked(key)
	if err != nil {
		return false, err
	}
	if deleted {
		engine.removeEntryLocked(key)
	}
	return deleted, nil
}

// Vacuum removes all values whose TTL has elapsed and returns the count.
func (engine *VolatileEngine) Vacuum() (int, error) {
	if engine == nil {
		return 0, ErrVolatileEngineClosed
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if engine.closed {
		return 0, ErrVolatileEngineClosed
	}
	before := len(engine.entries)
	if len(engine.expirations) > 0 {
		engine.expireLocked(engine.now())
	}
	return before - len(engine.entries), nil
}

// Stats returns bounded resident-memory and hit/miss accounting.
func (engine *VolatileEngine) Stats() VolatileEngineStats {
	if engine == nil {
		return VolatileEngineStats{Backend: VolatileEngineBackend, Durable: false, Closed: true}
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if !engine.closed && len(engine.expirations) > 0 {
		engine.expireLocked(engine.now())
	}
	return VolatileEngineStats{
		Backend:       VolatileEngineBackend,
		Durable:       false,
		Closed:        engine.closed,
		MaxBytes:      engine.maxBytes,
		MaxEntries:    engine.maxEntries,
		Entries:       len(engine.entries),
		ResidentBytes: engine.resident,
		Evictions:     engine.evictions,
		Expirations:   engine.expired,
		Hits:          engine.hits,
		Misses:        engine.misses,
	}
}

// Close destroys all resident values. It is idempotent and never writes them.
func (engine *VolatileEngine) Close() error {
	if engine == nil {
		return nil
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if engine.closed {
		return nil
	}
	engine.closed = true
	engine.trie.Destroy()
	engine.trie = nil
	engine.entries = nil
	engine.queue = nil
	engine.expirations = nil
	engine.resident = 0
	return nil
}

func (engine *VolatileEngine) makeRoomLocked(key string, bytes, oldBytes uint64, replacing bool) error {
	for {
		resident := engine.resident
		entries := len(engine.entries)
		if replacing {
			resident -= oldBytes
			entries--
		}
		withinBytes := engine.maxBytes == 0 || (resident <= engine.maxBytes && bytes <= engine.maxBytes-resident)
		withinEntries := engine.maxEntries == 0 || entries < engine.maxEntries
		if withinBytes && withinEntries {
			return nil
		}
		if !engine.evictOldestLocked(key) {
			return ErrVolatileCapacityExceeded
		}
	}
}

func (engine *VolatileEngine) evictOldestLocked(skip string) bool {
	available := len(engine.queue) - engine.queueHead
	for scanned := 0; scanned < available && engine.queueHead < len(engine.queue); scanned++ {
		item := engine.queue[engine.queueHead]
		engine.queueHead++
		entry, ok := engine.entries[item.key]
		if !ok || entry.version != item.version {
			continue
		}
		if item.key == skip {
			entry.queueIndex = len(engine.queue)
			engine.entries[item.key] = entry
			engine.queue = append(engine.queue, item)
			continue
		}
		if deleted, err := engine.trie.DeleteChecked(item.key); err == nil && deleted {
			engine.removeEntryLocked(item.key)
			engine.evictions++
			engine.compactQueueLocked()
			return true
		}
	}
	return false
}

func (engine *VolatileEngine) expireLocked(now time.Time) {
	for len(engine.expirations) > 0 && !engine.expirations[0].expiresAt.After(now) {
		item := heap.Pop(&engine.expirations).(volatileEngineExpiration)
		entry, ok := engine.entries[item.key]
		if !ok || entry.version != item.version || !entry.expiresAt.Equal(item.expiresAt) {
			continue
		}
		if deleted, err := engine.trie.DeleteChecked(item.key); err == nil && deleted {
			engine.removeEntryLocked(item.key)
			engine.expired++
		}
	}
}

func (engine *VolatileEngine) removeEntryLocked(key string) {
	entry, ok := engine.entries[key]
	if !ok {
		return
	}
	delete(engine.entries, key)
	if engine.resident >= entry.bytes {
		engine.resident -= entry.bytes
	} else {
		engine.resident = 0
	}
}

func (engine *VolatileEngine) compactQueueLocked() {
	if engine.queueHead < 1024 || engine.queueHead*2 < len(engine.queue) {
		return
	}
	copy(engine.queue, engine.queue[engine.queueHead:])
	engine.queue = engine.queue[:len(engine.queue)-engine.queueHead]
	engine.queueHead = 0
	for index, item := range engine.queue {
		entry, ok := engine.entries[item.key]
		if ok && entry.version == item.version {
			entry.queueIndex = index
			engine.entries[item.key] = entry
		}
	}
}

func (engine *VolatileEngine) compactExpirationsLocked() {
	if len(engine.expirations) <= 64 || len(engine.expirations) <= len(engine.entries)*2 {
		return
	}
	retained := make(volatileEngineExpirationHeap, 0, len(engine.entries))
	for key, entry := range engine.entries {
		if !entry.expiresAt.IsZero() {
			retained = append(retained, volatileEngineExpiration{key: key, version: entry.version, expiresAt: entry.expiresAt})
		}
	}
	engine.expirations = retained
	heap.Init(&engine.expirations)
}

func volatileEntryBytes(key string, valueBytes int) uint64 {
	return volatileEntryOverheadBytes + uint64(len(key)) + uint64(valueBytes)
}
