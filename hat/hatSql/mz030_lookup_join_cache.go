package hatSql

import (
	"errors"
	"sync"
	"sync/atomic"
)

const (
	// DefaultSQLLookupJoinCacheCapacity is a documented sizing suggestion for
	// callers that want a bounded lookup cache.
	DefaultSQLLookupJoinCacheCapacity = 256
	// DefaultSQLLookupJoinCacheMaxRowsPerEntry prevents one hot lookup key from
	// retaining an unbounded candidate list.
	DefaultSQLLookupJoinCacheMaxRowsPerEntry = 128
	// DefaultSQLLookupJoinCacheMaxBytesPerEntry bounds retained candidate data.
	DefaultSQLLookupJoinCacheMaxBytesPerEntry = 64 << 10
)

var (
	ErrSQLLookupJoinCacheCapacityInvalid = errors.New("hatriecache: SQL lookup join cache capacity must be non-negative")
	ErrSQLLookupJoinCacheRowsInvalid     = errors.New("hatriecache: SQL lookup join cache maximum rows must be non-negative")
	ErrSQLLookupJoinCacheBytesInvalid    = errors.New("hatriecache: SQL lookup join cache maximum bytes must be non-negative")
)

// SQLLookupJoinCacheOptions bounds a frontier-aware lookup cache. A zero row
// or byte bound selects the corresponding conservative default.
type SQLLookupJoinCacheOptions struct {
	Capacity         int
	MaxRowsPerEntry  int
	MaxBytesPerEntry int
}

// SQLLookupJoinCacheStats reports cache reuse and bounded-retention outcomes.
// Bypasses include unsupported values, unavailable frontiers, disabled caches,
// and entries that exceed the configured per-entry bound.
type SQLLookupJoinCacheStats struct {
	Entries       int
	Hits          uint64
	Misses        uint64
	Bypasses      uint64
	Evictions     uint64
	Invalidations uint64
}

type sqlLookupJoinCacheKey struct {
	sourceName string
	sourceKey  string
	field      string
	value      string
}

type sqlLookupJoinCacheEntry struct {
	frontier uint64
	rows     []Row
}

// SQLLookupJoinCache reuses immutable point-lookup candidates while an
// external source remains at the same frontier. It is process-local and
// intentionally does not persist across restarts; callers should create a new
// cache or call Clear after replacing a source implementation.
type SQLLookupJoinCache struct {
	mu               sync.Mutex
	capacity         int
	maxRowsPerEntry  int
	maxBytesPerEntry int
	entries          map[sqlLookupJoinCacheKey]sqlLookupJoinCacheEntry
	order            []sqlLookupJoinCacheKey
	hits             uint64
	misses           uint64
	bypasses         uint64
	evictions        uint64
	invalidations    uint64
}

// NewSQLLookupJoinCache creates a bounded cache with conservative per-entry
// limits. A non-positive capacity disables retention while leaving the query
// path unchanged.
func NewSQLLookupJoinCache(capacity int) *SQLLookupJoinCache {
	if capacity <= 0 {
		return &SQLLookupJoinCache{capacity: capacity}
	}
	cache, err := NewSQLLookupJoinCacheWithOptions(SQLLookupJoinCacheOptions{Capacity: capacity})
	if err != nil {
		return &SQLLookupJoinCache{capacity: capacity}
	}
	return cache
}

// NewSQLLookupJoinCacheWithOptions creates a cache with explicit memory
// controls. Capacity zero is a valid disabled configuration.
func NewSQLLookupJoinCacheWithOptions(options SQLLookupJoinCacheOptions) (*SQLLookupJoinCache, error) {
	if options.Capacity < 0 {
		return nil, ErrSQLLookupJoinCacheCapacityInvalid
	}
	if options.MaxRowsPerEntry < 0 {
		return nil, ErrSQLLookupJoinCacheRowsInvalid
	}
	if options.MaxBytesPerEntry < 0 {
		return nil, ErrSQLLookupJoinCacheBytesInvalid
	}
	if options.MaxRowsPerEntry == 0 {
		options.MaxRowsPerEntry = DefaultSQLLookupJoinCacheMaxRowsPerEntry
	}
	if options.MaxBytesPerEntry == 0 {
		options.MaxBytesPerEntry = DefaultSQLLookupJoinCacheMaxBytesPerEntry
	}
	return &SQLLookupJoinCache{
		capacity:         options.Capacity,
		maxRowsPerEntry:  options.MaxRowsPerEntry,
		maxBytesPerEntry: options.MaxBytesPerEntry,
		entries:          make(map[sqlLookupJoinCacheKey]sqlLookupJoinCacheEntry),
	}, nil
}

// Stats returns a stable snapshot of the bounded cache.
func (cache *SQLLookupJoinCache) Stats() SQLLookupJoinCacheStats {
	if cache == nil {
		return SQLLookupJoinCacheStats{}
	}
	cache.mu.Lock()
	entries := len(cache.entries)
	cache.mu.Unlock()
	return SQLLookupJoinCacheStats{
		Entries:       entries,
		Hits:          atomic.LoadUint64(&cache.hits),
		Misses:        atomic.LoadUint64(&cache.misses),
		Bypasses:      atomic.LoadUint64(&cache.bypasses),
		Evictions:     atomic.LoadUint64(&cache.evictions),
		Invalidations: atomic.LoadUint64(&cache.invalidations),
	}
}

// Clear removes all retained lookup candidates without changing counters.
func (cache *SQLLookupJoinCache) Clear() {
	if cache == nil {
		return
	}
	cache.mu.Lock()
	cache.entries = make(map[sqlLookupJoinCacheKey]sqlLookupJoinCacheEntry)
	cache.order = nil
	cache.mu.Unlock()
}

func (cache *SQLLookupJoinCache) recordBypass() {
	if cache != nil {
		atomic.AddUint64(&cache.bypasses, 1)
	}
}

func (cache *SQLLookupJoinCache) resolveAtFrontier(
	name, key, field string,
	value interface{},
	frontier uint64,
	frontierResolver SQLSourceFrontierResolver,
	resolve func(string, string, string, interface{}) ([]Row, bool, error),
) ([]Row, bool, error) {
	if resolve == nil {
		return nil, false, errors.New("hatriecache: SQL lookup join resolver is nil")
	}
	if cache == nil || cache.capacity <= 0 || value == nil {
		if cache != nil {
			cache.recordBypass()
		}
		return resolve(name, key, field, value)
	}
	encoded, ok := sqlHashJoinKey(value)
	if !ok {
		cache.recordBypass()
		return resolve(name, key, field, value)
	}
	cacheKey := sqlLookupJoinCacheKey{sourceName: name, sourceKey: key, field: field, value: encoded}
	cache.mu.Lock()
	entry, found := cache.entries[cacheKey]
	cache.mu.Unlock()
	if found && entry.frontier == frontier {
		atomic.AddUint64(&cache.hits, 1)
		return entry.rows, true, nil
	}
	if found {
		atomic.AddUint64(&cache.invalidations, 1)
	}
	atomic.AddUint64(&cache.misses, 1)
	rows, available, err := resolve(name, key, field, value)
	if err != nil || !available {
		return rows, available, err
	}
	if frontierResolver != nil {
		after, afterReady, afterAvailable, afterErr := frontierResolver.SQLSourceFrontier(name, key)
		if afterErr != nil || !afterReady || !afterAvailable || after != frontier {
			cache.recordBypass()
			return rows, available, nil
		}
	}
	if len(rows) > cache.maxRowsPerEntry || sqlRowsBytes(rows) > cache.maxBytesPerEntry {
		cache.recordBypass()
		return rows, available, nil
	}
	stored := cloneSQLLookupRows(rows)
	cache.mu.Lock()
	if _, exists := cache.entries[cacheKey]; !exists {
		cache.order = append(cache.order, cacheKey)
	}
	cache.entries[cacheKey] = sqlLookupJoinCacheEntry{frontier: frontier, rows: stored}
	for len(cache.order) > cache.capacity {
		oldest := cache.order[0]
		cache.order = cache.order[1:]
		delete(cache.entries, oldest)
		atomic.AddUint64(&cache.evictions, 1)
	}
	cache.mu.Unlock()
	return rows, available, nil
}

func cloneSQLLookupRows(rows []Row) []Row {
	if rows == nil {
		return nil
	}
	clone := make([]Row, len(rows))
	for index, row := range rows {
		clone[index] = cloneResultCacheRow(row)
	}
	return clone
}
