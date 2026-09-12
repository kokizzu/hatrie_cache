package hatSql

import (
	"container/list"
	"fmt"
	"sync"
)

const (
	// DefaultSQLCompiledQueryCacheMaxEntries bounds the number of reusable
	// compiled handles in the recommended cache configuration.
	DefaultSQLCompiledQueryCacheMaxEntries = 64
	// DefaultSQLCompiledQueryCacheMaxBytes bounds the cache's conservative
	// compiled-plan accounting weight.
	DefaultSQLCompiledQueryCacheMaxBytes int64 = 8 << 20
)

// SQLCompiledQueryCacheOptions bounds reusable compiled SQL handles by both
// entry count and an estimated plan weight. Both limits are enforced.
type SQLCompiledQueryCacheOptions struct {
	MaxEntries int
	MaxBytes   int64
}

// DefaultSQLCompiledQueryCacheOptions returns bounded, process-local defaults.
func DefaultSQLCompiledQueryCacheOptions() SQLCompiledQueryCacheOptions {
	return SQLCompiledQueryCacheOptions{
		MaxEntries: DefaultSQLCompiledQueryCacheMaxEntries,
		MaxBytes:   DefaultSQLCompiledQueryCacheMaxBytes,
	}
}

// SQLCompiledQueryCacheStats reports bounded compiled-plan cache use.
type SQLCompiledQueryCacheStats struct {
	Entries    int
	Bytes      int64
	MaxEntries int
	MaxBytes   int64
	Hits       uint64
	Misses     uint64
	Evictions  uint64
	Oversized  uint64
}

// CompiledQueryCacheStats is the package-native short name for
// SQLCompiledQueryCacheStats.
type CompiledQueryCacheStats = SQLCompiledQueryCacheStats

// SQLCompiledQueryCache stores immutable compiled SQL handles in a bounded
// least-recently-used cache. Handles are safe for concurrent execution and do
// not retain bound parameter values.
type SQLCompiledQueryCache struct {
	mu         sync.Mutex
	maxEntries int
	maxBytes   int64
	bytes      int64
	entries    map[sqlCompiledQueryCacheKey]sqlCompiledQueryCacheEntry
	order      *list.List
	hits       uint64
	misses     uint64
	evictions  uint64
	oversized  uint64
}

type sqlCompiledQueryCacheKey struct {
	source        string
	schemaVersion string
}

type sqlCompiledQueryCacheEntry struct {
	query  *CompiledSQLQuery
	weight int64
	order  *list.Element
}

// CompiledQueryCache is the package-native short name for
// SQLCompiledQueryCache.
type CompiledQueryCache = SQLCompiledQueryCache

// NewSQLCompiledQueryCache creates a bounded immutable compiled-plan cache.
func NewSQLCompiledQueryCache(options SQLCompiledQueryCacheOptions) (*SQLCompiledQueryCache, error) {
	if options.MaxEntries < 1 {
		return nil, fmt.Errorf("hatSql: compiled query cache max entries must be positive")
	}
	if options.MaxBytes < 1 {
		return nil, fmt.Errorf("hatSql: compiled query cache max bytes must be positive")
	}
	return &SQLCompiledQueryCache{
		maxEntries: options.MaxEntries,
		maxBytes:   options.MaxBytes,
		entries:    make(map[sqlCompiledQueryCacheKey]sqlCompiledQueryCacheEntry),
		order:      list.New(),
	}, nil
}

// NewCompiledQueryCache creates a bounded immutable compiled-plan cache.
func NewCompiledQueryCache(options SQLCompiledQueryCacheOptions) (*CompiledQueryCache, error) {
	return NewSQLCompiledQueryCache(options)
}

// Compile returns a cached immutable compiled handle for source.
func (cache *SQLCompiledQueryCache) Compile(source string) (*CompiledSQLQuery, error) {
	return cache.CompileWithSchemaVersion(source, "")
}

// CompileWithSchemaVersion keeps compiled plans in independent schema-version
// namespaces so callers can invalidate a changed source contract safely.
func (cache *SQLCompiledQueryCache) CompileWithSchemaVersion(source, schemaVersion string) (*CompiledSQLQuery, error) {
	if cache == nil {
		return CompileSQLQuery(source)
	}
	key := sqlCompiledQueryCacheKey{source: source, schemaVersion: schemaVersion}
	cache.mu.Lock()
	if entry, ok := cache.entries[key]; ok {
		cache.hits++
		cache.order.MoveToBack(entry.order)
		cache.mu.Unlock()
		return entry.query, nil
	}
	cache.mu.Unlock()

	query, err := CompileSQLQuery(source)
	if err != nil {
		return nil, err
	}
	weight := sqlCompiledQueryCacheWeight(source)

	cache.mu.Lock()
	defer cache.mu.Unlock()
	if entry, ok := cache.entries[key]; ok {
		cache.hits++
		cache.order.MoveToBack(entry.order)
		return entry.query, nil
	}
	cache.misses++
	if weight > cache.maxBytes {
		cache.oversized++
		return query, nil
	}
	for len(cache.entries) >= cache.maxEntries || cache.bytes+weight > cache.maxBytes {
		oldest := cache.order.Front()
		if oldest == nil {
			break
		}
		oldKey := oldest.Value.(sqlCompiledQueryCacheKey)
		oldEntry := cache.entries[oldKey]
		cache.order.Remove(oldest)
		delete(cache.entries, oldKey)
		cache.bytes -= oldEntry.weight
		cache.evictions++
	}
	entry := sqlCompiledQueryCacheEntry{query: query, weight: weight}
	entry.order = cache.order.PushBack(key)
	cache.entries[key] = entry
	cache.bytes += weight
	return query, nil
}

// CompileSQLQueryWithCache compiles source through cache when non-nil.
func CompileSQLQueryWithCache(source string, cache *SQLCompiledQueryCache) (*CompiledSQLQuery, error) {
	if cache == nil {
		return CompileSQLQuery(source)
	}
	return cache.Compile(source)
}

// Stats returns a stable bounded-cache snapshot.
func (cache *SQLCompiledQueryCache) Stats() SQLCompiledQueryCacheStats {
	if cache == nil {
		return SQLCompiledQueryCacheStats{}
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	return SQLCompiledQueryCacheStats{
		Entries:    len(cache.entries),
		Bytes:      cache.bytes,
		MaxEntries: cache.maxEntries,
		MaxBytes:   cache.maxBytes,
		Hits:       cache.hits,
		Misses:     cache.misses,
		Evictions:  cache.evictions,
		Oversized:  cache.oversized,
	}
}

// Invalidate removes every compiled plan while retaining counters and limits.
func (cache *SQLCompiledQueryCache) Invalidate() int {
	return cache.invalidateSchemaVersion(false, "")
}

// InvalidateSchemaVersion removes compiled plans in one schema namespace.
func (cache *SQLCompiledQueryCache) InvalidateSchemaVersion(schemaVersion string) int {
	return cache.invalidateSchemaVersion(true, schemaVersion)
}

func (cache *SQLCompiledQueryCache) invalidateSchemaVersion(scoped bool, schemaVersion string) int {
	if cache == nil {
		return 0
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	removed := 0
	for key, entry := range cache.entries {
		if scoped && key.schemaVersion != schemaVersion {
			continue
		}
		cache.order.Remove(entry.order)
		delete(cache.entries, key)
		cache.bytes -= entry.weight
		removed++
	}
	return removed
}

func sqlCompiledQueryCacheWeight(source string) int64 {
	const baseWeight int64 = 4096
	return baseWeight + int64(len(source))*8
}
