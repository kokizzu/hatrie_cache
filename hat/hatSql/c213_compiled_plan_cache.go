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
	// Coalesced counts callers that shared an in-flight exact-key compilation.
	Coalesced  uint64
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
	canonical  map[sqlCompiledQueryCanonicalCacheKey]sqlCompiledQueryCacheEntry
	flights    map[sqlCompiledQueryCacheKey]*sqlCompiledQueryCacheFlight
	compile    func(string) (*CompiledSQLQuery, error)
	order      *list.List
	hits       uint64
	misses     uint64
	coalesced  uint64
	evictions  uint64
	oversized  uint64
}

type sqlCompiledQueryCacheKey struct {
	source        string
	schemaVersion string
}

type sqlCompiledQueryCanonicalCacheKey struct {
	key           string
	schemaVersion string
}

type sqlCompiledQueryCacheEntry struct {
	query        *CompiledSQLQuery
	weight       int64
	order        *list.Element
	canonicalKey sqlCompiledQueryCanonicalCacheKey
}

type sqlCompiledQueryCacheFlight struct {
	done  chan struct{}
	query *CompiledSQLQuery
	err   error
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
		canonical:  make(map[sqlCompiledQueryCanonicalCacheKey]sqlCompiledQueryCacheEntry),
		flights:    make(map[sqlCompiledQueryCacheKey]*sqlCompiledQueryCacheFlight),
		compile:    CompileSQLQuery,
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
	if flight, ok := cache.flights[key]; ok {
		cache.coalesced++
		cache.mu.Unlock()
		<-flight.done
		return flight.query, flight.err
	}
	flight := &sqlCompiledQueryCacheFlight{done: make(chan struct{})}
	cache.flights[key] = flight
	cache.mu.Unlock()

	canonical, err := sqlPreparedQueryCacheKey(source, schemaVersion)
	if err != nil {
		return cache.finishCompiledQueryFlight(key, flight, nil, err)
	}
	canonicalKey := sqlCompiledQueryCanonicalCacheKey{key: canonical, schemaVersion: schemaVersion}
	cache.mu.Lock()
	if entry, ok := cache.canonical[canonicalKey]; ok {
		cache.hits++
		cache.order.MoveToBack(entry.order)
		query := entry.query
		result, resultErr := cache.finishCompiledQueryFlightLocked(key, flight, query, nil)
		cache.mu.Unlock()
		return result, resultErr
	}
	cache.mu.Unlock()

	compile := cache.compile
	if compile == nil {
		compile = CompileSQLQuery
	}
	query, err := compile(source)
	if err != nil {
		return cache.finishCompiledQueryFlight(key, flight, nil, err)
	}
	weight := sqlCompiledQueryCacheWeight(source)

	cache.mu.Lock()
	defer cache.mu.Unlock()
	if entry, ok := cache.entries[key]; ok {
		cache.hits++
		cache.order.MoveToBack(entry.order)
		return cache.finishCompiledQueryFlightLocked(key, flight, entry.query, nil)
	}
	if entry, ok := cache.canonical[canonicalKey]; ok {
		cache.hits++
		cache.order.MoveToBack(entry.order)
		return cache.finishCompiledQueryFlightLocked(key, flight, entry.query, nil)
	}
	cache.misses++
	if weight > cache.maxBytes {
		cache.oversized++
		return cache.finishCompiledQueryFlightLocked(key, flight, query, nil)
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
		delete(cache.canonical, oldEntry.canonicalKey)
		cache.bytes -= oldEntry.weight
		cache.evictions++
	}
	entry := sqlCompiledQueryCacheEntry{query: query, weight: weight, canonicalKey: canonicalKey}
	entry.order = cache.order.PushBack(key)
	cache.entries[key] = entry
	cache.canonical[canonicalKey] = entry
	cache.bytes += weight
	return cache.finishCompiledQueryFlightLocked(key, flight, query, nil)
}

func (cache *SQLCompiledQueryCache) finishCompiledQueryFlight(key sqlCompiledQueryCacheKey, flight *sqlCompiledQueryCacheFlight, query *CompiledSQLQuery, err error) (*CompiledSQLQuery, error) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	return cache.finishCompiledQueryFlightLocked(key, flight, query, err)
}

func (cache *SQLCompiledQueryCache) finishCompiledQueryFlightLocked(key sqlCompiledQueryCacheKey, flight *sqlCompiledQueryCacheFlight, query *CompiledSQLQuery, err error) (*CompiledSQLQuery, error) {
	delete(cache.flights, key)
	flight.query = query
	flight.err = err
	close(flight.done)
	return query, err
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
		Coalesced:  cache.coalesced,
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
		delete(cache.canonical, entry.canonicalKey)
		cache.bytes -= entry.weight
		removed++
	}
	return removed
}

func sqlCompiledQueryCacheWeight(source string) int64 {
	const baseWeight int64 = 4096
	return baseWeight + int64(len(source))*8
}
