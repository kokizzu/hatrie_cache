package hatSql

import (
	"container/list"
	"fmt"
	"sync"
)

const (
	// DefaultSQLArrangementPlanCacheMaxEntries bounds the number of cached
	// query/source arrangement plans when callers use zero-value limits.
	DefaultSQLArrangementPlanCacheMaxEntries = 64
	// DefaultSQLArrangementPlanCacheMaxBytes bounds retained metadata when
	// callers use zero-value limits.
	DefaultSQLArrangementPlanCacheMaxBytes int64 = 1 << 20
)

// SQLArrangementPlanCacheOptions bounds one version-scoped arrangement-plan
// cache. Zero limits use the package defaults; negative limits are invalid.
type SQLArrangementPlanCacheOptions struct {
	MaxEntries int
	MaxBytes   int64
}

// DefaultSQLArrangementPlanCacheOptions returns conservative cache limits.
func DefaultSQLArrangementPlanCacheOptions() SQLArrangementPlanCacheOptions {
	return SQLArrangementPlanCacheOptions{
		MaxEntries: DefaultSQLArrangementPlanCacheMaxEntries,
		MaxBytes:   DefaultSQLArrangementPlanCacheMaxBytes,
	}
}

// SQLArrangementPlanCacheStats reports bounded cache state and cumulative
// lookups. Counters are reset only when the cache is discarded.
type SQLArrangementPlanCacheStats struct {
	Entries    int
	MaxEntries int
	Bytes      int64
	MaxBytes   int64
	Hits       uint64
	Misses     uint64
	Evictions  uint64
}

type sqlArrangementPlanCacheKey struct {
	queryKey   string
	sourceKind string
	sourceKey  string
	version    string
}

type sqlArrangementPlanCacheEntry struct {
	key          sqlArrangementPlanCacheKey
	arrangements []SQLArrangementMetadata
	weight       int64
}

// SQLArrangementPlanCache reuses bounded EXPLAIN arrangement metadata for an
// exact normalized query/source/version identity. It is safe for concurrent
// readers and is opt-in through SQLQueryOptions.
type SQLArrangementPlanCache struct {
	mu         sync.Mutex
	maxEntries int
	maxBytes   int64
	bytes      int64
	entries    map[sqlArrangementPlanCacheKey]*list.Element
	order      *list.List
	hits       uint64
	misses     uint64
	evictions  uint64
}

// NewSQLArrangementPlanCache constructs a bounded arrangement-plan cache.
func NewSQLArrangementPlanCache(options SQLArrangementPlanCacheOptions) (*SQLArrangementPlanCache, error) {
	defaults := DefaultSQLArrangementPlanCacheOptions()
	if options.MaxEntries < 0 {
		return nil, fmt.Errorf("SQL arrangement plan cache max entries must not be negative: %d", options.MaxEntries)
	}
	if options.MaxBytes < 0 {
		return nil, fmt.Errorf("SQL arrangement plan cache max bytes must not be negative: %d", options.MaxBytes)
	}
	if options.MaxEntries == 0 {
		options.MaxEntries = defaults.MaxEntries
	}
	if options.MaxBytes == 0 {
		options.MaxBytes = defaults.MaxBytes
	}
	return &SQLArrangementPlanCache{
		maxEntries: options.MaxEntries,
		maxBytes:   options.MaxBytes,
		entries:    make(map[sqlArrangementPlanCacheKey]*list.Element),
		order:      list.New(),
	}, nil
}

// Stats returns a consistent snapshot of cache occupancy and counters.
func (cache *SQLArrangementPlanCache) Stats() SQLArrangementPlanCacheStats {
	if cache == nil {
		return SQLArrangementPlanCacheStats{}
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.initLocked()
	return SQLArrangementPlanCacheStats{
		Entries:    len(cache.entries),
		MaxEntries: cache.maxEntries,
		Bytes:      cache.bytes,
		MaxBytes:   cache.maxBytes,
		Hits:       cache.hits,
		Misses:     cache.misses,
		Evictions:  cache.evictions,
	}
}

// Invalidate removes every cached arrangement plan while retaining counters.
func (cache *SQLArrangementPlanCache) Invalidate() {
	if cache == nil {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.initLocked()
	cache.entries = make(map[sqlArrangementPlanCacheKey]*list.Element)
	cache.order.Init()
	cache.bytes = 0
}

// InvalidateVersion removes plans associated with one metadata version and
// returns the number removed. Version changes can also simply be handled by
// using the new version in SQLQueryOptions; this method is useful for memory
// reclamation before the bounded LRU would evict old versions.
func (cache *SQLArrangementPlanCache) InvalidateVersion(version string) int {
	if cache == nil || version == "" {
		return 0
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.initLocked()
	removed := 0
	for key, element := range cache.entries {
		if key.version != version {
			continue
		}
		cache.removeElementLocked(element)
		removed++
	}
	return removed
}

func (cache *SQLArrangementPlanCache) initLocked() {
	if cache.maxEntries <= 0 {
		cache.maxEntries = DefaultSQLArrangementPlanCacheMaxEntries
	}
	if cache.maxBytes <= 0 {
		cache.maxBytes = DefaultSQLArrangementPlanCacheMaxBytes
	}
	if cache.entries == nil {
		cache.entries = make(map[sqlArrangementPlanCacheKey]*list.Element)
	}
	if cache.order == nil {
		cache.order = list.New()
	}
}

func (cache *SQLArrangementPlanCache) resolve(query *sqlQuery, resolver SQLSourceResolver, source sqlSource, workload SQLArrangementWorkload, version string) []SQLArrangementMetadata {
	key := sqlArrangementPlanCacheKey{
		queryKey:   query.cacheKey,
		sourceKind: source.kind,
		sourceKey:  source.key,
		version:    version,
	}
	cache.mu.Lock()
	cache.initLocked()
	if element, ok := cache.entries[key]; ok {
		cache.order.MoveToFront(element)
		cache.hits++
		arrangements := cloneSQLArrangementMetadata(element.Value.(*sqlArrangementPlanCacheEntry).arrangements)
		cache.mu.Unlock()
		return arrangements
	}
	cache.misses++
	cache.mu.Unlock()

	arrangements := resolveSQLArrangementMetadata(resolver, source)
	sqlMarkArrangementRecommendation(arrangements, workload)
	if len(arrangements) == 0 {
		return arrangements
	}
	entry := &sqlArrangementPlanCacheEntry{
		key:          key,
		arrangements: cloneSQLArrangementMetadata(arrangements),
	}
	entry.weight = sqlArrangementPlanCacheEntryWeight(entry.key, entry.arrangements)
	cache.mu.Lock()
	cache.initLocked()
	if element, ok := cache.entries[key]; ok {
		cache.order.MoveToFront(element)
		arrangements = cloneSQLArrangementMetadata(element.Value.(*sqlArrangementPlanCacheEntry).arrangements)
		cache.mu.Unlock()
		return arrangements
	}
	if entry.weight <= cache.maxBytes {
		for len(cache.entries) >= cache.maxEntries || cache.bytes+entry.weight > cache.maxBytes {
			oldest := cache.order.Back()
			if oldest == nil {
				break
			}
			cache.removeElementLocked(oldest)
			cache.evictions++
		}
		element := cache.order.PushFront(entry)
		cache.entries[key] = element
		cache.bytes += entry.weight
	}
	cache.mu.Unlock()
	return arrangements
}

func (cache *SQLArrangementPlanCache) removeElementLocked(element *list.Element) {
	entry := element.Value.(*sqlArrangementPlanCacheEntry)
	delete(cache.entries, entry.key)
	cache.order.Remove(element)
	cache.bytes -= entry.weight
	if cache.bytes < 0 {
		cache.bytes = 0
	}
}

func sqlResolveArrangementPlan(query *sqlQuery, resolver SQLSourceResolver, source sqlSource, workload SQLArrangementWorkload, cache *SQLArrangementPlanCache, version string) []SQLArrangementMetadata {
	if cache != nil && query != nil && query.cacheKey != "" && version != "" {
		return cache.resolve(query, resolver, source, workload, version)
	}
	arrangements := resolveSQLArrangementMetadata(resolver, source)
	sqlMarkArrangementRecommendation(arrangements, workload)
	return arrangements
}

func sqlArrangementPlanCacheEntryWeight(key sqlArrangementPlanCacheKey, arrangements []SQLArrangementMetadata) int64 {
	weight := int64(64 + len(key.queryKey) + len(key.sourceKind) + len(key.sourceKey) + len(key.version))
	for _, arrangement := range arrangements {
		weight += int64(64 + len(arrangement.Key) + len(arrangement.Kind) + len(arrangement.Locality))
		for _, field := range arrangement.Fields {
			weight += int64(16 + len(field))
		}
	}
	return weight
}
