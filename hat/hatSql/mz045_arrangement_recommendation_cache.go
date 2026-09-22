package hatSql

import (
	"container/list"
	"fmt"
	"strings"
	"sync"
)

// DefaultSQLArrangementRecommendationCacheMaxEntries bounds the recommended
// process-local arrangement recommendation cache.
const DefaultSQLArrangementRecommendationCacheMaxEntries = 64

// SQLArrangementRecommendationCacheOptions bounds cached literal-independent
// arrangement choices. A zero MaxEntries selects the recommended default.
type SQLArrangementRecommendationCacheOptions struct {
	MaxEntries int
}

// SQLArrangementRecommendationCacheStats reports cache use and bounded LRU
// occupancy.
type SQLArrangementRecommendationCacheStats struct {
	Entries    int
	MaxEntries int
	Hits       uint64
	Misses     uint64
	Evictions  uint64
}

// SQLArrangementRecommendationCache stores deterministic arrangement choices
// for a caller-owned metadata version and normalized SQL workload. It is
// deliberately opt-in: an empty metadata version bypasses the cache because
// reusing a choice against changed arrangement metadata would be unsafe.
type SQLArrangementRecommendationCache struct {
	mu         sync.Mutex
	maxEntries int
	entries    map[uint64][]*list.Element
	lru        *list.List
	hits       uint64
	misses     uint64
	evictions  uint64
}

type sqlArrangementRecommendationCacheEntry struct {
	hash            uint64
	sourceName      string
	sourceKey       string
	metadataVersion string
	workload        SQLArrangementWorkload
	recommendation  SQLArrangementRecommendation
}

// NewSQLArrangementRecommendationCache creates a bounded recommendation cache.
func NewSQLArrangementRecommendationCache(options SQLArrangementRecommendationCacheOptions) (*SQLArrangementRecommendationCache, error) {
	if options.MaxEntries < 0 {
		return nil, fmt.Errorf("hatSql: arrangement recommendation cache max entries cannot be negative")
	}
	if options.MaxEntries == 0 {
		options.MaxEntries = DefaultSQLArrangementRecommendationCacheMaxEntries
	}
	return &SQLArrangementRecommendationCache{
		maxEntries: options.MaxEntries,
		entries:    make(map[uint64][]*list.Element),
		lru:        list.New(),
	}, nil
}

// Recommend returns a cached choice when metadataVersion is non-empty and the
// source/workload match an entry. The version is caller-owned and must change
// whenever candidate metadata changes. Empty versions use the existing direct
// selector and do not retain or count an entry.
func (cache *SQLArrangementRecommendationCache) Recommend(
	sourceName, sourceKey, metadataVersion string,
	candidates []SQLArrangementMetadata,
	workload SQLArrangementWorkload,
) SQLArrangementRecommendation {
	if cache == nil || strings.TrimSpace(metadataVersion) == "" {
		return RecommendSQLArrangement(candidates, workload)
	}
	hash := sqlArrangementRecommendationCacheHash(sourceName, sourceKey, metadataVersion, workload)
	cache.mu.Lock()
	for _, element := range cache.entries[hash] {
		entry := element.Value.(*sqlArrangementRecommendationCacheEntry)
		if entry.hash == hash &&
			strings.EqualFold(entry.sourceName, sourceName) &&
			entry.sourceKey == sourceKey &&
			entry.metadataVersion == metadataVersion &&
			sqlArrangementWorkloadMatches(entry.workload, workload) {
			cache.hits++
			cache.lru.MoveToFront(element)
			recommendation := entry.recommendation
			cache.mu.Unlock()
			return recommendation
		}
	}
	cache.misses++
	cache.mu.Unlock()

	recommendation := RecommendSQLArrangement(candidates, workload)
	canonicalWorkload := sqlNormalizeArrangementWorkload(workload)
	canonicalWorkload.LocalityHints = append([]string(nil), canonicalWorkload.LocalityHints...)
	cache.mu.Lock()
	defer cache.mu.Unlock()
	for _, element := range cache.entries[hash] {
		entry := element.Value.(*sqlArrangementRecommendationCacheEntry)
		if entry.hash == hash &&
			strings.EqualFold(entry.sourceName, sourceName) &&
			entry.sourceKey == sourceKey &&
			entry.metadataVersion == metadataVersion &&
			sqlArrangementWorkloadMatches(entry.workload, workload) {
			cache.lru.MoveToFront(element)
			return entry.recommendation
		}
	}
	entry := &sqlArrangementRecommendationCacheEntry{
		hash:            hash,
		sourceName:      sourceName,
		sourceKey:       sourceKey,
		metadataVersion: metadataVersion,
		workload:        canonicalWorkload,
		recommendation:  recommendation,
	}
	element := cache.lru.PushFront(entry)
	cache.entries[hash] = append(cache.entries[hash], element)
	for cache.lru.Len() > cache.maxEntries {
		oldest := cache.lru.Back()
		if oldest == nil {
			break
		}
		cache.removeElementLocked(oldest)
		cache.evictions++
	}
	return recommendation
}

// Stats returns a stable bounded cache snapshot.
func (cache *SQLArrangementRecommendationCache) Stats() SQLArrangementRecommendationCacheStats {
	if cache == nil {
		return SQLArrangementRecommendationCacheStats{}
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	return SQLArrangementRecommendationCacheStats{
		Entries:    cache.lru.Len(),
		MaxEntries: cache.maxEntries,
		Hits:       cache.hits,
		Misses:     cache.misses,
		Evictions:  cache.evictions,
	}
}

// Invalidate removes all cached choices while retaining counters and limits.
func (cache *SQLArrangementRecommendationCache) Invalidate() int {
	if cache == nil {
		return 0
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	removed := cache.lru.Len()
	cache.entries = make(map[uint64][]*list.Element)
	cache.lru.Init()
	return removed
}

func (cache *SQLArrangementRecommendationCache) removeElementLocked(element *list.Element) {
	entry := element.Value.(*sqlArrangementRecommendationCacheEntry)
	bucket := cache.entries[entry.hash]
	for index, candidate := range bucket {
		if candidate != element {
			continue
		}
		bucket = append(bucket[:index], bucket[index+1:]...)
		break
	}
	if len(bucket) == 0 {
		delete(cache.entries, entry.hash)
	} else {
		cache.entries[entry.hash] = bucket
	}
	cache.lru.Remove(element)
}

func sqlArrangementRecommendationCacheHash(sourceName, sourceKey, metadataVersion string, workload SQLArrangementWorkload) uint64 {
	hash := uint64(14695981039346656037)
	hash = sqlArrangementCacheHashFoldedString(hash, sourceName)
	hash = sqlArrangementCacheHashByte(hash, 0)
	hash = sqlArrangementCacheHashString(hash, sourceKey)
	hash = sqlArrangementCacheHashByte(hash, 0)
	hash = sqlArrangementCacheHashString(hash, metadataVersion)
	hash = sqlArrangementCacheHashByte(hash, 0)
	hash = sqlArrangementCacheHashFields(hash, 1, workload.FilterFields)
	hash = sqlArrangementCacheHashFields(hash, 2, workload.GroupByFields)
	hash = sqlArrangementCacheHashFields(hash, 3, workload.OrderByFields)
	hash = sqlArrangementCacheHashFields(hash, 4, workload.JoinFields)
	hash = sqlArrangementCacheHashLocalityHints(hash, workload.LocalityHints)
	return hash
}

func sqlArrangementCacheHashLocalityHints(hash uint64, hints []string) uint64 {
	hash = sqlArrangementCacheHashByte(hash, 5)
	hash = sqlArrangementCacheHashByte(hash, byte(len(hints)))
	for _, hint := range hints {
		hash = sqlArrangementCacheHashString(hash, hint)
		hash = sqlArrangementCacheHashByte(hash, 0)
	}
	return hash
}

func sqlArrangementCacheHashFields(hash uint64, category byte, fields []string) uint64 {
	hash = sqlArrangementCacheHashByte(hash, category)
	var normalized [maxSQLArrangementSelectorFields]string
	count := 0
	for _, field := range fields {
		field = sqlArrangementNormalizeField(field)
		if field == "" || sqlArrangementContainsField(normalized[:count], field) {
			continue
		}
		if count == len(normalized) {
			break
		}
		normalized[count] = field
		count++
	}
	hash = sqlArrangementCacheHashByte(hash, byte(count))
	for index := 0; index < count; index++ {
		hash = sqlArrangementCacheHashString(hash, normalized[index])
		hash = sqlArrangementCacheHashByte(hash, 0)
	}
	return hash
}

func sqlArrangementCacheHashString(hash uint64, value string) uint64 {
	hash = sqlArrangementCacheHashByte(hash, byte(len(value)))
	hash = sqlArrangementCacheHashByte(hash, byte(len(value)>>8))
	for index := 0; index < len(value); index++ {
		hash = sqlArrangementCacheHashByte(hash, value[index])
	}
	return hash
}

func sqlArrangementCacheHashFoldedString(hash uint64, value string) uint64 {
	hash = sqlArrangementCacheHashByte(hash, byte(len(value)))
	hash = sqlArrangementCacheHashByte(hash, byte(len(value)>>8))
	for index := 0; index < len(value); index++ {
		character := value[index]
		if character >= 'a' && character <= 'z' {
			character -= 'a' - 'A'
		}
		hash = sqlArrangementCacheHashByte(hash, character)
	}
	return hash
}

func sqlArrangementCacheHashByte(hash uint64, value byte) uint64 {
	return (hash ^ uint64(value)) * 1099511628211
}

func sqlArrangementWorkloadMatches(canonical, raw SQLArrangementWorkload) bool {
	return sqlArrangementFieldsMatch(canonical.FilterFields, raw.FilterFields) &&
		sqlArrangementFieldsMatch(canonical.GroupByFields, raw.GroupByFields) &&
		sqlArrangementFieldsMatch(canonical.OrderByFields, raw.OrderByFields) &&
		sqlArrangementFieldsMatch(canonical.JoinFields, raw.JoinFields) &&
		sqlArrangementLocalityHintsMatch(canonical.LocalityHints, raw.LocalityHints)
}

func sqlArrangementLocalityHintsMatch(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func sqlArrangementFieldsMatch(canonical, raw []string) bool {
	var normalized [maxSQLArrangementSelectorFields]string
	count := 0
	for _, field := range raw {
		field = sqlArrangementNormalizeField(field)
		if field == "" || sqlArrangementContainsField(normalized[:count], field) {
			continue
		}
		if count == len(normalized) {
			break
		}
		normalized[count] = field
		count++
	}
	if count != len(canonical) {
		return false
	}
	for index := 0; index < count; index++ {
		if normalized[index] != canonical[index] {
			return false
		}
	}
	return true
}
