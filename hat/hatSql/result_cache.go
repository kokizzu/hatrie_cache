package hatSql

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	json "github.com/goccy/go-json"
)

// ResultCache caches query results while the caller-supplied epoch remains
// unchanged. It is portable because the owner supplies both execution and
// invalidation; cache-server adapters can use their mutation epoch directly.
// ResultCacheDependency identifies a mutable source used to produce a cached
// result. Dependency-aware caches can invalidate all results that reference a
// source without scanning unrelated entries.
type ResultCacheDependency struct {
	Kind string
	Key  string
}

type resultCacheDependencyKey struct {
	kind string
	key  string
}

type ResultCache struct {
	mu                   sync.Mutex
	capacity             int
	entries              map[string]resultCacheEntry
	order                []string
	dependencyTracking   bool
	dependencyIndex      map[resultCacheDependencyKey]map[string]struct{}
	dependencyGeneration uint64
	hits                 uint64
	misses               uint64
	bypasses             uint64
	evictions            uint64
}

// ResultCacheStats reports cache reuse and retention outcomes. Misses count
// enabled-cache lookups that did not return a matching entry; bypasses count
// requests that could not use or retain the cache.
type ResultCacheStats struct {
	Entries   int
	Hits      uint64
	Misses    uint64
	Bypasses  uint64
	Evictions uint64
}

// SQLResultCacheStats is the SQL-facing name for ResultCacheStats.
type SQLResultCacheStats = ResultCacheStats

// SQLResultCache is the typed result-cache view used by SQL execution. It
// shares the bounded LRU implementation with ResultCache while preserving SQL
// value types on cached hits.
type SQLResultCache = ResultCache

type resultCacheEntry struct {
	epoch        uint64
	version      string
	typed        bool
	result       QueryResult
	dependencies []resultCacheDependencyKey
}

// NewResultCache creates a bounded cache. A non-positive capacity disables
// retention while preserving Execute behavior.
func NewResultCache(capacity int) *ResultCache {
	return newResultCache(capacity, false)
}

// NewResultCacheWithDependencies creates a cache with an opt-in source
// dependency index. Use InvalidateDependency or InvalidateDependencies after
// a source mutation. The default constructor does not retain this index.
func NewResultCacheWithDependencies(capacity int) *ResultCache {
	return newResultCache(capacity, true)
}

func newResultCache(capacity int, dependencyTracking bool) *ResultCache {
	cache := &ResultCache{
		capacity:           capacity,
		entries:            make(map[string]resultCacheEntry),
		dependencyTracking: dependencyTracking,
	}
	if dependencyTracking {
		cache.dependencyIndex = make(map[resultCacheDependencyKey]map[string]struct{})
	}
	return cache
}

// NewSQLResultCache creates a bounded cache for typed SQL results. Unlike
// ResultCache.Execute, its versioned execution path keeps exact SQL value
// types instead of applying the portable JSON normalization contract.
func NewSQLResultCache(capacity int) *SQLResultCache {
	return NewResultCache(capacity)
}

// NewSQLResultCacheWithDependencies creates a typed SQL result cache with an
// opt-in source dependency index. It is safe to use with the normal versioned
// SQL path, or with ResultCacheExplicitInvalidation for lower lookup cost when
// every source mutation calls InvalidateDependency.
func NewSQLResultCacheWithDependencies(capacity int) *SQLResultCache {
	return NewResultCacheWithDependencies(capacity)
}

// Stats returns a stable snapshot of cache entries and cumulative counters.
func (cache *ResultCache) Stats() ResultCacheStats {
	if cache == nil {
		return ResultCacheStats{}
	}
	cache.mu.Lock()
	entries := len(cache.entries)
	cache.mu.Unlock()
	return ResultCacheStats{
		Entries:   entries,
		Hits:      atomic.LoadUint64(&cache.hits),
		Misses:    atomic.LoadUint64(&cache.misses),
		Bypasses:  atomic.LoadUint64(&cache.bypasses),
		Evictions: atomic.LoadUint64(&cache.evictions),
	}
}

// RecordBypass records an eligible request that intentionally bypassed result
// cache lookup or retention. It is useful to cache-server adapters that reject
// a query before calling Execute or ExecuteVersioned.
func (cache *ResultCache) RecordBypass() {
	if cache == nil {
		return
	}
	atomic.AddUint64(&cache.bypasses, 1)
}

// Execute reuses one result only when epoch reports the same value before and
// after the supplied query execution. Returned results never alias the cache.
func (cache *ResultCache) Execute(ctx context.Context, key string, epoch func() uint64, execute func(context.Context) (QueryResult, error)) (QueryResult, error) {
	if execute == nil {
		return QueryResult{}, errors.New("hatSql: result cache executor is nil")
	}
	if cache == nil || cache.capacity <= 0 {
		if cache != nil {
			cache.RecordBypass()
		}
		return execute(ctx)
	}
	if epoch == nil {
		return QueryResult{}, errors.New("hatSql: result cache epoch is nil")
	}
	before := epoch()
	cache.mu.Lock()
	entry, ok := cache.entries[key]
	cache.mu.Unlock()
	if ok && !entry.typed && entry.epoch == before {
		atomic.AddUint64(&cache.hits, 1)
		return cloneResultCacheResult(entry.result), nil
	}
	atomic.AddUint64(&cache.misses, 1)
	result, err := execute(ctx)
	if err != nil {
		return result, err
	}
	if epoch() != before {
		cache.RecordBypass()
		return result, nil
	}
	stored, err := snapshotResultCacheResult(result)
	if err != nil {
		cache.RecordBypass()
		return result, nil
	}
	cache.mu.Lock()
	cache.storeEntryLocked(key, resultCacheEntry{epoch: before, result: stored})
	cache.mu.Unlock()
	return result, nil
}

// ExecuteVersioned reuses one typed SQL result only while version reports the
// same non-empty source snapshot before and after execution. Returned results
// and retained entries never alias one another. A false version availability
// result bypasses retention so a resolver without a freshness guarantee keeps
// its ordinary behavior.
func (cache *ResultCache) ExecuteVersioned(ctx context.Context, key string, version func() (string, bool), execute func(context.Context) (QueryResult, error)) (QueryResult, error) {
	return cache.executeVersioned(ctx, key, version, nil, execute)
}

// ExecuteVersionedWithDependencies is ExecuteVersioned with an optional
// source dependency set retained for selective invalidation. Version checking
// remains enabled, so this method is safe for callers that have not yet wired
// mutation notifications.
func (cache *ResultCache) ExecuteVersionedWithDependencies(ctx context.Context, key string, version func() (string, bool), dependencies []ResultCacheDependency, execute func(context.Context) (QueryResult, error)) (QueryResult, error) {
	return cache.executeVersioned(ctx, key, version, dependencies, execute)
}

func (cache *ResultCache) executeVersioned(ctx context.Context, key string, version func() (string, bool), dependencies []ResultCacheDependency, execute func(context.Context) (QueryResult, error)) (QueryResult, error) {
	if execute == nil {
		return QueryResult{}, errors.New("hatSql: result cache executor is nil")
	}
	if cache == nil || cache.capacity <= 0 {
		if cache != nil {
			cache.RecordBypass()
		}
		return execute(ctx)
	}
	if version == nil {
		return QueryResult{}, errors.New("hatSql: result cache version is nil")
	}
	before, available := version()
	if !available || before == "" {
		cache.RecordBypass()
		return execute(ctx)
	}
	cache.mu.Lock()
	entry, ok := cache.entries[key]
	cache.mu.Unlock()
	dependenciesMatch := true
	if cache.dependencyTracking && resultCacheDependenciesValid(dependencies) {
		dependenciesMatch = resultCacheDependenciesEqualPublic(entry.dependencies, dependencies)
	}
	if ok && entry.typed && entry.version == before && dependenciesMatch {
		atomic.AddUint64(&cache.hits, 1)
		return cloneResultCacheResult(entry.result), nil
	}
	atomic.AddUint64(&cache.misses, 1)
	result, err := execute(ctx)
	if err != nil {
		return result, err
	}
	after, available := version()
	if !available || after != before {
		cache.RecordBypass()
		return result, nil
	}
	stored := cloneResultCacheResult(result)
	entry = resultCacheEntry{version: before, typed: true, result: stored}
	if cache.dependencyTracking && resultCacheDependenciesValid(dependencies) {
		entry.dependencies, _ = normalizeResultCacheDependencies(dependencies)
	}
	cache.mu.Lock()
	cache.storeEntryLocked(key, entry)
	cache.mu.Unlock()
	return result, nil
}

// ExecuteWithDependencies reuses a typed result until the caller invalidates
// one of its dependencies. It is available only on a cache created with
// NewResultCacheWithDependencies, and is intended for mutation-driven paths
// that can guarantee invalidation ordering. Invalid or empty dependencies are
// executed without retention.
func (cache *ResultCache) ExecuteWithDependencies(ctx context.Context, key string, dependencies []ResultCacheDependency, execute func(context.Context) (QueryResult, error)) (QueryResult, error) {
	if execute == nil {
		return QueryResult{}, errors.New("hatSql: result cache executor is nil")
	}
	if cache == nil || cache.capacity <= 0 || !cache.dependencyTracking || !resultCacheDependenciesValid(dependencies) {
		if cache != nil {
			cache.RecordBypass()
		}
		return execute(ctx)
	}
	cache.mu.Lock()
	generation := cache.dependencyGeneration
	entry, ok := cache.entries[key]
	cache.mu.Unlock()
	if ok && entry.typed && resultCacheDependenciesEqualPublic(entry.dependencies, dependencies) {
		atomic.AddUint64(&cache.hits, 1)
		return cloneResultCacheResult(entry.result), nil
	}
	atomic.AddUint64(&cache.misses, 1)
	result, err := execute(ctx)
	if err != nil {
		return result, err
	}
	dependencyKeys, _ := normalizeResultCacheDependencies(dependencies)
	stored := resultCacheEntry{
		typed:        true,
		result:       cloneResultCacheResult(result),
		dependencies: dependencyKeys,
	}
	cache.mu.Lock()
	if generation != cache.dependencyGeneration {
		cache.mu.Unlock()
		cache.RecordBypass()
		return result, nil
	}
	cache.storeEntryLocked(key, stored)
	cache.mu.Unlock()
	return result, nil
}

// InvalidateDependency removes every retained result that depends on the
// given source and returns the number of removed entries. It is a no-op for a
// cache created without dependency tracking.
func (cache *ResultCache) InvalidateDependency(kind string, key string) int {
	return cache.InvalidateDependencies([]ResultCacheDependency{{Kind: kind, Key: key}})
}

// InvalidateDependencies removes the union of entries that depend on any
// supplied source. Duplicate dependencies and entries are counted once.
func (cache *ResultCache) InvalidateDependencies(dependencies []ResultCacheDependency) int {
	if cache == nil || !cache.dependencyTracking {
		return 0
	}
	dependencyKeys, validDependencies := normalizeResultCacheDependencies(dependencies)
	if !validDependencies {
		return 0
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.dependencyGeneration++
	keys := make(map[string]struct{})
	for _, dependency := range dependencyKeys {
		for key := range cache.dependencyIndex[dependency] {
			keys[key] = struct{}{}
		}
	}
	removed := 0
	for key := range keys {
		entry, ok := cache.entries[key]
		if !ok {
			continue
		}
		cache.removeDependencyIndexLocked(key, entry)
		delete(cache.entries, key)
		removed++
	}
	if removed != 0 {
		order := cache.order[:0]
		for _, key := range cache.order {
			if _, ok := cache.entries[key]; ok {
				order = append(order, key)
			}
		}
		cache.order = order
	}
	return removed
}

func normalizeResultCacheDependencies(dependencies []ResultCacheDependency) ([]resultCacheDependencyKey, bool) {
	if len(dependencies) == 0 {
		return nil, false
	}
	keys := make([]resultCacheDependencyKey, 0, len(dependencies))
	seen := make(map[resultCacheDependencyKey]struct{}, len(dependencies))
	for _, dependency := range dependencies {
		if dependency.Kind == "" || dependency.Key == "" {
			return nil, false
		}
		key := resultCacheDependencyKey{kind: dependency.Kind, key: dependency.Key}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	return keys, len(keys) != 0
}

func resultCacheDependenciesValid(dependencies []ResultCacheDependency) bool {
	if len(dependencies) == 0 {
		return false
	}
	for _, dependency := range dependencies {
		if dependency.Kind == "" || dependency.Key == "" {
			return false
		}
	}
	return true
}

func resultCacheDependenciesEqualPublic(left []resultCacheDependencyKey, right []ResultCacheDependency) bool {
	if len(left) == 0 || !resultCacheDependenciesValid(right) {
		return false
	}
	uniqueCount := 0
	for index, candidate := range right {
		duplicate := false
		for _, previous := range right[:index] {
			if previous == candidate {
				duplicate = true
				break
			}
		}
		if !duplicate {
			uniqueCount++
		}
	}
	if len(left) != uniqueCount {
		return false
	}
	for _, candidate := range left {
		found := false
		for _, dependency := range right {
			if dependency.Kind == candidate.kind && dependency.Key == candidate.key {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (cache *ResultCache) storeEntryLocked(key string, entry resultCacheEntry) {
	if previous, exists := cache.entries[key]; exists {
		cache.removeDependencyIndexLocked(key, previous)
	} else {
		cache.order = append(cache.order, key)
	}
	cache.entries[key] = entry
	cache.addDependencyIndexLocked(key, entry)
	for len(cache.order) > cache.capacity {
		oldest := cache.order[0]
		cache.order = cache.order[1:]
		previous, exists := cache.entries[oldest]
		if !exists {
			continue
		}
		cache.removeDependencyIndexLocked(oldest, previous)
		delete(cache.entries, oldest)
		atomic.AddUint64(&cache.evictions, 1)
	}
}

func (cache *ResultCache) addDependencyIndexLocked(key string, entry resultCacheEntry) {
	if !cache.dependencyTracking {
		return
	}
	for _, dependency := range entry.dependencies {
		keys := cache.dependencyIndex[dependency]
		if keys == nil {
			keys = make(map[string]struct{})
			cache.dependencyIndex[dependency] = keys
		}
		keys[key] = struct{}{}
	}
}

func (cache *ResultCache) removeDependencyIndexLocked(key string, entry resultCacheEntry) {
	if !cache.dependencyTracking {
		return
	}
	for _, dependency := range entry.dependencies {
		keys := cache.dependencyIndex[dependency]
		delete(keys, key)
		if len(keys) == 0 {
			delete(cache.dependencyIndex, dependency)
		}
	}
}

func (cache *ResultCache) rebuildDependencyIndexLocked() {
	if !cache.dependencyTracking {
		return
	}
	cache.dependencyIndex = make(map[resultCacheDependencyKey]map[string]struct{})
	for key, entry := range cache.entries {
		cache.addDependencyIndexLocked(key, entry)
	}
}

// snapshotResultCacheResult retains the existing JSON-shaped cache contract at
// insertion time. Cached hits can then clone that normalized representation
// structurally without reserializing the whole result.
func snapshotResultCacheResult(result QueryResult) (QueryResult, error) {
	encoded, err := json.Marshal(result)
	if err != nil {
		return QueryResult{}, err
	}
	var out QueryResult
	if err := json.Unmarshal(encoded, &out); err != nil {
		return QueryResult{}, err
	}
	return out, nil
}

func cloneResultCacheResult(result QueryResult) QueryResult {
	clone := result
	clone.Columns = append([]string(nil), result.Columns...)
	clone.Rows = make([]Row, len(result.Rows))
	for rowIndex, row := range result.Rows {
		clone.Rows[rowIndex] = cloneResultCacheRow(row)
	}
	clone.Plan = make([]ExplainStep, len(result.Plan))
	for index, step := range result.Plan {
		clone.Plan[index] = cloneResultCachePlanStep(step)
	}
	clone.PlanSnapshot = cloneSQLPlanSnapshot(result.PlanSnapshot)
	if result.Stats != nil {
		stats := *result.Stats
		clone.Stats = &stats
	}
	return clone
}

func cloneResultCacheRow(row Row) Row {
	if row == nil {
		return nil
	}
	clone := make(Row, len(row))
	for key, value := range row {
		clone[key] = cloneResultCacheValue(value)
	}
	return clone
}

func cloneResultCacheValue(value interface{}) interface{} {
	switch value := value.(type) {
	case []byte:
		return append([]byte(nil), value...)
	case Row:
		return cloneResultCacheRow(value)
	case map[string]interface{}:
		clone := make(map[string]interface{}, len(value))
		for key, child := range value {
			clone[key] = cloneResultCacheValue(child)
		}
		return clone
	case []interface{}:
		clone := make([]interface{}, len(value))
		for index, child := range value {
			clone[index] = cloneResultCacheValue(child)
		}
		return clone
	}
	return value
}

func cloneResultCachePlanStep(step ExplainStep) ExplainStep {
	clone := step
	clone.Arrangements = cloneSQLArrangementMetadata(step.Arrangements)
	clone.Lineage = make([]ColumnLineage, len(step.Lineage))
	for index, lineage := range step.Lineage {
		clone.Lineage[index] = ColumnLineage{Output: lineage.Output, SourceFields: append([]string(nil), lineage.SourceFields...)}
	}
	clone.EstimatedRows = cloneResultCacheInt(step.EstimatedRows)
	clone.EstimatedCost = cloneResultCacheInt(step.EstimatedCost)
	clone.EstimatedMemoryBytes = cloneResultCacheInt(step.EstimatedMemoryBytes)
	clone.ActualInputRows = cloneResultCacheInt(step.ActualInputRows)
	clone.ActualOutputRows = cloneResultCacheInt(step.ActualOutputRows)
	clone.ActualInputBytes = cloneResultCacheInt(step.ActualInputBytes)
	clone.ActualOutputBytes = cloneResultCacheInt(step.ActualOutputBytes)
	clone.EstimateErrorRows = cloneResultCacheInt(step.EstimateErrorRows)
	clone.EstimateErrorPercent = cloneResultCacheFloat64(step.EstimateErrorPercent)
	clone.ElapsedNanos = cloneResultCacheInt64(step.ElapsedNanos)
	return clone
}

func cloneResultCacheInt(value *int) *int {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func cloneResultCacheFloat64(value *float64) *float64 {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func cloneResultCacheInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}
