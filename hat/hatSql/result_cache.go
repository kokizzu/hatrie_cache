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
type ResultCache struct {
	mu        sync.Mutex
	capacity  int
	entries   map[string]resultCacheEntry
	order     []string
	hits      uint64
	misses    uint64
	bypasses  uint64
	evictions uint64
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
	epoch   uint64
	version string
	typed   bool
	result  QueryResult
}

// NewResultCache creates a bounded cache. A non-positive capacity disables
// retention while preserving Execute behavior.
func NewResultCache(capacity int) *ResultCache {
	return &ResultCache{capacity: capacity, entries: make(map[string]resultCacheEntry)}
}

// NewSQLResultCache creates a bounded cache for typed SQL results. Unlike
// ResultCache.Execute, its versioned execution path keeps exact SQL value
// types instead of applying the portable JSON normalization contract.
func NewSQLResultCache(capacity int) *SQLResultCache {
	return NewResultCache(capacity)
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
	defer cache.mu.Unlock()
	if _, exists := cache.entries[key]; !exists {
		cache.order = append(cache.order, key)
	}
	cache.entries[key] = resultCacheEntry{epoch: before, result: stored}
	for len(cache.order) > cache.capacity {
		oldest := cache.order[0]
		cache.order = cache.order[1:]
		delete(cache.entries, oldest)
		atomic.AddUint64(&cache.evictions, 1)
	}
	return result, nil
}

// ExecuteVersioned reuses one typed SQL result only while version reports the
// same non-empty source snapshot before and after execution. Returned results
// and retained entries never alias one another. A false version availability
// result bypasses retention so a resolver without a freshness guarantee keeps
// its ordinary behavior.
func (cache *ResultCache) ExecuteVersioned(ctx context.Context, key string, version func() (string, bool), execute func(context.Context) (QueryResult, error)) (QueryResult, error) {
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
	if ok && entry.typed && entry.version == before {
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
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if _, exists := cache.entries[key]; !exists {
		cache.order = append(cache.order, key)
	}
	cache.entries[key] = resultCacheEntry{version: before, typed: true, result: stored}
	for len(cache.order) > cache.capacity {
		oldest := cache.order[0]
		cache.order = cache.order[1:]
		delete(cache.entries, oldest)
		atomic.AddUint64(&cache.evictions, 1)
	}
	return result, nil
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
	clone.Lineage = make([]ColumnLineage, len(step.Lineage))
	for index, lineage := range step.Lineage {
		clone.Lineage[index] = ColumnLineage{Output: lineage.Output, SourceFields: append([]string(nil), lineage.SourceFields...)}
	}
	clone.EstimatedRows = cloneResultCacheInt(step.EstimatedRows)
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
