// Package hatDictionary provides a bounded cache for externally loaded
// dimension values. Values are strings so callers can keep them immutable and
// avoid a copy on every cache hit.
package hatDictionary

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrDictionaryNil  = errors.New("hatDictionary: dictionary is nil")
	ErrSourceNil      = errors.New("hatDictionary: source is nil")
	ErrKeyInvalid     = errors.New("hatDictionary: key is invalid")
	ErrOptionsInvalid = errors.New("hatDictionary: options are invalid")
	ErrRefreshLimit   = errors.New("hatDictionary: refresh key limit exceeded")
	ErrValueTooLarge  = errors.New("hatDictionary: value exceeds cache byte limit")
	ErrStale          = errors.New("hatDictionary: value is stale")
)

const (
	DefaultMaxEntries     = 4_096
	DefaultMaxBytes       = 16 << 20
	DefaultTTL            = 5 * time.Minute
	DefaultMaxRefreshKeys = 256
)

// Source loads a bounded batch of dictionary keys. Keys not present in the
// returned map are treated as source misses. Implementations should return
// independent immutable strings and should honor ctx cancellation.
type Source interface {
	Load(ctx context.Context, keys []string) (map[string]string, error)
}

// SourceFunc adapts a function into a Source.
type SourceFunc func(context.Context, []string) (map[string]string, error)

func (function SourceFunc) Load(ctx context.Context, keys []string) (map[string]string, error) {
	return function(ctx, keys)
}

// Options bounds retained dictionary state and source refresh work.
// Zero-valued limits use the documented defaults. StaleIfError is disabled by
// default because serving old dimension data must be an explicit availability
// tradeoff.
type Options struct {
	MaxEntries     int
	MaxBytes       int64
	TTL            time.Duration
	MaxRefreshKeys int
	StaleIfError   bool
	Now            func() time.Time
}

// LookupResult describes a dictionary lookup. Value is immutable from the
// dictionary's perspective because it is represented as a string.
type LookupResult struct {
	Value string
	Found bool
	Stale bool
}

// RefreshResult reports one successful batch refresh.
type RefreshResult struct {
	Requested int
	Loaded    int
	Missing   int
	Evicted   int
	Bytes     int64
}

// Stats reports current bounded state and cumulative activity counters.
type Stats struct {
	Entries        int
	Bytes          int64
	Hits           uint64
	Misses         uint64
	Refreshes      uint64
	RefreshErrors  uint64
	StaleFallbacks uint64
}

type dictionaryCounters struct {
	hits           uint64
	misses         uint64
	refreshes      uint64
	refreshErrors  uint64
	staleFallbacks uint64
}

// StaleError reports that a stale value was returned because the source
// refresh failed. errors.Is matches both ErrStale and the source error.
type StaleError struct {
	Cause error
}

func (err *StaleError) Error() string {
	if err == nil || err.Cause == nil {
		return ErrStale.Error()
	}
	return fmt.Sprintf("%s: %v", ErrStale, err.Cause)
}

func (err *StaleError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Cause
}

func (err *StaleError) Is(target error) bool {
	return target == ErrStale || (err != nil && errors.Is(err.Cause, target))
}

type dictionaryEntry struct {
	value     string
	fetchedAt time.Time
	touched   uint64
}

// Dictionary is a concurrency-safe, bounded external dictionary cache.
// Refreshes are serialized per dictionary to prevent a miss storm from
// issuing overlapping source loads.
type Dictionary struct {
	source  Source
	options Options
	now     func() time.Time

	mu      sync.RWMutex
	entries map[string]*dictionaryEntry
	bytes   int64
	tick    uint64
	stats   dictionaryCounters

	refreshMu sync.Mutex
}

// New creates a bounded dictionary cache.
func New(source Source, options Options) (*Dictionary, error) {
	if source == nil {
		return nil, ErrSourceNil
	}
	if options.MaxEntries < 0 || options.MaxBytes < 0 || options.MaxRefreshKeys < 0 || options.TTL < 0 {
		return nil, ErrOptionsInvalid
	}
	if options.MaxEntries == 0 {
		options.MaxEntries = DefaultMaxEntries
	}
	if options.MaxBytes == 0 {
		options.MaxBytes = DefaultMaxBytes
	}
	if options.MaxRefreshKeys == 0 {
		options.MaxRefreshKeys = DefaultMaxRefreshKeys
	}
	if options.TTL == 0 {
		options.TTL = DefaultTTL
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	return &Dictionary{
		source:  source,
		options: options,
		now:     options.Now,
		entries: make(map[string]*dictionaryEntry),
	}, nil
}

// Lookup returns a fresh cached value or refreshes the requested key. A
// missing source key returns Found=false. With StaleIfError enabled, an
// expired value is returned with Stale=true and a StaleError when refresh
// fails.
func (dictionary *Dictionary) Lookup(ctx context.Context, rawKey string) (LookupResult, error) {
	if dictionary == nil {
		return LookupResult{}, ErrDictionaryNil
	}
	key, err := normalizeKey(rawKey)
	if err != nil {
		return LookupResult{}, err
	}
	if err := contextError(ctx); err != nil {
		return LookupResult{}, err
	}
	entry, found, fresh := dictionary.readEntry(key)
	if fresh {
		dictionary.recordHit()
		return LookupResult{Value: entry.value, Found: true}, nil
	}
	dictionary.recordMiss()

	dictionary.refreshMu.Lock()
	defer dictionary.refreshMu.Unlock()
	entry, found, fresh = dictionary.readEntry(key)
	if fresh {
		dictionary.recordHit()
		return LookupResult{Value: entry.value, Found: true}, nil
	}
	staleValue := entry.value
	staleFound := found
	if _, err := dictionary.refreshBatch(ctx, []string{key}); err != nil {
		if staleFound && dictionary.options.StaleIfError {
			dictionary.recordStaleFallback()
			return LookupResult{Value: staleValue, Found: true, Stale: true}, &StaleError{Cause: err}
		}
		return LookupResult{}, err
	}
	entry, found, _ = dictionary.readEntry(key)
	if !found {
		return LookupResult{}, nil
	}
	return LookupResult{Value: entry.value, Found: true}, nil
}

// Refresh loads and stores a deduplicated batch. It rejects batches larger
// than MaxRefreshKeys before calling the source, so refresh work is bounded.
func (dictionary *Dictionary) Refresh(ctx context.Context, rawKeys []string) (RefreshResult, error) {
	if dictionary == nil {
		return RefreshResult{}, ErrDictionaryNil
	}
	keys, err := normalizeKeys(rawKeys, dictionary.options.MaxRefreshKeys)
	if err != nil {
		return RefreshResult{}, err
	}
	if err := contextError(ctx); err != nil {
		return RefreshResult{}, err
	}
	dictionary.refreshMu.Lock()
	defer dictionary.refreshMu.Unlock()
	return dictionary.refreshBatch(ctx, keys)
}

// Len returns the number of retained entries.
func (dictionary *Dictionary) Len() int {
	if dictionary == nil {
		return 0
	}
	dictionary.mu.RLock()
	defer dictionary.mu.RUnlock()
	return len(dictionary.entries)
}

// Bytes returns the logical key-plus-value bytes retained by the cache.
func (dictionary *Dictionary) Bytes() int64 {
	if dictionary == nil {
		return 0
	}
	dictionary.mu.RLock()
	defer dictionary.mu.RUnlock()
	return dictionary.bytes
}

// Stats returns bounded state and cumulative counters.
func (dictionary *Dictionary) Stats() Stats {
	if dictionary == nil {
		return Stats{}
	}
	dictionary.mu.RLock()
	defer dictionary.mu.RUnlock()
	stats := Stats{
		Hits:           atomic.LoadUint64(&dictionary.stats.hits),
		Misses:         atomic.LoadUint64(&dictionary.stats.misses),
		Refreshes:      atomic.LoadUint64(&dictionary.stats.refreshes),
		RefreshErrors:  atomic.LoadUint64(&dictionary.stats.refreshErrors),
		StaleFallbacks: atomic.LoadUint64(&dictionary.stats.staleFallbacks),
	}
	stats.Entries = len(dictionary.entries)
	stats.Bytes = dictionary.bytes
	return stats
}

func (dictionary *Dictionary) refreshBatch(ctx context.Context, keys []string) (RefreshResult, error) {
	result := RefreshResult{Requested: len(keys)}
	if len(keys) == 0 {
		return result, nil
	}
	if err := contextError(ctx); err != nil {
		return result, err
	}
	atomic.AddUint64(&dictionary.stats.refreshes, 1)
	values, err := dictionary.source.Load(ctx, keys)
	if err != nil {
		atomic.AddUint64(&dictionary.stats.refreshErrors, 1)
		return result, err
	}
	if values == nil {
		values = map[string]string{}
	}
	for _, key := range keys {
		value, found := values[key]
		if found && int64(len(key)+len(value)) > dictionary.options.MaxBytes {
			return result, fmt.Errorf("%w: key %q", ErrValueTooLarge, key)
		}
	}

	dictionary.mu.Lock()
	for _, key := range keys {
		value, found := values[key]
		old, hadOld := dictionary.entries[key]
		if !found {
			if hadOld {
				dictionary.bytes -= int64(len(key) + len(old.value))
				delete(dictionary.entries, key)
			}
			result.Missing++
			continue
		}
		if hadOld {
			dictionary.bytes -= int64(len(key) + len(old.value))
		}
		entry := &dictionaryEntry{value: value, fetchedAt: dictionary.now(), touched: atomic.AddUint64(&dictionary.tick, 1)}
		dictionary.entries[key] = entry
		dictionary.bytes += int64(len(key) + len(value))
		result.Loaded++
		result.Bytes += int64(len(value))
	}
	for len(dictionary.entries) > dictionary.options.MaxEntries || dictionary.bytes > dictionary.options.MaxBytes {
		key, ok := dictionary.oldestKeyLocked()
		if !ok {
			break
		}
		entry := dictionary.entries[key]
		dictionary.bytes -= int64(len(key) + len(entry.value))
		delete(dictionary.entries, key)
		result.Evicted++
	}
	dictionary.mu.Unlock()
	return result, nil
}

func (dictionary *Dictionary) readEntry(key string) (dictionaryEntry, bool, bool) {
	dictionary.mu.RLock()
	defer dictionary.mu.RUnlock()
	entry, found := dictionary.entries[key]
	if !found {
		return dictionaryEntry{}, false, false
	}
	touched := atomic.AddUint64(&dictionary.tick, 1)
	atomic.StoreUint64(&entry.touched, touched)
	return dictionaryEntry{value: entry.value, fetchedAt: entry.fetchedAt, touched: touched}, true, dictionary.now().Before(entry.fetchedAt.Add(dictionary.options.TTL))
}

func (dictionary *Dictionary) oldestKeyLocked() (string, bool) {
	oldestKey := ""
	var oldestTick uint64
	for key, entry := range dictionary.entries {
		touched := atomic.LoadUint64(&entry.touched)
		if oldestKey == "" || touched < oldestTick || (touched == oldestTick && key < oldestKey) {
			oldestKey = key
			oldestTick = touched
		}
	}
	return oldestKey, oldestKey != ""
}

func (dictionary *Dictionary) recordHit() {
	atomic.AddUint64(&dictionary.stats.hits, 1)
}

func (dictionary *Dictionary) recordMiss() {
	atomic.AddUint64(&dictionary.stats.misses, 1)
}

func (dictionary *Dictionary) recordStaleFallback() {
	atomic.AddUint64(&dictionary.stats.staleFallbacks, 1)
}

func normalizeKeys(rawKeys []string, limit int) ([]string, error) {
	if len(rawKeys) == 0 {
		return nil, nil
	}
	capacity := len(rawKeys)
	if capacity > limit {
		capacity = limit
	}
	keys := make([]string, 0, capacity)
	seen := make(map[string]struct{}, capacity)
	for _, rawKey := range rawKeys {
		key, err := normalizeKey(rawKey)
		if err != nil {
			return nil, err
		}
		if _, exists := seen[key]; exists {
			continue
		}
		if len(keys) >= limit {
			return nil, fmt.Errorf("%w: got more than %d unique keys", ErrRefreshLimit, limit)
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	return keys, nil
}

func normalizeKey(rawKey string) (string, error) {
	key := strings.TrimSpace(rawKey)
	if key == "" || strings.IndexByte(key, 0) >= 0 {
		return "", ErrKeyInvalid
	}
	return key, nil
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return errors.New("hatDictionary: nil context")
	}
	return ctx.Err()
}
