package hatStorage

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrRemotePartCacheDisabled        = errors.New("hatriecache: remote-part cache requires a positive byte budget")
	ErrRemotePartCacheInvalidConfig   = errors.New("hatriecache: remote-part cache configuration is invalid")
	ErrRemotePartCacheContextRequired = errors.New("hatriecache: remote-part cache context is required")
	ErrRemotePartCacheLoaderRequired  = errors.New("hatriecache: remote-part cache loader is required")
	ErrRemotePartCacheSizeMismatch    = errors.New("hatriecache: remote-part cache loader size does not match metadata")
	ErrRemotePartCacheNil             = errors.New("hatriecache: remote-part cache is nil")
)

// DefaultRemotePartCacheMaxEntries bounds the number of cached parts when the
// caller supplies only a byte budget.
const DefaultRemotePartCacheMaxEntries = 1024

const maxRemotePartCacheAdmissionEntries = 1 << 20

// DefaultRemotePartPrefetchConcurrency bounds concurrent remote reads when a
// caller leaves MaxConcurrent at zero.
const DefaultRemotePartPrefetchConcurrency = 2

// RemotePartCacheOptions controls the bounded immutable remote-part cache.
// MaxBytes is required; MaxEntries uses DefaultRemotePartCacheMaxEntries when
// zero. The cache allocates no part storage until the first miss.
type RemotePartCacheOptions struct {
	MaxBytes   uint64
	MaxEntries int
	// MinAccesses enables frequency admission when positive. Zero preserves
	// the legacy eager-admission behavior.
	MinAccesses uint64
}

// RemotePartPrefetchOptions controls one explicit bounded read-ahead pass.
// Priority is applied to every prefetched part. MaxConcurrent uses
// DefaultRemotePartPrefetchConcurrency when zero.
type RemotePartPrefetchOptions struct {
	MaxConcurrent int
	Priority      int
}

// RemotePartCacheLoader fetches one immutable remote part. The returned slice
// is copied once on a successful miss and is never mutated by the cache.
type RemotePartCacheLoader func(context.Context, RemotePartReference) ([]byte, error)

// RemotePartCacheStats is a point-in-time cache accounting snapshot.
type RemotePartCacheStats struct {
	Entries    int
	Bytes      uint64
	Hits       uint64
	Misses     uint64
	Loads      uint64
	Admissions uint64
	Evictions  uint64
	Uncached   uint64
}

type remotePartCacheKey struct {
	objectURI string
	checksum  string
	sizeBytes uint64
}

type remotePartCacheEntry struct {
	key      remotePartCacheKey
	data     []byte
	priority int
	lastUse  uint64
	pins     int
}

type remotePartCacheLoad struct {
	done  chan struct{}
	data  []byte
	entry *remotePartCacheEntry
	err   error
	admit bool
}

type remotePartCacheCandidate struct {
	accesses uint64
	lastUse  uint64
}

// RemotePartCache stores immutable remote parts with a bounded byte budget.
// Higher-priority entries survive eviction ahead of lower-priority entries;
// pinned entries are never evicted until all their handles are released.
type RemotePartCache struct {
	mu             sync.Mutex
	maxBytes       uint64
	maxEntries     int
	minAccesses    uint64
	candidateLimit int
	bytes          uint64
	clock          uint64
	entries        map[remotePartCacheKey]*remotePartCacheEntry
	loading        map[remotePartCacheKey]*remotePartCacheLoad
	candidates     map[remotePartCacheKey]remotePartCacheCandidate
	stats          RemotePartCacheStats
}

// RemotePartHandle pins one cached part until Release. The handle's bytes are
// immutable by contract and remain valid for the handle lifetime.
type RemotePartHandle struct {
	cache       *RemotePartCache
	entry       *remotePartCacheEntry
	data        []byte
	releaseOnce sync.Once
}

// NewRemotePartCache creates a bounded remote-part cache. A zero byte budget
// intentionally disables construction so enabling this feature is explicit.
func NewRemotePartCache(options RemotePartCacheOptions) (*RemotePartCache, error) {
	if options.MaxBytes == 0 {
		return nil, ErrRemotePartCacheDisabled
	}
	if options.MaxEntries < 0 {
		return nil, ErrRemotePartCacheInvalidConfig
	}
	if options.MaxEntries == 0 {
		options.MaxEntries = DefaultRemotePartCacheMaxEntries
	}
	candidateLimit := options.MaxEntries
	if candidateLimit > maxRemotePartCacheAdmissionEntries {
		candidateLimit = maxRemotePartCacheAdmissionEntries
	}
	if candidateLimit > int(^uint(0)>>1)/2 {
		candidateLimit = int(^uint(0)>>1) / 2
	}
	candidateLimit *= 2
	var candidates map[remotePartCacheKey]remotePartCacheCandidate
	if options.MinAccesses > 0 {
		candidates = make(map[remotePartCacheKey]remotePartCacheCandidate)
	}
	return &RemotePartCache{
		maxBytes:       options.MaxBytes,
		maxEntries:     options.MaxEntries,
		minAccesses:    options.MinAccesses,
		candidateLimit: candidateLimit,
		entries:        make(map[remotePartCacheKey]*remotePartCacheEntry),
		loading:        make(map[remotePartCacheKey]*remotePartCacheLoad),
		candidates:     candidates,
	}, nil
}

// Get returns an immutable cached part without pinning it. A returned slice
// keeps its backing storage alive even if a later eviction removes the cache
// entry; use Acquire when eviction should also be prevented during a read.
func (cache *RemotePartCache) Get(ctx context.Context, reference RemotePartReference, priority int, loader RemotePartCacheLoader) ([]byte, error) {
	data, _, err := cache.load(ctx, reference, priority, loader, false)
	return data, err
}

// Acquire returns an immutable part handle and pins a cached entry until
// Release. If the cache cannot admit the part because its budget is full of
// pinned entries, the handle serves the loaded bytes without retaining them.
func (cache *RemotePartCache) Acquire(ctx context.Context, reference RemotePartReference, priority int, loader RemotePartCacheLoader) (*RemotePartHandle, error) {
	data, entry, err := cache.load(ctx, reference, priority, loader, true)
	if err != nil {
		return nil, err
	}
	return &RemotePartHandle{cache: cache, entry: entry, data: data}, nil
}

// Prefetch loads an explicit set of immutable remote parts into the bounded
// cache. It returns only after all admitted work completes. Duplicate
// references are loaded once, MaxConcurrent bounds in-flight loaders, and a
// failed load cancels the remaining prefetch work without affecting the
// caller's context or existing cache entries.
func (cache *RemotePartCache) Prefetch(ctx context.Context, references []RemotePartReference, options RemotePartPrefetchOptions, loader RemotePartCacheLoader) error {
	if cache == nil {
		return ErrRemotePartCacheNil
	}
	if ctx == nil {
		return ErrRemotePartCacheContextRequired
	}
	if loader == nil {
		return ErrRemotePartCacheLoaderRequired
	}
	if options.MaxConcurrent < 0 {
		return ErrRemotePartCacheInvalidConfig
	}
	unique := make([]RemotePartReference, 0, len(references))
	seen := make(map[remotePartCacheKey]struct{}, len(references))
	for _, reference := range references {
		key, err := validateRemotePartCacheReference(reference)
		if err != nil {
			return err
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, reference)
	}
	if len(unique) == 0 {
		return nil
	}
	concurrency := options.MaxConcurrent
	if concurrency == 0 {
		concurrency = DefaultRemotePartPrefetchConcurrency
	}
	if concurrency > len(unique) {
		concurrency = len(unique)
	}
	if concurrency == 1 {
		for _, reference := range unique {
			if _, err := cache.Get(ctx, reference, options.Priority, loader); err != nil {
				return err
			}
		}
		return ctx.Err()
	}
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan RemotePartReference)
	var workers sync.WaitGroup
	var errorsMu sync.Mutex
	var firstErr error
	recordError := func(err error) {
		if err == nil {
			return
		}
		errorsMu.Lock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
		errorsMu.Unlock()
	}
	worker := func() {
		defer workers.Done()
		for {
			select {
			case <-workCtx.Done():
				return
			case reference, ok := <-jobs:
				if !ok {
					return
				}
				if _, err := cache.Get(workCtx, reference, options.Priority, loader); err != nil {
					recordError(err)
					return
				}
			}
		}
	}
	workers.Add(concurrency)
	for range concurrency {
		go worker()
	}

	for _, reference := range unique {
		select {
		case <-workCtx.Done():
			break
		case jobs <- reference:
		}
		if workCtx.Err() != nil {
			break
		}
	}
	close(jobs)
	workers.Wait()
	errorsMu.Lock()
	err := firstErr
	errorsMu.Unlock()
	if err != nil {
		return err
	}
	return ctx.Err()
}

// Bytes returns the immutable part bytes. It returns nil for a nil handle.
func (handle *RemotePartHandle) Bytes() []byte {
	if handle == nil {
		return nil
	}
	return handle.data
}

// Release unpins the handle. It is safe to call concurrently and repeatedly.
func (handle *RemotePartHandle) Release() {
	if handle == nil {
		return
	}
	handle.releaseOnce.Do(func() {
		if handle.cache == nil || handle.entry == nil {
			return
		}
		handle.cache.mu.Lock()
		if handle.entry.pins > 0 {
			handle.entry.pins--
		}
		handle.cache.mu.Unlock()
	})
}

// Stats returns a consistent accounting snapshot.
func (cache *RemotePartCache) Stats() RemotePartCacheStats {
	if cache == nil {
		return RemotePartCacheStats{}
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	stats := cache.stats
	stats.Entries = len(cache.entries)
	stats.Bytes = cache.bytes
	return stats
}

// Invalidate removes a cached part. Existing handles remain valid, but a new
// request for the reference performs a fresh load.
func (cache *RemotePartCache) Invalidate(reference RemotePartReference) bool {
	if cache == nil {
		return false
	}
	key := remotePartCacheKeyFrom(reference)
	cache.mu.Lock()
	defer cache.mu.Unlock()
	entry, ok := cache.entries[key]
	if !ok {
		return false
	}
	delete(cache.entries, key)
	delete(cache.candidates, key)
	cache.bytes -= uint64(len(entry.data))
	return true
}

func (cache *RemotePartCache) load(ctx context.Context, reference RemotePartReference, priority int, loader RemotePartCacheLoader, pin bool) ([]byte, *remotePartCacheEntry, error) {
	if cache == nil {
		return nil, nil, ErrRemotePartCacheNil
	}
	if ctx == nil {
		return nil, nil, ErrRemotePartCacheContextRequired
	}
	if loader == nil {
		return nil, nil, ErrRemotePartCacheLoaderRequired
	}
	key, err := validateRemotePartCacheReference(reference)
	if err != nil {
		return nil, nil, err
	}
	for {
		cache.mu.Lock()
		if entry, ok := cache.entries[key]; ok {
			cache.stats.Hits++
			cache.touchLocked(entry, priority)
			if pin {
				entry.pins++
			}
			data := entry.data
			cache.mu.Unlock()
			return data, entry, nil
		}
		admit := cache.noteAdmissionLocked(key)
		if current, ok := cache.loading[key]; ok {
			if admit {
				current.admit = true
			}
			done := current.done
			cache.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-done:
			}
			if current.err != nil {
				return nil, nil, current.err
			}
			if current.entry == nil {
				return current.data, nil, nil
			}
			continue
		}
		current := &remotePartCacheLoad{done: make(chan struct{}), admit: admit}
		cache.loading[key] = current
		cache.stats.Misses++
		cache.stats.Loads++
		cache.mu.Unlock()

		data, loadErr := loader(ctx, reference)
		if loadErr == nil {
			if err := ctx.Err(); err != nil {
				loadErr = err
			} else if reference.SizeBytes() != 0 && uint64(len(data)) != reference.SizeBytes() {
				loadErr = fmt.Errorf("%w: declared %d, got %d", ErrRemotePartCacheSizeMismatch, reference.SizeBytes(), len(data))
			}
		}
		var entry *remotePartCacheEntry
		var owned []byte
		if loadErr == nil {
			owned = append([]byte(nil), data...)
			cache.mu.Lock()
			if current.admit && cache.makeRoomLocked(uint64(len(owned))) {
				cache.clock++
				entry = &remotePartCacheEntry{
					key:      key,
					data:     owned,
					priority: priority,
					lastUse:  cache.clock,
				}
				if pin {
					entry.pins = 1
				}
				cache.entries[key] = entry
				cache.bytes += uint64(len(owned))
				cache.stats.Admissions++
			} else {
				cache.stats.Uncached++
			}
			cache.mu.Unlock()
		}

		cache.mu.Lock()
		delete(cache.loading, key)
		if loadErr != nil {
			delete(cache.candidates, key)
		}
		current.data = owned
		current.entry = entry
		current.err = loadErr
		close(current.done)
		cache.mu.Unlock()
		if loadErr != nil {
			return nil, nil, loadErr
		}
		return owned, entry, nil
	}
}

func (cache *RemotePartCache) noteAdmissionLocked(key remotePartCacheKey) bool {
	if cache.minAccesses == 0 {
		return true
	}
	cache.clock++
	candidate := cache.candidates[key]
	if candidate.accesses < ^uint64(0) {
		candidate.accesses++
	}
	candidate.lastUse = cache.clock
	if candidate.accesses >= cache.minAccesses {
		delete(cache.candidates, key)
		return true
	}
	cache.candidates[key] = candidate
	cache.pruneAdmissionCandidatesLocked()
	return false
}

func (cache *RemotePartCache) pruneAdmissionCandidatesLocked() {
	for len(cache.candidates) > cache.candidateLimit {
		var oldestKey remotePartCacheKey
		var oldestUse uint64
		first := true
		for key, candidate := range cache.candidates {
			if first || candidate.lastUse < oldestUse {
				oldestKey = key
				oldestUse = candidate.lastUse
				first = false
			}
		}
		delete(cache.candidates, oldestKey)
	}
}

func validateRemotePartCacheReference(reference RemotePartReference) (remotePartCacheKey, error) {
	if reference.ObjectURI() == "" || reference.LocalMetadataPath() == "" || reference.Checksum() == "" {
		return remotePartCacheKey{}, ErrRemotePartReferenceInvalid
	}
	return remotePartCacheKeyFrom(reference), nil
}

func remotePartCacheKeyFrom(reference RemotePartReference) remotePartCacheKey {
	return remotePartCacheKey{
		objectURI: reference.ObjectURI(),
		checksum:  reference.Checksum(),
		sizeBytes: reference.SizeBytes(),
	}
}

func (cache *RemotePartCache) touchLocked(entry *remotePartCacheEntry, priority int) {
	cache.clock++
	entry.lastUse = cache.clock
	if priority > entry.priority {
		entry.priority = priority
	}
}

func (cache *RemotePartCache) makeRoomLocked(incoming uint64) bool {
	if incoming > cache.maxBytes {
		return false
	}
	for len(cache.entries) >= cache.maxEntries || incoming > cache.maxBytes-cache.bytes {
		var victim *remotePartCacheEntry
		for _, candidate := range cache.entries {
			if candidate.pins > 0 {
				continue
			}
			if victim == nil || candidate.priority < victim.priority || candidate.priority == victim.priority && candidate.lastUse < victim.lastUse {
				victim = candidate
			}
		}
		if victim == nil {
			return false
		}
		delete(cache.entries, victim.key)
		cache.bytes -= uint64(len(victim.data))
		cache.stats.Evictions++
	}
	return true
}
