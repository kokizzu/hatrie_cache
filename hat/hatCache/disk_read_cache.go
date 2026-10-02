package hatCache

import (
	"container/list"
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"
)

var (
	ErrDiskStorageReadCacheNil             = errors.New("hatriecache: nil disk storage read cache")
	ErrDiskStorageReadCacheInvalidMaxBytes = errors.New("hatriecache: disk storage read cache max bytes must be non-negative")
	ErrDiskStorageReadCacheInvalidMaxValue = errors.New("hatriecache: disk storage read cache max value bytes must be non-negative")
)

// DiskStorageReadCacheOptions configures the opt-in cache used by
// DiskStorage.Get. MaxBytes is the hard resident-byte bound. MaxValueBytes
// prevents one large value from consuming that bound. AdmissionReads defaults
// to two reads when zero, which avoids retaining one-shot values.
type DiskStorageReadCacheOptions struct {
	MaxBytes       int64
	MaxValueBytes  int64
	AdmissionReads uint8
}

// DiskStorageReadCacheStats reports the current bounded cache state and its
// cumulative counters since the last configuration.
type DiskStorageReadCacheStats struct {
	Enabled        bool
	MaxBytes       int64
	MaxValueBytes  int64
	AdmissionReads uint8
	Entries        int
	Bytes          int64
	Hits           uint64
	Misses         uint64
	Admissions     uint64
	Evictions      uint64
}

type diskStorageReadCacheEntry struct {
	index int32
	value []byte
}

type diskStorageReadCacheCandidate struct {
	index int32
	reads uint8
}

type diskStorageReadCache struct {
	mu sync.Mutex

	maxBytes       int64
	maxValueBytes  int64
	admissionReads uint8
	candidateLimit int

	bytes      int64
	entries    map[int32]*list.Element
	lru        *list.List
	candidates map[int32]*list.Element
	candidateL *list.List

	hits       uint64
	misses     uint64
	admissions uint64
	evictions  uint64
}

var (
	diskStorageReadCacheRegistry      sync.Map
	diskStorageReadCacheRegistryUsers atomic.Int64
)

func diskStorageReadCacheKey(storage *DiskStorage) uintptr {
	return uintptr(unsafe.Pointer(storage))
}

func (ds *DiskStorage) loadReadCache() *diskStorageReadCache {
	if ds == nil || diskStorageReadCacheRegistryUsers.Load() == 0 {
		return nil
	}
	value, ok := diskStorageReadCacheRegistry.Load(diskStorageReadCacheKey(ds))
	if !ok {
		return nil
	}
	return value.(*diskStorageReadCache)
}

func normalizeDiskStorageReadCacheOptions(options DiskStorageReadCacheOptions) (DiskStorageReadCacheOptions, error) {
	if options.MaxBytes < 0 {
		return DiskStorageReadCacheOptions{}, ErrDiskStorageReadCacheInvalidMaxBytes
	}
	if options.MaxValueBytes < 0 {
		return DiskStorageReadCacheOptions{}, ErrDiskStorageReadCacheInvalidMaxValue
	}
	if options.MaxBytes == 0 {
		return DiskStorageReadCacheOptions{}, nil
	}
	if options.MaxValueBytes == 0 || options.MaxValueBytes > options.MaxBytes {
		options.MaxValueBytes = options.MaxBytes
	}
	if options.AdmissionReads == 0 {
		options.AdmissionReads = 2
	}
	return options, nil
}

func newDiskStorageReadCache(options DiskStorageReadCacheOptions) *diskStorageReadCache {
	if options.MaxBytes == 0 {
		return nil
	}
	candidateLimit := int(options.MaxBytes / 4096)
	if candidateLimit < 64 {
		candidateLimit = 64
	}
	if candidateLimit > 4096 {
		candidateLimit = 4096
	}
	return &diskStorageReadCache{
		maxBytes:       options.MaxBytes,
		maxValueBytes:  options.MaxValueBytes,
		admissionReads: options.AdmissionReads,
		candidateLimit: candidateLimit,
		entries:        make(map[int32]*list.Element),
		lru:            list.New(),
		candidates:     make(map[int32]*list.Element),
		candidateL:     list.New(),
	}
}

func (cache *diskStorageReadCache) get(index int32) ([]byte, bool) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	element, ok := cache.entries[index]
	if !ok {
		cache.misses++
		return nil, false
	}
	cache.lru.MoveToFront(element)
	cache.hits++
	entry := element.Value.(*diskStorageReadCacheEntry)
	return append([]byte(nil), entry.value...), true
}

func (cache *diskStorageReadCache) observeMiss(index int32, value []byte) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if int64(len(value)) > cache.maxValueBytes || int64(len(value)) > cache.maxBytes {
		cache.removeCandidateLocked(index)
		return
	}
	element, ok := cache.candidates[index]
	if !ok {
		if cache.admissionReads == 1 {
			cache.insertLocked(index, value)
			return
		}
		element = cache.candidateL.PushFront(&diskStorageReadCacheCandidate{index: index, reads: 1})
		cache.candidates[index] = element
		cache.trimCandidatesLocked()
		return
	}
	candidate := element.Value.(*diskStorageReadCacheCandidate)
	if candidate.reads < cache.admissionReads {
		candidate.reads++
	}
	if candidate.reads < cache.admissionReads {
		cache.candidateL.MoveToFront(element)
		return
	}
	cache.removeCandidateLocked(index)
	cache.insertLocked(index, value)
}

func (cache *diskStorageReadCache) insertLocked(index int32, value []byte) {
	valueBytes := int64(len(value))
	if valueBytes > cache.maxValueBytes || valueBytes > cache.maxBytes {
		return
	}
	if element, ok := cache.entries[index]; ok {
		cache.removeEntryLocked(element)
	}
	for cache.bytes+valueBytes > cache.maxBytes {
		oldest := cache.lru.Back()
		if oldest == nil {
			return
		}
		cache.removeEntryLocked(oldest)
		cache.evictions++
	}
	entry := &diskStorageReadCacheEntry{index: index, value: append([]byte(nil), value...)}
	cache.entries[index] = cache.lru.PushFront(entry)
	cache.bytes += valueBytes
	cache.admissions++
}

func (cache *diskStorageReadCache) trimCandidatesLocked() {
	for len(cache.candidates) > cache.candidateLimit {
		oldest := cache.candidateL.Back()
		if oldest == nil {
			return
		}
		candidate := oldest.Value.(*diskStorageReadCacheCandidate)
		cache.removeCandidateLocked(candidate.index)
	}
}

func (cache *diskStorageReadCache) removeCandidateLocked(index int32) {
	if element, ok := cache.candidates[index]; ok {
		delete(cache.candidates, index)
		cache.candidateL.Remove(element)
	}
}

func (cache *diskStorageReadCache) removeEntryLocked(element *list.Element) {
	entry := element.Value.(*diskStorageReadCacheEntry)
	delete(cache.entries, entry.index)
	cache.bytes -= int64(len(entry.value))
	cache.lru.Remove(element)
}

func (cache *diskStorageReadCache) invalidate(index int32) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.removeCandidateLocked(index)
	if element, ok := cache.entries[index]; ok {
		cache.removeEntryLocked(element)
	}
}

func (cache *diskStorageReadCache) stats() DiskStorageReadCacheStats {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	return DiskStorageReadCacheStats{
		Enabled:        true,
		MaxBytes:       cache.maxBytes,
		MaxValueBytes:  cache.maxValueBytes,
		AdmissionReads: cache.admissionReads,
		Entries:        len(cache.entries),
		Bytes:          cache.bytes,
		Hits:           cache.hits,
		Misses:         cache.misses,
		Admissions:     cache.admissions,
		Evictions:      cache.evictions,
	}
}

func (cache *diskStorageReadCache) options() DiskStorageReadCacheOptions {
	if cache == nil {
		return DiskStorageReadCacheOptions{}
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	return DiskStorageReadCacheOptions{
		MaxBytes:       cache.maxBytes,
		MaxValueBytes:  cache.maxValueBytes,
		AdmissionReads: cache.admissionReads,
	}
}

func (ds *DiskStorage) ConfigureReadCache(options DiskStorageReadCacheOptions) error {
	if ds == nil {
		return ErrDiskStorageReadCacheNil
	}
	normalized, err := normalizeDiskStorageReadCacheOptions(options)
	if err != nil {
		return err
	}
	key := diskStorageReadCacheKey(ds)
	if normalized.MaxBytes == 0 {
		if _, ok := diskStorageReadCacheRegistry.LoadAndDelete(key); ok {
			diskStorageReadCacheRegistryUsers.Add(-1)
		}
		return nil
	}
	cache := newDiskStorageReadCache(normalized)
	if _, loaded := diskStorageReadCacheRegistry.Load(key); !loaded {
		diskStorageReadCacheRegistryUsers.Add(1)
	}
	diskStorageReadCacheRegistry.Store(key, cache)
	return nil
}

// ReadCacheStats returns zero values while the default read cache is disabled.
func (ds *DiskStorage) ReadCacheStats() DiskStorageReadCacheStats {
	cache := ds.loadReadCache()
	if cache == nil {
		return DiskStorageReadCacheStats{}
	}
	return cache.stats()
}

func (ds *DiskStorage) invalidateReadCache(index int32) {
	if cache := ds.loadReadCache(); cache != nil {
		cache.invalidate(index)
	}
}

func (ds *DiskStorage) configuredReadCacheOptions() DiskStorageReadCacheOptions {
	if cache := ds.loadReadCache(); cache != nil {
		return cache.options()
	}
	return DiskStorageReadCacheOptions{}
}

func (ds *DiskStorage) clearReadCache() {
	if ds == nil {
		return
	}
	if _, ok := diskStorageReadCacheRegistry.LoadAndDelete(diskStorageReadCacheKey(ds)); ok {
		diskStorageReadCacheRegistryUsers.Add(-1)
	}
}

func (ds *DiskStorage) finalize() {
	ds.clearReadCache()
}

// ConfigureDiskReadCache enables the bounded read cache for large values kept
// in the trie disk backing store. Passing MaxBytes zero disables it.
func (ht *HatTrie) ConfigureDiskReadCache(options DiskStorageReadCacheOptions) error {
	if ht == nil {
		return ErrDiskStorageReadCacheNil
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()
	if ht.disks == nil {
		return ErrDiskStorageReadCacheNil
	}
	return ht.disks.ConfigureReadCache(options)
}

// DiskReadCacheStats reports the trie disk backing store read cache state.
func (ht *HatTrie) DiskReadCacheStats() DiskStorageReadCacheStats {
	if ht == nil {
		return DiskStorageReadCacheStats{}
	}
	ht.mu.RLock()
	defer ht.mu.RUnlock()
	if ht.disks == nil {
		return DiskStorageReadCacheStats{}
	}
	return ht.disks.ReadCacheStats()
}
