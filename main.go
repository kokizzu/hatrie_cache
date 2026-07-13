package hatriecache

/*
#cgo CFLAGS: -std=c99 -Wall -Wextra -I${SRCDIR}/luikore__hat-trie/src
#include <stdlib.h>
#include "luikore__hat-trie/src/hat-trie.h"
*/
import "C"

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"
	"unsafe"
)

// HatValue is the compact value stored in the underlying HAT-trie.
//
// Index is either an int32 counter value or an index into one of the backing
// storage pools. Flags stores the value type in the low six bits, with the high
// two bits reserved for disk and TTL state.
type HatValue struct {
	Index int32
	Flags uint8
}

const (
	DATAVALUE_SIZE_BYTE      = 5
	DATAVALUE_TYPE_BITS      = 6
	DATAVALUE_DISK_BIT_SHIFT = 6
	DATAVALUE_TTL_BIT_SHIFT  = 7
	DATAVALUE_TTL_TYPE_BITS  = uint8((1 << DATAVALUE_TYPE_BITS) - 1)
	DATAVALUE_VALUE_OFFSET   = 32
	DATAVALUE_VALUE_BITS     = (uint64(1) << DATAVALUE_VALUE_OFFSET) - 1
)

const (
	DATAVALUE_TYPE_NULL uint8 = iota
	DATAVALUE_TYPE_COUNTER
	DATAVALUE_TYPE_RAW_BYTES
	DATAVALUE_TYPE_RAW_STRING
	DATAVALUE_TYPE_MAP
	// Slice values are backed by a ring deque for stack/queue operations.
	DATAVALUE_TYPE_SLICE
	DATAVALUE_TYPE_LEVELDB_REF
	// TODO: add more types (set, priority queue, etc).
)

type Map = map[string]interface{}
type Slice = []interface{}

type deque struct {
	values []interface{}
	head   int
	size   int
}

func newDeque(value Slice) deque {
	cloned := cloneSlice(value)
	if cloned == nil {
		return deque{}
	}
	return deque{values: cloned, size: len(cloned)}
}

func (dq *deque) Len() int {
	if dq == nil {
		return 0
	}
	return dq.size
}

func (dq *deque) Slice() Slice {
	if dq == nil || dq.values == nil {
		return nil
	}
	if dq.size == 0 {
		return make(Slice, 0)
	}
	out := make(Slice, dq.size)
	dq.copyTo(out)
	return out
}

func (dq *deque) Push(values ...interface{}) {
	if len(values) == 0 {
		return
	}
	needed := dq.size + len(values)
	if len(dq.values) < needed {
		dq.resize(grownDequeCapacity(len(dq.values), needed))
	}
	for _, value := range values {
		idx := (dq.head + dq.size) % len(dq.values)
		dq.values[idx] = value
		dq.size++
	}
}

func (dq *deque) Pop() (interface{}, bool) {
	if dq == nil || dq.size == 0 {
		return nil, false
	}
	idx := (dq.head + dq.size - 1) % len(dq.values)
	value := dq.values[idx]
	dq.values[idx] = nil
	dq.size--
	dq.compactIfSparse()
	return value, true
}

func (dq *deque) Shift() (interface{}, bool) {
	if dq == nil || dq.size == 0 {
		return nil, false
	}
	value := dq.values[dq.head]
	dq.values[dq.head] = nil
	dq.size--
	if dq.size == 0 {
		dq.head = 0
	} else {
		dq.head = (dq.head + 1) % len(dq.values)
	}
	dq.compactIfSparse()
	return value, true
}

func (dq *deque) Head() (interface{}, bool) {
	if dq == nil || dq.size == 0 {
		return nil, false
	}
	return dq.values[dq.head], true
}

func (dq *deque) Tail() (interface{}, bool) {
	if dq == nil || dq.size == 0 {
		return nil, false
	}
	idx := (dq.head + dq.size - 1) % len(dq.values)
	return dq.values[idx], true
}

func (dq *deque) compactIfSparse() {
	if dq.values == nil {
		return
	}
	if dq.size == 0 {
		dq.values = make([]interface{}, 0)
		dq.head = 0
		return
	}
	if len(dq.values) <= 16 || dq.size*4 > len(dq.values) {
		return
	}
	dq.resize(maxInt(16, dq.size*2))
}

func (dq *deque) resize(capacity int) {
	if capacity < dq.size {
		capacity = dq.size
	}
	values := make([]interface{}, capacity)
	dq.copyTo(values)
	dq.values = values
	dq.head = 0
}

func (dq *deque) copyTo(out []interface{}) {
	if dq == nil || dq.size == 0 || len(dq.values) == 0 {
		return
	}
	if dq.head+dq.size <= len(dq.values) {
		copy(out, dq.values[dq.head:dq.head+dq.size])
		return
	}
	n := copy(out, dq.values[dq.head:])
	copy(out[n:], dq.values[:dq.size-n])
}

func grownDequeCapacity(current int, needed int) int {
	capacity := current * 2
	if capacity < 4 {
		capacity = 4
	}
	for capacity < needed {
		capacity *= 2
	}
	return capacity
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

const (
	NoTTL              time.Duration = -1
	DiskBytesThreshold               = 64 * 1024
)

type Entry struct {
	Key   string
	Value HatValue
}

type CacheStats struct {
	Reads             uint64    `json:"reads"`
	Hits              uint64    `json:"hits"`
	Misses            uint64    `json:"misses"`
	Writes            uint64    `json:"writes"`
	Deletes           uint64    `json:"deletes"`
	Expirations       uint64    `json:"expirations"`
	LastHit           time.Time `json:"last_hit,omitempty"`
	LastMiss          time.Time `json:"last_miss,omitempty"`
	LastWrite         time.Time `json:"last_write,omitempty"`
	HitRate           float64   `json:"hit_rate"`
	CumulativeHitRate float64   `json:"cumulative_hit_rate"`
}

// KeyStats records access metadata for one stored key.
type KeyStats struct {
	Reads             uint64    `json:"reads"`
	Hits              uint64    `json:"hits"`
	Misses            uint64    `json:"misses"`
	Writes            uint64    `json:"writes"`
	LastHit           time.Time `json:"last_hit,omitempty"`
	LastMiss          time.Time `json:"last_miss,omitempty"`
	LastWrite         time.Time `json:"last_write,omitempty"`
	HitRate           float64   `json:"hit_rate"`
	CumulativeHitRate float64   `json:"cumulative_hit_rate"`
}

func MarshalMapJSON(value Map) ([]byte, error) {
	return json.Marshal(value)
}

func UnmarshalMapJSON(data []byte) (Map, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()

	var value interface{}
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var extra interface{}
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, errors.New("hatriecache: invalid JSON map")
		}
		return nil, err
	}

	m, ok := value.(map[string]interface{})
	if !ok {
		return nil, errors.New("hatriecache: JSON value is not an object")
	}
	return Map(m), nil
}

func (hval HatValue) Empty() bool {
	return hval.Flags == 0
}

func (hval HatValue) Type() uint8 {
	return hval.Flags & DATAVALUE_TTL_TYPE_BITS
}

func (hval HatValue) Is(cmp uint8) bool {
	return hval.Type() == cmp
}

func (hval HatValue) IsCounter() bool {
	return hval.Is(DATAVALUE_TYPE_COUNTER)
}

func (hval HatValue) IsBytesAtRaws() bool {
	return hval.Is(DATAVALUE_TYPE_RAW_BYTES)
}

func (hval HatValue) IsStringAtRaws() bool {
	return hval.Is(DATAVALUE_TYPE_RAW_STRING)
}

func (hval HatValue) IsMap() bool {
	return hval.Is(DATAVALUE_TYPE_MAP)
}

func (hval HatValue) IsSlice() bool {
	return hval.Is(DATAVALUE_TYPE_SLICE)
}

func (hval HatValue) IsLevelDBReference() bool {
	return hval.Is(DATAVALUE_TYPE_LEVELDB_REF)
}

func (hval HatValue) HasTtl() bool {
	return hval.Flags&(1<<DATAVALUE_TTL_BIT_SHIFT) != 0
}

func (hval HatValue) OnDisk() bool {
	return hval.Flags&(1<<DATAVALUE_DISK_BIT_SHIFT) != 0
}

func (hval HatValue) String() string {
	switch hval.Type() {
	case DATAVALUE_TYPE_NULL:
		return "null hat value"
	case DATAVALUE_TYPE_COUNTER:
		return "int32 counter: " + strconv.FormatInt(int64(hval.Index), 10)
	case DATAVALUE_TYPE_RAW_BYTES:
		return "raw bytes at index: " + strconv.FormatInt(int64(hval.Index), 10)
	case DATAVALUE_TYPE_RAW_STRING:
		return "string at index: " + strconv.FormatInt(int64(hval.Index), 10)
	case DATAVALUE_TYPE_MAP:
		return "map at index: " + strconv.FormatInt(int64(hval.Index), 10)
	case DATAVALUE_TYPE_SLICE:
		return "slice at index: " + strconv.FormatInt(int64(hval.Index), 10)
	case DATAVALUE_TYPE_LEVELDB_REF:
		return "leveldb reference at index: " + strconv.FormatInt(int64(hval.Index), 10)
	}
	return "unknown type"
}

func (hval HatValue) ToUlong() C.ulong {
	encoded := uint64(uint32(hval.Index))
	encoded |= uint64(hval.Flags) << DATAVALUE_VALUE_OFFSET
	return C.ulong(encoded)
}

func (hval *HatValue) FromUlong(ulong C.ulong) {
	encoded := uint64(ulong)
	hval.Flags = uint8(encoded >> DATAVALUE_VALUE_OFFSET)
	hval.Index = int32(uint32(encoded & DATAVALUE_VALUE_BITS))
}

func (hval HatValue) toValue() C.value_t {
	return C.value_t(hval.ToUlong())
}

func (hval *HatValue) fromValue(value C.value_t) {
	hval.FromUlong(C.ulong(value))
}

// BytesStorage stores byte and string values outside the trie.
type BytesStorage struct {
	array     [][]byte
	reusables map[int32]bool
}

func CreateBytesStorage() *BytesStorage {
	return &BytesStorage{
		array:     [][]byte{},
		reusables: map[int32]bool{},
	}
}

func (bs *BytesStorage) Put(idx int32, value []byte) {
	if idx < 0 || int(idx) >= len(bs.array) {
		return
	}
	bs.array[idx] = cloneBytes(value)
	delete(bs.reusables, idx)
}

func (bs *BytesStorage) Append(value []byte) int32 {
	bs.array = append(bs.array, cloneBytes(value))
	return int32(len(bs.array) - 1)
}

func (bs *BytesStorage) Add(value []byte) int32 {
	if len(bs.reusables) > 0 {
		for idx := range bs.reusables {
			bs.array[idx] = cloneBytes(value)
			delete(bs.reusables, idx)
			return idx
		}
	}
	return bs.Append(value)
}

func (bs *BytesStorage) Del(idx int32) {
	if idx < 0 || int(idx) >= len(bs.array) {
		return
	}
	bs.array[idx] = nil
	bs.reusables[idx] = true
}

// DiskStorage stores large byte values outside the Go heap.
type DiskStorage struct {
	dir       string
	ownedDir  bool
	paths     []string
	reusables map[int32]bool
}

func CreateDiskStorage(dir string, ownedDir bool) (*DiskStorage, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return &DiskStorage{
		dir:       dir,
		ownedDir:  ownedDir,
		paths:     []string{},
		reusables: map[int32]bool{},
	}, nil
}

func (ds *DiskStorage) Put(idx int32, value []byte) error {
	if idx < 0 || int(idx) >= len(ds.paths) {
		return nil
	}
	if err := os.WriteFile(ds.paths[idx], value, 0o600); err != nil {
		return err
	}
	delete(ds.reusables, idx)
	return nil
}

func (ds *DiskStorage) Append(value []byte) (int32, error) {
	idx := int32(len(ds.paths))
	path := ds.pathFor(idx)
	if err := os.WriteFile(path, value, 0o600); err != nil {
		return 0, err
	}
	ds.paths = append(ds.paths, path)
	return idx, nil
}

func (ds *DiskStorage) Add(value []byte) (int32, error) {
	if len(ds.reusables) > 0 {
		for idx := range ds.reusables {
			if err := os.WriteFile(ds.paths[idx], value, 0o600); err != nil {
				return 0, err
			}
			delete(ds.reusables, idx)
			return idx, nil
		}
	}
	return ds.Append(value)
}

func (ds *DiskStorage) Get(idx int32) ([]byte, error) {
	if idx < 0 || int(idx) >= len(ds.paths) {
		return nil, nil
	}
	return os.ReadFile(ds.paths[idx])
}

func (ds *DiskStorage) Del(idx int32) {
	if idx < 0 || int(idx) >= len(ds.paths) {
		return
	}
	_ = os.Remove(ds.paths[idx])
	ds.reusables[idx] = true
}

func (ds *DiskStorage) Destroy() {
	if ds == nil {
		return
	}
	if ds.ownedDir {
		_ = os.RemoveAll(ds.dir)
		return
	}
	for _, path := range ds.paths {
		_ = os.Remove(path)
	}
}

func (ds *DiskStorage) pathFor(idx int32) string {
	return filepath.Join(ds.dir, "bytes-"+strconv.FormatInt(int64(idx), 10)+".bin")
}

// MapStorage stores map values outside the trie.
type MapStorage struct {
	array     []Map
	reusables map[int32]bool
}

func CreateMapStorage() *MapStorage {
	return &MapStorage{
		array:     []Map{},
		reusables: map[int32]bool{},
	}
}

func (ms *MapStorage) Put(idx int32, value Map) {
	if idx < 0 || int(idx) >= len(ms.array) {
		return
	}
	ms.array[idx] = cloneMap(value)
	delete(ms.reusables, idx)
}

func (ms *MapStorage) Append(value Map) int32 {
	ms.array = append(ms.array, cloneMap(value))
	return int32(len(ms.array) - 1)
}

func (ms *MapStorage) Add(value Map) int32 {
	if len(ms.reusables) > 0 {
		for idx := range ms.reusables {
			ms.array[idx] = cloneMap(value)
			delete(ms.reusables, idx)
			return idx
		}
	}
	return ms.Append(value)
}

func (ms *MapStorage) Del(idx int32) {
	if idx < 0 || int(idx) >= len(ms.array) {
		return
	}
	ms.array[idx] = nil
	ms.reusables[idx] = true
}

// SliceStorage stores slice values outside the trie.
type SliceStorage struct {
	array     []deque
	reusables map[int32]bool
}

func CreateSliceStorage() *SliceStorage {
	return &SliceStorage{
		array:     []deque{},
		reusables: map[int32]bool{},
	}
}

func (ss *SliceStorage) Put(idx int32, value Slice) {
	if idx < 0 || int(idx) >= len(ss.array) {
		return
	}
	ss.array[idx] = newDeque(value)
	delete(ss.reusables, idx)
}

func (ss *SliceStorage) Append(value Slice) int32 {
	ss.array = append(ss.array, newDeque(value))
	return int32(len(ss.array) - 1)
}

func (ss *SliceStorage) Add(value Slice) int32 {
	if len(ss.reusables) > 0 {
		for idx := range ss.reusables {
			ss.array[idx] = newDeque(value)
			delete(ss.reusables, idx)
			return idx
		}
	}
	return ss.Append(value)
}

func (ss *SliceStorage) Del(idx int32) {
	if idx < 0 || int(idx) >= len(ss.array) {
		return
	}
	ss.array[idx] = deque{}
	ss.reusables[idx] = true
}

type LevelDBReference struct {
	Key   string
	Type  string
	Store *LevelDBStore
}

type LevelDBReferenceStorage struct {
	array     []LevelDBReference
	reusables map[int32]bool
}

func CreateLevelDBReferenceStorage() *LevelDBReferenceStorage {
	return &LevelDBReferenceStorage{
		array:     []LevelDBReference{},
		reusables: map[int32]bool{},
	}
}

func (rs *LevelDBReferenceStorage) Append(value LevelDBReference) int32 {
	rs.array = append(rs.array, value)
	return int32(len(rs.array) - 1)
}

func (rs *LevelDBReferenceStorage) Add(value LevelDBReference) int32 {
	if len(rs.reusables) > 0 {
		for idx := range rs.reusables {
			rs.array[idx] = value
			delete(rs.reusables, idx)
			return idx
		}
	}
	return rs.Append(value)
}

func (rs *LevelDBReferenceStorage) Get(idx int32) (LevelDBReference, bool) {
	if rs == nil || idx < 0 || int(idx) >= len(rs.array) {
		return LevelDBReference{}, false
	}
	return rs.array[idx], !rs.reusables[idx]
}

func (rs *LevelDBReferenceStorage) Del(idx int32) {
	if rs == nil || idx < 0 || int(idx) >= len(rs.array) {
		return
	}
	rs.array[idx] = LevelDBReference{}
	rs.reusables[idx] = true
}

// HatTrie wraps the C HAT-trie and keeps larger Go values in typed backing
// pools referenced by compact HatValue records.
type HatTrie struct {
	mu       sync.RWMutex
	root     *C.hattrie_t
	raws     *BytesStorage
	disks    *DiskStorage
	maps     *MapStorage
	slices   *SliceStorage
	dbrefs   *LevelDBReferenceStorage
	expires  map[string]time.Time
	stats    CacheStats
	keyStats map[string]KeyStats
	now      func() time.Time
}

func CreateHatTrie() *HatTrie {
	diskDir, err := os.MkdirTemp("", "hatrie-cache-*")
	if err != nil {
		panic(err)
	}
	ht, err := CreateHatTrieWithDiskDir(diskDir, true)
	if err != nil {
		_ = os.RemoveAll(diskDir)
		panic(err)
	}
	return ht
}

func CreateHatTrieWithDiskDir(diskDir string, removeDiskDirOnDestroy bool) (*HatTrie, error) {
	disks, err := CreateDiskStorage(diskDir, removeDiskDirOnDestroy)
	if err != nil {
		return nil, err
	}
	ht := &HatTrie{
		root:     C.hattrie_create(),
		raws:     CreateBytesStorage(),
		disks:    disks,
		maps:     CreateMapStorage(),
		slices:   CreateSliceStorage(),
		dbrefs:   CreateLevelDBReferenceStorage(),
		expires:  map[string]time.Time{},
		keyStats: map[string]KeyStats{},
		now:      time.Now,
	}
	runtime.SetFinalizer(ht, (*HatTrie).Destroy)
	return ht, nil
}

func (ht *HatTrie) Destroy() {
	if ht == nil {
		return
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	if ht.root == nil {
		return
	}
	runtime.SetFinalizer(ht, nil)
	C.hattrie_free(ht.root)
	if ht.disks != nil {
		ht.disks.Destroy()
	}
	ht.root = nil
	ht.raws = nil
	ht.disks = nil
	ht.maps = nil
	ht.slices = nil
	ht.dbrefs = nil
	ht.expires = nil
	ht.keyStats = nil
	ht.now = nil
}

func (ht *HatTrie) Size() int {
	ht.mu.RLock()
	defer ht.mu.RUnlock()

	ht.ensureOpen()
	return int(C.hattrie_size(ht.root))
}

func (ht *HatTrie) Stats() CacheStats {
	ht.mu.RLock()
	defer ht.mu.RUnlock()

	ht.ensureOpen()
	return ht.stats
}

// StatsForKey returns access metadata for an existing key.
func (ht *HatTrie) StatsForKey(key string) (KeyStats, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	ht.ensureOpen()
	rawPtr := ht.tryLocation(key)
	if rawPtr == nil {
		delete(ht.keyStats, key)
		return KeyStats{}, false
	}
	hval := HatValue{}
	hval.fromValue(*rawPtr)
	if ht.expireIfNeededLocked(key, hval) {
		return KeyStats{}, false
	}
	stats, ok := ht.keyStats[key]
	return stats, ok
}

func (ht *HatTrie) restoreKeyStats(key string, stats *KeyStats) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	ht.ensureOpen()
	ht.restoreKeyStatsLocked(key, stats)
}

func (ht *HatTrie) restoreKeyStatsLocked(key string, stats *KeyStats) {
	if stats == nil {
		delete(ht.keyStats, key)
		return
	}
	restored := *stats
	restored.updateRates()
	ht.keyStats[key] = restored
}

func (ht *HatTrie) SaveStats(path string) error {
	stats := ht.Stats()
	data, err := json.MarshalIndent(stats, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return writeFileAtomic(path, data)
}

func (ht *HatTrie) LoadStats(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var stats CacheStats
	if err := json.Unmarshal(data, &stats); err != nil {
		return err
	}
	stats.updateRates()

	ht.mu.Lock()
	defer ht.mu.Unlock()

	ht.ensureOpen()
	ht.stats = stats
	return nil
}

// Expire sets a relative TTL for an existing key. A non-positive TTL deletes
// an existing key immediately. It returns false when the key is missing or has
// already expired.
func (ht *HatTrie) Expire(key string, ttl time.Duration) bool {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	if ttl <= 0 {
		deleted := ht.deleteLocked(key)
		if deleted {
			ht.recordDeleteLocked(key)
		}
		return deleted
	}
	ok := ht.expireAtLocked(key, ht.currentTime().Add(ttl))
	if ok {
		ht.recordWriteLocked(key)
	}
	return ok
}

// ExpireAt sets an absolute expiration time for an existing key. Expiration is
// enforced when the key is read or mutated.
func (ht *HatTrie) ExpireAt(key string, at time.Time) bool {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	ok := ht.expireAtLocked(key, at)
	if ok {
		ht.recordWriteLocked(key)
	}
	return ok
}

// Persist removes an existing key's expiration.
func (ht *HatTrie) Persist(key string) bool {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr := ht.tryLocation(key)
	if rawPtr == nil {
		delete(ht.expires, key)
		return false
	}

	hval := HatValue{}
	hval.fromValue(*rawPtr)
	if ht.expireIfNeededLocked(key, hval) {
		return false
	}
	if _, ok := ht.expires[key]; !ok {
		return false
	}

	delete(ht.expires, key)
	hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
	*rawPtr = hval.toValue()
	ht.recordWriteLocked(key)
	return true
}

// TTL returns the remaining TTL for key, or NoTTL when the key is missing,
// expired, or has no expiration.
func (ht *HatTrie) TTL(key string) time.Duration {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr := ht.tryLocation(key)
	if rawPtr == nil {
		delete(ht.expires, key)
		ht.recordReadLocked(false, key)
		return NoTTL
	}

	hval := HatValue{}
	hval.fromValue(*rawPtr)
	if ht.expireIfNeededLocked(key, hval) {
		ht.recordReadLocked(false, key)
		return NoTTL
	}

	expiresAt, ok := ht.expires[key]
	if !ok {
		ht.recordReadLocked(false, key)
		return NoTTL
	}
	ttl := expiresAt.Sub(ht.currentTime())
	if ttl <= 0 {
		if ht.deleteKnownLocked(key, hval) {
			ht.recordExpirationLocked(key)
		}
		ht.recordReadLocked(false, key)
		return NoTTL
	}
	ht.recordReadLocked(true, key)
	return ttl
}

// VacuumExpired removes expired keys immediately and returns the number of
// trie entries removed. It is safe to call concurrently with other operations.
func (ht *HatTrie) VacuumExpired() int {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	return ht.vacuumExpiredLocked()
}

// StartExpirationCleaner starts a background cleaner that periodically removes
// expired keys. The returned stop function is idempotent and waits for the
// cleaner goroutine to exit.
func (ht *HatTrie) StartExpirationCleaner(interval time.Duration) func() {
	if interval <= 0 {
		panic("hatriecache: expiration cleaner interval must be positive")
	}

	ticker := time.NewTicker(interval)
	done := make(chan struct{})
	stopped := make(chan struct{})
	var stopOnce sync.Once

	go func() {
		defer close(stopped)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				ht.VacuumExpired()
			case <-done:
				return
			}
		}
	}()

	return func() {
		stopOnce.Do(func() {
			close(done)
			<-stopped
		})
	}
}

// VacuumExpiredOnMemoryPressure removes expired keys when current heap
// allocation is at or above maxAllocBytes. It returns the number of removed
// entries, or zero when memory pressure is below the threshold.
func (ht *HatTrie) VacuumExpiredOnMemoryPressure(maxAllocBytes uint64) int {
	if maxAllocBytes == 0 {
		panic("hatriecache: memory pressure threshold must be positive")
	}

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	if mem.Alloc < maxAllocBytes {
		return 0
	}
	return ht.VacuumExpired()
}

// StartMemoryPressureVacuum starts a background cleaner that vacuums expired
// keys only while current heap allocation is at or above maxAllocBytes. The
// returned stop function is idempotent and waits for the goroutine to exit.
func (ht *HatTrie) StartMemoryPressureVacuum(interval time.Duration, maxAllocBytes uint64) func() {
	if interval <= 0 {
		panic("hatriecache: memory pressure vacuum interval must be positive")
	}
	if maxAllocBytes == 0 {
		panic("hatriecache: memory pressure threshold must be positive")
	}

	ticker := time.NewTicker(interval)
	done := make(chan struct{})
	stopped := make(chan struct{})
	var stopOnce sync.Once

	go func() {
		defer close(stopped)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				ht.VacuumExpiredOnMemoryPressure(maxAllocBytes)
			case <-done:
				return
			}
		}
	}()

	return func() {
		stopOnce.Do(func() {
			close(done)
			<-stopped
		})
	}
}

// Keys returns all non-expired keys. When sorted is true, keys are returned in
// bytewise lexicographic order.
func (ht *HatTrie) Keys(sorted bool) []string {
	return ht.KeysWithPrefix("", sorted)
}

// KeysWithPrefix returns all non-expired keys that start with prefix. Prefixes
// and keys may contain NUL bytes.
func (ht *HatTrie) KeysWithPrefix(prefix string, sorted bool) []string {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	entries := ht.entriesWithPrefixLocked(prefix, sorted)
	keys := make([]string, len(entries))
	for i, entry := range entries {
		keys[i] = entry.Key
	}
	return keys
}

// Entries returns all non-expired key/value metadata pairs. Returned HatValue
// records are copies and remain valid after later trie mutations.
func (ht *HatTrie) Entries(sorted bool) []Entry {
	return ht.EntriesWithPrefix("", sorted)
}

// EntriesWithPrefix returns all non-expired key/value metadata pairs whose keys
// start with prefix.
func (ht *HatTrie) EntriesWithPrefix(prefix string, sorted bool) []Entry {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	return ht.entriesWithPrefixLocked(prefix, sorted)
}

// Exists reports whether key exists without hydrating a cold LevelDB value.
func (ht *HatTrie) Exists(key string) bool {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.peekLocked(key)
	hit := !hval.Empty()
	ht.recordReadLocked(hit, key)
	return hit
}

func (ht *HatTrie) ensureOpen() {
	if ht == nil || ht.root == nil {
		panic("hatriecache: use of destroyed HatTrie")
	}
}

func (ht *HatTrie) currentTime() time.Time {
	if ht.now == nil {
		return time.Now()
	}
	return ht.now()
}

func (ht *HatTrie) recordReadLocked(hit bool, keys ...string) {
	now := ht.currentTime()
	ht.stats.Reads++
	if hit {
		ht.stats.Hits++
		ht.stats.LastHit = now
	} else {
		ht.stats.Misses++
		ht.stats.LastMiss = now
	}
	ht.stats.updateRates()

	for _, key := range keys {
		if key == "" {
			continue
		}
		stats, tracked := ht.keyStats[key]
		if !hit && !tracked {
			continue
		}
		stats.Reads++
		if hit {
			stats.Hits++
			stats.LastHit = now
		} else {
			stats.Misses++
			stats.LastMiss = now
		}
		stats.updateRates()
		ht.keyStats[key] = stats
	}
}

func (ht *HatTrie) recordWriteLocked(keys ...string) {
	now := ht.currentTime()
	ht.stats.Writes++
	ht.stats.LastWrite = now

	for _, key := range keys {
		if key == "" {
			continue
		}
		stats := ht.keyStats[key]
		stats.Writes++
		stats.LastWrite = now
		stats.updateRates()
		ht.keyStats[key] = stats
	}
}

func (ht *HatTrie) recordDeleteLocked(key string) {
	ht.stats.Deletes++
	ht.recordWriteLocked()
	delete(ht.keyStats, key)
}

func (ht *HatTrie) recordExpirationLocked(keys ...string) {
	ht.stats.Expirations++
	ht.recordWriteLocked()
	for _, key := range keys {
		delete(ht.keyStats, key)
	}
}

func (stats *CacheStats) updateRates() {
	if stats.Reads == 0 {
		stats.HitRate = 0
		stats.CumulativeHitRate = 0
		return
	}
	rate := float64(stats.Hits) / float64(stats.Reads)
	stats.HitRate = rate
	stats.CumulativeHitRate = rate
}

func (stats *KeyStats) updateRates() {
	if stats.Reads == 0 {
		stats.HitRate = 0
		stats.CumulativeHitRate = 0
		return
	}
	rate := float64(stats.Hits) / float64(stats.Reads)
	stats.HitRate = rate
	stats.CumulativeHitRate = rate
}

func (ht *HatTrie) upsertLocation(key string) *C.value_t {
	ht.ensureOpen()
	cstr := C.CString(key)
	defer C.free(unsafe.Pointer(cstr))

	return C.hattrie_get(ht.root, cstr, C.size_t(len(key)))
}

func (ht *HatTrie) tryLocation(key string) *C.value_t {
	ht.ensureOpen()
	cstr := C.CString(key)
	defer C.free(unsafe.Pointer(cstr))

	return C.hattrie_tryget(ht.root, cstr, C.size_t(len(key)))
}

func (ht *HatTrie) returnStorage(hval HatValue) {
	if hval.Empty() {
		return
	}
	switch hval.Type() {
	case DATAVALUE_TYPE_MAP:
		ht.maps.Del(hval.Index)
	case DATAVALUE_TYPE_SLICE:
		ht.slices.Del(hval.Index)
	case DATAVALUE_TYPE_LEVELDB_REF:
		ht.dbrefs.Del(hval.Index)
	case DATAVALUE_TYPE_RAW_BYTES:
		if hval.OnDisk() {
			ht.disks.Del(hval.Index)
		} else {
			ht.raws.Del(hval.Index)
		}
	case DATAVALUE_TYPE_RAW_STRING:
		ht.raws.Del(hval.Index)
	}
}

func (ht *HatTrie) upsertFreshLocation(key string) (*C.value_t, HatValue) {
	rawPtr := ht.upsertLocation(key)
	hval := HatValue{}
	hval.fromValue(*rawPtr)
	if hval.Empty() {
		delete(ht.expires, key)
		return rawPtr, hval
	}
	if ht.expireIfNeededLocked(key, hval) {
		rawPtr = ht.upsertLocation(key)
		hval = HatValue{}
	}
	return rawPtr, hval
}

func (ht *HatTrie) expireAtLocked(key string, at time.Time) bool {
	rawPtr := ht.tryLocation(key)
	if rawPtr == nil {
		delete(ht.expires, key)
		return false
	}

	hval := HatValue{}
	hval.fromValue(*rawPtr)
	if ht.expireIfNeededLocked(key, hval) {
		return false
	}

	ht.expires[key] = at
	hval.Flags |= 1 << DATAVALUE_TTL_BIT_SHIFT
	*rawPtr = hval.toValue()
	return true
}

func (ht *HatTrie) expireIfNeededLocked(key string, hval HatValue) bool {
	expiresAt, ok := ht.expires[key]
	if !ok {
		return false
	}
	if ht.currentTime().Before(expiresAt) {
		return false
	}
	deleted := ht.deleteKnownLocked(key, hval)
	if deleted {
		ht.recordExpirationLocked(key)
	}
	return deleted
}

func (ht *HatTrie) deleteLocked(key string) bool {
	rawPtr := ht.tryLocation(key)
	if rawPtr == nil {
		delete(ht.expires, key)
		return false
	}

	hval := HatValue{}
	hval.fromValue(*rawPtr)
	return ht.deleteKnownLocked(key, hval)
}

func (ht *HatTrie) deleteKnownLocked(key string, hval HatValue) bool {
	cstr := C.CString(key)
	defer C.free(unsafe.Pointer(cstr))

	if C.hattrie_del(ht.root, cstr, C.size_t(len(key))) != 0 {
		return false
	}
	delete(ht.expires, key)
	delete(ht.keyStats, key)
	ht.returnStorage(hval)
	return true
}

func (ht *HatTrie) vacuumExpiredLocked() int {
	ht.ensureOpen()

	now := ht.currentTime()
	removed := 0
	for key, expiresAt := range ht.expires {
		if now.Before(expiresAt) {
			continue
		}

		rawPtr := ht.tryLocation(key)
		if rawPtr == nil {
			delete(ht.expires, key)
			continue
		}

		hval := HatValue{}
		hval.fromValue(*rawPtr)
		if ht.deleteKnownLocked(key, hval) {
			ht.recordExpirationLocked(key)
			removed++
		}
	}
	return removed
}

func (ht *HatTrie) entriesWithPrefixLocked(prefix string, sorted bool) []Entry {
	ht.ensureOpen()

	var iter *C.hattrie_iter_t
	if prefix == "" {
		iter = C.hattrie_iter_begin(ht.root, C.bool(sorted))
	} else {
		cprefix := C.CString(prefix)
		defer C.free(unsafe.Pointer(cprefix))
		iter = C.hattrie_iter_with_prefix(ht.root, C.bool(sorted), cprefix, C.size_t(len(prefix)))
	}

	type expiredEntry struct {
		key   string
		value HatValue
	}
	entries := []Entry{}
	expired := []expiredEntry{}

	for !bool(C.hattrie_iter_finished(iter)) {
		var keyLen C.size_t
		keyPtr := C.hattrie_iter_key(iter, &keyLen)
		key := string(C.GoBytes(unsafe.Pointer(keyPtr), C.int(keyLen)))
		if prefix != "" {
			key = prefix + key
		}

		valPtr := C.hattrie_iter_val(iter)
		hval := HatValue{}
		if valPtr != nil {
			hval.fromValue(*valPtr)
		}

		if expiresAt, ok := ht.expires[key]; ok && !ht.currentTime().Before(expiresAt) {
			expired = append(expired, expiredEntry{key: key, value: hval})
		} else {
			entries = append(entries, Entry{Key: key, Value: hval})
		}
		C.hattrie_iter_next(iter)
	}
	C.hattrie_iter_free(iter)

	for _, entry := range expired {
		if ht.deleteKnownLocked(entry.key, entry.value) {
			ht.recordExpirationLocked(entry.key)
		}
	}

	return entries
}

func (ht *HatTrie) Get(key string) HatValue {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	ht.recordReadLocked(!hval.Empty(), key)
	return hval
}

func (ht *HatTrie) peekLocked(key string) HatValue {
	rawPtr := ht.tryLocation(key)
	if rawPtr == nil {
		delete(ht.expires, key)
		return HatValue{}
	}
	hval := HatValue{}
	hval.fromValue(*rawPtr)
	if ht.expireIfNeededLocked(key, hval) {
		return HatValue{}
	}
	return hval
}

func (ht *HatTrie) getLocked(key string) HatValue {
	iter := ht.tryLocation(key)
	hval := HatValue{}
	if iter != nil {
		hval.fromValue(*iter)
		if ht.expireIfNeededLocked(key, hval) {
			return HatValue{}
		}
		if hval.IsLevelDBReference() {
			hydrated, err := ht.hydrateLevelDBReferenceLocked(key, hval)
			if err != nil {
				panic(err)
			}
			return hydrated
		}
	} else {
		delete(ht.expires, key)
	}
	return hval
}

func (ht *HatTrie) hydrateLevelDBReferenceLocked(key string, hval HatValue) (HatValue, error) {
	ref, ok := ht.dbrefs.Get(hval.Index)
	if !ok || ref.Store == nil {
		ht.deleteKnownLocked(key, hval)
		return HatValue{}, nil
	}

	entry, ok, err := ref.Store.Entry(ref.Key)
	if err != nil {
		return HatValue{}, err
	}
	if !ok {
		ht.deleteKnownLocked(key, hval)
		return HatValue{}, nil
	}
	if entry.Key != key {
		return HatValue{}, errors.New("hatriecache: leveldb reference key mismatch")
	}
	if entry.ExpiresAt != nil && !ht.currentTime().Before(*entry.ExpiresAt) {
		if ht.deleteKnownLocked(key, hval) {
			ht.recordExpirationLocked(key)
		}
		return HatValue{}, nil
	}
	operation, err := validateSnapshotEntry(entry)
	if err != nil {
		return HatValue{}, err
	}
	return ht.applySnapshotOperationLocked(operation)
}

// Delete removes key and returns whether it existed.
func (ht *HatTrie) Delete(key string) bool {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	deleted := ht.deleteLocked(key)
	if deleted {
		ht.recordDeleteLocked(key)
	}
	return deleted
}

// Del removes key if it exists.
func (ht *HatTrie) Del(key string) {
	ht.Delete(key)
}

// UpsertCounter sets key to an int32 counter.
func (ht *HatTrie) UpsertCounter(key string, val int32) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertFreshLocation(key)
	if !hval.IsCounter() {
		ht.returnStorage(hval)
	}
	delete(ht.expires, key)
	*rawPtr = HatValue{Index: val, Flags: DATAVALUE_TYPE_COUNTER}.toValue()
	ht.recordWriteLocked(key)
}

// IncrementCounter increments key by by. If key is not a counter, it is reset
// to by.
func (ht *HatTrie) IncrementCounter(key string, by int32) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertFreshLocation(key)
	if hval.IsCounter() {
		hval.Index += by
	} else {
		ht.returnStorage(hval)
		delete(ht.expires, key)
		hval.Flags = DATAVALUE_TYPE_COUNTER
		hval.Index = by
	}
	*rawPtr = hval.toValue()
	ht.recordWriteLocked(key)
}

// GetCounter returns 0 if key is missing or does not hold a counter.
func (ht *HatTrie) GetCounter(key string) int32 {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	ht.recordReadLocked(hval.IsCounter(), key)
	if hval.IsCounter() {
		return hval.Index
	}
	return 0
}

// UpsertString sets key to a string.
func (ht *HatTrie) UpsertString(key string, val string) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertFreshLocation(key)
	if hval.IsStringAtRaws() {
		ht.raws.Put(hval.Index, []byte(val))
		delete(ht.expires, key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return
	}

	ht.returnStorage(hval)
	delete(ht.expires, key)
	idx := ht.raws.Add([]byte(val))
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_RAW_STRING}.toValue()
	ht.recordWriteLocked(key)
}

// AppendString appends str to key. If key is not a string, it is reset to str.
func (ht *HatTrie) AppendString(key string, str string) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertFreshLocation(key)
	if hval.IsStringAtRaws() {
		old := ht.raws.array[hval.Index]
		next := make([]byte, 0, len(old)+len(str))
		next = append(next, old...)
		next = append(next, str...)
		ht.raws.Put(hval.Index, next)
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return
	}

	ht.returnStorage(hval)
	delete(ht.expires, key)
	idx := ht.raws.Add([]byte(str))
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_RAW_STRING}.toValue()
	ht.recordWriteLocked(key)
}

// PrependString prepends str to key. If key is not a string, it is reset to str.
func (ht *HatTrie) PrependString(key string, str string) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertFreshLocation(key)
	if hval.IsStringAtRaws() {
		old := ht.raws.array[hval.Index]
		next := make([]byte, 0, len(str)+len(old))
		next = append(next, str...)
		next = append(next, old...)
		ht.raws.Put(hval.Index, next)
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return
	}

	ht.returnStorage(hval)
	delete(ht.expires, key)
	idx := ht.raws.Add([]byte(str))
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_RAW_STRING}.toValue()
	ht.recordWriteLocked(key)
}

// GetString returns an empty string if key is missing or not a string/counter.
func (ht *HatTrie) GetString(key string) string {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	ht.recordReadLocked(hval.IsStringAtRaws() || hval.IsCounter(), key)
	if hval.IsStringAtRaws() {
		return string(ht.raws.array[hval.Index])
	}
	if hval.IsCounter() {
		return strconv.FormatInt(int64(hval.Index), 10)
	}
	return ""
}

// UpsertBytes sets key to a byte slice.
func (ht *HatTrie) UpsertBytes(key string, val []byte) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertFreshLocation(key)
	if hval.IsBytesAtRaws() {
		delete(ht.expires, key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		ht.storeBytesLocked(rawPtr, hval, val)
		ht.recordWriteLocked(key)
		return
	}

	ht.returnStorage(hval)
	delete(ht.expires, key)
	ht.storeBytesLocked(rawPtr, HatValue{}, val)
	ht.recordWriteLocked(key)
}

// GetBytes returns nil if key is missing or not a string/bytes value.
func (ht *HatTrie) GetBytes(key string) []byte {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	ht.recordReadLocked(hval.IsStringAtRaws() || hval.IsBytesAtRaws(), key)
	if hval.IsStringAtRaws() {
		return cloneBytes(ht.raws.array[hval.Index])
	}
	if hval.IsBytesAtRaws() {
		if hval.OnDisk() {
			value, err := ht.disks.Get(hval.Index)
			if err != nil {
				panic(err)
			}
			return value
		}
		return cloneBytes(ht.raws.array[hval.Index])
	}
	return nil
}

func (ht *HatTrie) storeBytesLocked(rawPtr *C.value_t, old HatValue, val []byte) {
	if len(val) > DiskBytesThreshold {
		if old.IsBytesAtRaws() && old.OnDisk() {
			if err := ht.disks.Put(old.Index, val); err != nil {
				panic(err)
			}
			*rawPtr = old.toValue()
			return
		}
		if old.IsBytesAtRaws() && !old.OnDisk() {
			ht.raws.Del(old.Index)
		}
		idx, err := ht.disks.Add(val)
		if err != nil {
			panic(err)
		}
		*rawPtr = HatValue{
			Index: idx,
			Flags: DATAVALUE_TYPE_RAW_BYTES | (1 << DATAVALUE_DISK_BIT_SHIFT),
		}.toValue()
		return
	}

	if old.IsBytesAtRaws() && old.OnDisk() {
		ht.disks.Del(old.Index)
	} else if old.IsBytesAtRaws() {
		ht.raws.Put(old.Index, val)
		*rawPtr = old.toValue()
		return
	}
	idx := ht.raws.Add(val)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_RAW_BYTES}.toValue()
}

func (ht *HatTrie) UpsertMap(key string, val Map) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertFreshLocation(key)
	if hval.IsMap() {
		ht.maps.Put(hval.Index, val)
		delete(ht.expires, key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return
	}

	ht.returnStorage(hval)
	delete(ht.expires, key)
	idx := ht.maps.Add(val)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_MAP}.toValue()
	ht.recordWriteLocked(key)
}

func (ht *HatTrie) UpsertMapJSON(key string, data []byte) error {
	value, err := UnmarshalMapJSON(data)
	if err != nil {
		return err
	}
	ht.UpsertMap(key, value)
	return nil
}

func (ht *HatTrie) GetMapJSON(key string) ([]byte, bool, error) {
	value := ht.GetMap(key)
	if value == nil {
		return nil, false, nil
	}
	data, err := MarshalMapJSON(value)
	if err != nil {
		return nil, true, err
	}
	return data, true, nil
}

func (ht *HatTrie) PutMap(key string, subkey string, val interface{}) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertFreshLocation(key)
	if hval.IsMap() {
		old := ht.maps.array[hval.Index]
		if old == nil {
			old = Map{}
		}
		old[subkey] = val
		ht.maps.Put(hval.Index, old)
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return
	}

	ht.returnStorage(hval)
	delete(ht.expires, key)
	idx := ht.maps.Add(Map{subkey: val})
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_MAP}.toValue()
	ht.recordWriteLocked(key)
}

func (ht *HatTrie) PeekMap(key, subkey string) interface{} {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	m, ok := ht.mapRefLocked(key)
	if !ok {
		ht.recordReadLocked(false, key)
		return nil
	}
	val, exists := m[subkey]
	ht.recordReadLocked(exists, key)
	return val
}

func (ht *HatTrie) TakeMap(key, subkey string) interface{} {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	m, ok := ht.mapRefLocked(key)
	if !ok {
		ht.recordReadLocked(false, key)
		return nil
	}
	val, exists := m[subkey]
	ht.recordReadLocked(exists, key)
	if exists {
		delete(m, subkey)
		ht.recordWriteLocked(key)
	}
	return val
}

func (ht *HatTrie) GetMap(key string) Map {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	m, ok := ht.mapRefLocked(key)
	ht.recordReadLocked(ok, key)
	if !ok {
		return nil
	}
	return cloneMap(m)
}

func (ht *HatTrie) mapRefLocked(key string) (Map, bool) {
	hval := ht.getLocked(key)
	if hval.IsMap() {
		return ht.maps.array[hval.Index], true
	}
	return nil, false
}

func (ht *HatTrie) UpsertSlice(key string, val Slice) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertFreshLocation(key)
	if hval.IsSlice() {
		ht.slices.Put(hval.Index, val)
		delete(ht.expires, key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return
	}

	ht.returnStorage(hval)
	delete(ht.expires, key)
	idx := ht.slices.Add(val)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_SLICE}.toValue()
	ht.recordWriteLocked(key)
}

func (ht *HatTrie) PushSlice(key string, val interface{}, vals ...interface{}) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertFreshLocation(key)
	if hval.IsSlice() {
		values := make(Slice, 0, 1+len(vals))
		values = append(values, val)
		values = append(values, vals...)
		ht.slices.array[hval.Index].Push(values...)
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return
	}

	ht.returnStorage(hval)
	delete(ht.expires, key)
	arr := make(Slice, 0, 1+len(vals))
	arr = append(arr, val)
	arr = append(arr, vals...)
	idx := ht.slices.Add(arr)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_SLICE}.toValue()
	ht.recordWriteLocked(key)
}

func (ht *HatTrie) PopSlice(key string) interface{} {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	if !hval.IsSlice() {
		ht.recordReadLocked(false, key)
		return nil
	}

	val, ok := ht.slices.array[hval.Index].Pop()
	if !ok {
		ht.recordReadLocked(false, key)
		return nil
	}
	ht.recordReadLocked(true, key)
	ht.recordWriteLocked(key)
	return val
}

func (ht *HatTrie) ShiftSlice(key string) interface{} {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	if !hval.IsSlice() {
		ht.recordReadLocked(false, key)
		return nil
	}

	val, ok := ht.slices.array[hval.Index].Shift()
	if !ok {
		ht.recordReadLocked(false, key)
		return nil
	}
	ht.recordReadLocked(true, key)
	ht.recordWriteLocked(key)
	return val
}

func (ht *HatTrie) HeadSlice(key string) interface{} {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	dq, ok := ht.sliceRefLocked(key)
	val, hit := dq.Head()
	hit = ok && hit
	ht.recordReadLocked(hit, key)
	if hit {
		return val
	}
	return nil
}

func (ht *HatTrie) TailSlice(key string) interface{} {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	dq, ok := ht.sliceRefLocked(key)
	val, hit := dq.Tail()
	hit = ok && hit
	ht.recordReadLocked(hit, key)
	if hit {
		return val
	}
	return nil
}

func (ht *HatTrie) GetSlice(key string) Slice {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	dq, ok := ht.sliceRefLocked(key)
	ht.recordReadLocked(ok, key)
	if !ok {
		return nil
	}
	return dq.Slice()
}

func (ht *HatTrie) sliceRefLocked(key string) (*deque, bool) {
	hval := ht.getLocked(key)
	if hval.IsSlice() {
		return &ht.slices.array[hval.Index], true
	}
	return nil, false
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	out := make([]byte, len(value))
	copy(out, value)
	return out
}

func cloneMap(value Map) Map {
	if value == nil {
		return nil
	}
	out := make(Map, len(value))
	for key, val := range value {
		out[key] = val
	}
	return out
}

func cloneSlice(value Slice) Slice {
	if value == nil {
		return nil
	}
	out := make(Slice, len(value))
	copy(out, value)
	return out
}
