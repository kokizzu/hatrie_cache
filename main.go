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
	mathbits "math/bits"
	"os"
	"path/filepath"
	"runtime"
	"sort"
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
	DATAVALUE_TYPE_SET
	DATAVALUE_TYPE_PRIORITY_QUEUE
	DATAVALUE_TYPE_BLOOM_FILTER
	DATAVALUE_TYPE_COUNT_MIN_SKETCH
	DATAVALUE_TYPE_HYPERLOGLOG
	DATAVALUE_TYPE_TOP_K
	DATAVALUE_TYPE_CUCKOO_FILTER
	DATAVALUE_TYPE_ROARING_BITMAP
	DATAVALUE_TYPE_QUANTILE_SKETCH
	DATAVALUE_TYPE_FENWICK_TREE
	DATAVALUE_TYPE_SPARSE_BITSET
	DATAVALUE_TYPE_RESERVOIR_SAMPLE
	DATAVALUE_TYPE_XOR_FILTER
)

type Map = map[string]interface{}
type Slice = []interface{}
type Set = []interface{}

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
	for idx, value := range out {
		out[idx] = cloneValue(value)
	}
	return out
}

func (dq *deque) Push(values ...interface{}) {
	if len(values) == 0 {
		return
	}
	dq.ensureCapacity(dq.size + len(values))
	for _, value := range values {
		dq.pushValue(value)
	}
}

func (dq *deque) PushOne(value interface{}, values ...interface{}) {
	dq.ensureCapacity(dq.size + 1 + len(values))
	dq.pushValue(value)
	for _, value := range values {
		dq.pushValue(value)
	}
}

func (dq *deque) ensureCapacity(needed int) {
	if len(dq.values) < needed {
		dq.resize(grownDequeCapacity(len(dq.values), needed))
	}
}

func (dq *deque) pushValue(value interface{}) {
	idx := (dq.head + dq.size) % len(dq.values)
	dq.values[idx] = cloneValue(value)
	dq.size++
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

type reusableIndexes struct {
	stack []int32
	bits  []uint64
	count int
}

func (indexes *reusableIndexes) Len() int {
	if indexes == nil {
		return 0
	}
	return indexes.count
}

func (indexes *reusableIndexes) Has(idx int32) bool {
	if indexes == nil || idx < 0 {
		return false
	}
	word, mask := reusableIndexBit(idx)
	if word >= len(indexes.bits) {
		return false
	}
	return indexes.bits[word]&mask != 0
}

func (indexes *reusableIndexes) Mark(idx int32) bool {
	if indexes == nil || idx < 0 {
		return false
	}
	word, mask := reusableIndexBit(idx)
	if word >= len(indexes.bits) {
		next := make([]uint64, word+1)
		copy(next, indexes.bits)
		indexes.bits = next
	}
	if indexes.bits[word]&mask != 0 {
		return false
	}
	indexes.bits[word] |= mask
	indexes.stack = append(indexes.stack, idx)
	indexes.count++
	return true
}

func (indexes *reusableIndexes) Take() (int32, bool) {
	if indexes == nil {
		return 0, false
	}
	for len(indexes.stack) > 0 {
		last := len(indexes.stack) - 1
		idx := indexes.stack[last]
		indexes.stack[last] = 0
		indexes.stack = indexes.stack[:last]
		if indexes.Use(idx) {
			return idx, true
		}
	}
	return 0, false
}

func (indexes *reusableIndexes) Use(idx int32) bool {
	if indexes == nil || idx < 0 {
		return false
	}
	word, mask := reusableIndexBit(idx)
	if word >= len(indexes.bits) || indexes.bits[word]&mask == 0 {
		return false
	}
	indexes.bits[word] &^= mask
	indexes.count--
	return true
}

func (indexes *reusableIndexes) Compact(limit int) {
	if indexes == nil {
		return
	}
	if limit <= 0 {
		for i := range indexes.stack {
			indexes.stack[i] = 0
		}
		for i := range indexes.bits {
			indexes.bits[i] = 0
		}
		indexes.stack = nil
		indexes.bits = nil
		indexes.count = 0
		return
	}

	neededWords := (limit + 63) / 64
	if neededWords < len(indexes.bits) {
		for i := neededWords; i < len(indexes.bits); i++ {
			indexes.bits[i] = 0
		}
		indexes.bits = indexes.bits[:neededWords]
	}
	if neededWords == len(indexes.bits) && neededWords > 0 && limit%64 != 0 {
		indexes.bits[neededWords-1] &= (uint64(1) << uint(limit%64)) - 1
	}

	indexes.count = 0
	for _, word := range indexes.bits {
		indexes.count += mathbits.OnesCount64(word)
	}

	nextStack := indexes.stack[:0]
	for _, idx := range indexes.stack {
		if idx >= 0 && int(idx) < limit && indexes.Has(idx) {
			nextStack = append(nextStack, idx)
		}
	}
	for i := len(nextStack); i < len(indexes.stack); i++ {
		indexes.stack[i] = 0
	}
	indexes.stack = nextStack
	indexes.compactBackingSlices()
}

func (indexes *reusableIndexes) compactBackingSlices() {
	if len(indexes.stack) == 0 {
		indexes.stack = nil
	} else if cap(indexes.stack) > 16 && len(indexes.stack)*4 < cap(indexes.stack) {
		next := make([]int32, len(indexes.stack))
		copy(next, indexes.stack)
		indexes.stack = next
	}

	if len(indexes.bits) == 0 {
		indexes.bits = nil
	} else if cap(indexes.bits) > 16 && len(indexes.bits)*4 < cap(indexes.bits) {
		next := make([]uint64, len(indexes.bits))
		copy(next, indexes.bits)
		indexes.bits = next
	}
}

func reusableIndexBit(idx int32) (int, uint64) {
	value := int(idx)
	return value / 64, uint64(1) << uint(value%64)
}

func trimReusableTail[T any](values []T, reusables *reusableIndexes) []T {
	var zero T
	trimmed := false
	for len(values) > 0 {
		idx := int32(len(values) - 1)
		if !reusables.Has(idx) {
			if trimmed {
				reusables.Compact(len(values))
			}
			return values
		}
		values[len(values)-1] = zero
		reusables.Use(idx)
		values = values[:len(values)-1]
		trimmed = true
	}
	if trimmed {
		reusables.Compact(0)
	}
	return values
}

const (
	NoTTL              time.Duration = -1
	DiskBytesThreshold               = 64 * 1024
)

type Entry struct {
	Key   string
	Value HatValue
}

type expirationEntry struct {
	key string
	at  time.Time
}

type expirationHeap []expirationEntry

func (heap expirationHeap) Len() int {
	return len(heap)
}

func (heap expirationHeap) Peek() (expirationEntry, bool) {
	if len(heap) == 0 {
		return expirationEntry{}, false
	}
	return heap[0], true
}

func (heap *expirationHeap) Push(entry expirationEntry) {
	*heap = append(*heap, entry)
	heap.siftUp(len(*heap) - 1)
}

func (heap *expirationHeap) Pop() (expirationEntry, bool) {
	if heap == nil || len(*heap) == 0 {
		return expirationEntry{}, false
	}
	values := *heap
	root := values[0]
	last := len(values) - 1
	values[0] = values[last]
	values[last] = expirationEntry{}
	values = values[:last]
	*heap = values
	if len(values) > 0 {
		heap.siftDown(0)
	}
	return root, true
}

func (heap *expirationHeap) Clear() {
	if heap == nil {
		return
	}
	for idx := range *heap {
		(*heap)[idx] = expirationEntry{}
	}
	*heap = (*heap)[:0]
}

func (heap expirationHeap) siftUp(idx int) {
	for idx > 0 {
		parent := (idx - 1) / 2
		if !heap[idx].before(heap[parent]) {
			return
		}
		heap[idx], heap[parent] = heap[parent], heap[idx]
		idx = parent
	}
}

func (heap expirationHeap) siftDown(idx int) {
	for {
		left := 2*idx + 1
		if left >= len(heap) {
			return
		}
		smallest := left
		right := left + 1
		if right < len(heap) && heap[right].before(heap[left]) {
			smallest = right
		}
		if !heap[smallest].before(heap[idx]) {
			return
		}
		heap[idx], heap[smallest] = heap[smallest], heap[idx]
		idx = smallest
	}
}

func (entry expirationEntry) before(other expirationEntry) bool {
	if !entry.at.Equal(other.at) {
		return entry.at.Before(other.at)
	}
	return entry.key < other.key
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

func (hval HatValue) IsSet() bool {
	return hval.Is(DATAVALUE_TYPE_SET)
}

func (hval HatValue) IsPriorityQueue() bool {
	return hval.Is(DATAVALUE_TYPE_PRIORITY_QUEUE)
}

func (hval HatValue) IsBloomFilter() bool {
	return hval.Is(DATAVALUE_TYPE_BLOOM_FILTER)
}

func (hval HatValue) IsCountMinSketch() bool {
	return hval.Is(DATAVALUE_TYPE_COUNT_MIN_SKETCH)
}

func (hval HatValue) IsHyperLogLog() bool {
	return hval.Is(DATAVALUE_TYPE_HYPERLOGLOG)
}

func (hval HatValue) IsTopK() bool {
	return hval.Is(DATAVALUE_TYPE_TOP_K)
}

func (hval HatValue) IsCuckooFilter() bool {
	return hval.Is(DATAVALUE_TYPE_CUCKOO_FILTER)
}

func (hval HatValue) IsRoaringBitmap() bool {
	return hval.Is(DATAVALUE_TYPE_ROARING_BITMAP)
}

func (hval HatValue) IsQuantileSketch() bool {
	return hval.Is(DATAVALUE_TYPE_QUANTILE_SKETCH)
}

func (hval HatValue) IsFenwickTree() bool {
	return hval.Is(DATAVALUE_TYPE_FENWICK_TREE)
}

func (hval HatValue) IsSparseBitset() bool {
	return hval.Is(DATAVALUE_TYPE_SPARSE_BITSET)
}

func (hval HatValue) IsReservoirSample() bool {
	return hval.Is(DATAVALUE_TYPE_RESERVOIR_SAMPLE)
}

func (hval HatValue) IsXorFilter() bool {
	return hval.Is(DATAVALUE_TYPE_XOR_FILTER)
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
	case DATAVALUE_TYPE_SET:
		return "set at index: " + strconv.FormatInt(int64(hval.Index), 10)
	case DATAVALUE_TYPE_PRIORITY_QUEUE:
		return "priority queue at index: " + strconv.FormatInt(int64(hval.Index), 10)
	case DATAVALUE_TYPE_BLOOM_FILTER:
		return "bloom filter at index: " + strconv.FormatInt(int64(hval.Index), 10)
	case DATAVALUE_TYPE_COUNT_MIN_SKETCH:
		return "count-min sketch at index: " + strconv.FormatInt(int64(hval.Index), 10)
	case DATAVALUE_TYPE_HYPERLOGLOG:
		return "hyperloglog at index: " + strconv.FormatInt(int64(hval.Index), 10)
	case DATAVALUE_TYPE_TOP_K:
		return "top-k at index: " + strconv.FormatInt(int64(hval.Index), 10)
	case DATAVALUE_TYPE_CUCKOO_FILTER:
		return "cuckoo filter at index: " + strconv.FormatInt(int64(hval.Index), 10)
	case DATAVALUE_TYPE_ROARING_BITMAP:
		return "roaring bitmap at index: " + strconv.FormatInt(int64(hval.Index), 10)
	case DATAVALUE_TYPE_QUANTILE_SKETCH:
		return "quantile sketch at index: " + strconv.FormatInt(int64(hval.Index), 10)
	case DATAVALUE_TYPE_FENWICK_TREE:
		return "fenwick tree at index: " + strconv.FormatInt(int64(hval.Index), 10)
	case DATAVALUE_TYPE_SPARSE_BITSET:
		return "sparse bitset at index: " + strconv.FormatInt(int64(hval.Index), 10)
	case DATAVALUE_TYPE_RESERVOIR_SAMPLE:
		return "reservoir sample at index: " + strconv.FormatInt(int64(hval.Index), 10)
	case DATAVALUE_TYPE_XOR_FILTER:
		return "xor filter at index: " + strconv.FormatInt(int64(hval.Index), 10)
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
	reusables reusableIndexes
}

func CreateBytesStorage() *BytesStorage {
	return &BytesStorage{
		array: [][]byte{},
	}
}

func (bs *BytesStorage) Put(idx int32, value []byte) {
	if idx < 0 || int(idx) >= len(bs.array) {
		return
	}
	bs.array[idx] = cloneBytes(value)
	bs.reusables.Use(idx)
}

func (bs *BytesStorage) putOwned(idx int32, value []byte) {
	if idx < 0 || int(idx) >= len(bs.array) {
		return
	}
	bs.array[idx] = value
	bs.reusables.Use(idx)
}

func (bs *BytesStorage) Append(value []byte) int32 {
	bs.array = append(bs.array, cloneBytes(value))
	return int32(len(bs.array) - 1)
}

func (bs *BytesStorage) appendOwned(value []byte) int32 {
	bs.array = append(bs.array, value)
	return int32(len(bs.array) - 1)
}

func (bs *BytesStorage) Add(value []byte) int32 {
	if idx, ok := bs.reusables.Take(); ok {
		bs.array[idx] = cloneBytes(value)
		return idx
	}
	return bs.Append(value)
}

func (bs *BytesStorage) addOwned(value []byte) int32 {
	if idx, ok := bs.reusables.Take(); ok {
		bs.array[idx] = value
		return idx
	}
	return bs.appendOwned(value)
}

func (bs *BytesStorage) Del(idx int32) {
	if idx < 0 || int(idx) >= len(bs.array) {
		return
	}
	bs.array[idx] = nil
	bs.reusables.Mark(idx)
	bs.array = trimReusableTail(bs.array, &bs.reusables)
}

// DiskStorage stores large byte values outside the Go heap.
type DiskStorage struct {
	dir       string
	ownedDir  bool
	paths     []string
	reusables reusableIndexes
}

func CreateDiskStorage(dir string, ownedDir bool) (*DiskStorage, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return &DiskStorage{
		dir:      dir,
		ownedDir: ownedDir,
		paths:    []string{},
	}, nil
}

func (ds *DiskStorage) Put(idx int32, value []byte) error {
	if idx < 0 || int(idx) >= len(ds.paths) {
		return nil
	}
	if err := writeFileAtomic(ds.ensurePath(idx), value); err != nil {
		return err
	}
	ds.reusables.Use(idx)
	return nil
}

func (ds *DiskStorage) Append(value []byte) (int32, error) {
	idx := int32(len(ds.paths))
	path := ds.pathFor(idx)
	if err := writeFileAtomic(path, value); err != nil {
		return 0, err
	}
	ds.paths = append(ds.paths, path)
	return idx, nil
}

func (ds *DiskStorage) Add(value []byte) (int32, error) {
	if idx, ok := ds.reusables.Take(); ok {
		if err := writeFileAtomic(ds.ensurePath(idx), value); err != nil {
			ds.reusables.Mark(idx)
			return 0, err
		}
		return idx, nil
	}
	return ds.Append(value)
}

func (ds *DiskStorage) Get(idx int32) ([]byte, error) {
	if idx < 0 || int(idx) >= len(ds.paths) {
		return nil, nil
	}
	if ds.paths[idx] == "" {
		return nil, nil
	}
	return os.ReadFile(ds.paths[idx])
}

func (ds *DiskStorage) Del(idx int32) {
	if idx < 0 || int(idx) >= len(ds.paths) {
		return
	}
	if ds.paths[idx] != "" {
		_ = os.Remove(ds.paths[idx])
		ds.paths[idx] = ""
	}
	ds.reusables.Mark(idx)
	ds.paths = trimReusableTail(ds.paths, &ds.reusables)
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
		if path != "" {
			_ = os.Remove(path)
		}
	}
}

func (ds *DiskStorage) ensurePath(idx int32) string {
	if ds.paths[idx] == "" {
		ds.paths[idx] = ds.pathFor(idx)
	}
	return ds.paths[idx]
}

func (ds *DiskStorage) pathFor(idx int32) string {
	return filepath.Join(ds.dir, "bytes-"+strconv.FormatInt(int64(idx), 10)+".bin")
}

// MapStorage stores map values outside the trie.
type MapStorage struct {
	array     []Map
	deleted   []int
	reusables reusableIndexes
}

func CreateMapStorage() *MapStorage {
	return &MapStorage{
		array: []Map{},
	}
}

func (ms *MapStorage) Put(idx int32, value Map) {
	if idx < 0 || int(idx) >= len(ms.array) {
		return
	}
	ms.array[idx] = cloneMap(value)
	ms.deleted[idx] = 0
	ms.reusables.Use(idx)
}

func (ms *MapStorage) PutEntry(idx int32, subkey string, value interface{}) {
	if idx < 0 || int(idx) >= len(ms.array) {
		return
	}
	if ms.array[idx] == nil {
		ms.array[idx] = Map{}
		ms.deleted[idx] = 0
	}
	ms.array[idx][subkey] = cloneValue(value)
	ms.reusables.Use(idx)
}

func (ms *MapStorage) PutEntries(idx int32, fields Map) {
	if idx < 0 || int(idx) >= len(ms.array) || len(fields) == 0 {
		return
	}
	if ms.array[idx] == nil {
		ms.array[idx] = make(Map, len(fields))
		ms.deleted[idx] = 0
	}
	for subkey, value := range fields {
		ms.array[idx][subkey] = cloneValue(value)
	}
	ms.reusables.Use(idx)
}

func (ms *MapStorage) Append(value Map) int32 {
	ms.array = append(ms.array, cloneMap(value))
	ms.deleted = append(ms.deleted, 0)
	return int32(len(ms.array) - 1)
}

func (ms *MapStorage) AppendEntry(subkey string, value interface{}) int32 {
	ms.array = append(ms.array, Map{subkey: cloneValue(value)})
	ms.deleted = append(ms.deleted, 0)
	return int32(len(ms.array) - 1)
}

func (ms *MapStorage) Add(value Map) int32 {
	if idx, ok := ms.reusables.Take(); ok {
		ms.array[idx] = cloneMap(value)
		ms.deleted[idx] = 0
		return idx
	}
	return ms.Append(value)
}

func (ms *MapStorage) AddEntry(subkey string, value interface{}) int32 {
	if idx, ok := ms.reusables.Take(); ok {
		ms.array[idx] = Map{subkey: cloneValue(value)}
		ms.deleted[idx] = 0
		return idx
	}
	return ms.AppendEntry(subkey, value)
}

func (ms *MapStorage) TakeEntry(idx int32, subkey string) (interface{}, bool) {
	if idx < 0 || int(idx) >= len(ms.array) {
		return nil, false
	}
	m := ms.array[idx]
	val, exists := m[subkey]
	if !exists {
		return nil, false
	}
	delete(m, subkey)
	ms.deleted[idx]++
	ms.compactIfSparse(idx)
	return val, true
}

func (ms *MapStorage) Del(idx int32) {
	if idx < 0 || int(idx) >= len(ms.array) {
		return
	}
	ms.array[idx] = nil
	ms.deleted[idx] = 0
	ms.reusables.Mark(idx)
	ms.array = trimReusableTail(ms.array, &ms.reusables)
	ms.deleted = ms.deleted[:len(ms.array)]
}

func (ms *MapStorage) compactIfSparse(idx int32) {
	m := ms.array[idx]
	if len(m) == 0 {
		ms.array[idx] = Map{}
		ms.deleted[idx] = 0
		return
	}
	if ms.deleted[idx] < 32 || ms.deleted[idx] < len(m) {
		return
	}
	next := make(Map, len(m))
	for key, value := range m {
		next[key] = value
	}
	ms.array[idx] = next
	ms.deleted[idx] = 0
}

// SliceStorage stores slice values outside the trie.
type SliceStorage struct {
	array     []deque
	reusables reusableIndexes
}

func CreateSliceStorage() *SliceStorage {
	return &SliceStorage{
		array: []deque{},
	}
}

func (ss *SliceStorage) Put(idx int32, value Slice) {
	if idx < 0 || int(idx) >= len(ss.array) {
		return
	}
	ss.array[idx] = newDeque(value)
	ss.reusables.Use(idx)
}

func (ss *SliceStorage) Append(value Slice) int32 {
	ss.array = append(ss.array, newDeque(value))
	return int32(len(ss.array) - 1)
}

func (ss *SliceStorage) AppendValues(value interface{}, values ...interface{}) int32 {
	dq := deque{}
	dq.PushOne(value, values...)
	ss.array = append(ss.array, dq)
	return int32(len(ss.array) - 1)
}

func (ss *SliceStorage) Add(value Slice) int32 {
	if idx, ok := ss.reusables.Take(); ok {
		ss.array[idx] = newDeque(value)
		return idx
	}
	return ss.Append(value)
}

func (ss *SliceStorage) AddValues(value interface{}, values ...interface{}) int32 {
	if idx, ok := ss.reusables.Take(); ok {
		dq := deque{}
		dq.PushOne(value, values...)
		ss.array[idx] = dq
		return idx
	}
	return ss.AppendValues(value, values...)
}

func (ss *SliceStorage) Del(idx int32) {
	if idx < 0 || int(idx) >= len(ss.array) {
		return
	}
	ss.array[idx] = deque{}
	ss.reusables.Mark(idx)
	ss.array = trimReusableTail(ss.array, &ss.reusables)
}

type setData struct {
	items   map[string]interface{}
	deleted int
}

func newSetData(values Set) setData {
	out := setData{}
	out.Add(values...)
	return out
}

func newSetDataValues(value interface{}, values ...interface{}) setData {
	out := setData{}
	out.AddOne(value, values...)
	return out
}

func (set *setData) Len() int {
	if set == nil {
		return 0
	}
	return len(set.items)
}

func (set *setData) Add(values ...interface{}) int {
	if len(values) == 0 {
		return 0
	}
	set.ensureCapacity(len(values))
	added := 0
	for _, value := range values {
		added += set.addValue(value)
	}
	return added
}

func (set *setData) AddOne(value interface{}, values ...interface{}) int {
	set.ensureCapacity(1 + len(values))
	added := set.addValue(value)
	for _, value := range values {
		added += set.addValue(value)
	}
	return added
}

func (set *setData) Remove(values ...interface{}) int {
	if set == nil || set.items == nil {
		return 0
	}
	removed := 0
	for _, value := range values {
		removed += set.removeValue(value)
	}
	return removed
}

func (set *setData) RemoveOne(value interface{}, values ...interface{}) int {
	if set == nil || set.items == nil {
		return 0
	}
	removed := set.removeValue(value)
	for _, value := range values {
		removed += set.removeValue(value)
	}
	return removed
}

func (set *setData) ensureCapacity(capacity int) {
	if set.items == nil {
		set.items = make(map[string]interface{}, capacity)
		set.deleted = 0
	}
}

func (set *setData) addValue(value interface{}) int {
	key := mustSetItemKey(value)
	if _, exists := set.items[key]; exists {
		return 0
	}
	set.items[key] = cloneValue(value)
	return 1
}

func (set *setData) removeValue(value interface{}) int {
	key := mustSetItemKey(value)
	if _, exists := set.items[key]; !exists {
		return 0
	}
	delete(set.items, key)
	set.deleted++
	set.compactIfSparse()
	return 1
}

func (set *setData) Has(value interface{}) bool {
	if set == nil || set.items == nil {
		return false
	}
	_, ok := set.items[mustSetItemKey(value)]
	return ok
}

func (set *setData) Values() Set {
	if set == nil {
		return nil
	}
	if len(set.items) == 0 {
		return make(Set, 0)
	}
	keys := make([]string, 0, len(set.items))
	for key := range set.items {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make(Set, len(keys))
	for idx, key := range keys {
		out[idx] = cloneValue(set.items[key])
	}
	return out
}

func (set *setData) compactIfSparse() {
	if set.items == nil {
		return
	}
	if len(set.items) == 0 {
		set.items = nil
		set.deleted = 0
		return
	}
	if set.deleted < 32 || set.deleted < len(set.items) {
		return
	}
	next := make(map[string]interface{}, len(set.items))
	for key, value := range set.items {
		next[key] = value
	}
	set.items = next
	set.deleted = 0
}

// SetStorage stores set values outside the trie.
type SetStorage struct {
	array     []setData
	reusables reusableIndexes
}

func CreateSetStorage() *SetStorage {
	return &SetStorage{
		array: []setData{},
	}
}

func (ss *SetStorage) Put(idx int32, value Set) {
	if idx < 0 || int(idx) >= len(ss.array) {
		return
	}
	ss.array[idx] = newSetData(value)
	ss.reusables.Use(idx)
}

func (ss *SetStorage) Append(value Set) int32 {
	ss.array = append(ss.array, newSetData(value))
	return int32(len(ss.array) - 1)
}

func (ss *SetStorage) AppendValues(value interface{}, values ...interface{}) int32 {
	ss.array = append(ss.array, newSetDataValues(value, values...))
	return int32(len(ss.array) - 1)
}

func (ss *SetStorage) Add(value Set) int32 {
	if idx, ok := ss.reusables.Take(); ok {
		ss.array[idx] = newSetData(value)
		return idx
	}
	return ss.Append(value)
}

func (ss *SetStorage) AddValues(value interface{}, values ...interface{}) int32 {
	if idx, ok := ss.reusables.Take(); ok {
		ss.array[idx] = newSetDataValues(value, values...)
		return idx
	}
	return ss.AppendValues(value, values...)
}

func (ss *SetStorage) Del(idx int32) {
	if idx < 0 || int(idx) >= len(ss.array) {
		return
	}
	ss.array[idx] = setData{}
	ss.reusables.Mark(idx)
	ss.array = trimReusableTail(ss.array, &ss.reusables)
}

type LevelDBReference struct {
	Key   string
	Type  string
	Store *LevelDBStore
}

type LevelDBReferenceStorage struct {
	array     []LevelDBReference
	reusables reusableIndexes
}

func CreateLevelDBReferenceStorage() *LevelDBReferenceStorage {
	return &LevelDBReferenceStorage{
		array: []LevelDBReference{},
	}
}

func (rs *LevelDBReferenceStorage) Append(value LevelDBReference) int32 {
	rs.array = append(rs.array, value)
	return int32(len(rs.array) - 1)
}

func (rs *LevelDBReferenceStorage) Add(value LevelDBReference) int32 {
	if idx, ok := rs.reusables.Take(); ok {
		rs.array[idx] = value
		return idx
	}
	return rs.Append(value)
}

func (rs *LevelDBReferenceStorage) Get(idx int32) (LevelDBReference, bool) {
	if rs == nil || idx < 0 || int(idx) >= len(rs.array) {
		return LevelDBReference{}, false
	}
	return rs.array[idx], !rs.reusables.Has(idx)
}

func (rs *LevelDBReferenceStorage) Del(idx int32) {
	if rs == nil || idx < 0 || int(idx) >= len(rs.array) {
		return
	}
	rs.array[idx] = LevelDBReference{}
	rs.reusables.Mark(idx)
	rs.array = trimReusableTail(rs.array, &rs.reusables)
}

// HatTrie wraps the C HAT-trie and keeps larger Go values in typed backing
// pools referenced by compact HatValue records.
type HatTrie struct {
	mu               sync.RWMutex
	root             *C.hattrie_t
	raws             *BytesStorage
	disks            *DiskStorage
	maps             *MapStorage
	slices           *SliceStorage
	sets             *SetStorage
	priorityQueues   *PriorityQueueStorage
	bloomFilters     *BloomFilterStorage
	countMinSketches *CountMinSketchStorage
	hyperLogLogs     *HyperLogLogStorage
	topKs            *TopKStorage
	cuckooFilters    *CuckooFilterStorage
	roaringBitmaps   *RoaringBitmapStorage
	quantileSketches *QuantileSketchStorage
	fenwickTrees     *FenwickTreeStorage
	sparseBitsets    *SparseBitsetStorage
	reservoirSamples *ReservoirSampleStorage
	xorFilters       *XorFilterStorage
	dbrefs           *LevelDBReferenceStorage
	expires          map[string]time.Time
	expirations      expirationHeap
	stats            CacheStats
	keyStats         map[string]KeyStats
	now              func() time.Time
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
		root:             C.hattrie_create(),
		raws:             CreateBytesStorage(),
		disks:            disks,
		maps:             CreateMapStorage(),
		slices:           CreateSliceStorage(),
		sets:             CreateSetStorage(),
		priorityQueues:   CreatePriorityQueueStorage(),
		bloomFilters:     CreateBloomFilterStorage(),
		countMinSketches: CreateCountMinSketchStorage(),
		hyperLogLogs:     CreateHyperLogLogStorage(),
		topKs:            CreateTopKStorage(),
		cuckooFilters:    CreateCuckooFilterStorage(),
		roaringBitmaps:   CreateRoaringBitmapStorage(),
		quantileSketches: CreateQuantileSketchStorage(),
		fenwickTrees:     CreateFenwickTreeStorage(),
		sparseBitsets:    CreateSparseBitsetStorage(),
		reservoirSamples: CreateReservoirSampleStorage(),
		xorFilters:       CreateXorFilterStorage(),
		dbrefs:           CreateLevelDBReferenceStorage(),
		expires:          map[string]time.Time{},
		keyStats:         map[string]KeyStats{},
		now:              time.Now,
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
	ht.sets = nil
	ht.priorityQueues = nil
	ht.bloomFilters = nil
	ht.countMinSketches = nil
	ht.hyperLogLogs = nil
	ht.topKs = nil
	ht.cuckooFilters = nil
	ht.roaringBitmaps = nil
	ht.quantileSketches = nil
	ht.fenwickTrees = nil
	ht.sparseBitsets = nil
	ht.reservoirSamples = nil
	ht.xorFilters = nil
	ht.dbrefs = nil
	ht.expires = nil
	ht.expirations.Clear()
	ht.expirations = nil
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
		ht.clearExpirationLocked(key)
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

	ht.clearExpirationLocked(key)
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
		ht.clearExpirationLocked(key)
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

	cstr, keyLen := cKey(key)
	value := C.hattrie_get(ht.root, cstr, keyLen)
	runtime.KeepAlive(key)
	return value
}

func (ht *HatTrie) tryLocation(key string) *C.value_t {
	ht.ensureOpen()

	cstr, keyLen := cKey(key)
	value := C.hattrie_tryget(ht.root, cstr, keyLen)
	runtime.KeepAlive(key)
	return value
}

func cKey(key string) (*C.char, C.size_t) {
	if key == "" {
		return nil, 0
	}
	return (*C.char)(unsafe.Pointer(unsafe.StringData(key))), C.size_t(len(key))
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
	case DATAVALUE_TYPE_SET:
		ht.sets.Del(hval.Index)
	case DATAVALUE_TYPE_PRIORITY_QUEUE:
		ht.priorityQueues.Del(hval.Index)
	case DATAVALUE_TYPE_BLOOM_FILTER:
		ht.bloomFilters.Del(hval.Index)
	case DATAVALUE_TYPE_COUNT_MIN_SKETCH:
		ht.countMinSketches.Del(hval.Index)
	case DATAVALUE_TYPE_HYPERLOGLOG:
		ht.hyperLogLogs.Del(hval.Index)
	case DATAVALUE_TYPE_TOP_K:
		ht.topKs.Del(hval.Index)
	case DATAVALUE_TYPE_CUCKOO_FILTER:
		ht.cuckooFilters.Del(hval.Index)
	case DATAVALUE_TYPE_ROARING_BITMAP:
		ht.roaringBitmaps.Del(hval.Index)
	case DATAVALUE_TYPE_QUANTILE_SKETCH:
		ht.quantileSketches.Del(hval.Index)
	case DATAVALUE_TYPE_FENWICK_TREE:
		ht.fenwickTrees.Del(hval.Index)
	case DATAVALUE_TYPE_SPARSE_BITSET:
		ht.sparseBitsets.Del(hval.Index)
	case DATAVALUE_TYPE_RESERVOIR_SAMPLE:
		ht.reservoirSamples.Del(hval.Index)
	case DATAVALUE_TYPE_XOR_FILTER:
		ht.xorFilters.Del(hval.Index)
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
		ht.clearExpirationLocked(key)
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
		ht.clearExpirationLocked(key)
		return false
	}

	hval := HatValue{}
	hval.fromValue(*rawPtr)
	if ht.expireIfNeededLocked(key, hval) {
		return false
	}
	if !ht.currentTime().Before(at) {
		deleted := ht.deleteKnownLocked(key, hval)
		if deleted {
			ht.recordDeleteLocked(key)
		}
		return deleted
	}

	ht.setExpirationLocked(key, at, rawPtr, hval)
	return true
}

func (ht *HatTrie) setExpirationLocked(key string, at time.Time, rawPtr *C.value_t, hval HatValue) HatValue {
	ht.expires[key] = at
	ht.expirations.Push(expirationEntry{key: key, at: at})
	hval.Flags |= 1 << DATAVALUE_TTL_BIT_SHIFT
	if rawPtr != nil {
		*rawPtr = hval.toValue()
	}
	ht.compactExpirationHeapLocked()
	return hval
}

func (ht *HatTrie) clearExpirationLocked(key string) {
	if _, ok := ht.expires[key]; !ok {
		return
	}
	delete(ht.expires, key)
	ht.compactExpirationHeapLocked()
}

func (ht *HatTrie) compactExpirationHeapLocked() {
	const minHeapEntries = 64
	total := ht.expirations.Len()
	live := len(ht.expires)
	if total == 0 {
		return
	}
	if live == 0 {
		ht.expirations.Clear()
		return
	}
	if total < minHeapEntries || total <= live*4 {
		return
	}
	ht.rebuildExpirationHeapLocked()
}

func (ht *HatTrie) rebuildExpirationHeapLocked() {
	next := make(expirationHeap, 0, len(ht.expires))
	for key, at := range ht.expires {
		next.Push(expirationEntry{key: key, at: at})
	}
	ht.expirations = next
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
		ht.clearExpirationLocked(key)
		return false
	}

	hval := HatValue{}
	hval.fromValue(*rawPtr)
	return ht.deleteKnownLocked(key, hval)
}

func (ht *HatTrie) deleteKnownLocked(key string, hval HatValue) bool {
	cstr, keyLen := cKey(key)

	deleted := C.hattrie_del(ht.root, cstr, keyLen)
	runtime.KeepAlive(key)
	if deleted != 0 {
		return false
	}
	ht.clearExpirationLocked(key)
	delete(ht.keyStats, key)
	ht.returnStorage(hval)
	return true
}

func (ht *HatTrie) vacuumExpiredLocked() int {
	ht.ensureOpen()

	now := ht.currentTime()
	removed := 0
	for {
		entry, ok := ht.expirations.Peek()
		if !ok || now.Before(entry.at) {
			break
		}
		_, _ = ht.expirations.Pop()
		expiresAt, ok := ht.expires[entry.key]
		if !ok || !expiresAt.Equal(entry.at) {
			continue
		}
		if now.Before(expiresAt) {
			continue
		}

		rawPtr := ht.tryLocation(entry.key)
		if rawPtr == nil {
			delete(ht.expires, entry.key)
			continue
		}

		hval := HatValue{}
		hval.fromValue(*rawPtr)
		if ht.deleteKnownLocked(entry.key, hval) {
			ht.recordExpirationLocked(entry.key)
			removed++
		}
	}
	ht.compactExpirationHeapLocked()
	return removed
}

func (ht *HatTrie) entriesWithPrefixLocked(prefix string, sorted bool) []Entry {
	ht.ensureOpen()

	var iter *C.hattrie_iter_t
	if prefix == "" {
		iter = C.hattrie_iter_begin(ht.root, C.bool(sorted))
	} else {
		cprefix, prefixLen := cKey(prefix)
		iter = C.hattrie_iter_with_prefix(ht.root, C.bool(sorted), cprefix, prefixLen)
		runtime.KeepAlive(prefix)
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
		ht.clearExpirationLocked(key)
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
		ht.clearExpirationLocked(key)
	}
	return hval
}

// HydrateLevelDBReferences materializes all lazy LevelDB-backed values into the
// trie. Use it before closing a LevelDBStore when a hot-loaded trie must keep
// serving cold values without the store handle.
func (ht *HatTrie) HydrateLevelDBReferences() (int, error) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	ht.ensureOpen()
	entries := ht.entriesWithPrefixLocked("", true)
	hydrated := 0
	for _, entry := range entries {
		if !entry.Value.IsLevelDBReference() {
			continue
		}
		hval, err := ht.hydrateLevelDBReferenceLocked(entry.Key, entry.Value)
		if err != nil {
			return hydrated, err
		}
		if !hval.Empty() {
			hydrated++
		}
	}
	return hydrated, nil
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
	ht.clearExpirationLocked(key)
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
		ht.clearExpirationLocked(key)
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
		ht.raws.putOwned(hval.Index, []byte(val))
		ht.clearExpirationLocked(key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.raws.addOwned([]byte(val))
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
		ht.raws.putOwned(hval.Index, next)
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.raws.addOwned([]byte(str))
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
		ht.raws.putOwned(hval.Index, next)
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.raws.addOwned([]byte(str))
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
		ht.clearExpirationLocked(key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		ht.storeBytesLocked(rawPtr, hval, val)
		ht.recordWriteLocked(key)
		return
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
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
		ht.clearExpirationLocked(key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
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
		ht.maps.PutEntry(hval.Index, subkey, val)
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.maps.AddEntry(subkey, val)
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
	return cloneValue(val)
}

func (ht *HatTrie) TakeMap(key, subkey string) interface{} {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	if !hval.IsMap() {
		ht.recordReadLocked(false, key)
		return nil
	}
	val, exists := ht.maps.TakeEntry(hval.Index, subkey)
	ht.recordReadLocked(exists, key)
	if exists {
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
		ht.clearExpirationLocked(key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.slices.Add(val)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_SLICE}.toValue()
	ht.recordWriteLocked(key)
}

func (ht *HatTrie) PushSlice(key string, val interface{}, vals ...interface{}) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertFreshLocation(key)
	if hval.IsSlice() {
		ht.slices.array[hval.Index].PushOne(val, vals...)
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.slices.AddValues(val, vals...)
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
		return cloneValue(val)
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
		return cloneValue(val)
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

func (ht *HatTrie) UpsertSet(key string, val Set) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertFreshLocation(key)
	if hval.IsSet() {
		ht.sets.Put(hval.Index, val)
		ht.clearExpirationLocked(key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.sets.Add(val)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_SET}.toValue()
	ht.recordWriteLocked(key)
}

func (ht *HatTrie) AddSet(key string, val interface{}, vals ...interface{}) int {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertFreshLocation(key)
	if hval.IsSet() {
		added := ht.sets.array[hval.Index].AddOne(val, vals...)
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return added
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.sets.AddValues(val, vals...)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_SET}.toValue()
	ht.recordWriteLocked(key)
	return ht.sets.array[idx].Len()
}

func (ht *HatTrie) RemoveSet(key string, val interface{}, vals ...interface{}) int {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	if !hval.IsSet() {
		ht.recordReadLocked(false, key)
		return 0
	}

	removed := ht.sets.array[hval.Index].RemoveOne(val, vals...)
	ht.recordReadLocked(removed > 0, key)
	if removed > 0 {
		ht.recordWriteLocked(key)
	}
	return removed
}

func (ht *HatTrie) HasSet(key string, val interface{}) bool {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	if !hval.IsSet() {
		ht.recordReadLocked(false, key)
		return false
	}
	hit := ht.sets.array[hval.Index].Has(val)
	ht.recordReadLocked(hit, key)
	return hit
}

func (ht *HatTrie) GetSet(key string) Set {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	if !hval.IsSet() {
		ht.recordReadLocked(false, key)
		return nil
	}
	ht.recordReadLocked(true, key)
	return ht.sets.array[hval.Index].Values()
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
		out[key] = cloneValue(val)
	}
	return out
}

func cloneSlice(value Slice) Slice {
	if value == nil {
		return nil
	}
	out := make(Slice, len(value))
	for idx, val := range value {
		out[idx] = cloneValue(val)
	}
	return out
}

func cloneValue(value interface{}) interface{} {
	switch v := value.(type) {
	case []byte:
		return cloneBytes(v)
	case Map:
		return cloneMap(v)
	case Slice:
		return cloneSlice(v)
	case PriorityQueue:
		return clonePriorityQueue(v)
	default:
		return value
	}
}

func setItemKey(value interface{}) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func mustSetItemKey(value interface{}) string {
	key, err := setItemKey(value)
	if err != nil {
		panic(err)
	}
	return key
}
