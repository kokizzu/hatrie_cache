package hatriecache

/*
#cgo CFLAGS: -std=c99 -Wall -Wextra -I${SRCDIR}/luikore__hat-trie/src
#include <stdlib.h>
#include "luikore__hat-trie/src/hat-trie.h"
*/
import "C"

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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

	json "github.com/goccy/go-json"
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
	DATAVALUE_TYPE_RADIX_TREE
)

type Map = map[string]interface{}
type Slice = []interface{}
type Set = []interface{}

type deque struct {
	values []interface{}
	head   int
	size   int
}

var errDequeCapacityTooLarge = errors.New("hatriecache: slice capacity is too large")
var errRawValueCapacityTooLarge = errors.New("hatriecache: raw value capacity is too large")
var errBatchSizeTooLarge = errors.New("hatriecache: batch size is too large")

// ErrNilHatTrie reports a nil trie passed to a persistence API.
var ErrNilHatTrie = errors.New("hatriecache: trie is nil")
var maxRawValueCapacity = int(^uint(0) >> 1)

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
	_ = dq.PushChecked(values...)
}

func (dq *deque) PushChecked(values ...interface{}) error {
	if len(values) == 0 {
		return nil
	}
	needed, ok := checkedDequeNeeded(dq.size, len(values))
	if !ok {
		return errDequeCapacityTooLarge
	}
	if err := dq.ensureCapacityChecked(needed); err != nil {
		return err
	}
	for _, value := range values {
		dq.pushValue(value)
	}
	return nil
}

func (dq *deque) PushOne(value interface{}, values ...interface{}) {
	_ = dq.PushOneChecked(value, values...)
}

func (dq *deque) PushOneChecked(value interface{}, values ...interface{}) error {
	additional, ok := checkedDequeNeeded(1, len(values))
	if !ok {
		return errDequeCapacityTooLarge
	}
	needed, ok := checkedDequeNeeded(dq.size, additional)
	if !ok {
		return errDequeCapacityTooLarge
	}
	if err := dq.ensureCapacityChecked(needed); err != nil {
		return err
	}
	dq.pushValue(value)
	for _, value := range values {
		dq.pushValue(value)
	}
	return nil
}

func (dq *deque) ensureCapacity(needed int) {
	_ = dq.ensureCapacityChecked(needed)
}

func (dq *deque) ensureCapacityChecked(needed int) error {
	if len(dq.values) < needed {
		capacity, ok := grownDequeCapacity(len(dq.values), needed)
		if !ok {
			return errDequeCapacityTooLarge
		}
		dq.resize(capacity)
	}
	return nil
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

func (dq *deque) popRetain() (interface{}, bool) {
	if dq == nil || dq.size == 0 {
		return nil, false
	}
	idx := (dq.head + dq.size - 1) % len(dq.values)
	value := dq.values[idx]
	dq.values[idx] = nil
	dq.size--
	if dq.size == 0 {
		dq.head = 0
		return value, true
	}
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

func grownDequeCapacity(current int, needed int) (int, bool) {
	if current < 0 || needed < 0 {
		return 0, false
	}
	if current >= needed {
		return current, true
	}
	max := int(^uint(0) >> 1)
	if current > max/2 {
		return 0, false
	}
	capacity := current * 2
	if capacity < 4 {
		capacity = 4
	}
	for capacity < needed {
		if capacity > max/2 {
			return 0, false
		}
		capacity *= 2
	}
	return capacity, true
}

func checkedDequeNeeded(size int, additional int) (int, bool) {
	return checkedIntSum(size, additional)
}

func checkedByteCapacity(left int, right int) (int, bool) {
	if left < 0 || right < 0 {
		return 0, false
	}
	if left > maxRawValueCapacity-right {
		return 0, false
	}
	return left + right, true
}

func checkedBatchSize(first int, rest int) (int, bool) {
	return checkedIntSum(first, rest)
}

func checkedIntSum(left int, right int) (int, bool) {
	if left < 0 || right < 0 {
		return 0, false
	}
	max := int(^uint(0) >> 1)
	if left > max-right {
		return 0, false
	}
	return left + right, true
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
	NoTTL               time.Duration = -1
	DiskBytesThreshold                = 64 * 1024
	maxHATTrieKeyLength               = 1<<15 - 1
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

func validateMapValue(value Map) error {
	if _, err := MarshalMapJSON(value); err != nil {
		return fmt.Errorf("hatriecache: unsupported map value: %w", err)
	}
	return nil
}

func validateSliceValue(value Slice) error {
	if _, err := json.Marshal(value); err != nil {
		return fmt.Errorf("hatriecache: unsupported slice value: %w", err)
	}
	return nil
}

func validateSliceValues(value interface{}, values ...interface{}) error {
	capacity, ok := checkedBatchSize(1, len(values))
	if !ok {
		return errBatchSizeTooLarge
	}
	items := make(Slice, 0, capacity)
	items = append(items, value)
	items = append(items, values...)
	return validateSliceValue(items)
}

func validateKey(key string) error {
	if len(key) > maxHATTrieKeyLength {
		return fmt.Errorf("hatriecache: key length %d exceeds maximum %d", len(key), maxHATTrieKeyLength)
	}
	return nil
}

func validKey(key string) bool {
	return len(key) <= maxHATTrieKeyLength
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

func (hval HatValue) IsRadixTree() bool {
	return hval.Is(DATAVALUE_TYPE_RADIX_TREE)
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
	case DATAVALUE_TYPE_RADIX_TREE:
		return "radix tree at index: " + strconv.FormatInt(int64(hval.Index), 10)
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
	array       [][]byte
	strings     []string
	stringValid []bool
	reusables   reusableIndexes
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
	bs.strings[idx] = ""
	bs.stringValid[idx] = false
	bs.reusables.Use(idx)
}

func (bs *BytesStorage) putOwned(idx int32, value []byte) {
	if idx < 0 || int(idx) >= len(bs.array) {
		return
	}
	bs.array[idx] = value
	bs.strings[idx] = ""
	bs.stringValid[idx] = false
	bs.reusables.Use(idx)
}

func (bs *BytesStorage) putStringOwned(idx int32, value string) {
	if idx < 0 || int(idx) >= len(bs.array) {
		return
	}
	bs.array[idx] = []byte(value)
	bs.strings[idx] = value
	bs.stringValid[idx] = true
	bs.reusables.Use(idx)
}

func (bs *BytesStorage) Append(value []byte) int32 {
	bs.array = append(bs.array, cloneBytes(value))
	bs.strings = append(bs.strings, "")
	bs.stringValid = append(bs.stringValid, false)
	return int32(len(bs.array) - 1)
}

func (bs *BytesStorage) appendOwned(value []byte) int32 {
	bs.array = append(bs.array, value)
	bs.strings = append(bs.strings, "")
	bs.stringValid = append(bs.stringValid, false)
	return int32(len(bs.array) - 1)
}

func (bs *BytesStorage) appendStringOwned(value string) int32 {
	bs.array = append(bs.array, []byte(value))
	bs.strings = append(bs.strings, value)
	bs.stringValid = append(bs.stringValid, true)
	return int32(len(bs.array) - 1)
}

func (bs *BytesStorage) Add(value []byte) int32 {
	if idx, ok := bs.reusables.Take(); ok {
		bs.array[idx] = cloneBytes(value)
		bs.strings[idx] = ""
		bs.stringValid[idx] = false
		return idx
	}
	return bs.Append(value)
}

func (bs *BytesStorage) addOwned(value []byte) int32 {
	if idx, ok := bs.reusables.Take(); ok {
		bs.array[idx] = value
		bs.strings[idx] = ""
		bs.stringValid[idx] = false
		return idx
	}
	return bs.appendOwned(value)
}

func (bs *BytesStorage) addStringOwned(value string) int32 {
	if idx, ok := bs.reusables.Take(); ok {
		bs.array[idx] = []byte(value)
		bs.strings[idx] = value
		bs.stringValid[idx] = true
		return idx
	}
	return bs.appendStringOwned(value)
}

func (bs *BytesStorage) Del(idx int32) {
	if idx < 0 || int(idx) >= len(bs.array) {
		return
	}
	bs.array[idx] = nil
	bs.strings[idx] = ""
	bs.stringValid[idx] = false
	bs.reusables.Mark(idx)
	bs.array = trimReusableTail(bs.array, &bs.reusables)
	bs.strings = bs.strings[:len(bs.array)]
	bs.stringValid = bs.stringValid[:len(bs.array)]
}

func (bs *BytesStorage) stringValue(idx int32) string {
	if idx < 0 || int(idx) >= len(bs.array) {
		return ""
	}
	if bs.stringValid[idx] {
		return bs.strings[idx]
	}
	value := string(bs.array[idx])
	bs.strings[idx] = value
	bs.stringValid[idx] = true
	return value
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
	path, restored := ds.writePath(idx)
	if err := writeFileAtomic(path, value); err != nil {
		return err
	}
	if restored {
		ds.paths[idx] = path
	}
	ds.reusables.Use(idx)
	return nil
}

func (ds *DiskStorage) PutStream(idx int32, write func(io.Writer) error) error {
	if idx < 0 || int(idx) >= len(ds.paths) {
		return nil
	}
	path, restored := ds.writePath(idx)
	if err := writeFileAtomicStream(path, write); err != nil {
		return err
	}
	if restored {
		ds.paths[idx] = path
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

func (ds *DiskStorage) AppendStream(write func(io.Writer) error) (int32, error) {
	idx := int32(len(ds.paths))
	path := ds.pathFor(idx)
	if err := writeFileAtomicStream(path, write); err != nil {
		return 0, err
	}
	ds.paths = append(ds.paths, path)
	return idx, nil
}

func (ds *DiskStorage) Add(value []byte) (int32, error) {
	if idx, ok := ds.reusables.Take(); ok {
		path, restored := ds.writePath(idx)
		if err := writeFileAtomic(path, value); err != nil {
			ds.reusables.Mark(idx)
			return 0, err
		}
		if restored {
			ds.paths[idx] = path
		}
		return idx, nil
	}
	return ds.Append(value)
}

func (ds *DiskStorage) AddStream(write func(io.Writer) error) (int32, error) {
	if idx, ok := ds.reusables.Take(); ok {
		path, restored := ds.writePath(idx)
		if err := writeFileAtomicStream(path, write); err != nil {
			ds.reusables.Mark(idx)
			return 0, err
		}
		if restored {
			ds.paths[idx] = path
		}
		return idx, nil
	}
	return ds.AppendStream(write)
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

func (ds *DiskStorage) open(idx int32) (*os.File, int64, error) {
	if idx < 0 || int(idx) >= len(ds.paths) {
		return nil, 0, nil
	}
	path := ds.paths[idx]
	if path == "" {
		return nil, 0, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, 0, err
	}
	if info.IsDir() {
		_ = file.Close()
		return nil, 0, errors.New("hatriecache: disk bytes path is a directory")
	}
	return file, info.Size(), nil
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
	defer func() {
		ds.paths = nil
		ds.reusables.Compact(0)
	}()
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

func (ds *DiskStorage) writePath(idx int32) (string, bool) {
	if ds.paths[idx] != "" {
		return ds.paths[idx], false
	}
	return ds.pathFor(idx), true
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
	idx, _ := ss.AppendValuesChecked(value, values...)
	return idx
}

func (ss *SliceStorage) AppendValuesChecked(value interface{}, values ...interface{}) (int32, error) {
	dq := deque{}
	if err := dq.PushOneChecked(value, values...); err != nil {
		return -1, err
	}
	ss.array = append(ss.array, dq)
	return int32(len(ss.array) - 1), nil
}

func (ss *SliceStorage) Add(value Slice) int32 {
	if idx, ok := ss.reusables.Take(); ok {
		ss.array[idx] = newDeque(value)
		return idx
	}
	return ss.Append(value)
}

func (ss *SliceStorage) AddValues(value interface{}, values ...interface{}) int32 {
	idx, _ := ss.AddValuesChecked(value, values...)
	return idx
}

func (ss *SliceStorage) AddValuesChecked(value interface{}, values ...interface{}) (int32, error) {
	if idx, ok := ss.reusables.Take(); ok {
		dq := deque{}
		if err := dq.PushOneChecked(value, values...); err != nil {
			ss.reusables.Mark(idx)
			return -1, err
		}
		ss.array[idx] = dq
		return idx, nil
	}
	return ss.AppendValuesChecked(value, values...)
}

func (ss *SliceStorage) Del(idx int32) {
	if idx < 0 || int(idx) >= len(ss.array) {
		return
	}
	ss.array[idx] = deque{}
	ss.reusables.Mark(idx)
	ss.array = trimReusableTail(ss.array, &ss.reusables)
}

const smallSetEntryLimit = 2

type setData struct {
	items   map[string]interface{}
	small   []setSmallEntry
	deleted int
}

type setSmallEntry struct {
	key   string
	value interface{}
}

func newSetData(values Set) setData {
	out, _ := newSetDataChecked(values)
	return out
}

func newSetDataChecked(values Set) (setData, error) {
	out := setData{}
	if _, err := out.AddChecked(values...); err != nil {
		return setData{}, err
	}
	return out, nil
}

func newSetDataValues(value interface{}, values ...interface{}) setData {
	out, _ := newSetDataValuesChecked(value, values...)
	return out
}

func newSetDataValuesChecked(value interface{}, values ...interface{}) (setData, error) {
	out := setData{}
	if _, err := out.AddOneChecked(value, values...); err != nil {
		return setData{}, err
	}
	return out, nil
}

func (set *setData) Len() int {
	if set == nil {
		return 0
	}
	if set.items == nil {
		return len(set.small)
	}
	return len(set.items)
}

func (set *setData) Add(values ...interface{}) int {
	added, _ := set.AddChecked(values...)
	return added
}

func (set *setData) AddChecked(values ...interface{}) (int, error) {
	if len(values) == 0 {
		return 0, nil
	}
	keys, err := setItemKeys(values...)
	if err != nil {
		return 0, err
	}
	return set.addValuesWithKeys(keys, values), nil
}

func (set *setData) AddOne(value interface{}, values ...interface{}) int {
	added, _ := set.AddOneChecked(value, values...)
	return added
}

func (set *setData) AddOneChecked(value interface{}, values ...interface{}) (int, error) {
	keys, err := setItemKeysOne(value, values...)
	if err != nil {
		return 0, err
	}
	return set.addOneWithKeys(keys, value, values...), nil
}

func (set *setData) Remove(values ...interface{}) int {
	removed, _ := set.RemoveChecked(values...)
	return removed
}

func (set *setData) RemoveChecked(values ...interface{}) (int, error) {
	if len(values) == 0 {
		return 0, nil
	}
	keys, err := setItemKeys(values...)
	if err != nil {
		return 0, err
	}
	if set == nil || (set.items == nil && len(set.small) == 0) {
		return 0, nil
	}
	return set.removeKeys(keys), nil
}

func (set *setData) RemoveOne(value interface{}, values ...interface{}) int {
	removed, _ := set.RemoveOneChecked(value, values...)
	return removed
}

func (set *setData) RemoveOneChecked(value interface{}, values ...interface{}) (int, error) {
	keys, err := setItemKeysOne(value, values...)
	if err != nil {
		return 0, err
	}
	if set == nil || (set.items == nil && len(set.small) == 0) {
		return 0, nil
	}
	return set.removeKeys(keys), nil
}

func (set *setData) ensureCapacity(capacity int) {
	if set.items != nil {
		return
	}
	if capacity > smallSetEntryLimit {
		set.promoteSmall(capacity)
		return
	}
	if set.small == nil && capacity > 0 {
		set.small = make([]setSmallEntry, 0, capacity)
	}
}

func (set *setData) addValuesWithKeys(keys []string, values []interface{}) int {
	set.ensureCapacity(len(set.small) + len(values))
	added := 0
	for idx, value := range values {
		added += set.addKeyValue(keys[idx], value)
	}
	return added
}

func (set *setData) addOneWithKeys(keys []string, value interface{}, values ...interface{}) int {
	set.ensureCapacity(len(set.small) + len(keys))
	added := set.addKeyValue(keys[0], value)
	for idx, value := range values {
		added += set.addKeyValue(keys[idx+1], value)
	}
	return added
}

func (set *setData) addKeyValue(key string, value interface{}) int {
	if set.items == nil {
		if set.smallIndexByKey(key) >= 0 {
			return 0
		}
		if len(set.small) < smallSetEntryLimit {
			set.small = append(set.small, setSmallEntry{key: key, value: cloneValue(value)})
			return 1
		}
		set.promoteSmall(len(set.small) + 1)
	}
	if _, exists := set.items[key]; exists {
		return 0
	}
	set.items[key] = cloneValue(value)
	return 1
}

func (set *setData) addPlainString(value string) int {
	if set.items == nil {
		if set.smallIndexByPlainString(value) >= 0 {
			return 0
		}
		if len(set.small) < smallSetEntryLimit {
			set.small = append(set.small, setSmallEntry{value: value})
			return 1
		}
		set.promoteSmall(len(set.small) + 1)
	}
	key := jsonPlainStringKey(value)
	if _, exists := set.items[key]; exists {
		return 0
	}
	set.items[key] = value
	return 1
}

func (set *setData) removeKeys(keys []string) int {
	removed := 0
	for _, key := range keys {
		removed += set.removeKey(key)
	}
	return removed
}

func (set *setData) removeKey(key string) int {
	if set.items == nil {
		idx := set.smallIndexByKey(key)
		if idx < 0 {
			return 0
		}
		copy(set.small[idx:], set.small[idx+1:])
		var zero setSmallEntry
		set.small[len(set.small)-1] = zero
		set.small = set.small[:len(set.small)-1]
		return 1
	}
	if _, exists := set.items[key]; !exists {
		return 0
	}
	delete(set.items, key)
	set.deleted++
	set.compactIfSparse()
	return 1
}

func (set *setData) Has(value interface{}) bool {
	ok, _ := set.HasChecked(value)
	return ok
}

func (set *setData) HasChecked(value interface{}) (bool, error) {
	key, err := setItemKey(value)
	if err != nil {
		return false, err
	}
	if set == nil || (set.items == nil && len(set.small) == 0) {
		return false, nil
	}
	return set.hasKey(key), nil
}

func (set *setData) hasPlainString(value string) bool {
	if set == nil {
		return false
	}
	if set.items == nil {
		return set.smallIndexByPlainString(value) >= 0
	}
	_, ok := set.items[jsonPlainStringKey(value)]
	return ok
}

func (set *setData) hasKey(key string) bool {
	if set == nil {
		return false
	}
	if set.items == nil {
		return set.smallIndexByKey(key) >= 0
	}
	_, ok := set.items[key]
	return ok
}

func (set *setData) Values() Set {
	if set == nil {
		return nil
	}
	if set.items == nil {
		if len(set.small) == 0 {
			return make(Set, 0)
		}
		if len(set.small) == 1 {
			return Set{cloneValue(set.small[0].value)}
		}
		if len(set.small) == 2 {
			first, second := set.small[0], set.small[1]
			if setSmallEntryLess(second, first) {
				first, second = second, first
			}
			return Set{cloneValue(first.value), cloneValue(second.value)}
		}
		keys := make([]string, len(set.small))
		for idx := range set.small {
			keys[idx] = set.small[idx].jsonKey()
		}
		sort.Strings(keys)
		out := make(Set, len(keys))
		for idx, key := range keys {
			entryIdx := set.smallIndexByKey(key)
			if entryIdx >= 0 {
				out[idx] = cloneValue(set.small[entryIdx].value)
			}
		}
		return out
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

func setSmallEntryLess(left setSmallEntry, right setSmallEntry) bool {
	leftValue, leftPlain := left.value.(string)
	rightValue, rightPlain := right.value.(string)
	if left.key == "" && right.key == "" && leftPlain && rightPlain {
		return leftValue < rightValue
	}
	return left.jsonKey() < right.jsonKey()
}

func (set *setData) smallIndexByKey(key string) int {
	if set == nil {
		return -1
	}
	for idx := range set.small {
		if set.small[idx].matchesKey(key) {
			return idx
		}
	}
	return -1
}

func (set *setData) smallIndexByPlainString(value string) int {
	if set == nil {
		return -1
	}
	for idx := range set.small {
		if set.small[idx].matchesPlainString(value) {
			return idx
		}
	}
	return -1
}

func (set *setData) promoteSmall(capacity int) {
	if set == nil || set.items != nil {
		return
	}
	if capacity < len(set.small) {
		capacity = len(set.small)
	}
	set.items = make(map[string]interface{}, capacity)
	for _, entry := range set.small {
		set.items[entry.jsonKey()] = entry.value
	}
	set.small = nil
	set.deleted = 0
}

func (entry setSmallEntry) jsonKey() string {
	if entry.key != "" {
		return entry.key
	}
	if value, ok := entry.value.(string); ok {
		return jsonPlainStringKey(value)
	}
	data, err := setItemKey(entry.value)
	if err != nil {
		return ""
	}
	return data
}

func (entry setSmallEntry) matchesKey(key string) bool {
	if entry.key != "" {
		return entry.key == key
	}
	value, ok := entry.value.(string)
	return ok && jsonPlainStringMatchesKey(value, key)
}

func (entry setSmallEntry) matchesPlainString(value string) bool {
	if entry.key != "" {
		return jsonPlainStringMatchesKey(value, entry.key)
	}
	stored, ok := entry.value.(string)
	return ok && stored == value
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

func (ss *SetStorage) PutData(idx int32, value setData) {
	if idx < 0 || int(idx) >= len(ss.array) {
		return
	}
	ss.array[idx] = value
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

func (ss *SetStorage) AppendData(value setData) int32 {
	ss.array = append(ss.array, value)
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

func (ss *SetStorage) AddData(value setData) int32 {
	if idx, ok := ss.reusables.Take(); ok {
		ss.array[idx] = value
		return idx
	}
	return ss.AppendData(value)
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
	Key            string
	Type           string
	Store          *LevelDBStore
	ExpiresAt      *time.Time
	Stats          *KeyStats
	RecordBytes    int
	RecordChecksum uint64
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

func (rs *LevelDBReferenceStorage) Set(idx int32, value LevelDBReference) bool {
	if rs == nil || idx < 0 || int(idx) >= len(rs.array) || rs.reusables.Has(idx) {
		return false
	}
	rs.array[idx] = value
	return true
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
	radixTrees       *RadixTreeStorage
	dbrefs           *LevelDBReferenceStorage
	expires          map[string]time.Time
	expirations      expirationHeap
	hotKey           string
	hotValue         HatValue
	hotValid         bool
	stats            CacheStats
	keyStats         map[string]*KeyStats
	levelDBSpillKeys map[string]struct{}
	levelDBHotBytes  int64
	levelDBHotValues map[string]int64
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
		radixTrees:       CreateRadixTreeStorage(),
		dbrefs:           CreateLevelDBReferenceStorage(),
		expires:          map[string]time.Time{},
		keyStats:         map[string]*KeyStats{},
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
	ht.radixTrees = nil
	ht.dbrefs = nil
	ht.expires = nil
	ht.expirations.Clear()
	ht.expirations = nil
	ht.hotKey = ""
	ht.hotValue = HatValue{}
	ht.hotValid = false
	ht.keyStats = nil
	ht.levelDBSpillKeys = nil
	ht.levelDBHotBytes = 0
	ht.levelDBHotValues = nil
	ht.now = nil
}

func (ht *HatTrie) Size() int {
	if ht == nil {
		return 0
	}
	ht.mu.RLock()
	defer ht.mu.RUnlock()

	ht.ensureOpen()
	return int(C.hattrie_size(ht.root))
}

func (ht *HatTrie) Stats() CacheStats {
	if ht == nil {
		return CacheStats{}
	}
	ht.mu.RLock()
	defer ht.mu.RUnlock()

	ht.ensureOpen()
	stats := ht.stats
	stats.updateRates()
	return stats
}

// StatsForKey returns access metadata for an existing key.
func (ht *HatTrie) StatsForKey(key string) (KeyStats, bool) {
	stats, ok, _ := ht.StatsForKeyChecked(key)
	return stats, ok
}

// StatsForKeyChecked returns access metadata for an existing key and reports
// key validation errors.
func (ht *HatTrie) StatsForKeyChecked(key string) (KeyStats, bool, error) {
	if ht == nil {
		return KeyStats{}, false, ErrNilHatTrie
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	if err := validateKey(key); err != nil {
		return KeyStats{}, false, err
	}
	ht.ensureOpen()
	rawPtr := ht.tryLocation(key)
	if rawPtr == nil {
		delete(ht.keyStats, key)
		return KeyStats{}, false, nil
	}
	hval := HatValue{}
	hval.fromValue(*rawPtr)
	if ht.expireIfNeededLocked(key, hval) {
		return KeyStats{}, false, nil
	}
	stats, ok := ht.keyStats[key]
	if ok && stats != nil {
		out := *stats
		out.updateRates()
		return out, true, nil
	}
	return KeyStats{}, false, nil
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
	ht.keyStats[key] = &restored
}

func (ht *HatTrie) SaveStats(path string) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	return writeJSONFileAtomic(path, ht.Stats())
}

func (ht *HatTrie) LoadStats(path string) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	stats, err := decodeCacheStatsJSONReader(file)
	if err != nil {
		return err
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	ht.ensureOpen()
	ht.stats = stats
	return nil
}

func decodeCacheStatsJSONReader(reader io.Reader) (CacheStats, error) {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	var stats CacheStats
	if err := decoder.Decode(&stats); err != nil {
		return CacheStats{}, err
	}
	var extra struct{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return CacheStats{}, errors.New("hatriecache: invalid stats JSON")
		}
		return CacheStats{}, err
	}
	if err := validateCacheStatsSnapshot(stats); err != nil {
		return CacheStats{}, err
	}
	stats.updateRates()
	return stats, nil
}

// Expire sets a relative TTL for an existing key. A non-positive TTL deletes
// an existing key immediately. It returns false when the key is missing or has
// already expired.
func (ht *HatTrie) Expire(key string, ttl time.Duration) bool {
	ok, _ := ht.ExpireChecked(key, ttl)
	return ok
}

// ExpireChecked sets a relative TTL for an existing key and reports key
// validation errors.
func (ht *HatTrie) ExpireChecked(key string, ttl time.Duration) (bool, error) {
	if ht == nil {
		return false, ErrNilHatTrie
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	if err := validateKey(key); err != nil {
		return false, err
	}
	if ttl <= 0 {
		deleted := ht.deleteLocked(key)
		if deleted {
			ht.recordDeleteLocked(key)
		}
		return deleted, nil
	}
	ok, deleted := ht.expireAtLocked(key, ht.currentTime().Add(ttl))
	if ok {
		if deleted {
			ht.recordDeleteLocked(key)
		} else {
			ht.recordWriteLocked(key)
		}
	}
	return ok, nil
}

// ExpireAt sets an absolute expiration time for an existing key. Expiration is
// enforced when the key is read or mutated.
func (ht *HatTrie) ExpireAt(key string, at time.Time) bool {
	ok, _ := ht.ExpireAtChecked(key, at)
	return ok
}

// ExpireAtChecked sets an absolute expiration time for an existing key and
// reports key validation errors.
func (ht *HatTrie) ExpireAtChecked(key string, at time.Time) (bool, error) {
	if ht == nil {
		return false, ErrNilHatTrie
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	if err := validateKey(key); err != nil {
		return false, err
	}
	ok, deleted := ht.expireAtLocked(key, at)
	if ok {
		if deleted {
			ht.recordDeleteLocked(key)
		} else {
			ht.recordWriteLocked(key)
		}
	}
	return ok, nil
}

// Persist removes an existing key's expiration.
func (ht *HatTrie) Persist(key string) bool {
	ok, _ := ht.PersistChecked(key)
	return ok
}

// PersistChecked removes an existing key's expiration and reports key
// validation errors.
func (ht *HatTrie) PersistChecked(key string) (bool, error) {
	if ht == nil {
		return false, ErrNilHatTrie
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	if err := validateKey(key); err != nil {
		return false, err
	}
	rawPtr := ht.tryLocation(key)
	if rawPtr == nil {
		ht.clearExpirationLocked(key)
		return false, nil
	}

	hval := HatValue{}
	hval.fromValue(*rawPtr)
	if ht.expireIfNeededLocked(key, hval) {
		return false, nil
	}
	if _, ok := ht.expires[key]; !ok {
		return false, nil
	}

	ht.clearExpirationLocked(key)
	hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
	*rawPtr = hval.toValue()
	ht.recordWriteLocked(key)
	return true, nil
}

// TTL returns the remaining TTL for key, or NoTTL when the key is missing,
// expired, or has no expiration.
func (ht *HatTrie) TTL(key string) time.Duration {
	ttl, _ := ht.TTLChecked(key)
	return ttl
}

// TTLChecked returns the remaining TTL for key and reports key validation
// errors.
func (ht *HatTrie) TTLChecked(key string) (time.Duration, error) {
	if ht == nil {
		return NoTTL, ErrNilHatTrie
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	if err := validateKey(key); err != nil {
		return NoTTL, err
	}
	rawPtr := ht.tryLocation(key)
	if rawPtr == nil {
		ht.clearExpirationLocked(key)
		ht.recordReadLocked(false, key)
		return NoTTL, nil
	}

	hval := HatValue{}
	hval.fromValue(*rawPtr)
	if ht.expireIfNeededLocked(key, hval) {
		ht.recordReadLocked(false, key)
		return NoTTL, nil
	}

	expiresAt, ok := ht.expires[key]
	if !ok {
		ht.recordReadLocked(false, key)
		return NoTTL, nil
	}
	ttl := expiresAt.Sub(ht.currentTime())
	if ttl <= 0 {
		if ht.deleteKnownLocked(key, hval) {
			ht.recordExpirationLocked(key)
		}
		ht.recordReadLocked(false, key)
		return NoTTL, nil
	}
	ht.recordReadLocked(true, key)
	return ttl, nil
}

// VacuumExpired removes expired keys immediately and returns the number of
// trie entries removed. It is safe to call concurrently with other operations.
func (ht *HatTrie) VacuumExpired() int {
	if ht == nil {
		return 0
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	return ht.vacuumExpiredLocked()
}

// StartExpirationCleaner starts a background cleaner that periodically removes
// expired keys. The returned stop function is idempotent and waits for the
// cleaner goroutine to exit.
func (ht *HatTrie) StartExpirationCleaner(interval time.Duration) func() {
	return ht.StartExpirationCleanerContext(context.Background(), interval)
}

// StartExpirationCleanerContext starts a background cleaner that periodically
// removes expired keys until stop is called or ctx is canceled. The returned
// stop function is idempotent and waits for the cleaner goroutine to exit.
func (ht *HatTrie) StartExpirationCleanerContext(ctx context.Context, interval time.Duration) func() {
	if interval <= 0 {
		panic("hatriecache: expiration cleaner interval must be positive")
	}
	if ht == nil {
		return func() {}
	}
	if ctx == nil {
		ctx = context.Background()
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
				if !ht.vacuumExpiredIfOpen() {
					return
				}
			case <-ctx.Done():
				return
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

func (ht *HatTrie) vacuumExpiredIfOpen() bool {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	if ht.root == nil {
		return false
	}
	ht.vacuumExpiredLocked()
	return true
}

// VacuumExpiredOnMemoryPressure removes expired keys when current heap
// allocation is at or above maxAllocBytes. It returns the number of removed
// entries, or zero when memory pressure is below the threshold.
func (ht *HatTrie) VacuumExpiredOnMemoryPressure(maxAllocBytes uint64) int {
	if maxAllocBytes == 0 {
		panic("hatriecache: memory pressure threshold must be positive")
	}
	if ht == nil {
		return 0
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
	return ht.StartMemoryPressureVacuumContext(context.Background(), interval, maxAllocBytes)
}

// StartMemoryPressureVacuumContext starts a background cleaner that vacuums
// expired keys only while current heap allocation is at or above maxAllocBytes
// until stop is called or ctx is canceled. The returned stop function is
// idempotent and waits for the goroutine to exit.
func (ht *HatTrie) StartMemoryPressureVacuumContext(ctx context.Context, interval time.Duration, maxAllocBytes uint64) func() {
	if interval <= 0 {
		panic("hatriecache: memory pressure vacuum interval must be positive")
	}
	if maxAllocBytes == 0 {
		panic("hatriecache: memory pressure threshold must be positive")
	}
	if ht == nil {
		return func() {}
	}
	if ctx == nil {
		ctx = context.Background()
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
				if !ht.vacuumExpiredOnMemoryPressureIfOpen(maxAllocBytes) {
					return
				}
			case <-ctx.Done():
				return
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

func (ht *HatTrie) vacuumExpiredOnMemoryPressureIfOpen(maxAllocBytes uint64) bool {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	ht.mu.Lock()
	defer ht.mu.Unlock()

	if ht.root == nil {
		return false
	}
	if mem.Alloc < maxAllocBytes {
		return true
	}
	ht.vacuumExpiredLocked()
	return true
}

// Keys returns all non-expired keys. When sorted is true, keys are returned in
// bytewise lexicographic order.
func (ht *HatTrie) Keys(sorted bool) []string {
	return ht.KeysWithPrefix("", sorted)
}

// KeysWithPrefix returns all non-expired keys that start with prefix. Prefixes
// and keys may contain NUL bytes.
func (ht *HatTrie) KeysWithPrefix(prefix string, sorted bool) []string {
	keys, err := ht.KeysWithPrefixChecked(prefix, sorted)
	if err != nil {
		return []string{}
	}
	return keys
}

// KeysWithPrefixChecked returns all non-expired keys that start with prefix and
// reports prefix validation errors.
func (ht *HatTrie) KeysWithPrefixChecked(prefix string, sorted bool) ([]string, error) {
	if ht == nil {
		return nil, ErrNilHatTrie
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	now := time.Time{}
	if len(ht.expires) > 0 {
		now = ht.currentTime()
	}
	keys := []string{}
	err := ht.scanEntriesWithPrefixAtLockedChecked(prefix, sorted, now, func(entry Entry) error {
		keys = append(keys, entry.Key)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return keys, nil
}

// Entries returns all non-expired key/value metadata pairs. Returned HatValue
// records are copies and remain valid after later trie mutations.
func (ht *HatTrie) Entries(sorted bool) []Entry {
	return ht.EntriesWithPrefix("", sorted)
}

// EntriesWithPrefix returns all non-expired key/value metadata pairs whose keys
// start with prefix.
func (ht *HatTrie) EntriesWithPrefix(prefix string, sorted bool) []Entry {
	entries, err := ht.EntriesWithPrefixChecked(prefix, sorted)
	if err != nil {
		return []Entry{}
	}
	return entries
}

// EntriesWithPrefixChecked returns all non-expired key/value metadata pairs
// whose keys start with prefix and reports prefix validation errors.
func (ht *HatTrie) EntriesWithPrefixChecked(prefix string, sorted bool) ([]Entry, error) {
	if ht == nil {
		return nil, ErrNilHatTrie
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	return ht.entriesWithPrefixLockedChecked(prefix, sorted)
}

// Exists reports whether key exists without hydrating a cold LevelDB value.
func (ht *HatTrie) Exists(key string) bool {
	ok, _ := ht.ExistsChecked(key)
	return ok
}

// ExistsChecked reports whether key exists without hydrating a cold LevelDB
// value and reports key validation errors.
func (ht *HatTrie) ExistsChecked(key string) (bool, error) {
	if ht == nil {
		return false, ErrNilHatTrie
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	if err := validateKey(key); err != nil {
		return false, err
	}
	hval := ht.peekLocked(key)
	hit := !hval.Empty()
	ht.recordReadLocked(hit, key)
	return hit, nil
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

	for _, key := range keys {
		stats, tracked := ht.keyStats[key]
		if !hit && !tracked {
			continue
		}
		if stats == nil {
			stats = &KeyStats{}
			ht.keyStats[key] = stats
		}
		stats.Reads++
		if hit {
			stats.Hits++
			stats.LastHit = now
		} else {
			stats.Misses++
			stats.LastMiss = now
		}
	}
}

func (ht *HatTrie) recordWriteLocked(keys ...string) {
	now := ht.currentTime()
	ht.stats.Writes++
	ht.stats.LastWrite = now

	for _, key := range keys {
		ht.clearHotKeyLocked(key)
		ht.updateLevelDBSpillCandidateForKeyLocked(key)
		ht.updateLevelDBHotByteAccountingForKeyLocked(key)
		stats := ht.keyStats[key]
		if stats == nil {
			stats = &KeyStats{}
			ht.keyStats[key] = stats
		}
		stats.Writes++
		stats.LastWrite = now
	}
}

func (ht *HatTrie) recordDeleteLocked(key string) {
	ht.stats.Deletes++
	ht.recordWriteLocked()
	ht.deleteLevelDBSpillCandidateLocked(key)
	ht.deleteLevelDBHotByteAccountingLocked(key)
	delete(ht.keyStats, key)
}

func (ht *HatTrie) recordExpirationLocked(keys ...string) {
	ht.stats.Expirations++
	ht.recordWriteLocked()
	for _, key := range keys {
		ht.deleteLevelDBSpillCandidateLocked(key)
		ht.deleteLevelDBHotByteAccountingLocked(key)
		delete(ht.keyStats, key)
	}
}

func (ht *HatTrie) cachedValueLocked(key string) (HatValue, bool) {
	if ht.hotValid && ht.hotKey == key {
		return ht.hotValue, true
	}
	return HatValue{}, false
}

func (ht *HatTrie) cacheValueLocked(key string, hval HatValue) {
	if hval.Empty() {
		ht.clearHotKeyLocked(key)
		return
	}
	ht.hotKey = key
	ht.hotValue = hval
	ht.hotValid = true
}

func (ht *HatTrie) clearHotKeyLocked(keys ...string) {
	if !ht.hotValid {
		return
	}
	if len(keys) == 0 {
		ht.hotKey = ""
		ht.hotValue = HatValue{}
		ht.hotValid = false
		return
	}
	for _, key := range keys {
		if ht.hotKey == key {
			ht.hotKey = ""
			ht.hotValue = HatValue{}
			ht.hotValid = false
			return
		}
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

func clonedUpdatedKeyStats(stats *KeyStats) *KeyStats {
	if stats == nil {
		return nil
	}
	cloned := *stats
	cloned.updateRates()
	return &cloned
}

func validateCacheStatsSnapshot(stats CacheStats) error {
	return validateReadStatsSnapshot(stats.Reads, stats.Hits, stats.Misses, "cache stats")
}

func validateKeyStatsSnapshot(stats *KeyStats) error {
	if stats == nil {
		return nil
	}
	return validateReadStatsSnapshot(stats.Reads, stats.Hits, stats.Misses, "key stats")
}

func validateReadStatsSnapshot(reads uint64, hits uint64, misses uint64, label string) error {
	if hits > reads || misses > reads || reads-hits != misses {
		return fmt.Errorf("hatriecache: %s reads must equal hits plus misses", label)
	}
	return nil
}

func (ht *HatTrie) upsertLocation(key string) *C.value_t {
	ht.ensureOpen()
	if !validKey(key) {
		return nil
	}

	cstr, keyLen := cKey(key)
	value := C.hattrie_get(ht.root, cstr, keyLen)
	runtime.KeepAlive(key)
	return value
}

func (ht *HatTrie) tryLocation(key string) *C.value_t {
	ht.ensureOpen()
	if !validKey(key) {
		return nil
	}

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
	case DATAVALUE_TYPE_RADIX_TREE:
		ht.radixTrees.Del(hval.Index)
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

func (ht *HatTrie) upsertReplacementLocation(key string) (*C.value_t, HatValue, error) {
	if err := validateKey(key); err != nil {
		return nil, HatValue{}, err
	}
	rawPtr := ht.upsertLocation(key)
	hval := HatValue{}
	hval.fromValue(*rawPtr)
	if hval.Empty() {
		ht.clearExpirationLocked(key)
		return rawPtr, hval, nil
	}
	if ht.expireIfNeededLocked(key, hval) {
		rawPtr = ht.upsertLocation(key)
		hval = HatValue{}
	}
	return rawPtr, hval, nil
}

func (ht *HatTrie) freshLocationCheckedLocked(key string) (*C.value_t, HatValue, error) {
	if err := validateKey(key); err != nil {
		return nil, HatValue{}, err
	}
	rawPtr := ht.tryLocation(key)
	hval := HatValue{}
	if rawPtr == nil {
		ht.clearExpirationLocked(key)
		return nil, hval, nil
	}

	hval.fromValue(*rawPtr)
	if ht.expireIfNeededLocked(key, hval) {
		return nil, HatValue{}, nil
	}
	if hval.IsLevelDBReference() {
		hydrated, err := ht.hydrateLevelDBReferenceLocked(key, hval)
		if err != nil {
			return nil, HatValue{}, err
		}
		if hydrated.Empty() {
			return nil, HatValue{}, nil
		}
		rawPtr = ht.upsertLocation(key)
		hval = hydrated
	}
	return rawPtr, hval, nil
}

func (ht *HatTrie) expireAtLocked(key string, at time.Time) (bool, bool) {
	rawPtr := ht.tryLocation(key)
	if rawPtr == nil {
		ht.clearExpirationLocked(key)
		return false, false
	}

	hval := HatValue{}
	hval.fromValue(*rawPtr)
	if ht.expireIfNeededLocked(key, hval) {
		return false, false
	}
	if !ht.currentTime().Before(at) {
		deleted := ht.deleteKnownLocked(key, hval)
		return deleted, deleted
	}

	ht.setExpirationLocked(key, at, rawPtr, hval)
	return true, false
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
	if !validKey(key) {
		return false
	}
	rawPtr := ht.tryLocation(key)
	if rawPtr == nil {
		ht.clearExpirationLocked(key)
		ht.clearHotKeyLocked(key)
		return false
	}

	hval := HatValue{}
	hval.fromValue(*rawPtr)
	return ht.deleteKnownLocked(key, hval)
}

func (ht *HatTrie) deleteKnownLocked(key string, hval HatValue) bool {
	if !validKey(key) {
		return false
	}
	cstr, keyLen := cKey(key)

	deleted := C.hattrie_del(ht.root, cstr, keyLen)
	runtime.KeepAlive(key)
	if deleted != 0 {
		return false
	}
	ht.clearHotKeyLocked(key)
	ht.clearExpirationLocked(key)
	ht.deleteLevelDBSpillCandidateLocked(key)
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
	now := time.Time{}
	if len(ht.expires) > 0 {
		now = ht.currentTime()
	}
	return ht.entriesWithPrefixAtLocked(prefix, sorted, now)
}

func (ht *HatTrie) entriesWithPrefixAtLocked(prefix string, sorted bool, now time.Time) []Entry {
	entries, _ := ht.entriesWithPrefixAtLockedChecked(prefix, sorted, now)
	return entries
}

func (ht *HatTrie) entriesWithPrefixLockedChecked(prefix string, sorted bool) ([]Entry, error) {
	now := time.Time{}
	if len(ht.expires) > 0 {
		now = ht.currentTime()
	}
	return ht.entriesWithPrefixAtLockedChecked(prefix, sorted, now)
}

func (ht *HatTrie) entriesWithPrefixAtLockedChecked(prefix string, sorted bool, now time.Time) ([]Entry, error) {
	entries := []Entry{}
	err := ht.scanEntriesWithPrefixAtLockedChecked(prefix, sorted, now, func(entry Entry) error {
		entries = append(entries, entry)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func (ht *HatTrie) scanEntriesWithPrefixAtLockedChecked(prefix string, sorted bool, now time.Time, visit func(Entry) error) error {
	ht.ensureOpen()
	if err := validateKey(prefix); err != nil {
		return err
	}

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
	expired := []expiredEntry{}
	var scanErr error

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

		if expiresAt, ok := ht.expires[key]; ok && !now.Before(expiresAt) {
			expired = append(expired, expiredEntry{key: key, value: hval})
		} else {
			if visit != nil {
				if err := visit(Entry{Key: key, Value: hval}); err != nil {
					scanErr = err
					break
				}
			}
		}
		C.hattrie_iter_next(iter)
	}
	C.hattrie_iter_free(iter)

	for _, entry := range expired {
		if ht.deleteKnownLocked(entry.key, entry.value) {
			ht.recordExpirationLocked(entry.key)
		}
	}

	return scanErr
}

func (ht *HatTrie) Get(key string) HatValue {
	hval, _ := ht.GetChecked(key)
	return hval
}

func (ht *HatTrie) GetChecked(key string) (HatValue, error) {
	if ht == nil {
		return HatValue{}, ErrNilHatTrie
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return HatValue{}, err
	}
	ht.recordReadLocked(!hval.Empty(), key)
	return hval, nil
}

func (ht *HatTrie) peekLocked(key string) HatValue {
	if !validKey(key) {
		return HatValue{}
	}
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

func (ht *HatTrie) peekCachedLocked(key string) HatValue {
	if hval, ok := ht.cachedValueLocked(key); ok {
		if ht.expireIfNeededLocked(key, hval) {
			ht.clearHotKeyLocked(key)
			return HatValue{}
		}
		return hval
	}
	hval := ht.peekLocked(key)
	if !hval.Empty() {
		ht.cacheValueLocked(key, hval)
	}
	return hval
}

func (ht *HatTrie) getLocked(key string) HatValue {
	hval, _ := ht.getLockedChecked(key)
	return hval
}

func (ht *HatTrie) getLockedChecked(key string) (HatValue, error) {
	if err := validateKey(key); err != nil {
		return HatValue{}, err
	}
	if hval, ok := ht.cachedValueLocked(key); ok {
		if ht.expireIfNeededLocked(key, hval) {
			ht.clearHotKeyLocked(key)
			return HatValue{}, nil
		}
		if hval.IsLevelDBReference() {
			hydrated, err := ht.hydrateLevelDBReferenceLocked(key, hval)
			if err != nil {
				return HatValue{}, err
			}
			ht.cacheValueLocked(key, hydrated)
			return hydrated, nil
		}
		return hval, nil
	}
	iter := ht.tryLocation(key)
	hval := HatValue{}
	if iter != nil {
		hval.fromValue(*iter)
		if ht.expireIfNeededLocked(key, hval) {
			ht.clearHotKeyLocked(key)
			return HatValue{}, nil
		}
		if hval.IsLevelDBReference() {
			hydrated, err := ht.hydrateLevelDBReferenceLocked(key, hval)
			if err != nil {
				return HatValue{}, err
			}
			ht.cacheValueLocked(key, hydrated)
			return hydrated, nil
		}
		ht.cacheValueLocked(key, hval)
	} else {
		ht.clearExpirationLocked(key)
		ht.clearHotKeyLocked(key)
	}
	return hval, nil
}

// HydrateLevelDBReferences materializes all lazy LevelDB-backed values into the
// trie. Use it before closing a LevelDBStore when a hot-loaded trie must keep
// serving cold values without the store handle.
func (ht *HatTrie) HydrateLevelDBReferences() (int, error) {
	if ht == nil {
		return 0, ErrNilHatTrie
	}
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
	operation, err := snapshotOperationForEntry(entry)
	if err != nil {
		return HatValue{}, err
	}
	return ht.applySnapshotOperationLocked(operation)
}

// Delete removes key and returns whether it existed.
func (ht *HatTrie) Delete(key string) bool {
	deleted, _ := ht.DeleteChecked(key)
	return deleted
}

// DeleteChecked removes key and reports key validation errors.
func (ht *HatTrie) DeleteChecked(key string) (bool, error) {
	if ht == nil {
		return false, ErrNilHatTrie
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	if err := validateKey(key); err != nil {
		return false, err
	}
	deleted := ht.deleteLocked(key)
	if deleted {
		ht.recordDeleteLocked(key)
	}
	return deleted, nil
}

// Del removes key if it exists.
func (ht *HatTrie) Del(key string) {
	ht.Delete(key)
}

// UpsertCounter sets key to an int32 counter.
func (ht *HatTrie) UpsertCounter(key string, val int32) {
	_ = ht.UpsertCounterChecked(key, val)
}

// UpsertCounterChecked sets key to an int32 counter and reports validation
// errors.
func (ht *HatTrie) UpsertCounterChecked(key string, val int32) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval, err := ht.upsertReplacementLocation(key)
	if err != nil {
		return err
	}
	if !hval.IsCounter() {
		ht.returnStorage(hval)
	}
	ht.clearExpirationLocked(key)
	*rawPtr = HatValue{Index: val, Flags: DATAVALUE_TYPE_COUNTER}.toValue()
	ht.recordWriteLocked(key)
	return nil
}

// IncrementCounter increments key by by. If key is not a counter, it is reset
// to by.
func (ht *HatTrie) IncrementCounter(key string, by int32) {
	_, _ = ht.IncrementCounterChecked(key, by)
}

func (ht *HatTrie) IncrementCounterChecked(key string, by int32) (int32, error) {
	value, _, err := ht.incrementCounterChecked(key, by, false)
	return value, err
}

func (ht *HatTrie) incrementCounterChecked(key string, by int32, checkOverflow bool) (int32, bool, error) {
	if ht == nil {
		return 0, false, ErrNilHatTrie
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return 0, false, err
	}
	if hval.IsCounter() {
		if checkOverflow {
			next := int64(hval.Index) + int64(by)
			if next < minCommandInt32 || next > maxCommandInt32 {
				return hval.Index, false, nil
			}
			hval.Index = int32(next)
		} else {
			hval.Index += by
		}
	} else {
		if rawPtr == nil {
			rawPtr = ht.upsertLocation(key)
		}
		ht.returnStorage(hval)
		ht.clearExpirationLocked(key)
		hval.Flags = DATAVALUE_TYPE_COUNTER
		hval.Index = by
	}
	*rawPtr = hval.toValue()
	ht.recordWriteLocked(key)
	return hval.Index, true, nil
}

// GetCounter returns 0 if key is missing or does not hold a counter.
func (ht *HatTrie) GetCounter(key string) int32 {
	value, _, _ := ht.GetCounterChecked(key)
	return value
}

func (ht *HatTrie) GetCounterChecked(key string) (int32, bool, error) {
	if ht == nil {
		return 0, false, ErrNilHatTrie
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return 0, false, err
	}
	ht.recordReadLocked(hval.IsCounter(), key)
	if hval.IsCounter() {
		return hval.Index, true, nil
	}
	return 0, false, nil
}

// UpsertString sets key to a string.
func (ht *HatTrie) UpsertString(key string, val string) {
	_ = ht.UpsertStringChecked(key, val)
}

// UpsertStringChecked sets key to a string and reports validation errors.
func (ht *HatTrie) UpsertStringChecked(key string, val string) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval, err := ht.upsertReplacementLocation(key)
	if err != nil {
		return err
	}
	if hval.IsStringAtRaws() {
		ht.raws.putStringOwned(hval.Index, val)
		ht.clearExpirationLocked(key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return nil
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.raws.addStringOwned(val)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_RAW_STRING}.toValue()
	ht.recordWriteLocked(key)
	return nil
}

// AppendString appends str to key. If key is not a string, it is reset to str.
func (ht *HatTrie) AppendString(key string, str string) {
	_, _ = ht.AppendStringChecked(key, str)
}

func (ht *HatTrie) AppendStringChecked(key string, str string) (string, error) {
	if ht == nil {
		return "", ErrNilHatTrie
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return "", err
	}
	if hval.IsStringAtRaws() {
		old := ht.raws.stringValue(hval.Index)
		if str == "" {
			return old, nil
		}
		capacity, ok := checkedByteCapacity(len(old), len(str))
		if !ok {
			return "", errRawValueCapacityTooLarge
		}
		next := old + str
		if len(next) != capacity {
			return "", errRawValueCapacityTooLarge
		}
		ht.raws.putStringOwned(hval.Index, next)
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return next, nil
	}

	if rawPtr == nil {
		rawPtr = ht.upsertLocation(key)
	}
	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.raws.addStringOwned(str)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_RAW_STRING}.toValue()
	ht.recordWriteLocked(key)
	return str, nil
}

// PrependString prepends str to key. If key is not a string, it is reset to str.
func (ht *HatTrie) PrependString(key string, str string) {
	_, _ = ht.PrependStringChecked(key, str)
}

func (ht *HatTrie) PrependStringChecked(key string, str string) (string, error) {
	if ht == nil {
		return "", ErrNilHatTrie
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return "", err
	}
	if hval.IsStringAtRaws() {
		old := ht.raws.stringValue(hval.Index)
		if str == "" {
			return old, nil
		}
		capacity, ok := checkedByteCapacity(len(str), len(old))
		if !ok {
			return "", errRawValueCapacityTooLarge
		}
		next := str + old
		if len(next) != capacity {
			return "", errRawValueCapacityTooLarge
		}
		ht.raws.putStringOwned(hval.Index, next)
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return next, nil
	}

	if rawPtr == nil {
		rawPtr = ht.upsertLocation(key)
	}
	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.raws.addStringOwned(str)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_RAW_STRING}.toValue()
	ht.recordWriteLocked(key)
	return str, nil
}

// GetString returns an empty string if key is missing or not a string/counter.
func (ht *HatTrie) GetString(key string) string {
	value, _, _ := ht.GetStringChecked(key)
	return value
}

func (ht *HatTrie) GetStringChecked(key string) (string, bool, error) {
	if ht == nil {
		return "", false, ErrNilHatTrie
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return "", false, err
	}
	ht.recordReadLocked(hval.IsStringAtRaws() || hval.IsCounter(), key)
	if hval.IsStringAtRaws() {
		return ht.raws.stringValue(hval.Index), true, nil
	}
	if hval.IsCounter() {
		return strconv.FormatInt(int64(hval.Index), 10), true, nil
	}
	return "", false, nil
}

// UpsertBytes sets key to a byte slice.
func (ht *HatTrie) UpsertBytes(key string, val []byte) {
	_ = ht.UpsertBytesChecked(key, val)
}

func (ht *HatTrie) UpsertBytesChecked(key string, val []byte) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	if err := validateKey(key); err != nil {
		return err
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr := ht.tryLocation(key)
	hval := HatValue{}
	if rawPtr == nil {
		ht.clearExpirationLocked(key)
	} else {
		hval.fromValue(*rawPtr)
		if ht.expireIfNeededLocked(key, hval) {
			rawPtr = nil
			hval = HatValue{}
		}
	}

	next, err := ht.storeBytesValueLocked(hval, val)
	if err != nil {
		return err
	}

	if rawPtr == nil {
		rawPtr = ht.upsertLocation(key)
	}
	if !hval.Empty() && !hval.IsBytesAtRaws() {
		ht.returnStorage(hval)
	}
	ht.clearExpirationLocked(key)
	*rawPtr = next.toValue()
	ht.recordWriteLocked(key)
	return nil
}

// GetBytes returns nil if key is missing or not a string/bytes value.
func (ht *HatTrie) GetBytes(key string) []byte {
	value, _ := ht.GetBytesChecked(key)
	return value
}

func (ht *HatTrie) GetBytesChecked(key string) ([]byte, error) {
	if ht == nil {
		return nil, ErrNilHatTrie
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return nil, err
	}
	if hval.IsStringAtRaws() {
		ht.recordReadLocked(true, key)
		return cloneBytes(ht.raws.array[hval.Index]), nil
	}
	if hval.IsBytesAtRaws() {
		if hval.OnDisk() {
			value, err := ht.disks.Get(hval.Index)
			if err != nil {
				ht.recordReadLocked(false, key)
				return nil, err
			}
			ht.recordReadLocked(true, key)
			return value, nil
		}
		ht.recordReadLocked(true, key)
		return cloneBytes(ht.raws.array[hval.Index]), nil
	}
	ht.recordReadLocked(false, key)
	return nil, nil
}

func (ht *HatTrie) storeBytesValueLocked(old HatValue, val []byte) (HatValue, error) {
	if len(val) > DiskBytesThreshold {
		if old.IsBytesAtRaws() && old.OnDisk() {
			if err := ht.disks.Put(old.Index, val); err != nil {
				return HatValue{}, err
			}
			return HatValue{
				Index: old.Index,
				Flags: DATAVALUE_TYPE_RAW_BYTES | (1 << DATAVALUE_DISK_BIT_SHIFT),
			}, nil
		}
		idx, err := ht.disks.Add(val)
		if err != nil {
			return HatValue{}, err
		}
		if old.IsBytesAtRaws() && !old.OnDisk() {
			ht.raws.Del(old.Index)
		}
		return HatValue{
			Index: idx,
			Flags: DATAVALUE_TYPE_RAW_BYTES | (1 << DATAVALUE_DISK_BIT_SHIFT),
		}, nil
	}

	if old.IsBytesAtRaws() && old.OnDisk() {
		idx := ht.raws.Add(val)
		ht.disks.Del(old.Index)
		return HatValue{Index: idx, Flags: DATAVALUE_TYPE_RAW_BYTES}, nil
	} else if old.IsBytesAtRaws() {
		ht.raws.Put(old.Index, val)
		return HatValue{Index: old.Index, Flags: DATAVALUE_TYPE_RAW_BYTES}, nil
	}
	idx := ht.raws.Add(val)
	return HatValue{Index: idx, Flags: DATAVALUE_TYPE_RAW_BYTES}, nil
}

func (ht *HatTrie) UpsertMap(key string, val Map) {
	if ht == nil {
		return
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	_ = ht.upsertMapLocked(key, val)
}

func (ht *HatTrie) UpsertMapChecked(key string, val Map) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	if err := validateMapValue(val); err != nil {
		return err
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	return ht.upsertMapLocked(key, val)
}

func (ht *HatTrie) upsertMapLocked(key string, val Map) error {
	rawPtr, hval, err := ht.upsertReplacementLocation(key)
	if err != nil {
		return err
	}
	if hval.IsMap() {
		ht.maps.Put(hval.Index, val)
		ht.clearExpirationLocked(key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return nil
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.maps.Add(val)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_MAP}.toValue()
	ht.recordWriteLocked(key)
	return nil
}

func (ht *HatTrie) UpsertMapJSON(key string, data []byte) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	value, err := UnmarshalMapJSON(data)
	if err != nil {
		return err
	}
	return ht.UpsertMapChecked(key, value)
}

func (ht *HatTrie) GetMapJSON(key string) ([]byte, bool, error) {
	value, ok, err := ht.GetMapChecked(key)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	data, err := MarshalMapJSON(value)
	if err != nil {
		return nil, true, err
	}
	return data, true, nil
}

func (ht *HatTrie) PutMap(key string, subkey string, val interface{}) {
	_ = ht.putMapEntries(key, Map{subkey: val})
}

func (ht *HatTrie) PutMapChecked(key string, subkey string, val interface{}) error {
	return ht.PutMapEntriesChecked(key, Map{subkey: val})
}

func (ht *HatTrie) PutMapEntriesChecked(key string, fields Map) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	if len(fields) == 0 {
		return nil
	}
	if err := validateMapValue(fields); err != nil {
		return err
	}
	return ht.putMapEntries(key, fields)
}

func (ht *HatTrie) putMapEntries(key string, fields Map) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	if len(fields) == 0 {
		return nil
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return err
	}
	if hval.IsMap() {
		ht.maps.PutEntries(hval.Index, fields)
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return nil
	}

	if rawPtr == nil {
		rawPtr = ht.upsertLocation(key)
	}
	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.maps.Add(fields)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_MAP}.toValue()
	ht.recordWriteLocked(key)
	return nil
}

func (ht *HatTrie) PeekMap(key, subkey string) interface{} {
	value, _, _ := ht.PeekMapChecked(key, subkey)
	return value
}

func (ht *HatTrie) PeekMapChecked(key, subkey string) (interface{}, bool, error) {
	if ht == nil {
		return nil, false, ErrNilHatTrie
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	m, ok, err := ht.mapRefLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return nil, false, err
	}
	if !ok {
		ht.recordReadLocked(false, key)
		return nil, false, nil
	}
	val, exists := m[subkey]
	ht.recordReadLocked(exists, key)
	if !exists {
		return nil, false, nil
	}
	return cloneValue(val), true, nil
}

func (ht *HatTrie) TakeMap(key, subkey string) interface{} {
	value, _, _ := ht.TakeMapChecked(key, subkey)
	return value
}

func (ht *HatTrie) TakeMapChecked(key, subkey string) (interface{}, bool, error) {
	if ht == nil {
		return nil, false, ErrNilHatTrie
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return nil, false, err
	}
	if !hval.IsMap() {
		ht.recordReadLocked(false, key)
		return nil, false, nil
	}
	val, exists := ht.maps.TakeEntry(hval.Index, subkey)
	ht.recordReadLocked(exists, key)
	if exists {
		ht.recordWriteLocked(key)
	}
	return val, exists, nil
}

func (ht *HatTrie) GetMap(key string) Map {
	value, _, _ := ht.GetMapChecked(key)
	return value
}

func (ht *HatTrie) GetMapChecked(key string) (Map, bool, error) {
	if ht == nil {
		return nil, false, ErrNilHatTrie
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	m, ok, err := ht.mapRefLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return nil, false, err
	}
	ht.recordReadLocked(ok, key)
	if !ok {
		return nil, false, nil
	}
	return cloneMap(m), true, nil
}

func (ht *HatTrie) mapRefLocked(key string) (Map, bool) {
	m, ok, _ := ht.mapRefLockedChecked(key)
	return m, ok
}

func (ht *HatTrie) mapRefLockedChecked(key string) (Map, bool, error) {
	hval, err := ht.getLockedChecked(key)
	if err != nil {
		return nil, false, err
	}
	if hval.IsMap() {
		return ht.maps.array[hval.Index], true, nil
	}
	return nil, false, nil
}

func (ht *HatTrie) UpsertSlice(key string, val Slice) {
	if ht == nil {
		return
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	_ = ht.upsertSliceLocked(key, val)
}

func (ht *HatTrie) UpsertSliceChecked(key string, val Slice) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	if err := validateSliceValue(val); err != nil {
		return err
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	return ht.upsertSliceLocked(key, val)
}

func (ht *HatTrie) upsertSliceLocked(key string, val Slice) error {
	rawPtr, hval, err := ht.upsertReplacementLocation(key)
	if err != nil {
		return err
	}
	if hval.IsSlice() {
		ht.slices.Put(hval.Index, val)
		ht.clearExpirationLocked(key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return nil
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.slices.Add(val)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_SLICE}.toValue()
	ht.recordWriteLocked(key)
	return nil
}

func (ht *HatTrie) PushSlice(key string, val interface{}, vals ...interface{}) {
	_ = ht.pushSlice(key, val, vals...)
}

func (ht *HatTrie) PushSliceChecked(key string, val interface{}, vals ...interface{}) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	if err := validateSliceValues(val, vals...); err != nil {
		return err
	}
	return ht.pushSlice(key, val, vals...)
}

func (ht *HatTrie) pushSlice(key string, val interface{}, vals ...interface{}) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return err
	}
	if hval.IsSlice() {
		if err := ht.slices.array[hval.Index].PushOneChecked(val, vals...); err != nil {
			return err
		}
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return nil
	}

	idx, err := ht.slices.AddValuesChecked(val, vals...)
	if err != nil {
		return err
	}
	if rawPtr == nil {
		rawPtr = ht.upsertLocation(key)
	}
	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_SLICE}.toValue()
	ht.recordWriteLocked(key)
	return nil
}

func (ht *HatTrie) PopSlice(key string) interface{} {
	value, _, _ := ht.PopSliceChecked(key)
	return value
}

func (ht *HatTrie) PopSliceChecked(key string) (interface{}, bool, error) {
	if ht == nil {
		return nil, false, ErrNilHatTrie
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return nil, false, err
	}
	if !hval.IsSlice() {
		ht.recordReadLocked(false, key)
		return nil, false, nil
	}

	val, ok := ht.slices.array[hval.Index].Pop()
	if !ok {
		ht.recordReadLocked(false, key)
		return nil, false, nil
	}
	ht.recordReadLocked(true, key)
	ht.recordWriteLocked(key)
	return val, true, nil
}

func (ht *HatTrie) ShiftSlice(key string) interface{} {
	value, _, _ := ht.ShiftSliceChecked(key)
	return value
}

func (ht *HatTrie) ShiftSliceChecked(key string) (interface{}, bool, error) {
	if ht == nil {
		return nil, false, ErrNilHatTrie
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return nil, false, err
	}
	if !hval.IsSlice() {
		ht.recordReadLocked(false, key)
		return nil, false, nil
	}

	val, ok := ht.slices.array[hval.Index].Shift()
	if !ok {
		ht.recordReadLocked(false, key)
		return nil, false, nil
	}
	ht.recordReadLocked(true, key)
	ht.recordWriteLocked(key)
	return val, true, nil
}

func (ht *HatTrie) HeadSlice(key string) interface{} {
	value, _, _ := ht.HeadSliceChecked(key)
	return value
}

func (ht *HatTrie) HeadSliceChecked(key string) (interface{}, bool, error) {
	if ht == nil {
		return nil, false, ErrNilHatTrie
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	dq, ok, err := ht.sliceRefLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return nil, false, err
	}
	val, hit := dq.Head()
	hit = ok && hit
	ht.recordReadLocked(hit, key)
	if hit {
		return cloneValue(val), true, nil
	}
	return nil, false, nil
}

func (ht *HatTrie) TailSlice(key string) interface{} {
	value, _, _ := ht.TailSliceChecked(key)
	return value
}

func (ht *HatTrie) TailSliceChecked(key string) (interface{}, bool, error) {
	if ht == nil {
		return nil, false, ErrNilHatTrie
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	dq, ok, err := ht.sliceRefLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return nil, false, err
	}
	val, hit := dq.Tail()
	hit = ok && hit
	ht.recordReadLocked(hit, key)
	if hit {
		return cloneValue(val), true, nil
	}
	return nil, false, nil
}

func (ht *HatTrie) GetSlice(key string) Slice {
	value, _, _ := ht.GetSliceChecked(key)
	return value
}

func (ht *HatTrie) GetSliceChecked(key string) (Slice, bool, error) {
	if ht == nil {
		return nil, false, ErrNilHatTrie
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return nil, false, err
	}
	if !hval.IsSlice() {
		ht.recordReadLocked(false, key)
		return nil, false, nil
	}
	ht.recordReadLocked(true, key)
	return ht.slices.array[hval.Index].Slice(), true, nil
}

func (ht *HatTrie) sliceRefLocked(key string) (*deque, bool) {
	dq, ok, _ := ht.sliceRefLockedChecked(key)
	return dq, ok
}

func (ht *HatTrie) sliceRefLockedChecked(key string) (*deque, bool, error) {
	hval, err := ht.getLockedChecked(key)
	if err != nil {
		return nil, false, err
	}
	if hval.IsSlice() {
		return &ht.slices.array[hval.Index], true, nil
	}
	return nil, false, nil
}

func (ht *HatTrie) UpsertSet(key string, val Set) {
	_ = ht.UpsertSetChecked(key, val)
}

func (ht *HatTrie) UpsertSetChecked(key string, val Set) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	data, err := newSetDataChecked(val)
	if err != nil {
		return err
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval, err := ht.upsertReplacementLocation(key)
	if err != nil {
		return err
	}
	if hval.IsSet() {
		ht.sets.PutData(hval.Index, data)
		ht.clearExpirationLocked(key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return nil
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.sets.AddData(data)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_SET}.toValue()
	ht.recordWriteLocked(key)
	return nil
}

func (ht *HatTrie) AddSet(key string, val interface{}, vals ...interface{}) int {
	added, _ := ht.AddSetChecked(key, val, vals...)
	return added
}

func (ht *HatTrie) AddSetChecked(key string, val interface{}, vals ...interface{}) (int, error) {
	if ht == nil {
		return 0, ErrNilHatTrie
	}
	keys, err := setItemKeysOne(val, vals...)
	if err != nil {
		return 0, err
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return 0, err
	}
	if hval.IsSet() {
		added := ht.sets.array[hval.Index].addOneWithKeys(keys, val, vals...)
		*rawPtr = hval.toValue()
		if added > 0 {
			ht.recordWriteLocked(key)
		}
		return added, nil
	}

	var data setData
	added := data.addOneWithKeys(keys, val, vals...)
	if rawPtr == nil {
		rawPtr = ht.upsertLocation(key)
	}
	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.sets.AddData(data)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_SET}.toValue()
	ht.recordWriteLocked(key)
	return added, nil
}

func (ht *HatTrie) RemoveSet(key string, val interface{}, vals ...interface{}) int {
	removed, _ := ht.RemoveSetChecked(key, val, vals...)
	return removed
}

func (ht *HatTrie) RemoveSetChecked(key string, val interface{}, vals ...interface{}) (int, error) {
	if ht == nil {
		return 0, ErrNilHatTrie
	}
	keys, err := setItemKeysOne(val, vals...)
	if err != nil {
		return 0, err
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return 0, err
	}
	if !hval.IsSet() {
		ht.recordReadLocked(false, key)
		return 0, nil
	}

	removed := ht.sets.array[hval.Index].removeKeys(keys)
	ht.recordReadLocked(removed > 0, key)
	if removed > 0 {
		ht.recordWriteLocked(key)
	}
	return removed, nil
}

func (ht *HatTrie) HasSet(key string, val interface{}) bool {
	hit, _ := ht.HasSetChecked(key, val)
	return hit
}

func (ht *HatTrie) HasSetChecked(key string, val interface{}) (bool, error) {
	if ht == nil {
		return false, ErrNilHatTrie
	}
	valueKey, err := setItemKey(val)
	if err != nil {
		return false, err
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return false, err
	}
	if !hval.IsSet() {
		ht.recordReadLocked(false, key)
		return false, nil
	}
	hit := ht.sets.array[hval.Index].hasKey(valueKey)
	ht.recordReadLocked(hit, key)
	return hit, nil
}

func (ht *HatTrie) GetSet(key string) Set {
	value, _, _ := ht.GetSetChecked(key)
	return value
}

func (ht *HatTrie) GetSetChecked(key string) (Set, bool, error) {
	if ht == nil {
		return nil, false, ErrNilHatTrie
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return nil, false, err
	}
	if !hval.IsSet() {
		ht.recordReadLocked(false, key)
		return nil, false, nil
	}
	ht.recordReadLocked(true, key)
	return ht.sets.array[hval.Index].Values(), true, nil
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
		return "", fmt.Errorf("hatriecache: unsupported set value: %w", err)
	}
	return string(data), nil
}

func setItemKeys(values ...interface{}) ([]string, error) {
	keys := make([]string, len(values))
	for idx, value := range values {
		key, err := setItemKey(value)
		if err != nil {
			return nil, err
		}
		keys[idx] = key
	}
	return keys, nil
}

func setItemKeysOne(value interface{}, values ...interface{}) ([]string, error) {
	count, ok := checkedBatchSize(1, len(values))
	if !ok {
		return nil, errBatchSizeTooLarge
	}
	keys := make([]string, count)
	key, err := setItemKey(value)
	if err != nil {
		return nil, err
	}
	keys[0] = key
	for idx, value := range values {
		key, err := setItemKey(value)
		if err != nil {
			return nil, err
		}
		keys[idx+1] = key
	}
	return keys, nil
}

func jsonPlainStringKey(value string) string {
	return `"` + value + `"`
}

func jsonPlainStringMatchesKey(value string, key string) bool {
	if len(key) != len(value)+2 || key[0] != '"' || key[len(key)-1] != '"' {
		return false
	}
	return key[1:len(key)-1] == value
}
