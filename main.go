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
	"math"
	mathbits "math/bits"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/cespare/xxhash/v2"
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
	defaultHatTrieScanBatchEntries  = 256
	defaultHatTrieScanBatchKeyBytes = 4 << 10
	// DefaultCounterWriteStripes keeps the constrained counter fast path opt-in.
	DefaultCounterWriteStripes = 0
	// MaxCounterWriteStripes bounds retained mutex memory.
	MaxCounterWriteStripes = 256
)

type hatTriePackedKeyRecord struct {
	keyOffset uint32
	keyLength uint32
}

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

func (heap *expirationHeap) Push(entry expirationEntry, indexes map[string]uint32) {
	if index, ok := indexes[entry.key]; ok {
		heap.Update(entry.key, entry.at, int(index), indexes)
		return
	}
	*heap = append(*heap, entry)
	indexes[entry.key] = uint32(len(*heap) - 1)
	heap.siftUp(len(*heap)-1, indexes)
}

func (heap *expirationHeap) Remove(key string, indexes map[string]uint32) (expirationEntry, bool) {
	position, ok := indexes[key]
	index := int(position)
	if !ok || heap == nil || index < 0 || index >= len(*heap) || (*heap)[index].key != key {
		return expirationEntry{}, false
	}
	values := *heap
	removed := values[index]
	last := len(values) - 1
	delete(indexes, key)
	if index != last {
		values[index] = values[last]
		indexes[values[index].key] = uint32(index)
	}
	values[last] = expirationEntry{}
	values = values[:last]
	*heap = values
	if index < len(values) {
		if index > 0 && values[index].before(values[(index-1)/2]) {
			heap.siftUp(index, indexes)
		} else {
			heap.siftDown(index, indexes)
		}
	}
	return removed, true
}

func (heap *expirationHeap) Update(key string, at time.Time, index int, indexes map[string]uint32) bool {
	if heap == nil || index < 0 || index >= len(*heap) || (*heap)[index].key != key {
		return false
	}
	previous := (*heap)[index].at
	(*heap)[index].at = at
	if at.Before(previous) {
		heap.siftUp(index, indexes)
	} else if at.After(previous) {
		heap.siftDown(index, indexes)
	}
	return true
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

func (heap expirationHeap) siftUp(idx int, indexes map[string]uint32) {
	for idx > 0 {
		parent := (idx - 1) / 2
		if !heap[idx].before(heap[parent]) {
			return
		}
		heap.swap(idx, parent, indexes)
		idx = parent
	}
}

func (heap expirationHeap) siftDown(idx int, indexes map[string]uint32) {
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
		heap.swap(idx, smallest, indexes)
		idx = smallest
	}
}

func (heap expirationHeap) swap(left int, right int, indexes map[string]uint32) {
	heap[left], heap[right] = heap[right], heap[left]
	indexes[heap[left].key] = uint32(left)
	indexes[heap[right].key] = uint32(right)
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

const unsetAtomicCacheTime = int64(-1 << 63)

type atomicCacheStats struct {
	hits        atomic.Uint64
	misses      atomic.Uint64
	writes      atomic.Uint64
	deletes     atomic.Uint64
	expirations atomic.Uint64
	lastHit     atomic.Int64
	lastMiss    atomic.Int64
	lastWrite   atomic.Int64
}

func (stats *atomicCacheStats) initialize() {
	stats.lastHit.Store(unsetAtomicCacheTime)
	stats.lastMiss.Store(unsetAtomicCacheTime)
	stats.lastWrite.Store(unsetAtomicCacheTime)
}

func (stats *atomicCacheStats) snapshot() CacheStats {
	hits := stats.hits.Load()
	misses := stats.misses.Load()
	out := CacheStats{
		Reads:       hits + misses,
		Hits:        hits,
		Misses:      misses,
		Writes:      stats.writes.Load(),
		Deletes:     stats.deletes.Load(),
		Expirations: stats.expirations.Load(),
		LastHit:     loadAtomicCacheTime(&stats.lastHit),
		LastMiss:    loadAtomicCacheTime(&stats.lastMiss),
		LastWrite:   loadAtomicCacheTime(&stats.lastWrite),
	}
	out.updateRates()
	return out
}

func (stats *atomicCacheStats) restore(value CacheStats) {
	stats.hits.Store(value.Hits)
	stats.misses.Store(value.Misses)
	stats.writes.Store(value.Writes)
	stats.deletes.Store(value.Deletes)
	stats.expirations.Store(value.Expirations)
	storeAtomicCacheTime(&stats.lastHit, value.LastHit)
	storeAtomicCacheTime(&stats.lastMiss, value.LastMiss)
	storeAtomicCacheTime(&stats.lastWrite, value.LastWrite)
}

func updateAtomicCacheTime(target *atomic.Int64, value time.Time) {
	encoded := value.UnixNano()
	for {
		current := target.Load()
		if current != unsetAtomicCacheTime && current >= encoded {
			return
		}
		if target.CompareAndSwap(current, encoded) {
			return
		}
	}
}

func loadAtomicCacheTime(source *atomic.Int64) time.Time {
	encoded := source.Load()
	if encoded == unsetAtomicCacheTime {
		return time.Time{}
	}
	return time.Unix(0, encoded)
}

func storeAtomicCacheTime(target *atomic.Int64, value time.Time) {
	if value.IsZero() {
		target.Store(unsetAtomicCacheTime)
		return
	}
	target.Store(value.UnixNano())
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

// KeyStatsMode controls how much per-key telemetry the trie retains. Cache-wide
// statistics remain exact in every mode.
type KeyStatsMode string

const (
	KeyStatsModeBounded KeyStatsMode = "bounded"
	KeyStatsModeFull    KeyStatsMode = "full"
	KeyStatsModeOff     KeyStatsMode = "off"

	DefaultKeyStatsMode     = KeyStatsModeOff
	DefaultKeyStatsCapacity = 100_000
	keyStatsEvictionSamples = 5
)

// KeyStatsPolicy describes the active per-key telemetry policy. Tracked is the
// current number of keys with retained detailed statistics.
type KeyStatsPolicy struct {
	Mode     KeyStatsMode `json:"mode"`
	Capacity int          `json:"capacity"`
	Tracked  int          `json:"tracked"`
}

type trackedKeyStats struct {
	Reads         uint64
	Hits          uint64
	Misses        uint64
	Writes        uint64
	lastHitSec    int64
	lastMissSec   int64
	lastWriteSec  int64
	lastHitNsec   uint32
	lastMissNsec  uint32
	lastWriteNsec uint32
	slot          uint32
}

const keyStatsUnsetSecond = int64(-1 << 63)
const keyStatsNoSlot = ^uint32(0)

func newTrackedKeyStats() *trackedKeyStats {
	return &trackedKeyStats{
		lastHitSec:   keyStatsUnsetSecond,
		lastMissSec:  keyStatsUnsetSecond,
		lastWriteSec: keyStatsUnsetSecond,
		slot:         keyStatsNoSlot,
	}
}

func compactKeyStatsTime(value time.Time) (int64, uint32) {
	if value.IsZero() {
		return keyStatsUnsetSecond, 0
	}
	return value.Unix(), uint32(value.Nanosecond())
}

func expandedKeyStatsTime(seconds int64, nanoseconds uint32) time.Time {
	if seconds == keyStatsUnsetSecond {
		return time.Time{}
	}
	return time.Unix(seconds, int64(nanoseconds))
}

func (stats *trackedKeyStats) expanded() KeyStats {
	if stats == nil {
		return KeyStats{}
	}
	out := KeyStats{
		Reads:     stats.Reads,
		Hits:      stats.Hits,
		Misses:    stats.Misses,
		Writes:    stats.Writes,
		LastHit:   expandedKeyStatsTime(stats.lastHitSec, stats.lastHitNsec),
		LastMiss:  expandedKeyStatsTime(stats.lastMissSec, stats.lastMissNsec),
		LastWrite: expandedKeyStatsTime(stats.lastWriteSec, stats.lastWriteNsec),
	}
	out.updateRates()
	return out
}

func (stats *trackedKeyStats) restore(value KeyStats) {
	stats.Reads = value.Reads
	stats.Hits = value.Hits
	stats.Misses = value.Misses
	stats.Writes = value.Writes
	stats.lastHitSec, stats.lastHitNsec = compactKeyStatsTime(value.LastHit)
	stats.lastMissSec, stats.lastMissNsec = compactKeyStatsTime(value.LastMiss)
	stats.lastWriteSec, stats.lastWriteNsec = compactKeyStatsTime(value.LastWrite)
}

func (stats *trackedKeyStats) setLastHit(value time.Time) {
	stats.lastHitSec, stats.lastHitNsec = compactKeyStatsTime(value)
}

func (stats *trackedKeyStats) setLastMiss(value time.Time) {
	stats.lastMissSec, stats.lastMissNsec = compactKeyStatsTime(value)
}

func (stats *trackedKeyStats) setLastWrite(value time.Time) {
	stats.lastWriteSec, stats.lastWriteNsec = compactKeyStatsTime(value)
}

func (stats *trackedKeyStats) lastActivity() (int64, uint32) {
	seconds, nanoseconds := stats.lastWriteSec, stats.lastWriteNsec
	if compactKeyStatsTimeAfter(stats.lastHitSec, stats.lastHitNsec, seconds, nanoseconds) {
		seconds, nanoseconds = stats.lastHitSec, stats.lastHitNsec
	}
	if compactKeyStatsTimeAfter(stats.lastMissSec, stats.lastMissNsec, seconds, nanoseconds) {
		seconds, nanoseconds = stats.lastMissSec, stats.lastMissNsec
	}
	return seconds, nanoseconds
}

func compactKeyStatsTimeAfter(leftSec int64, leftNsec uint32, rightSec int64, rightNsec uint32) bool {
	return leftSec > rightSec || leftSec == rightSec && leftNsec > rightNsec
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

// StringStorage stores immutable strings outside the trie without retaining a
// mirrored byte slice.
type StringStorage struct {
	array     []string
	reusables reusableIndexes
}

func CreateStringStorage() *StringStorage {
	return &StringStorage{array: []string{}}
}

func (ss *StringStorage) Put(idx int32, value string) {
	if idx < 0 || int(idx) >= len(ss.array) {
		return
	}
	ss.array[idx] = value
	ss.reusables.Use(idx)
}

func (ss *StringStorage) Append(value string) int32 {
	ss.array = append(ss.array, value)
	return int32(len(ss.array) - 1)
}

func (ss *StringStorage) Add(value string) int32 {
	if idx, ok := ss.reusables.Take(); ok {
		ss.array[idx] = value
		return idx
	}
	return ss.Append(value)
}

func (ss *StringStorage) Del(idx int32) {
	if idx < 0 || int(idx) >= len(ss.array) {
		return
	}
	ss.array[idx] = ""
	ss.reusables.Mark(idx)
	ss.array = trimReusableTail(ss.array, &ss.reusables)
}

func (ss *StringStorage) Get(idx int32) string {
	if idx < 0 || int(idx) >= len(ss.array) {
		return ""
	}
	return ss.array[idx]
}

// BytesStorage stores byte values outside the trie.
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
	dir           string
	rootDir       string
	ownedDir      bool
	generationDir bool
	paths         []string
	reusables     reusableIndexes
}

func CreateDiskStorage(dir string, ownedDir bool) (*DiskStorage, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return &DiskStorage{
		dir:      dir,
		rootDir:  dir,
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
		_ = os.RemoveAll(ds.configuredRoot())
		return
	}
	for _, path := range ds.paths {
		if path != "" {
			_ = os.Remove(path)
		}
	}
	if ds.generationDir {
		_ = os.RemoveAll(ds.dir)
	}
}

func (ds *DiskStorage) configuredRoot() string {
	if ds == nil || ds.rootDir == "" {
		if ds == nil {
			return ""
		}
		return ds.dir
	}
	return ds.rootDir
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

const smallMapEntryLimit = 2

type smallMapEntry struct {
	key   string
	value interface{}
}

type smallMapData struct {
	entries [smallMapEntryLimit]smallMapEntry
	length  uint8
}

func newSmallMapData(value Map) smallMapData {
	var out smallMapData
	for key, item := range value {
		out.entries[out.length] = smallMapEntry{key: key, value: cloneValue(item)}
		out.length++
	}
	return out
}

func newSmallMapEntry(subkey string, value interface{}) smallMapData {
	return smallMapData{
		entries: [smallMapEntryLimit]smallMapEntry{{key: subkey, value: cloneValue(value)}},
		length:  1,
	}
}

func (data *smallMapData) index(subkey string) int {
	if data == nil {
		return -1
	}
	for index := 0; index < int(data.length); index++ {
		if data.entries[index].key == subkey {
			return index
		}
	}
	return -1
}

func (data *smallMapData) peek(subkey string) (interface{}, bool) {
	index := data.index(subkey)
	if index < 0 {
		return nil, false
	}
	return data.entries[index].value, true
}

func (data *smallMapData) putEntry(subkey string, value interface{}) bool {
	if index := data.index(subkey); index >= 0 {
		data.entries[index].value = cloneValue(value)
		return true
	}
	if int(data.length) >= smallMapEntryLimit {
		return false
	}
	data.entries[data.length] = smallMapEntry{key: subkey, value: cloneValue(value)}
	data.length++
	return true
}

func (data *smallMapData) putEntries(fields Map) bool {
	additional := 0
	for subkey := range fields {
		if data.index(subkey) < 0 {
			additional++
		}
	}
	if int(data.length)+additional > smallMapEntryLimit {
		return false
	}
	for subkey, value := range fields {
		data.putEntry(subkey, value)
	}
	return true
}

func (data *smallMapData) takeEntry(subkey string) (interface{}, bool) {
	index := data.index(subkey)
	if index < 0 {
		return nil, false
	}
	value := data.entries[index].value
	last := int(data.length) - 1
	if index != last {
		data.entries[index] = data.entries[last]
	}
	data.entries[last] = smallMapEntry{}
	data.length--
	return value, true
}

func (data *smallMapData) materialize(cloneValues bool) Map {
	if data == nil {
		return nil
	}
	out := make(Map, int(data.length))
	for index := 0; index < int(data.length); index++ {
		entry := data.entries[index]
		if cloneValues {
			out[entry.key] = cloneValue(entry.value)
		} else {
			out[entry.key] = entry.value
		}
	}
	return out
}

func (data *smallMapData) jsonString() (string, error) {
	if data == nil {
		return "null", nil
	}
	var builder strings.Builder
	estimated := 2
	for index := 0; index < int(data.length); index++ {
		estimated += len(data.entries[index].key) + 4
		if value, ok := data.entries[index].value.(string); ok {
			estimated += len(value) + 2
		}
	}
	builder.Grow(estimated)
	builder.WriteByte('{')
	first := &data.entries[0]
	var second *smallMapEntry
	if data.length == 2 {
		second = &data.entries[1]
		if second.key < first.key {
			first, second = second, first
		}
	}
	if data.length > 0 {
		if err := writeSmallMapJSONEntry(&builder, first); err != nil {
			return "", err
		}
	}
	if second != nil {
		builder.WriteByte(',')
		if err := writeSmallMapJSONEntry(&builder, second); err != nil {
			return "", err
		}
	}
	builder.WriteByte('}')
	return builder.String(), nil
}

func writeSmallMapJSONEntry(builder *strings.Builder, entry *smallMapEntry) error {
	writeJSONString(builder, entry.key)
	builder.WriteByte(':')
	return writeSmallMapJSONValue(builder, entry.value)
}

func writeSmallMapJSONValue(builder *strings.Builder, value interface{}) error {
	switch typed := value.(type) {
	case nil:
		builder.WriteString("null")
	case string:
		writeJSONString(builder, typed)
	case bool:
		if typed {
			builder.WriteString("true")
		} else {
			builder.WriteString("false")
		}
	case int:
		writeJSONInt(builder, int64(typed))
	case int8:
		writeJSONInt(builder, int64(typed))
	case int16:
		writeJSONInt(builder, int64(typed))
	case int32:
		writeJSONInt(builder, int64(typed))
	case int64:
		writeJSONInt(builder, typed)
	case uint:
		writeJSONUint(builder, uint64(typed))
	case uint8:
		writeJSONUint(builder, uint64(typed))
	case uint16:
		writeJSONUint(builder, uint64(typed))
	case uint32:
		writeJSONUint(builder, uint64(typed))
	case uint64:
		writeJSONUint(builder, typed)
	default:
		encoded, err := json.Marshal(value)
		if err != nil {
			return err
		}
		builder.Write(encoded)
	}
	return nil
}

func writeJSONInt(builder *strings.Builder, value int64) {
	var buffer [20]byte
	builder.Write(strconv.AppendInt(buffer[:0], value, 10))
}

func writeJSONUint(builder *strings.Builder, value uint64) {
	var buffer [20]byte
	builder.Write(strconv.AppendUint(buffer[:0], value, 10))
}

func writeJSONString(builder *strings.Builder, value string) {
	const hex = "0123456789abcdef"
	builder.WriteByte('"')
	start := 0
	for index := 0; index < len(value); {
		character := value[index]
		if character < utf8.RuneSelf {
			if character >= 0x20 && character != '\\' && character != '"' && character != '<' && character != '>' && character != '&' {
				index++
				continue
			}
			builder.WriteString(value[start:index])
			switch character {
			case '\\', '"':
				builder.WriteByte('\\')
				builder.WriteByte(character)
			case '\b':
				builder.WriteString("\\b")
			case '\f':
				builder.WriteString("\\f")
			case '\n':
				builder.WriteString("\\n")
			case '\r':
				builder.WriteString("\\r")
			case '\t':
				builder.WriteString("\\t")
			default:
				builder.WriteString("\\u00")
				builder.WriteByte(hex[character>>4])
				builder.WriteByte(hex[character&0x0f])
			}
			index++
			start = index
			continue
		}

		runeValue, size := utf8.DecodeRuneInString(value[index:])
		if runeValue == utf8.RuneError && size == 1 {
			builder.WriteString(value[start:index])
			builder.WriteString("\\ufffd")
			index++
			start = index
			continue
		}
		if runeValue == '\u2028' || runeValue == '\u2029' {
			builder.WriteString(value[start:index])
			builder.WriteString("\\u202")
			builder.WriteByte(hex[byte(runeValue-'\u2020')])
			index += size
			start = index
			continue
		}
		index += size
	}
	builder.WriteString(value[start:])
	builder.WriteByte('"')
}

func encodeSmallMapIndex(index int32) int32 {
	return ^index
}

func decodeSmallMapIndex(index int32) (int32, bool) {
	if index >= 0 {
		return 0, false
	}
	return ^index, true
}

// MapStorage stores map values outside the trie. One- and two-field maps use
// a packed pool; negative indexes encode that internal representation.
type MapStorage struct {
	array          []Map
	deleted        []int
	reusables      reusableIndexes
	small          []smallMapData
	smallReusables reusableIndexes
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

func (ms *MapStorage) putAdaptive(idx int32, value Map) int32 {
	if smallIndex, ok := decodeSmallMapIndex(idx); ok {
		if len(value) > 0 && len(value) <= smallMapEntryLimit && int(smallIndex) < len(ms.small) && !ms.smallReusables.Has(smallIndex) {
			ms.small[smallIndex] = newSmallMapData(value)
			return idx
		}
		ms.delSmall(smallIndex)
		return ms.Add(value)
	}
	if len(value) > 0 && len(value) <= smallMapEntryLimit {
		ms.Del(idx)
		return ms.addSmall(value)
	}
	ms.Put(idx, value)
	return idx
}

func (ms *MapStorage) putEntryAdaptive(idx int32, subkey string, value interface{}) int32 {
	if smallIndex, ok := decodeSmallMapIndex(idx); ok {
		if int(smallIndex) >= len(ms.small) || ms.smallReusables.Has(smallIndex) {
			return idx
		}
		data := &ms.small[smallIndex]
		if data.putEntry(subkey, value) {
			return idx
		}
		promoted := data.materialize(false)
		promoted[subkey] = cloneValue(value)
		next := ms.addOwned(promoted)
		ms.delSmall(smallIndex)
		return next
	}
	ms.PutEntry(idx, subkey, value)
	return idx
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

func (ms *MapStorage) putEntriesAdaptive(idx int32, fields Map) int32 {
	if smallIndex, ok := decodeSmallMapIndex(idx); ok {
		if int(smallIndex) >= len(ms.small) || ms.smallReusables.Has(smallIndex) || len(fields) == 0 {
			return idx
		}
		data := &ms.small[smallIndex]
		if data.putEntries(fields) {
			return idx
		}
		promoted := data.materialize(false)
		for subkey, value := range fields {
			promoted[subkey] = cloneValue(value)
		}
		next := ms.addOwned(promoted)
		ms.delSmall(smallIndex)
		return next
	}
	ms.PutEntries(idx, fields)
	return idx
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

func (ms *MapStorage) addAdaptive(value Map) int32 {
	if len(value) > 0 && len(value) <= smallMapEntryLimit {
		return ms.addSmall(value)
	}
	return ms.Add(value)
}

func (ms *MapStorage) addOwned(value Map) int32 {
	if idx, ok := ms.reusables.Take(); ok {
		ms.array[idx] = value
		ms.deleted[idx] = 0
		return idx
	}
	ms.array = append(ms.array, value)
	ms.deleted = append(ms.deleted, 0)
	return int32(len(ms.array) - 1)
}

func (ms *MapStorage) addSmall(value Map) int32 {
	data := newSmallMapData(value)
	if idx, ok := ms.smallReusables.Take(); ok {
		ms.small[idx] = data
		return encodeSmallMapIndex(idx)
	}
	ms.small = append(ms.small, data)
	return encodeSmallMapIndex(int32(len(ms.small) - 1))
}

func (ms *MapStorage) AddEntry(subkey string, value interface{}) int32 {
	if idx, ok := ms.reusables.Take(); ok {
		ms.array[idx] = Map{subkey: cloneValue(value)}
		ms.deleted[idx] = 0
		return idx
	}
	return ms.AppendEntry(subkey, value)
}

func (ms *MapStorage) addEntryAdaptive(subkey string, value interface{}) int32 {
	data := newSmallMapEntry(subkey, value)
	if idx, ok := ms.smallReusables.Take(); ok {
		ms.small[idx] = data
		return encodeSmallMapIndex(idx)
	}
	ms.small = append(ms.small, data)
	return encodeSmallMapIndex(int32(len(ms.small) - 1))
}

func (ms *MapStorage) TakeEntry(idx int32, subkey string) (interface{}, bool) {
	if smallIndex, ok := decodeSmallMapIndex(idx); ok {
		if int(smallIndex) >= len(ms.small) || ms.smallReusables.Has(smallIndex) {
			return nil, false
		}
		return ms.small[smallIndex].takeEntry(subkey)
	}
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
	if smallIndex, ok := decodeSmallMapIndex(idx); ok {
		ms.delSmall(smallIndex)
		return
	}
	if idx < 0 || int(idx) >= len(ms.array) {
		return
	}
	ms.array[idx] = nil
	ms.deleted[idx] = 0
	if int(idx) == len(ms.array)-1 {
		ms.array = ms.array[:idx]
		ms.deleted = ms.deleted[:idx]
		ms.array = trimReusableTail(ms.array, &ms.reusables)
		ms.deleted = ms.deleted[:len(ms.array)]
		return
	}
	ms.reusables.Mark(idx)
	ms.array = trimReusableTail(ms.array, &ms.reusables)
	ms.deleted = ms.deleted[:len(ms.array)]
}

func (ms *MapStorage) delSmall(idx int32) {
	if idx < 0 || int(idx) >= len(ms.small) {
		return
	}
	ms.small[idx] = smallMapData{}
	if int(idx) == len(ms.small)-1 {
		ms.small = ms.small[:idx]
		ms.small = trimReusableTail(ms.small, &ms.smallReusables)
		return
	}
	ms.smallReusables.Mark(idx)
	ms.small = trimReusableTail(ms.small, &ms.smallReusables)
}

func (ms *MapStorage) peek(idx int32, subkey string) (interface{}, bool) {
	if smallIndex, ok := decodeSmallMapIndex(idx); ok {
		return ms.small[smallIndex].peek(subkey)
	}
	value, ok := ms.array[idx][subkey]
	return value, ok
}

func (ms *MapStorage) jsonString(idx int32) (string, error) {
	if smallIndex, ok := decodeSmallMapIndex(idx); ok {
		if int(smallIndex) >= len(ms.small) || ms.smallReusables.Has(smallIndex) {
			return "", errors.New("hatriecache: map backing index is missing")
		}
		return ms.small[smallIndex].jsonString()
	}
	if idx < 0 || int(idx) >= len(ms.array) || ms.reusables.Has(idx) {
		return "", errors.New("hatriecache: map backing index is missing")
	}
	return jsonEncodedString(ms.array[idx])
}

func (ms *MapStorage) jsonSize(idx int32) (int64, error) {
	if smallIndex, ok := decodeSmallMapIndex(idx); ok {
		if int(smallIndex) >= len(ms.small) || ms.smallReusables.Has(smallIndex) {
			return 0, errors.New("hatriecache: map backing index is missing")
		}
		encoded, err := ms.small[smallIndex].jsonString()
		return int64(len(encoded)), err
	}
	if idx < 0 || int(idx) >= len(ms.array) || ms.reusables.Has(idx) {
		return 0, errors.New("hatriecache: map backing index is missing")
	}
	return jsonEncodedSize(ms.array[idx])
}

func (ms *MapStorage) clone(idx int32) (Map, bool) {
	if smallIndex, ok := decodeSmallMapIndex(idx); ok {
		if int(smallIndex) >= len(ms.small) || ms.smallReusables.Has(smallIndex) {
			return nil, false
		}
		return ms.small[smallIndex].materialize(true), true
	}
	if idx < 0 || int(idx) >= len(ms.array) || ms.reusables.Has(idx) {
		return nil, false
	}
	return cloneMap(ms.array[idx]), true
}

func (ms *MapStorage) length(idx int32) (int, bool) {
	if smallIndex, ok := decodeSmallMapIndex(idx); ok {
		if int(smallIndex) >= len(ms.small) || ms.smallReusables.Has(smallIndex) {
			return 0, false
		}
		return int(ms.small[smallIndex].length), true
	}
	if idx < 0 || int(idx) >= len(ms.array) || ms.reusables.Has(idx) {
		return 0, false
	}
	return len(ms.array[idx]), true
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

type packedOneStringSet struct {
	value interface{}
}

type packedTwoStringSet struct {
	values [2]interface{}
}

type packedStringSetValue struct {
	values [2]interface{}
	length uint8
}

func packedStringSetFromValues(values Set) (packedStringSetValue, bool) {
	if len(values) > 2 {
		return packedStringSetValue{}, false
	}
	var packed packedStringSetValue
	for _, value := range values {
		text, ok := value.(string)
		if !ok {
			return packedStringSetValue{}, false
		}
		if (packed.length > 0 && packed.values[0].(string) == text) || (packed.length > 1 && packed.values[1].(string) == text) {
			continue
		}
		if packed.length == 2 {
			return packedStringSetValue{}, false
		}
		packed.values[packed.length] = value
		packed.length++
	}
	return packed, true
}

func encodePackedStringSetIndex(index int32, twoValuePool bool) int32 {
	payload := uint32(index) << 1
	if twoValuePool {
		payload |= 1
	}
	return int32(^payload)
}

func decodePackedStringSetIndex(index int32) (poolIndex int32, twoValuePool bool, ok bool) {
	if index >= 0 {
		return 0, false, false
	}
	payload := ^uint32(index)
	return int32(payload >> 1), payload&1 != 0, true
}

// SetStorage stores set values outside the trie. One- and two-value plain
// string sets use packed pools selected by private negative indexes.
type SetStorage struct {
	array       []setData
	reusables   reusableIndexes
	oneStrings  []packedOneStringSet
	oneReusable reusableIndexes
	twoStrings  []packedTwoStringSet
	twoReusable reusableIndexes
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

func (ss *SetStorage) addAdaptive(value Set) int32 {
	if packed, ok := packedStringSetFromValues(value); ok {
		return ss.addPacked(packed)
	}
	return ss.Add(value)
}

func (ss *SetStorage) putPacked(idx int32, value packedStringSetValue) int32 {
	poolIndex, twoValuePool, packed := decodePackedStringSetIndex(idx)
	wantTwo := value.length == 2
	if packed && twoValuePool == wantTwo {
		if wantTwo {
			ss.twoStrings[poolIndex] = packedTwoStringSet{values: value.values}
		} else {
			ss.oneStrings[poolIndex] = packedOneStringSet{value: value.values[0]}
		}
		return idx
	}
	ss.Del(idx)
	return ss.addPacked(value)
}

func (ss *SetStorage) putDataAdaptive(idx int32, value setData) int32 {
	if _, _, packed := decodePackedStringSetIndex(idx); packed {
		ss.Del(idx)
		return ss.AddData(value)
	}
	ss.PutData(idx, value)
	return idx
}

func (ss *SetStorage) addPacked(value packedStringSetValue) int32 {
	if value.length <= 1 {
		data := packedOneStringSet{value: value.values[0]}
		if idx, ok := ss.oneReusable.Take(); ok {
			ss.oneStrings[idx] = data
			return encodePackedStringSetIndex(idx, false)
		}
		ss.oneStrings = append(ss.oneStrings, data)
		return encodePackedStringSetIndex(int32(len(ss.oneStrings)-1), false)
	}
	data := packedTwoStringSet{values: value.values}
	if idx, ok := ss.twoReusable.Take(); ok {
		ss.twoStrings[idx] = data
		return encodePackedStringSetIndex(idx, true)
	}
	ss.twoStrings = append(ss.twoStrings, data)
	return encodePackedStringSetIndex(int32(len(ss.twoStrings)-1), true)
}

func (ss *SetStorage) addPlainString(idx int32, value string) (int32, int) {
	poolIndex, twoValuePool, packed := decodePackedStringSetIndex(idx)
	if !packed {
		return idx, ss.array[idx].addPlainString(value)
	}
	if !twoValuePool {
		data := &ss.oneStrings[poolIndex]
		if data.value != nil && data.value.(string) == value {
			return idx, 0
		}
		if data.value == nil {
			data.value = value
			return idx, 1
		}
		next := packedStringSetValue{values: [2]interface{}{data.value, value}, length: 2}
		nextIndex := ss.addPacked(next)
		ss.delPacked(poolIndex, false)
		return nextIndex, 1
	}

	data := &ss.twoStrings[poolIndex]
	for valueIndex := range data.values {
		if data.values[valueIndex] != nil && data.values[valueIndex].(string) == value {
			return idx, 0
		}
	}
	if data.values[0] == nil {
		data.values[0] = value
		return idx, 1
	}
	if data.values[1] == nil {
		data.values[1] = value
		return idx, 1
	}
	promoted := setDataFromPlainStrings(data.values[0], data.values[1], value)
	nextIndex := ss.AddData(promoted)
	ss.delPacked(poolIndex, true)
	return nextIndex, 1
}

func (ss *SetStorage) addGeneric(idx int32, keys []string, value interface{}, values ...interface{}) (int32, int) {
	if _, _, packed := decodePackedStringSetIndex(idx); !packed {
		return idx, ss.array[idx].addOneWithKeys(keys, value, values...)
	}
	data := ss.packedData(idx)
	added := data.addOneWithKeys(keys, value, values...)
	nextIndex := ss.AddData(data)
	ss.Del(idx)
	return nextIndex, added
}

func (ss *SetStorage) removePlainString(idx int32, value string) int {
	poolIndex, twoValuePool, packed := decodePackedStringSetIndex(idx)
	if !packed {
		return ss.array[idx].removeKey(jsonPlainStringKey(value))
	}
	if !twoValuePool {
		data := &ss.oneStrings[poolIndex]
		if data.value == nil || data.value.(string) != value {
			return 0
		}
		data.value = nil
		return 1
	}
	data := &ss.twoStrings[poolIndex]
	for valueIndex := range data.values {
		if data.values[valueIndex] == nil || data.values[valueIndex].(string) != value {
			continue
		}
		if valueIndex == 0 {
			data.values[0] = data.values[1]
		}
		data.values[1] = nil
		return 1
	}
	return 0
}

func (ss *SetStorage) removeKeys(idx int32, keys []string) int {
	if _, _, packed := decodePackedStringSetIndex(idx); packed {
		return 0
	}
	return ss.array[idx].removeKeys(keys)
}

func (ss *SetStorage) hasPlainString(idx int32, value string) bool {
	poolIndex, twoValuePool, packed := decodePackedStringSetIndex(idx)
	if !packed {
		return ss.array[idx].hasPlainString(value)
	}
	if !twoValuePool {
		data := ss.oneStrings[poolIndex]
		return data.value != nil && data.value.(string) == value
	}
	data := ss.twoStrings[poolIndex]
	return (data.values[0] != nil && data.values[0].(string) == value) || (data.values[1] != nil && data.values[1].(string) == value)
}

func (ss *SetStorage) hasKey(idx int32, key string) bool {
	if _, _, packed := decodePackedStringSetIndex(idx); packed {
		return false
	}
	return ss.array[idx].hasKey(key)
}

func (ss *SetStorage) values(idx int32) Set {
	poolIndex, twoValuePool, packed := decodePackedStringSetIndex(idx)
	if !packed {
		return ss.array[idx].Values()
	}
	if !twoValuePool {
		data := ss.oneStrings[poolIndex]
		if data.value == nil {
			return make(Set, 0)
		}
		return Set{data.value}
	}
	data := ss.twoStrings[poolIndex]
	if data.values[0] == nil {
		return make(Set, 0)
	}
	if data.values[1] == nil {
		return Set{data.values[0]}
	}
	first, second := data.values[0], data.values[1]
	if second.(string) < first.(string) {
		first, second = second, first
	}
	return Set{first, second}
}

func (ss *SetStorage) length(idx int32) (int, bool) {
	poolIndex, twoValuePool, packed := decodePackedStringSetIndex(idx)
	if !packed {
		if idx < 0 || int(idx) >= len(ss.array) || ss.reusables.Has(idx) {
			return 0, false
		}
		return ss.array[idx].Len(), true
	}
	if !twoValuePool {
		if poolIndex < 0 || int(poolIndex) >= len(ss.oneStrings) || ss.oneReusable.Has(poolIndex) {
			return 0, false
		}
		if ss.oneStrings[poolIndex].value != nil {
			return 1, true
		}
		return 0, true
	}
	if poolIndex < 0 || int(poolIndex) >= len(ss.twoStrings) || ss.twoReusable.Has(poolIndex) {
		return 0, false
	}
	if ss.twoStrings[poolIndex].values[0] == nil {
		return 0, true
	}
	if ss.twoStrings[poolIndex].values[1] == nil {
		return 1, true
	}
	return 2, true
}

func (ss *SetStorage) packedData(idx int32) setData {
	values := ss.values(idx)
	data, _ := newSetDataChecked(values)
	return data
}

func setDataFromPlainStrings(values ...interface{}) setData {
	data := setData{items: make(map[string]interface{}, len(values))}
	for _, value := range values {
		key := jsonPlainStringKey(value.(string))
		data.items[key] = value
	}
	return data
}

func (ss *SetStorage) Del(idx int32) {
	if poolIndex, twoValuePool, packed := decodePackedStringSetIndex(idx); packed {
		ss.delPacked(poolIndex, twoValuePool)
		return
	}
	if idx < 0 || int(idx) >= len(ss.array) {
		return
	}
	ss.array[idx] = setData{}
	ss.reusables.Mark(idx)
	ss.array = trimReusableTail(ss.array, &ss.reusables)
}

func (ss *SetStorage) delPacked(idx int32, twoValuePool bool) {
	if twoValuePool {
		if idx < 0 || int(idx) >= len(ss.twoStrings) {
			return
		}
		ss.twoStrings[idx] = packedTwoStringSet{}
		if int(idx) == len(ss.twoStrings)-1 {
			ss.twoStrings = ss.twoStrings[:idx]
			ss.twoStrings = trimReusableTail(ss.twoStrings, &ss.twoReusable)
			return
		}
		ss.twoReusable.Mark(idx)
		ss.twoStrings = trimReusableTail(ss.twoStrings, &ss.twoReusable)
		return
	}
	if idx < 0 || int(idx) >= len(ss.oneStrings) {
		return
	}
	ss.oneStrings[idx] = packedOneStringSet{}
	if int(idx) == len(ss.oneStrings)-1 {
		ss.oneStrings = ss.oneStrings[:idx]
		ss.oneStrings = trimReusableTail(ss.oneStrings, &ss.oneReusable)
		return
	}
	ss.oneReusable.Mark(idx)
	ss.oneStrings = trimReusableTail(ss.oneStrings, &ss.oneReusable)
}

type LevelDBReference struct {
	Key            string
	Type           string
	Store          persistentReferenceStore
	ExpiresAt      *time.Time
	Stats          *KeyStats
	RecordBytes    int
	RecordChecksum uint64
	Token          uint64
}

type levelDBHydrationKey struct {
	index int32
	token uint64
}

type levelDBHydrationCall struct {
	done  chan struct{}
	entry snapshotEntry
	ok    bool
	err   error
}

type LevelDBReferenceStorage struct {
	array     []levelDBReferenceRecord
	reusables reusableIndexes
	stores    []persistentReferenceStore
}

type levelDBReferenceRecord struct {
	key            string
	stats          *KeyStats
	recordChecksum uint64
	token          uint64
	expiresAtSec   int64
	recordBytes    uint32
	expiresAtNsec  uint32
	storeID        uint32
	typeID         uint8
}

const levelDBReferenceNoExpiration = int64(-1 << 63)

func CreateLevelDBReferenceStorage() *LevelDBReferenceStorage {
	return &LevelDBReferenceStorage{array: []levelDBReferenceRecord{}}
}

func (rs *LevelDBReferenceStorage) Append(value LevelDBReference) int32 {
	rs.array = append(rs.array, rs.compact(value))
	return int32(len(rs.array) - 1)
}

func (rs *LevelDBReferenceStorage) Add(value LevelDBReference) int32 {
	if idx, ok := rs.reusables.Take(); ok {
		rs.array[idx] = rs.compact(value)
		return idx
	}
	return rs.Append(value)
}

func (rs *LevelDBReferenceStorage) Get(idx int32) (LevelDBReference, bool) {
	if rs == nil || idx < 0 || int(idx) >= len(rs.array) {
		return LevelDBReference{}, false
	}
	if rs.reusables.Has(idx) {
		return LevelDBReference{}, false
	}
	return rs.expand(rs.array[idx]), true
}

func (rs *LevelDBReferenceStorage) Set(idx int32, value LevelDBReference) bool {
	if rs == nil || idx < 0 || int(idx) >= len(rs.array) || rs.reusables.Has(idx) {
		return false
	}
	rs.array[idx] = rs.compact(value)
	return true
}

func (rs *LevelDBReferenceStorage) Del(idx int32) {
	if rs == nil || idx < 0 || int(idx) >= len(rs.array) {
		return
	}
	rs.array[idx] = levelDBReferenceRecord{}
	rs.reusables.Mark(idx)
	rs.array = trimReusableTail(rs.array, &rs.reusables)
}

func (rs *LevelDBReferenceStorage) compact(value LevelDBReference) levelDBReferenceRecord {
	expiresAtSec := levelDBReferenceNoExpiration
	expiresAtNsec := uint32(0)
	if value.ExpiresAt != nil {
		expiresAtSec = value.ExpiresAt.Unix()
		expiresAtNsec = uint32(value.ExpiresAt.Nanosecond())
	}
	recordBytes := uint32(value.RecordBytes)
	if value.RecordBytes < 0 || uint64(value.RecordBytes) > uint64(^uint32(0)) {
		recordBytes = ^uint32(0)
	}
	return levelDBReferenceRecord{
		key:            value.Key,
		stats:          value.Stats,
		recordChecksum: value.RecordChecksum,
		token:          value.Token,
		expiresAtSec:   expiresAtSec,
		recordBytes:    recordBytes,
		expiresAtNsec:  expiresAtNsec,
		storeID:        rs.storeID(value.Store),
		typeID:         levelDBReferenceTypeID(value.Type),
	}
}

func (rs *LevelDBReferenceStorage) expand(record levelDBReferenceRecord) LevelDBReference {
	var expiresAt *time.Time
	if record.expiresAtSec != levelDBReferenceNoExpiration {
		value := time.Unix(record.expiresAtSec, int64(record.expiresAtNsec)).UTC()
		expiresAt = &value
	}
	var store persistentReferenceStore
	if record.storeID > 0 && int(record.storeID) <= len(rs.stores) {
		store = rs.stores[record.storeID-1]
	}
	return LevelDBReference{
		Key:            record.key,
		Type:           levelDBReferenceType(record.typeID),
		Store:          store,
		ExpiresAt:      expiresAt,
		Stats:          record.stats,
		RecordBytes:    int(record.recordBytes),
		RecordChecksum: record.recordChecksum,
		Token:          record.token,
	}
}

func (rs *LevelDBReferenceStorage) storeID(store persistentReferenceStore) uint32 {
	if store == nil {
		return 0
	}
	for index, existing := range rs.stores {
		if persistentReferenceStoresEqual(existing, store) {
			return uint32(index + 1)
		}
	}
	if uint64(len(rs.stores)) >= uint64(^uint32(0)) {
		panic("hatriecache: too many persistent reference stores")
	}
	rs.stores = append(rs.stores, store)
	return uint32(len(rs.stores))
}

func persistentReferenceStoresEqual(left persistentReferenceStore, right persistentReferenceStore) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	leftType := reflect.TypeOf(left)
	if leftType != reflect.TypeOf(right) || !leftType.Comparable() {
		return false
	}
	return left == right
}

func levelDBReferenceTypeID(value string) uint8 {
	switch value {
	case "counter":
		return 1
	case "string":
		return 2
	case "bytes":
		return 3
	case "map":
		return 4
	case "slice":
		return 5
	case "set":
		return 6
	case "priority_queue":
		return 7
	case "bloom_filter":
		return 8
	case "count_min_sketch":
		return 9
	case "hyperloglog":
		return 10
	case "top_k":
		return 11
	case "cuckoo_filter":
		return 12
	case "roaring_bitmap":
		return 13
	case "quantile_sketch":
		return 14
	case "fenwick_tree":
		return 15
	case "sparse_bitset":
		return 16
	case "reservoir_sample":
		return 17
	case "xor_filter":
		return 18
	case "radix_tree":
		return 19
	default:
		return 0
	}
}

func levelDBReferenceType(id uint8) string {
	switch id {
	case 1:
		return "counter"
	case 2:
		return "string"
	case 3:
		return "bytes"
	case 4:
		return "map"
	case 5:
		return "slice"
	case 6:
		return "set"
	case 7:
		return "priority_queue"
	case 8:
		return "bloom_filter"
	case 9:
		return "count_min_sketch"
	case 10:
		return "hyperloglog"
	case 11:
		return "top_k"
	case 12:
		return "cuckoo_filter"
	case 13:
		return "roaring_bitmap"
	case 14:
		return "quantile_sketch"
	case 15:
		return "fenwick_tree"
	case 16:
		return "sparse_bitset"
	case 17:
		return "reservoir_sample"
	case 18:
		return "xor_filter"
	case 19:
		return "radix_tree"
	default:
		return ""
	}
}

// HatTrie wraps the C HAT-trie and keeps larger Go values in typed backing
// pools referenced by compact HatValue records.
type HatTrie struct {
	mu                         sync.RWMutex
	telemetryMu                sync.Mutex
	snapshotCaptureMu          sync.Mutex
	replicationReadOnlyScanMu  sync.Mutex
	counterWriteStripes        []sync.RWMutex
	counterWriteStripeMask     uint64
	counterFastPathWrites      uint64
	root                       *C.hattrie_t
	strings                    *StringStorage
	raws                       *BytesStorage
	disks                      *DiskStorage
	maps                       *MapStorage
	slices                     *SliceStorage
	sets                       *SetStorage
	priorityQueues             *PriorityQueueStorage
	bloomFilters               *BloomFilterStorage
	countMinSketches           *CountMinSketchStorage
	hyperLogLogs               *HyperLogLogStorage
	topKs                      *TopKStorage
	cuckooFilters              *CuckooFilterStorage
	roaringBitmaps             *RoaringBitmapStorage
	quantileSketches           *QuantileSketchStorage
	fenwickTrees               *FenwickTreeStorage
	sparseBitsets              *SparseBitsetStorage
	reservoirSamples           *ReservoirSampleStorage
	xorFilters                 *XorFilterStorage
	radixTrees                 *RadixTreeStorage
	dbrefs                     *LevelDBReferenceStorage
	hydrationMu                sync.Mutex
	hydrations                 map[levelDBHydrationKey]*levelDBHydrationCall
	nextDBReferenceToken       uint64
	nativeCommandBatchCalls    uint64
	journalScalarBatchCalls    uint64
	nativeCommandBatchScratch  nativeCommandBatchScratch
	localPartitions            atomic.Pointer[localPartitionSet]
	expires                    map[string]uint32
	expirations                expirationHeap
	hotKey                     string
	hotValue                   HatValue
	hotValid                   bool
	stats                      atomicCacheStats
	keyStatsGlobal             CacheStats
	keyStats                   map[string]*trackedKeyStats
	keyStatsMode               KeyStatsMode
	keyStatsCapacity           int
	keyStatsSlots              []string
	keyStatsFree               []uint32
	keyStatsHand               int
	levelDBSpillKeys           map[string]struct{}
	levelDBHotBytes            int64
	levelDBHotValues           map[string]int64
	mutationEpoch              uint64
	memoryCompactionEpoch      uint64
	replicationMerkle          *replicationMerkleIndex
	persistentDirtyTracker     *LevelDBDirtyTracker
	snapshotMutations          *snapshotMutationTracker
	snapshotCapturePageHook    func(int)
	snapshotRestoreStageHook   func(*HatTrie) error
	snapshotRestoreCutoverHook func(time.Duration)
	now                        func() time.Time
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
		strings:          CreateStringStorage(),
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
		expires:          map[string]uint32{},
		keyStats:         map[string]*trackedKeyStats{},
		keyStatsMode:     DefaultKeyStatsMode,
		keyStatsCapacity: 0,
		now:              time.Now,
	}
	ht.stats.initialize()
	runtime.SetFinalizer(ht, (*HatTrie).Destroy)
	return ht, nil
}

func (ht *HatTrie) Destroy() {
	if ht == nil {
		return
	}
	if partitions := ht.localPartitions.Swap(nil); partitions != nil {
		for _, child := range partitions.tries {
			child.Destroy()
		}
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
	ht.strings = nil
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
	ht.hydrationMu.Lock()
	ht.hydrations = nil
	ht.hydrationMu.Unlock()
	ht.nextDBReferenceToken = 0
	ht.expires = nil
	ht.expirations.Clear()
	ht.expirations = nil
	ht.hotKey = ""
	ht.hotValue = HatValue{}
	ht.hotValid = false
	ht.keyStats = nil
	ht.keyStatsMode = ""
	ht.keyStatsCapacity = 0
	ht.keyStatsSlots = nil
	ht.keyStatsFree = nil
	ht.keyStatsHand = 0
	ht.levelDBSpillKeys = nil
	ht.levelDBHotBytes = 0
	ht.levelDBHotValues = nil
	ht.replicationMerkle = nil
	ht.persistentDirtyTracker = nil
	ht.counterWriteStripes = nil
	ht.counterWriteStripeMask = 0
	ht.now = nil
}

// SetPersistentDirtyTracker connects successful in-memory mutations to an
// incremental persistent-store saver. Passing nil disables tracking.
func (ht *HatTrie) SetPersistentDirtyTracker(tracker *LevelDBDirtyTracker) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	if partitions := ht.localPartitionSet(); partitions != nil {
		for _, child := range partitions.tries {
			if err := child.SetPersistentDirtyTracker(tracker); err != nil {
				return err
			}
		}
		return nil
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()
	ht.ensureOpen()
	ht.persistentDirtyTracker = tracker
	return nil
}

// CounterWriteStripingStats describes the optional existing-counter write
// fast path. Stripe count zero means the feature is disabled.
type CounterWriteStripingStats struct {
	Enabled        bool   `json:"enabled"`
	Stripes        int    `json:"stripes"`
	FastPathWrites uint64 `json:"fast_path_writes"`
}

// ConfigureCounterWriteStripes enables a striped fast path for updates to
// existing non-TTL counters. Zero disables it. A power of two from 2 through
// 256 keeps key routing cheap and bounds retained mutex memory.
func (ht *HatTrie) ConfigureCounterWriteStripes(stripes int) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	if err := ValidateCounterWriteStripes(stripes); err != nil {
		return err
	}
	if partitions := ht.localPartitionSet(); partitions != nil {
		for _, child := range partitions.tries {
			if err := child.ConfigureCounterWriteStripes(stripes); err != nil {
				return err
			}
		}
		return nil
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()
	ht.ensureOpen()
	if stripes == 0 {
		ht.counterWriteStripes = nil
		ht.counterWriteStripeMask = 0
		return nil
	}
	ht.counterWriteStripes = make([]sync.RWMutex, stripes)
	ht.counterWriteStripeMask = uint64(stripes - 1)
	return nil
}

// ValidateCounterWriteStripes checks the counter stripe policy without
// changing a trie.
func ValidateCounterWriteStripes(stripes int) error {
	if stripes < 0 || stripes == 1 || stripes > MaxCounterWriteStripes || (stripes != 0 && stripes&(stripes-1) != 0) {
		return fmt.Errorf("hatriecache: counter write stripes must be zero or a power of two from 2 through %d", MaxCounterWriteStripes)
	}
	return nil
}

// CounterWriteStripingStats returns the active stripe policy and cumulative
// number of updates completed through the fast path.
func (ht *HatTrie) CounterWriteStripingStats() CounterWriteStripingStats {
	if ht == nil {
		return CounterWriteStripingStats{}
	}
	if partitions := ht.localPartitionSet(); partitions != nil {
		result := CounterWriteStripingStats{}
		for _, child := range partitions.tries {
			stats := child.CounterWriteStripingStats()
			result.Enabled = result.Enabled || stats.Enabled
			if result.Stripes == 0 {
				result.Stripes = stats.Stripes
			}
			result.FastPathWrites += stats.FastPathWrites
		}
		return result
	}
	ht.mu.RLock()
	defer ht.mu.RUnlock()
	ht.ensureOpen()
	stripes := len(ht.counterWriteStripes)
	return CounterWriteStripingStats{
		Enabled:        stripes != 0,
		Stripes:        stripes,
		FastPathWrites: atomic.LoadUint64(&ht.counterFastPathWrites),
	}
}

func (ht *HatTrie) Size() int {
	if ht == nil {
		return 0
	}
	if partitions := ht.localPartitionSet(); partitions != nil {
		total := 0
		for _, child := range partitions.tries {
			total += child.Size()
		}
		return total
	}
	ht.mu.RLock()
	defer ht.mu.RUnlock()

	ht.ensureOpen()
	return ht.sizeLocked()
}

func (ht *HatTrie) sizeLocked() int {
	return int(C.hattrie_size(ht.root))
}

func (ht *HatTrie) Stats() CacheStats {
	if ht == nil {
		return CacheStats{}
	}
	if partitions := ht.localPartitionSet(); partitions != nil {
		return aggregateLocalPartitionStats(partitions)
	}
	ht.mu.RLock()
	defer ht.mu.RUnlock()
	ht.ensureOpen()
	if ht.keyStatsMode == KeyStatsModeOff {
		return ht.stats.snapshot()
	}
	ht.telemetryMu.Lock()
	defer ht.telemetryMu.Unlock()
	stats := ht.keyStatsGlobal
	stats.updateRates()
	return stats
}

func (ht *HatTrie) cacheStatsLocked() CacheStats {
	if ht.keyStatsMode == KeyStatsModeOff {
		return ht.stats.snapshot()
	}
	stats := ht.keyStatsGlobal
	stats.updateRates()
	return stats
}

func (ht *HatTrie) restoreCacheStatsLocked(stats CacheStats) {
	ht.stats.restore(stats)
	ht.keyStatsGlobal = stats
}

// KeyStatsPolicy returns the active per-key telemetry policy.
func (ht *HatTrie) KeyStatsPolicy() KeyStatsPolicy {
	if ht == nil {
		return KeyStatsPolicy{}
	}
	if partitions := ht.localPartitionSet(); partitions != nil {
		ht.mu.RLock()
		policy := KeyStatsPolicy{Mode: ht.keyStatsMode, Capacity: ht.keyStatsCapacity}
		ht.mu.RUnlock()
		for _, child := range partitions.tries {
			policy.Tracked += child.KeyStatsPolicy().Tracked
		}
		return policy
	}
	ht.mu.RLock()
	defer ht.mu.RUnlock()
	ht.telemetryMu.Lock()
	defer ht.telemetryMu.Unlock()

	ht.ensureOpen()
	return KeyStatsPolicy{
		Mode:     ht.keyStatsMode,
		Capacity: ht.keyStatsCapacity,
		Tracked:  len(ht.keyStats),
	}
}

// ConfigureKeyStats changes per-key telemetry retention. Bounded mode requires
// a positive capacity. Full and off modes ignore capacity and report it as zero.
func (ht *HatTrie) ConfigureKeyStats(mode KeyStatsMode, capacity int) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	if mode != KeyStatsModeBounded && mode != KeyStatsModeFull && mode != KeyStatsModeOff {
		return fmt.Errorf("hatriecache: invalid key stats mode %q", mode)
	}
	if mode == KeyStatsModeBounded && capacity <= 0 {
		return errors.New("hatriecache: bounded key stats capacity must be positive")
	}
	if partitions := ht.localPartitionSet(); partitions != nil {
		if err := configureLocalPartitionKeyStats(partitions, mode, capacity); err != nil {
			return err
		}
		ht.mu.Lock()
		defer ht.mu.Unlock()
		ht.ensureOpen()
		ht.configureKeyStatsLocked(mode, capacity)
		return nil
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()
	ht.ensureOpen()
	ht.configureKeyStatsLocked(mode, capacity)
	return nil
}

func (ht *HatTrie) configureKeyStatsLocked(mode KeyStatsMode, capacity int) {
	if ht.keyStatsMode == KeyStatsModeOff && mode != KeyStatsModeOff {
		ht.keyStatsGlobal = ht.stats.snapshot()
	} else if ht.keyStatsMode != KeyStatsModeOff && mode == KeyStatsModeOff {
		ht.stats.restore(ht.keyStatsGlobal)
	}
	if mode == KeyStatsModeOff {
		ht.keyStats = map[string]*trackedKeyStats{}
		ht.keyStatsMode = mode
		ht.keyStatsCapacity = 0
		ht.keyStatsSlots = nil
		ht.keyStatsFree = nil
		ht.keyStatsHand = 0
		return
	}
	if mode == KeyStatsModeFull {
		ht.keyStatsMode = mode
		ht.keyStatsCapacity = 0
		ht.keyStatsSlots = nil
		ht.keyStatsFree = nil
		ht.keyStatsHand = 0
		for _, stats := range ht.keyStats {
			stats.slot = keyStatsNoSlot
		}
		return
	}

	keys := make([]string, 0, len(ht.keyStats))
	for key := range ht.keyStats {
		keys = append(keys, key)
	}
	sort.SliceStable(keys, func(left int, right int) bool {
		leftStats := ht.keyStats[keys[left]]
		rightStats := ht.keyStats[keys[right]]
		leftSec, leftNsec := leftStats.lastActivity()
		rightSec, rightNsec := rightStats.lastActivity()
		if leftSec == rightSec && leftNsec == rightNsec {
			return keys[left] < keys[right]
		}
		return compactKeyStatsTimeAfter(leftSec, leftNsec, rightSec, rightNsec)
	})
	if len(keys) > capacity {
		for _, key := range keys[capacity:] {
			delete(ht.keyStats, key)
		}
		keys = keys[:capacity]
	}

	ht.keyStatsMode = mode
	ht.keyStatsCapacity = capacity
	ht.keyStatsSlots = make([]string, 0, len(keys))
	ht.keyStatsFree = nil
	ht.keyStatsHand = 0
	for idx, key := range keys {
		stats := ht.keyStats[key]
		stats.slot = uint32(idx)
		ht.keyStatsSlots = append(ht.keyStatsSlots, key)
	}
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
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.StatsForKeyChecked(key)
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	if err := validateKey(key); err != nil {
		return KeyStats{}, false, err
	}
	ht.ensureOpen()
	rawPtr := ht.tryLocation(key)
	if rawPtr == nil {
		ht.removeKeyStatsLocked(key)
		return KeyStats{}, false, nil
	}
	hval := HatValue{}
	hval.fromValue(*rawPtr)
	if ht.expireIfNeededLocked(key, hval) {
		return KeyStats{}, false, nil
	}
	stats, ok := ht.keyStats[key]
	if ok && stats != nil {
		return stats.expanded(), true, nil
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
		ht.removeKeyStatsLocked(key)
		return
	}
	restored := ht.ensureKeyStatsLocked(key)
	if restored == nil {
		return
	}
	restored.restore(*stats)
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
	if partitions := ht.localPartitionSet(); partitions != nil {
		first := partitions.tries[0]
		first.mu.Lock()
		defer first.mu.Unlock()
		first.ensureOpen()
		first.stats.restore(stats)
		first.keyStatsGlobal = stats
		return nil
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	ht.ensureOpen()
	ht.stats.restore(stats)
	ht.keyStatsGlobal = stats
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
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.ExpireChecked(key, ttl)
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
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.ExpireAtChecked(key, at)
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
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.PersistChecked(key)
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
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.TTLChecked(key)
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

	expiresAt, ok := ht.expirationAtLocked(key)
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
	if partitions := ht.localPartitionSet(); partitions != nil {
		results, _ := runLocalPartitionTasks(partitions, func(child *HatTrie) (int, error) {
			return child.VacuumExpired(), nil
		})
		removed := 0
		for _, count := range results {
			removed += count
		}
		return removed
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
	if partitions := ht.localPartitionSet(); partitions != nil {
		results, _ := runLocalPartitionTasks(partitions, func(child *HatTrie) (bool, error) {
			return child.vacuumExpiredIfOpen(), nil
		})
		for _, open := range results {
			if !open {
				return false
			}
		}
		return true
	}
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
	if partitions := ht.localPartitionSet(); partitions != nil {
		if mem.Alloc < maxAllocBytes {
			return true
		}
		results, _ := runLocalPartitionTasks(partitions, func(child *HatTrie) (bool, error) {
			return child.vacuumExpiredIfOpen(), nil
		})
		for _, open := range results {
			if !open {
				return false
			}
		}
		return true
	}

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
	if partitions := ht.localPartitionSet(); partitions != nil {
		return localPartitionKeys(partitions, prefix, sorted)
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
	if partitions := ht.localPartitionSet(); partitions != nil {
		return localPartitionEntries(partitions, prefix, sorted)
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
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.ExistsChecked(key)
	}
	ht.mu.RLock()
	hval, fallback, err := ht.readValueRLockedChecked(key, false)
	if !fallback {
		if err != nil {
			ht.mu.RUnlock()
			return false, err
		}
		hit := !hval.Empty()
		ht.recordReadLocked(hit, key)
		ht.mu.RUnlock()
		return hit, nil
	}
	ht.mu.RUnlock()

	ht.mu.Lock()
	defer ht.mu.Unlock()

	if err := validateKey(key); err != nil {
		return false, err
	}
	hval = ht.peekLocked(key)
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
	if ht.keyStatsMode == KeyStatsModeOff {
		ht.recordGlobalRead(now, hit)
		return
	}

	ht.telemetryMu.Lock()
	defer ht.telemetryMu.Unlock()
	ht.recordGlobalReadSerialized(now, hit)

	for _, key := range keys {
		stats, tracked := ht.keyStats[key]
		if !hit && !tracked {
			continue
		}
		if stats == nil {
			stats = ht.ensureKeyStatsLocked(key)
			if stats == nil {
				continue
			}
		}
		stats.Reads++
		if hit {
			stats.Hits++
			stats.setLastHit(now)
		} else {
			stats.Misses++
			stats.setLastMiss(now)
		}
	}
}

func (ht *HatTrie) recordGlobalRead(now time.Time, hit bool) {
	if hit {
		updateAtomicCacheTime(&ht.stats.lastHit, now)
		ht.stats.hits.Add(1)
		return
	}
	updateAtomicCacheTime(&ht.stats.lastMiss, now)
	ht.stats.misses.Add(1)
}

func (ht *HatTrie) recordGlobalReadSerialized(now time.Time, hit bool) {
	if hit {
		ht.keyStatsGlobal.Hits++
		ht.keyStatsGlobal.Reads++
		ht.keyStatsGlobal.LastHit = now
		return
	}
	ht.keyStatsGlobal.Misses++
	ht.keyStatsGlobal.Reads++
	ht.keyStatsGlobal.LastMiss = now
}

func (ht *HatTrie) recordWriteLocked(keys ...string) {
	if ht.persistentDirtyTracker != nil {
		for _, key := range keys {
			ht.persistentDirtyTracker.Mark(key)
		}
	}
	ht.trackSnapshotMutationsLocked(keys...)
	ht.updateReplicationMerkleLocked(keys...)
	ht.mutationEpoch++
	now := ht.currentTime()
	for _, key := range keys {
		ht.clearHotKeyLocked(key)
		ht.updateLevelDBSpillCandidateForKeyLocked(key)
		ht.updateLevelDBHotByteAccountingForKeyLocked(key)
	}
	if ht.keyStatsMode == KeyStatsModeOff {
		ht.recordGlobalWrite(now)
		return
	}

	ht.telemetryMu.Lock()
	defer ht.telemetryMu.Unlock()
	ht.recordGlobalWriteSerialized(now)

	for _, key := range keys {
		stats := ht.keyStats[key]
		if stats == nil {
			stats = ht.ensureKeyStatsLocked(key)
			if stats == nil {
				continue
			}
		}
		stats.Writes++
		stats.setLastWrite(now)
	}
}

func (ht *HatTrie) recordJournalScalarBatchWritesLocked(records []CommandJournalRecord) {
	if len(records) == 0 {
		return
	}
	for _, record := range records {
		key := strings.TrimSpace(record.Request.Key)
		if ht.persistentDirtyTracker != nil {
			ht.persistentDirtyTracker.Mark(key)
		}
		ht.trackSnapshotMutationsLocked(key)
		ht.updateReplicationMerkleLocked(key)
		ht.mutationEpoch++
		ht.clearHotKeyLocked(key)
		ht.updateLevelDBSpillCandidateForKeyLocked(key)
		ht.updateLevelDBHotByteAccountingForKeyLocked(key)
	}
	now := ht.currentTime()
	if ht.keyStatsMode == KeyStatsModeOff {
		updateAtomicCacheTime(&ht.stats.lastWrite, now)
		ht.stats.writes.Add(uint64(len(records)))
		return
	}

	ht.telemetryMu.Lock()
	defer ht.telemetryMu.Unlock()
	ht.keyStatsGlobal.Writes += uint64(len(records))
	ht.keyStatsGlobal.LastWrite = now
	for _, record := range records {
		key := strings.TrimSpace(record.Request.Key)
		stats := ht.keyStats[key]
		if stats == nil {
			stats = ht.ensureKeyStatsLocked(key)
			if stats == nil {
				continue
			}
		}
		stats.Writes++
		stats.setLastWrite(now)
	}
}

func (ht *HatTrie) recordCompactJournalBatchWritesLocked(records []compactCommandJournalRecord) {
	if len(records) == 0 {
		return
	}
	for _, record := range records {
		key := strings.TrimSpace(record.Key)
		if ht.persistentDirtyTracker != nil {
			ht.persistentDirtyTracker.Mark(key)
		}
		ht.trackSnapshotMutationsLocked(key)
		ht.updateReplicationMerkleLocked(key)
		ht.mutationEpoch++
		ht.clearHotKeyLocked(key)
		ht.updateLevelDBSpillCandidateForKeyLocked(key)
		ht.updateLevelDBHotByteAccountingForKeyLocked(key)
	}
	now := ht.currentTime()
	if ht.keyStatsMode == KeyStatsModeOff {
		updateAtomicCacheTime(&ht.stats.lastWrite, now)
		ht.stats.writes.Add(uint64(len(records)))
		return
	}

	ht.telemetryMu.Lock()
	defer ht.telemetryMu.Unlock()
	ht.keyStatsGlobal.Writes += uint64(len(records))
	ht.keyStatsGlobal.LastWrite = now
	for _, record := range records {
		key := strings.TrimSpace(record.Key)
		stats := ht.keyStats[key]
		if stats == nil {
			stats = ht.ensureKeyStatsLocked(key)
			if stats == nil {
				continue
			}
		}
		stats.Writes++
		stats.setLastWrite(now)
	}
}

func (ht *HatTrie) recordGlobalWrite(now time.Time) {
	updateAtomicCacheTime(&ht.stats.lastWrite, now)
	ht.stats.writes.Add(1)
}

func (ht *HatTrie) recordGlobalWriteSerialized(now time.Time) {
	ht.keyStatsGlobal.Writes++
	ht.keyStatsGlobal.LastWrite = now
}

func (ht *HatTrie) recordDeleteLocked(key string) {
	if ht.persistentDirtyTracker != nil {
		ht.persistentDirtyTracker.Mark(key)
	}
	ht.trackSnapshotMutationsLocked(key)
	ht.updateReplicationMerkleLocked(key)
	if ht.keyStatsMode == KeyStatsModeOff {
		ht.stats.deletes.Add(1)
	} else {
		ht.keyStatsGlobal.Deletes++
	}
	ht.recordWriteLocked()
	ht.deleteLevelDBSpillCandidateLocked(key)
	ht.deleteLevelDBHotByteAccountingLocked(key)
	ht.removeKeyStatsLocked(key)
}

func (ht *HatTrie) recordExpirationLocked(keys ...string) {
	if ht.persistentDirtyTracker != nil {
		for _, key := range keys {
			ht.persistentDirtyTracker.Mark(key)
		}
	}
	ht.trackSnapshotMutationsLocked(keys...)
	ht.updateReplicationMerkleLocked(keys...)
	if ht.keyStatsMode == KeyStatsModeOff {
		ht.stats.expirations.Add(1)
	} else {
		ht.keyStatsGlobal.Expirations++
	}
	ht.recordWriteLocked()
	for _, key := range keys {
		ht.deleteLevelDBSpillCandidateLocked(key)
		ht.deleteLevelDBHotByteAccountingLocked(key)
		ht.removeKeyStatsLocked(key)
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

func clonedUpdatedKeyStats(stats *trackedKeyStats) *KeyStats {
	if stats == nil {
		return nil
	}
	cloned := stats.expanded()
	return &cloned
}

func (ht *HatTrie) ensureKeyStatsLocked(key string) *trackedKeyStats {
	if stats := ht.keyStats[key]; stats != nil {
		return stats
	}
	if ht.keyStatsMode == KeyStatsModeOff {
		return nil
	}

	stats := newTrackedKeyStats()
	if ht.keyStatsMode == KeyStatsModeFull {
		ht.keyStats[key] = stats
		return stats
	}
	if ht.keyStatsCapacity <= 0 {
		return nil
	}

	if freeCount := len(ht.keyStatsFree); freeCount > 0 {
		idx := int(ht.keyStatsFree[freeCount-1])
		ht.keyStatsFree = ht.keyStatsFree[:freeCount-1]
		stats.slot = uint32(idx)
		ht.keyStatsSlots[idx] = key
		ht.keyStats[key] = stats
		return stats
	}
	if len(ht.keyStatsSlots) < ht.keyStatsCapacity {
		stats.slot = uint32(len(ht.keyStatsSlots))
		ht.keyStatsSlots = append(ht.keyStatsSlots, key)
		ht.keyStats[key] = stats
		return stats
	}

	victim := ht.keyStatsVictimSlotLocked()
	oldKey := ht.keyStatsSlots[victim]
	delete(ht.keyStats, oldKey)
	stats.slot = uint32(victim)
	ht.keyStatsSlots[victim] = key
	ht.keyStats[key] = stats
	return stats
}

func (ht *HatTrie) removeKeyStatsLocked(key string) {
	stats := ht.keyStats[key]
	if stats == nil {
		return
	}
	delete(ht.keyStats, key)
	if ht.keyStatsMode != KeyStatsModeBounded || stats.slot == keyStatsNoSlot {
		return
	}
	idx := int(stats.slot)
	if idx < 0 || idx >= len(ht.keyStatsSlots) || ht.keyStatsSlots[idx] != key {
		return
	}
	ht.keyStatsSlots[idx] = ""
	ht.keyStatsFree = append(ht.keyStatsFree, stats.slot)
}

func (ht *HatTrie) keyStatsVictimSlotLocked() int {
	sampleCount := keyStatsEvictionSamples
	if sampleCount > len(ht.keyStatsSlots) {
		sampleCount = len(ht.keyStatsSlots)
	}
	victim := -1
	var victimSec int64
	var victimNsec uint32
	for sample := 0; sample < sampleCount; sample++ {
		idx := ht.keyStatsHand
		ht.keyStatsHand++
		if ht.keyStatsHand == len(ht.keyStatsSlots) {
			ht.keyStatsHand = 0
		}
		key := ht.keyStatsSlots[idx]
		stats := ht.keyStats[key]
		if stats == nil || stats.slot != uint32(idx) {
			return idx
		}
		seconds, nanoseconds := stats.lastActivity()
		if victim < 0 || compactKeyStatsTimeAfter(victimSec, victimNsec, seconds, nanoseconds) {
			victim = idx
			victimSec = seconds
			victimNsec = nanoseconds
		}
	}
	return victim
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

func (ht *HatTrie) visitPackedValuesWithoutStats(keys []byte, records []hatTriePackedKeyRecord, visit func(index int, key string, value HatValue) error) (int, error) {
	return ht.visitPackedValuesWithoutStatsFrom(keys, records, 0, visit)
}

func (ht *HatTrie) visitPackedValuesWithoutStatsFrom(keys []byte, records []hatTriePackedKeyRecord, startIndex int, visit func(index int, key string, value HatValue) error) (int, error) {
	if ht == nil {
		return 0, ErrNilHatTrie
	}
	if startIndex < 0 || startIndex > len(records) {
		return 0, errors.New("hatriecache: packed trie key start is outside records")
	}
	if startIndex == len(records) {
		return 0, nil
	}
	if unsafe.Sizeof(hatTriePackedKeyRecord{}) != unsafe.Sizeof(C.hattrie_key_record_t{}) {
		return 0, errors.New("hatriecache: packed trie key record layout mismatch")
	}
	if uint64(len(keys)) > uint64(math.MaxUint32) {
		return 0, errors.New("hatriecache: packed trie key arena exceeds 4 GiB")
	}
	for _, record := range records[startIndex:] {
		offset := uint64(record.keyOffset)
		length := uint64(record.keyLength)
		if offset > uint64(len(keys)) || length > uint64(len(keys))-offset || length > uint64(maxHATTrieKeyLength) {
			return 0, errors.New("hatriecache: packed trie key is outside arena")
		}
	}

	nativeBatches := 0
	nativeValues := make([]C.value_t, defaultHatTrieScanBatchEntries)
	for start := startIndex; start < len(records); start += defaultHatTrieScanBatchEntries {
		end := start + defaultHatTrieScanBatchEntries
		if end > len(records) {
			end = len(records)
		}
		ht.mu.Lock()
		ht.ensureOpen()
		count := C.hattrie_tryget_batch(
			ht.root,
			(*C.char)(unsafe.Pointer(unsafe.SliceData(keys))),
			C.size_t(len(keys)),
			(*C.hattrie_key_record_t)(unsafe.Pointer(&records[start])),
			C.size_t(end-start),
			unsafe.SliceData(nativeValues),
		)
		runtime.KeepAlive(keys)
		nativeBatches++
		if int(count) != end-start {
			ht.mu.Unlock()
			return nativeBatches, errors.New("hatriecache: native packed trie lookup stopped early")
		}
		for index := start; index < end; index++ {
			record := records[index]
			keyStart := int(record.keyOffset)
			keyBytes := keys[keyStart : keyStart+int(record.keyLength)]
			key := ""
			if len(keyBytes) > 0 {
				key = unsafe.String(unsafe.SliceData(keyBytes), len(keyBytes))
			}
			hval := HatValue{}
			hval.fromValue(nativeValues[index-start])
			if hval.Empty() {
				ht.clearExpirationLocked(key)
				ht.clearHotKeyLocked(key)
			} else if ht.expireIfNeededLocked(key, hval) {
				hval = HatValue{}
				ht.clearHotKeyLocked(key)
			} else {
				ht.cacheValueLocked(key, hval)
			}
			if visit != nil {
				if err := visit(index, key, hval); err != nil {
					ht.mu.Unlock()
					return nativeBatches, err
				}
			}
		}
		ht.mu.Unlock()
	}
	return nativeBatches, nil
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
		ht.strings.Del(hval.Index)
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
	if index, ok := ht.expires[key]; ok {
		ht.expirations.Update(key, at, int(index), ht.expires)
	} else {
		ht.expirations.Push(expirationEntry{key: key, at: at}, ht.expires)
	}
	hval.Flags |= 1 << DATAVALUE_TTL_BIT_SHIFT
	if rawPtr != nil {
		*rawPtr = hval.toValue()
	}
	return hval
}

func (ht *HatTrie) clearExpirationLocked(key string) {
	if _, ok := ht.expires[key]; !ok {
		return
	}
	ht.expirations.Remove(key, ht.expires)
}

func (ht *HatTrie) compactExpirationHeapLocked() {
	if len(ht.expires) == 0 {
		ht.expirations.Clear()
	}
}

func (ht *HatTrie) rebuildExpirationHeapLocked() {
	next := make(expirationHeap, 0, len(ht.expires))
	indexes := make(map[string]uint32, len(ht.expires))
	for _, entry := range ht.expirations {
		next.Push(entry, indexes)
	}
	ht.expirations = next
	ht.expires = indexes
}

func (ht *HatTrie) expireIfNeededLocked(key string, hval HatValue) bool {
	expiresAt, ok := ht.expirationAtLocked(key)
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

func (ht *HatTrie) expirationAtLocked(key string) (time.Time, bool) {
	position, ok := ht.expires[key]
	index := int(position)
	if !ok || index < 0 || index >= len(ht.expirations) || ht.expirations[index].key != key {
		return time.Time{}, false
	}
	return ht.expirations[index].at, true
}

func (ht *HatTrie) expirationTimeLocked(key string) time.Time {
	at, _ := ht.expirationAtLocked(key)
	return at
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
	ht.removeKeyStatsLocked(key)
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
		ht.expirations.Remove(entry.key, ht.expires)

		rawPtr := ht.tryLocation(entry.key)
		if rawPtr == nil {
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
	cursor, err := ht.newScanCursorLocked(prefix, sorted)
	if err != nil {
		return err
	}
	defer cursor.closeLocked(ht)

	for {
		entry, ok := cursor.currentLiveEntryLocked(ht, now)
		if !ok {
			return nil
		}
		if visit != nil {
			if err := visit(entry); err != nil {
				return err
			}
		}
		cursor.consume()
	}
}

type scanExpiredEntry struct {
	key   string
	value HatValue
}

type hatTrieScanCursor struct {
	iter              *C.hattrie_iter_t
	prefix            string
	packedKeys        bool
	keysOnly          bool
	generation        uint64
	loaded            bool
	finished          bool
	entry             Entry
	expired           []scanExpiredEntry
	visited           int
	batchRecords      [defaultHatTrieScanBatchEntries]C.hattrie_iter_record_t
	batchKeys         []byte
	batchKeyArena     []byte
	batchExpandedKeys []byte
	batchKeyOffsets   [defaultHatTrieScanBatchEntries]int
	batchKeyLengths   [defaultHatTrieScanBatchEntries]int
	batchCount        int
	batchIndex        int
	batchRequiredKeys int
	batchFinished     bool
	nativeBatchReads  int
	keyArenaAllocs    int
}

func (ht *HatTrie) newScanCursorLocked(prefix string, sorted bool) (*hatTrieScanCursor, error) {
	return ht.newScanCursorLockedMode(prefix, sorted, false)
}

func (ht *HatTrie) newPackedScanCursorLocked(prefix string, sorted bool) (*hatTrieScanCursor, error) {
	return ht.newScanCursorLockedMode(prefix, sorted, true)
}

func (ht *HatTrie) newPackedKeyOnlyScanCursorLocked(prefix string, sorted bool) (*hatTrieScanCursor, error) {
	return ht.newScanCursorLockedOptions(prefix, sorted, true, true)
}

func (ht *HatTrie) newScanCursorLockedMode(prefix string, sorted bool, packedKeys bool) (*hatTrieScanCursor, error) {
	return ht.newScanCursorLockedOptions(prefix, sorted, packedKeys, false)
}

func (ht *HatTrie) newScanCursorLockedOptions(prefix string, sorted bool, packedKeys bool, keysOnly bool) (*hatTrieScanCursor, error) {
	ht.ensureOpen()
	if err := validateKey(prefix); err != nil {
		return nil, err
	}

	var iter *C.hattrie_iter_t
	if prefix == "" {
		if keysOnly {
			iter = C.hattrie_iter_keys_begin(ht.root, C.bool(sorted))
		} else {
			iter = C.hattrie_iter_begin(ht.root, C.bool(sorted))
		}
	} else {
		cprefix, prefixLen := cKey(prefix)
		if keysOnly {
			iter = C.hattrie_iter_keys_with_prefix(ht.root, C.bool(sorted), cprefix, prefixLen)
		} else {
			iter = C.hattrie_iter_with_prefix(ht.root, C.bool(sorted), cprefix, prefixLen)
		}
		runtime.KeepAlive(prefix)
	}
	return &hatTrieScanCursor{
		iter:       iter,
		prefix:     prefix,
		packedKeys: packedKeys,
		keysOnly:   keysOnly,
		generation: atomic.LoadUint64(&ht.mutationEpoch),
		batchKeys:  make([]byte, defaultHatTrieScanBatchKeyBytes),
	}, nil
}

func (cursor *hatTrieScanCursor) loadCurrentEntry() bool {
	if cursor == nil || cursor.finished {
		return false
	}
	if cursor.loaded {
		return true
	}

	if !cursor.loadNativeBatch() {
		return false
	}
	recordIndex := cursor.batchIndex
	record := cursor.batchRecords[recordIndex]
	cursor.batchIndex++
	keyOffset := cursor.batchKeyOffsets[recordIndex]
	keyLength := cursor.batchKeyLengths[recordIndex]
	key := ""
	if keyLength != 0 {
		key = unsafe.String(unsafe.SliceData(cursor.batchKeyArena[keyOffset:keyOffset+keyLength]), keyLength)
	}
	hval := HatValue{}
	hval.fromValue(record.value)
	cursor.entry = Entry{Key: key, Value: hval}
	cursor.loaded = true
	cursor.visited++
	return true
}

func (cursor *hatTrieScanCursor) loadNativeBatch() bool {
	if cursor == nil || cursor.finished {
		return false
	}
	if cursor.batchIndex < cursor.batchCount {
		return true
	}
	if cursor.batchFinished {
		cursor.finished = true
		return false
	}

	for {
		if cursor.batchRequiredKeys > len(cursor.batchKeys) {
			cursor.batchKeys = make([]byte, cursor.batchRequiredKeys)
		}
		cursor.batchRequiredKeys = 0
		cursor.batchCount = 0
		cursor.batchIndex = 0
		var required C.size_t
		var finished C.bool
		var count C.size_t
		if cursor.keysOnly {
			count = C.hattrie_iter_read_keys_batch(
				cursor.iter,
				(*C.char)(unsafe.Pointer(unsafe.SliceData(cursor.batchKeys))),
				C.size_t(len(cursor.batchKeys)),
				&cursor.batchRecords[0],
				C.size_t(len(cursor.batchRecords)),
				&required,
				&finished,
			)
		} else {
			count = C.hattrie_iter_read_batch(
				cursor.iter,
				(*C.char)(unsafe.Pointer(unsafe.SliceData(cursor.batchKeys))),
				C.size_t(len(cursor.batchKeys)),
				&cursor.batchRecords[0],
				C.size_t(len(cursor.batchRecords)),
				&required,
				&finished,
			)
		}
		runtime.KeepAlive(cursor)
		cursor.nativeBatchReads++
		cursor.batchCount = int(count)
		cursor.batchRequiredKeys = int(required)
		cursor.batchFinished = bool(finished)
		if cursor.batchCount > 0 {
			cursor.prepareBatchKeyArena()
			return true
		}
		if cursor.batchRequiredKeys > len(cursor.batchKeys) {
			continue
		}
		cursor.finished = true
		return false
	}
}

func (cursor *hatTrieScanCursor) prepareBatchKeyArena() {
	if cursor == nil || cursor.batchCount == 0 {
		return
	}
	used := 0
	for index := 0; index < cursor.batchCount; index++ {
		record := cursor.batchRecords[index]
		end := int(record.key_offset) + int(record.key_len)
		if end > used {
			used = end
		}
	}
	if cursor.prefix == "" {
		if cursor.packedKeys {
			cursor.batchKeyArena = cursor.batchKeys[:used]
		} else {
			cursor.batchKeyArena = append([]byte(nil), cursor.batchKeys[:used]...)
			cursor.keyArenaAllocs++
		}
		for index := 0; index < cursor.batchCount; index++ {
			record := cursor.batchRecords[index]
			cursor.batchKeyOffsets[index] = int(record.key_offset)
			cursor.batchKeyLengths[index] = int(record.key_len)
		}
		return
	}

	total := 0
	for index := 0; index < cursor.batchCount; index++ {
		total += len(cursor.prefix) + int(cursor.batchRecords[index].key_len)
	}
	var arena []byte
	if cursor.packedKeys {
		if cap(cursor.batchExpandedKeys) < total {
			cursor.batchExpandedKeys = make([]byte, total)
			cursor.keyArenaAllocs++
		} else {
			cursor.batchExpandedKeys = cursor.batchExpandedKeys[:total]
		}
		arena = cursor.batchExpandedKeys
	} else {
		arena = make([]byte, total)
		cursor.keyArenaAllocs++
	}
	offset := 0
	for index := 0; index < cursor.batchCount; index++ {
		record := cursor.batchRecords[index]
		suffixStart := int(record.key_offset)
		suffixEnd := suffixStart + int(record.key_len)
		cursor.batchKeyOffsets[index] = offset
		cursor.batchKeyLengths[index] = len(cursor.prefix) + int(record.key_len)
		offset += copy(arena[offset:], cursor.prefix)
		offset += copy(arena[offset:], cursor.batchKeys[suffixStart:suffixEnd])
	}
	cursor.batchKeyArena = arena
}

func (cursor *hatTrieScanCursor) currentLiveEntryLocked(ht *HatTrie, now time.Time) (Entry, bool) {
	for cursor.loadCurrentEntry() {
		entry := cursor.entry
		if expiresAt, ok := ht.expirationAtLocked(entry.Key); ok && !now.Before(expiresAt) {
			if cursor.packedKeys {
				entry.Key = strings.Clone(entry.Key)
			}
			cursor.expired = append(cursor.expired, scanExpiredEntry{key: entry.Key, value: entry.Value})
			cursor.consume()
			continue
		}
		return entry, true
	}
	return Entry{}, false
}

func (cursor *hatTrieScanCursor) consume() {
	if cursor == nil || !cursor.loaded {
		return
	}
	cursor.loaded = false
	cursor.entry = Entry{}
}

func (cursor *hatTrieScanCursor) currentEntryEndsScan() bool {
	return cursor != nil && cursor.loaded && cursor.batchIndex == cursor.batchCount && cursor.batchFinished
}

func (cursor *hatTrieScanCursor) closeLocked(ht *HatTrie) {
	if cursor == nil {
		return
	}
	if cursor.iter != nil {
		C.hattrie_iter_free(cursor.iter)
		cursor.iter = nil
	}
	expired := cursor.expired
	cursor.expired = nil
	cursor.loaded = false
	cursor.finished = true
	cursor.entry = Entry{}
	cursor.batchKeys = nil
	cursor.batchKeyArena = nil
	cursor.batchExpandedKeys = nil
	cursor.batchCount = 0
	cursor.batchIndex = 0
	cursor.batchRequiredKeys = 0
	cursor.batchFinished = true
	for _, entry := range expired {
		if ht.deleteKnownLocked(entry.key, entry.value) {
			ht.recordExpirationLocked(entry.key)
		}
	}
}

func (ht *HatTrie) Get(key string) HatValue {
	hval, _ := ht.GetChecked(key)
	return hval
}

func (ht *HatTrie) GetChecked(key string) (HatValue, error) {
	if ht == nil {
		return HatValue{}, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.GetChecked(key)
	}
	ht.mu.RLock()
	hval, fallback, err := ht.readValueRLockedChecked(key, true)
	if !fallback {
		ht.recordReadLocked(err == nil && !hval.Empty(), key)
		ht.mu.RUnlock()
		return hval, err
	}
	ht.mu.RUnlock()

	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err = ht.getLockedChecked(key)
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

// readValueRLockedChecked returns ordinary in-memory values without mutating
// trie state. The caller must retry under the exclusive lock when fallback is
// true so expiration and lazy LevelDB hydration retain their existing behavior.
func (ht *HatTrie) readValueRLockedChecked(key string, hydrateLevelDB bool) (hval HatValue, fallback bool, err error) {
	if err := validateKey(key); err != nil {
		return HatValue{}, false, err
	}
	ht.ensureOpen()
	stripe := ht.counterWriteStripeRLocked(key)
	if stripe != nil {
		stripe.RLock()
		defer stripe.RUnlock()
	}
	if cached, ok := ht.cachedValueLocked(key); ok {
		if ht.readValueNeedsExclusiveLockRLocked(key, cached, hydrateLevelDB) {
			return HatValue{}, true, nil
		}
		return cached, false, nil
	}
	rawPtr := ht.tryLocation(key)
	if rawPtr == nil {
		if _, hasExpiration := ht.expires[key]; hasExpiration {
			return HatValue{}, true, nil
		}
		return HatValue{}, false, nil
	}
	hval.fromValue(*rawPtr)
	if ht.readValueNeedsExclusiveLockRLocked(key, hval, hydrateLevelDB) {
		return HatValue{}, true, nil
	}
	return hval, false, nil
}

func (ht *HatTrie) readValueNeedsExclusiveLockRLocked(key string, hval HatValue, hydrateLevelDB bool) bool {
	if hydrateLevelDB && hval.IsLevelDBReference() {
		return true
	}
	if !hval.HasTtl() {
		return false
	}
	expiresAt, ok := ht.expirationAtLocked(key)
	return !ok || !ht.currentTime().Before(expiresAt)
}

func (ht *HatTrie) counterWriteStripeRLocked(key string) *sync.RWMutex {
	if len(ht.counterWriteStripes) == 0 {
		return nil
	}
	return &ht.counterWriteStripes[xxhash.Sum64String(key)&ht.counterWriteStripeMask]
}

func (ht *HatTrie) canUseCounterWriteFastPathRLocked(key string) bool {
	return len(ht.counterWriteStripes) != 0 &&
		ht.keyStatsMode == KeyStatsModeOff &&
		ht.snapshotMutations == nil &&
		ht.replicationMerkle == nil &&
		ht.levelDBSpillKeys == nil &&
		ht.levelDBHotValues == nil &&
		(!ht.hotValid || ht.hotKey != key)
}

func (ht *HatTrie) tryUpsertCounterStriped(key string, value int32) bool {
	ht.mu.RLock()
	defer ht.mu.RUnlock()
	ht.ensureOpen()
	if !ht.canUseCounterWriteFastPathRLocked(key) {
		return false
	}
	stripe := ht.counterWriteStripeRLocked(key)
	stripe.Lock()
	defer stripe.Unlock()

	rawPtr := ht.tryLocation(key)
	if rawPtr == nil {
		return false
	}
	current := HatValue{}
	current.fromValue(*rawPtr)
	if !current.IsCounter() || current.HasTtl() {
		return false
	}
	*rawPtr = HatValue{Index: value, Flags: DATAVALUE_TYPE_COUNTER}.toValue()
	ht.recordCounterWriteFastPath(key)
	return true
}

func (ht *HatTrie) tryIncrementCounterStriped(key string, by int32, checkOverflow bool) (int32, bool, bool) {
	ht.mu.RLock()
	defer ht.mu.RUnlock()
	ht.ensureOpen()
	if !ht.canUseCounterWriteFastPathRLocked(key) {
		return 0, false, false
	}
	stripe := ht.counterWriteStripeRLocked(key)
	stripe.Lock()
	defer stripe.Unlock()

	rawPtr := ht.tryLocation(key)
	if rawPtr == nil {
		return 0, false, false
	}
	current := HatValue{}
	current.fromValue(*rawPtr)
	if !current.IsCounter() || current.HasTtl() {
		return 0, false, false
	}
	if checkOverflow {
		next := int64(current.Index) + int64(by)
		if next < minCommandInt32 || next > maxCommandInt32 {
			return current.Index, false, true
		}
		current.Index = int32(next)
	} else {
		current.Index += by
	}
	*rawPtr = current.toValue()
	ht.recordCounterWriteFastPath(key)
	return current.Index, true, true
}

func (ht *HatTrie) recordCounterWriteFastPath(key string) {
	if ht.persistentDirtyTracker != nil {
		ht.persistentDirtyTracker.Mark(key)
	}
	atomic.AddUint64(&ht.mutationEpoch, 1)
	now := ht.currentTime()
	updateAtomicCacheTime(&ht.stats.lastWrite, now)
	ht.stats.writes.Add(1)
	atomic.AddUint64(&ht.counterFastPathWrites, 1)
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
	for hval.IsLevelDBReference() {
		ref, ok := ht.dbrefs.Get(hval.Index)
		if !ok || ref.Store == nil {
			ht.deleteKnownLocked(key, hval)
			ht.updateReplicationMerkleLocked(key)
			return HatValue{}, nil
		}

		entry, present, err := ht.loadLevelDBReferenceUnlocked(ref, hval)
		currentPtr := ht.tryLocation(key)
		if currentPtr == nil {
			return HatValue{}, nil
		}
		current := HatValue{}
		current.fromValue(*currentPtr)
		if ht.expireIfNeededLocked(key, current) {
			return HatValue{}, nil
		}
		currentRef, currentRefOK := ht.dbrefs.Get(current.Index)
		if current != hval || !current.IsLevelDBReference() || !currentRefOK || currentRef.Token != ref.Token {
			hval = current
			continue
		}
		if err != nil {
			return HatValue{}, err
		}
		if !present {
			ht.deleteKnownLocked(key, hval)
			ht.updateReplicationMerkleLocked(key)
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
		operation, operationErr := snapshotOperationForEntry(entry)
		if operationErr != nil {
			return HatValue{}, operationErr
		}
		updated, applyErr := ht.applySnapshotOperationLocked(operation)
		if applyErr == nil {
			ht.updateReplicationMerkleLocked(key)
		}
		return updated, applyErr
	}
	return hval, nil
}

// loadLevelDBReferenceUnlocked leaves and reacquires ht.mu around backend I/O.
// The caller must hold ht.mu and receives it held again on return.
func (ht *HatTrie) loadLevelDBReferenceUnlocked(ref LevelDBReference, hval HatValue) (snapshotEntry, bool, error) {
	hydrationKey := levelDBHydrationKey{index: hval.Index, token: ref.Token}
	ht.hydrationMu.Lock()
	call, waiting := ht.hydrations[hydrationKey]
	if !waiting {
		if ht.hydrations == nil {
			ht.hydrations = make(map[levelDBHydrationKey]*levelDBHydrationCall)
		}
		call = &levelDBHydrationCall{done: make(chan struct{})}
		ht.hydrations[hydrationKey] = call
	}
	ht.hydrationMu.Unlock()

	ht.mu.Unlock()
	if waiting {
		<-call.done
	} else {
		call.entry, call.ok, call.err = ref.Store.Entry(ref.Key)
		close(call.done)
	}
	ht.mu.Lock()

	ht.hydrationMu.Lock()
	if ht.hydrations[hydrationKey] == call {
		delete(ht.hydrations, hydrationKey)
	}
	ht.hydrationMu.Unlock()
	return call.entry, call.ok, call.err
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
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.DeleteChecked(key)
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
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.UpsertCounterChecked(key, val)
	}
	if err := validateKey(key); err != nil {
		return err
	}
	if ht.tryUpsertCounterStriped(key, val) {
		return nil
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
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.incrementCounterChecked(key, by, checkOverflow)
	}
	if err := validateKey(key); err != nil {
		return 0, false, err
	}
	if value, updated, handled := ht.tryIncrementCounterStriped(key, by, checkOverflow); handled {
		return value, updated, nil
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
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.GetCounterChecked(key)
	}
	ht.mu.RLock()
	hval, fallback, err := ht.readValueRLockedChecked(key, true)
	if !fallback {
		hit := err == nil && hval.IsCounter()
		value := int32(0)
		if hit {
			value = hval.Index
		}
		ht.recordReadLocked(hit, key)
		ht.mu.RUnlock()
		return value, hit, err
	}
	ht.mu.RUnlock()

	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err = ht.getLockedChecked(key)
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
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.UpsertStringChecked(key, val)
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval, err := ht.upsertReplacementLocation(key)
	if err != nil {
		return err
	}
	if hval.IsStringAtRaws() {
		ht.strings.Put(hval.Index, val)
		ht.clearExpirationLocked(key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return nil
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.strings.Add(val)
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
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.AppendStringChecked(key, str)
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return "", err
	}
	if hval.IsStringAtRaws() {
		old := ht.strings.Get(hval.Index)
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
		ht.strings.Put(hval.Index, next)
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return next, nil
	}

	if rawPtr == nil {
		rawPtr = ht.upsertLocation(key)
	}
	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.strings.Add(str)
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
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.PrependStringChecked(key, str)
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return "", err
	}
	if hval.IsStringAtRaws() {
		old := ht.strings.Get(hval.Index)
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
		ht.strings.Put(hval.Index, next)
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return next, nil
	}

	if rawPtr == nil {
		rawPtr = ht.upsertLocation(key)
	}
	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.strings.Add(str)
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
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.GetStringChecked(key)
	}
	ht.mu.RLock()
	hval, fallback, err := ht.readValueRLockedChecked(key, true)
	if !fallback {
		hit := err == nil && (hval.IsStringAtRaws() || hval.IsCounter())
		var value string
		if hval.IsStringAtRaws() {
			value = ht.strings.Get(hval.Index)
		} else if hval.IsCounter() {
			value = strconv.FormatInt(int64(hval.Index), 10)
		}
		ht.recordReadLocked(hit, key)
		ht.mu.RUnlock()
		return value, hit, err
	}
	ht.mu.RUnlock()

	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err = ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return "", false, err
	}
	ht.recordReadLocked(hval.IsStringAtRaws() || hval.IsCounter(), key)
	if hval.IsStringAtRaws() {
		return ht.strings.Get(hval.Index), true, nil
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
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.UpsertBytesChecked(key, val)
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
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.GetBytesChecked(key)
	}
	ht.mu.RLock()
	hval, fallback, err := ht.readValueRLockedChecked(key, true)
	if !fallback {
		var value []byte
		hit := false
		if err == nil && hval.IsStringAtRaws() {
			value = []byte(ht.strings.Get(hval.Index))
			hit = true
		} else if err == nil && hval.IsBytesAtRaws() {
			if hval.OnDisk() {
				value, err = ht.disks.Get(hval.Index)
			} else {
				value = cloneBytes(ht.raws.array[hval.Index])
			}
			hit = err == nil
		}
		ht.recordReadLocked(hit, key)
		ht.mu.RUnlock()
		return value, err
	}
	ht.mu.RUnlock()

	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err = ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return nil, err
	}
	if hval.IsStringAtRaws() {
		ht.recordReadLocked(true, key)
		return []byte(ht.strings.Get(hval.Index)), nil
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
	if partition := ht.localPartitionForKey(key); partition != nil {
		partition.UpsertMap(key, val)
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
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.UpsertMapChecked(key, val)
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
		hval.Index = ht.maps.putAdaptive(hval.Index, val)
		ht.clearExpirationLocked(key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return nil
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.maps.addAdaptive(val)
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
	if partition := ht.localPartitionForKey(key); partition != nil {
		partition.PutMap(key, subkey, val)
		return
	}
	_ = ht.putMapEntry(key, subkey, val)
}

func (ht *HatTrie) PutMapChecked(key string, subkey string, val interface{}) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.PutMapChecked(key, subkey, val)
	}
	if err := validateMapValue(Map{subkey: val}); err != nil {
		return err
	}
	return ht.putMapEntry(key, subkey, val)
}

func (ht *HatTrie) PutMapEntriesChecked(key string, fields Map) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.PutMapEntriesChecked(key, fields)
	}
	if len(fields) == 0 {
		return nil
	}
	if err := validateMapValue(fields); err != nil {
		return err
	}
	return ht.putMapEntries(key, fields)
}

func (ht *HatTrie) putMapEntry(key string, subkey string, value interface{}) error {
	if ht == nil {
		return ErrNilHatTrie
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return err
	}
	if hval.IsMap() {
		hval.Index = ht.maps.putEntryAdaptive(hval.Index, subkey, value)
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return nil
	}

	if rawPtr == nil {
		rawPtr = ht.upsertLocation(key)
	}
	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.maps.addEntryAdaptive(subkey, value)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_MAP}.toValue()
	ht.recordWriteLocked(key)
	return nil
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
		hval.Index = ht.maps.putEntriesAdaptive(hval.Index, fields)
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return nil
	}

	if rawPtr == nil {
		rawPtr = ht.upsertLocation(key)
	}
	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.maps.addAdaptive(fields)
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
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.PeekMapChecked(key, subkey)
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
	val, exists := ht.maps.peek(hval.Index, subkey)
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
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.TakeMapChecked(key, subkey)
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
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.GetMapChecked(key)
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
	value, ok := ht.maps.clone(hval.Index)
	ht.recordReadLocked(ok, key)
	if !ok {
		return nil, false, nil
	}
	return value, true, nil
}

func (ht *HatTrie) UpsertSlice(key string, val Slice) {
	if ht == nil {
		return
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		partition.UpsertSlice(key, val)
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
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.UpsertSliceChecked(key, val)
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
	if partition := ht.localPartitionForKey(key); partition != nil {
		partition.PushSlice(key, val, vals...)
		return
	}
	_ = ht.pushSlice(key, val, vals...)
}

func (ht *HatTrie) PushSliceChecked(key string, val interface{}, vals ...interface{}) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.PushSliceChecked(key, val, vals...)
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
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.PopSliceChecked(key)
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
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.ShiftSliceChecked(key)
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
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.HeadSliceChecked(key)
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
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.TailSliceChecked(key)
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
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.GetSliceChecked(key)
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
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.UpsertSetChecked(key, val)
	}
	packed, usePacked := packedStringSetFromValues(val)
	var data setData
	if !usePacked {
		var err error
		data, err = newSetDataChecked(val)
		if err != nil {
			return err
		}
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval, err := ht.upsertReplacementLocation(key)
	if err != nil {
		return err
	}
	if hval.IsSet() {
		if usePacked {
			hval.Index = ht.sets.putPacked(hval.Index, packed)
		} else {
			hval.Index = ht.sets.putDataAdaptive(hval.Index, data)
		}
		ht.clearExpirationLocked(key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return nil
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := int32(0)
	if usePacked {
		idx = ht.sets.addPacked(packed)
	} else {
		idx = ht.sets.AddData(data)
	}
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
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.AddSetChecked(key, val, vals...)
	}
	allStrings := setValuesAreStrings(val, vals...)
	var keys []string
	if allStrings {
		if _, ok := checkedBatchSize(1, len(vals)); !ok {
			return 0, errBatchSizeTooLarge
		}
	} else {
		var err error
		keys, err = setItemKeysOne(val, vals...)
		if err != nil {
			return 0, err
		}
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return 0, err
	}
	if hval.IsSet() {
		added := 0
		if allStrings {
			hval.Index, added = ht.sets.addPlainString(hval.Index, val.(string))
			for _, value := range vals {
				var count int
				hval.Index, count = ht.sets.addPlainString(hval.Index, value.(string))
				added += count
			}
		} else {
			hval.Index, added = ht.sets.addGeneric(hval.Index, keys, val, vals...)
		}
		*rawPtr = hval.toValue()
		if added > 0 {
			ht.recordWriteLocked(key)
		}
		return added, nil
	}

	if rawPtr == nil {
		rawPtr = ht.upsertLocation(key)
	}
	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	added := 0
	var idx int32
	if allStrings {
		packed := packedStringSetValue{values: [2]interface{}{val}, length: 1}
		idx = ht.sets.addPacked(packed)
		added = 1
		for _, value := range vals {
			var count int
			idx, count = ht.sets.addPlainString(idx, value.(string))
			added += count
		}
	} else {
		var data setData
		added = data.addOneWithKeys(keys, val, vals...)
		idx = ht.sets.AddData(data)
	}
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
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.RemoveSetChecked(key, val, vals...)
	}
	allStrings := setValuesAreStrings(val, vals...)
	var keys []string
	if allStrings {
		if _, ok := checkedBatchSize(1, len(vals)); !ok {
			return 0, errBatchSizeTooLarge
		}
	} else {
		var err error
		keys, err = setItemKeysOne(val, vals...)
		if err != nil {
			return 0, err
		}
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

	removed := 0
	if allStrings {
		removed = ht.sets.removePlainString(hval.Index, val.(string))
		for _, value := range vals {
			removed += ht.sets.removePlainString(hval.Index, value.(string))
		}
	} else if _, _, packed := decodePackedStringSetIndex(hval.Index); packed {
		if value, ok := val.(string); ok {
			removed += ht.sets.removePlainString(hval.Index, value)
		}
		for _, value := range vals {
			if text, ok := value.(string); ok {
				removed += ht.sets.removePlainString(hval.Index, text)
			}
		}
	} else {
		removed = ht.sets.removeKeys(hval.Index, keys)
	}
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
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.HasSetChecked(key, val)
	}
	text, plainString := val.(string)
	var valueKey string
	if !plainString {
		var err error
		valueKey, err = setItemKey(val)
		if err != nil {
			return false, err
		}
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
	hit := false
	if plainString {
		hit = ht.sets.hasPlainString(hval.Index, text)
	} else {
		hit = ht.sets.hasKey(hval.Index, valueKey)
	}
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
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.GetSetChecked(key)
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
	return ht.sets.values(hval.Index), true, nil
}

func setValuesAreStrings(value interface{}, values ...interface{}) bool {
	if _, ok := value.(string); !ok {
		return false
	}
	for _, value := range values {
		if _, ok := value.(string); !ok {
			return false
		}
	}
	return true
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
	if !jsonPlainStringNeedsCanonicalKey(value) {
		return `"` + value + `"`
	}
	data, _ := json.Marshal(value)
	return string(data)
}

func jsonPlainStringNeedsCanonicalKey(value string) bool {
	for index := 0; index < len(value); index++ {
		character := value[index]
		if character < 0x20 || character >= 0x7f || character == '"' || character == '\\' || character == '<' || character == '>' || character == '&' {
			return true
		}
	}
	return false
}

func jsonPlainStringMatchesKey(value string, key string) bool {
	if jsonPlainStringNeedsCanonicalKey(value) {
		return key == jsonPlainStringKey(value)
	}
	return len(key) == len(value)+2 && key[0] == '"' && key[len(key)-1] == '"' && key[1:len(key)-1] == value
}
