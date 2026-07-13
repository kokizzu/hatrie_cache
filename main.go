package hatriecache

/*
#cgo CFLAGS: -std=c99 -Wall -Wextra -I${SRCDIR}/luikore__hat-trie/src
#include <stdlib.h>
#include "luikore__hat-trie/src/hat-trie.h"
*/
import "C"

import (
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
	DATAVALUE_TYPE_SLICE
	// TODO: add more types (deque, priority queue, etc).
)

type Map = map[string]interface{}
type Slice = []interface{}

const NoTTL time.Duration = -1

type Entry struct {
	Key   string
	Value HatValue
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
	array     []Slice
	reusables map[int32]bool
}

func CreateSliceStorage() *SliceStorage {
	return &SliceStorage{
		array:     []Slice{},
		reusables: map[int32]bool{},
	}
}

func (ss *SliceStorage) Put(idx int32, value Slice) {
	if idx < 0 || int(idx) >= len(ss.array) {
		return
	}
	ss.array[idx] = cloneSlice(value)
	delete(ss.reusables, idx)
}

func (ss *SliceStorage) Append(value Slice) int32 {
	ss.array = append(ss.array, cloneSlice(value))
	return int32(len(ss.array) - 1)
}

func (ss *SliceStorage) Add(value Slice) int32 {
	if len(ss.reusables) > 0 {
		for idx := range ss.reusables {
			ss.array[idx] = cloneSlice(value)
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
	ss.array[idx] = nil
	ss.reusables[idx] = true
}

// HatTrie wraps the C HAT-trie and keeps larger Go values in typed backing
// pools referenced by compact HatValue records.
type HatTrie struct {
	mu      sync.RWMutex
	root    *C.hattrie_t
	raws    *BytesStorage
	maps    *MapStorage
	slices  *SliceStorage
	expires map[string]time.Time
	now     func() time.Time
}

func CreateHatTrie() *HatTrie {
	ht := &HatTrie{
		root:    C.hattrie_create(),
		raws:    CreateBytesStorage(),
		maps:    CreateMapStorage(),
		slices:  CreateSliceStorage(),
		expires: map[string]time.Time{},
		now:     time.Now,
	}
	runtime.SetFinalizer(ht, (*HatTrie).Destroy)
	return ht
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
	ht.root = nil
	ht.raws = nil
	ht.maps = nil
	ht.slices = nil
	ht.expires = nil
	ht.now = nil
}

func (ht *HatTrie) Size() int {
	ht.mu.RLock()
	defer ht.mu.RUnlock()

	ht.ensureOpen()
	return int(C.hattrie_size(ht.root))
}

// Expire sets a relative TTL for an existing key. A non-positive TTL deletes
// an existing key immediately. It returns false when the key is missing or has
// already expired.
func (ht *HatTrie) Expire(key string, ttl time.Duration) bool {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	if ttl <= 0 {
		return ht.deleteLocked(key)
	}
	return ht.expireAtLocked(key, ht.currentTime().Add(ttl))
}

// ExpireAt sets an absolute expiration time for an existing key. Expiration is
// enforced when the key is read or mutated.
func (ht *HatTrie) ExpireAt(key string, at time.Time) bool {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	return ht.expireAtLocked(key, at)
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
		return NoTTL
	}

	hval := HatValue{}
	hval.fromValue(*rawPtr)
	if ht.expireIfNeededLocked(key, hval) {
		return NoTTL
	}

	expiresAt, ok := ht.expires[key]
	if !ok {
		return NoTTL
	}
	ttl := expiresAt.Sub(ht.currentTime())
	if ttl <= 0 {
		ht.deleteKnownLocked(key, hval)
		return NoTTL
	}
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
	case DATAVALUE_TYPE_RAW_BYTES, DATAVALUE_TYPE_RAW_STRING:
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
	return ht.deleteKnownLocked(key, hval)
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
		ht.deleteKnownLocked(entry.key, entry.value)
	}

	return entries
}

func (ht *HatTrie) Get(key string) HatValue {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	return ht.getLocked(key)
}

func (ht *HatTrie) getLocked(key string) HatValue {
	iter := ht.tryLocation(key)
	hval := HatValue{}
	if iter != nil {
		hval.fromValue(*iter)
		if ht.expireIfNeededLocked(key, hval) {
			return HatValue{}
		}
	} else {
		delete(ht.expires, key)
	}
	return hval
}

// Delete removes key and returns whether it existed.
func (ht *HatTrie) Delete(key string) bool {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	return ht.deleteLocked(key)
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
}

// GetCounter returns 0 if key is missing or does not hold a counter.
func (ht *HatTrie) GetCounter(key string) int32 {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
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
		return
	}

	ht.returnStorage(hval)
	delete(ht.expires, key)
	idx := ht.raws.Add([]byte(val))
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_RAW_STRING}.toValue()
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
		return
	}

	ht.returnStorage(hval)
	delete(ht.expires, key)
	idx := ht.raws.Add([]byte(str))
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_RAW_STRING}.toValue()
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
		return
	}

	ht.returnStorage(hval)
	delete(ht.expires, key)
	idx := ht.raws.Add([]byte(str))
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_RAW_STRING}.toValue()
}

// GetString returns an empty string if key is missing or not a string/counter.
func (ht *HatTrie) GetString(key string) string {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
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
		ht.raws.Put(hval.Index, val)
		delete(ht.expires, key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		*rawPtr = hval.toValue()
		return
	}

	ht.returnStorage(hval)
	delete(ht.expires, key)
	idx := ht.raws.Add(val)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_RAW_BYTES}.toValue()
}

// GetBytes returns nil if key is missing or not a string/bytes value.
func (ht *HatTrie) GetBytes(key string) []byte {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	if hval.IsStringAtRaws() || hval.IsBytesAtRaws() {
		return cloneBytes(ht.raws.array[hval.Index])
	}
	return nil
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
		return
	}

	ht.returnStorage(hval)
	delete(ht.expires, key)
	idx := ht.maps.Add(val)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_MAP}.toValue()
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
		return
	}

	ht.returnStorage(hval)
	delete(ht.expires, key)
	idx := ht.maps.Add(Map{subkey: val})
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_MAP}.toValue()
}

func (ht *HatTrie) PeekMap(key, subkey string) interface{} {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	m, ok := ht.mapRefLocked(key)
	if !ok {
		return nil
	}
	return m[subkey]
}

func (ht *HatTrie) TakeMap(key, subkey string) interface{} {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	m, ok := ht.mapRefLocked(key)
	if !ok {
		return nil
	}
	val := m[subkey]
	delete(m, subkey)
	return val
}

func (ht *HatTrie) GetMap(key string) Map {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	m, ok := ht.mapRefLocked(key)
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
		return
	}

	ht.returnStorage(hval)
	delete(ht.expires, key)
	idx := ht.slices.Add(val)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_SLICE}.toValue()
}

func (ht *HatTrie) PushSlice(key string, val interface{}, vals ...interface{}) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertFreshLocation(key)
	if hval.IsSlice() {
		old := ht.slices.array[hval.Index]
		old = append(old, val)
		if len(vals) > 0 {
			old = append(old, vals...)
		}
		ht.slices.Put(hval.Index, old)
		*rawPtr = hval.toValue()
		return
	}

	ht.returnStorage(hval)
	delete(ht.expires, key)
	arr := Slice{val}
	if len(vals) > 0 {
		arr = append(arr, vals...)
	}
	idx := ht.slices.Add(arr)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_SLICE}.toValue()
}

func (ht *HatTrie) PopSlice(key string) interface{} {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	if !hval.IsSlice() {
		return nil
	}

	old := ht.slices.array[hval.Index]
	last := len(old) - 1
	if last < 0 {
		return nil
	}
	val := old[last]
	ht.slices.Put(hval.Index, old[:last])
	return val
}

func (ht *HatTrie) ShiftSlice(key string) interface{} {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	if !hval.IsSlice() {
		return nil
	}

	old := ht.slices.array[hval.Index]
	if len(old) == 0 {
		return nil
	}
	val := old[0]
	ht.slices.Put(hval.Index, old[1:])
	return val
}

func (ht *HatTrie) HeadSlice(key string) interface{} {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	sl, ok := ht.sliceRefLocked(key)
	if ok && len(sl) > 0 {
		return sl[0]
	}
	return nil
}

func (ht *HatTrie) TailSlice(key string) interface{} {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	sl, ok := ht.sliceRefLocked(key)
	if ok && len(sl) > 0 {
		return sl[len(sl)-1]
	}
	return nil
}

func (ht *HatTrie) GetSlice(key string) Slice {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	sl, ok := ht.sliceRefLocked(key)
	if !ok {
		return nil
	}
	return cloneSlice(sl)
}

func (ht *HatTrie) sliceRefLocked(key string) (Slice, bool) {
	hval := ht.getLocked(key)
	if hval.IsSlice() {
		return ht.slices.array[hval.Index], true
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
