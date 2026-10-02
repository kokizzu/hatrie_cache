package hatDataStructure

import (
	"errors"
	"sync"
)

const (
	// DefaultMultiKeyIndexMaxKeysPerEntry bounds the number of distinct keys
	// derived from one record unless a caller provides a smaller limit.
	DefaultMultiKeyIndexMaxKeysPerEntry = 64
	maxMultiKeyIndexKeysPerEntry        = 4096
	multiKeyIndexStackKeys              = 16
)

var (
	// ErrMultiKeyIndexNil indicates that an operation was attempted on a nil
	// index.
	ErrMultiKeyIndexNil = errors.New("hatDataStructure: multi-key index is nil")
	// ErrMultiKeyIndexExtractorRequired indicates that no key extractor was
	// supplied.
	ErrMultiKeyIndexExtractorRequired = errors.New("hatDataStructure: multi-key index extractor is required")
	// ErrMultiKeyIndexLimit indicates that a configured key bound was exceeded.
	ErrMultiKeyIndexLimit = errors.New("hatDataStructure: multi-key index limit exceeded")
)

// MultiKeyIndexOptions controls the initial map sizing and per-record key
// bound. Duplicate elements from one record are indexed only once.
type MultiKeyIndexOptions struct {
	Capacity        int
	MaxKeysPerEntry int
	MaxItems        int
}

// MultiKeyIndexEntry is one exact-match result with its stable ID, matched
// key, and value.
type MultiKeyIndexEntry[T any, K comparable] struct {
	ID    uint64
	Key   K
	Value T
}

type multiKeyIndexEntry[T any, K comparable] struct {
	keys  []K
	value T
}

// MultiKeyIndex maps every distinct key derived from a record to that
// record's stable ID. It uses the same compact singleton/two-item/rest posting
// representation as HashIndex and keeps a reverse ID map for exact updates
// and deletes.
type MultiKeyIndex[T any, K comparable] struct {
	mu              sync.RWMutex
	extractor       func(T) []K
	maxKeysPerEntry int
	maxItems        int
	entries         map[uint64]multiKeyIndexEntry[T, K]
	postings        map[K]u64PostingList
}

// NewMultiKeyIndex creates an empty typed multikey index.
func NewMultiKeyIndex[T any, K comparable](extractor func(T) []K, options MultiKeyIndexOptions) (*MultiKeyIndex[T, K], error) {
	if extractor == nil {
		return nil, ErrMultiKeyIndexExtractorRequired
	}
	if options.Capacity < 0 {
		options.Capacity = 0
	}
	if options.MaxItems < 0 {
		return nil, ErrMultiKeyIndexLimit
	}
	maxKeys := options.MaxKeysPerEntry
	if maxKeys == 0 {
		maxKeys = DefaultMultiKeyIndexMaxKeysPerEntry
	}
	if maxKeys < 1 || maxKeys > maxMultiKeyIndexKeysPerEntry {
		return nil, ErrMultiKeyIndexLimit
	}
	return &MultiKeyIndex[T, K]{
		extractor:       extractor,
		maxKeysPerEntry: maxKeys,
		maxItems:        options.MaxItems,
		entries:         make(map[uint64]multiKeyIndexEntry[T, K], options.Capacity),
		postings:        make(map[K]u64PostingList, options.Capacity),
	}, nil
}

// Upsert inserts or replaces a value for id. A record with repeated keys is
// represented once per distinct key. An oversized input is rejected before
// any index state changes.
func (index *MultiKeyIndex[T, K]) Upsert(id uint64, value T) error {
	if index == nil {
		return ErrMultiKeyIndexNil
	}
	rawKeys := index.extractor(value)
	if len(rawKeys) > index.maxKeysPerEntry {
		return ErrMultiKeyIndexLimit
	}

	var stackKeys [multiKeyIndexStackKeys]K
	keys := stackKeys[:0]
	for _, key := range rawKeys {
		duplicate := false
		for _, existingKey := range keys {
			if existingKey == key {
				duplicate = true
				break
			}
		}
		if !duplicate {
			keys = append(keys, key)
		}
	}

	index.mu.Lock()
	defer index.mu.Unlock()
	index.ensureInitializedLocked()

	current, exists := index.entries[id]
	if !exists && index.maxItems > 0 && len(index.entries) >= index.maxItems {
		return ErrMultiKeyIndexLimit
	}
	if exists && multiKeyIndexKeySetsEqual(current.keys, keys) {
		index.entries[id] = multiKeyIndexEntry[T, K]{keys: current.keys, value: value}
		return nil
	}

	var storedKeys []K
	if exists && len(current.keys) == len(keys) {
		storedKeys = current.keys
	} else if len(keys) > 0 {
		storedKeys = make([]K, len(keys))
	}
	if exists {
		for _, key := range current.keys {
			index.removeKeyLocked(key, id)
		}
	}
	copy(storedKeys, keys)
	for _, key := range storedKeys {
		index.insertPostingLocked(key, id)
	}
	index.entries[id] = multiKeyIndexEntry[T, K]{keys: storedKeys, value: value}
	return nil
}

// Delete removes id and reports whether it was present.
func (index *MultiKeyIndex[T, K]) Delete(id uint64) bool {
	if index == nil {
		return false
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	entry, exists := index.entries[id]
	if !exists {
		return false
	}
	delete(index.entries, id)
	for _, key := range entry.keys {
		index.removeKeyLocked(key, id)
	}
	return true
}

// LookupOne returns the lowest stable ID matching key. Use Lookup for all
// matching values.
func (index *MultiKeyIndex[T, K]) LookupOne(key K) (MultiKeyIndexEntry[T, K], bool) {
	if index == nil {
		return MultiKeyIndexEntry[T, K]{}, false
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	posting, ok := index.postings[key]
	if !ok {
		return MultiKeyIndexEntry[T, K]{}, false
	}
	entry, ok := index.entries[posting.first]
	if !ok {
		return MultiKeyIndexEntry[T, K]{}, false
	}
	return MultiKeyIndexEntry[T, K]{ID: posting.first, Key: key, Value: entry.value}, true
}

// Contains reports whether key has at least one indexed ID.
func (index *MultiKeyIndex[T, K]) Contains(key K) bool {
	if index == nil {
		return false
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	_, ok := index.postings[key]
	return ok
}

// ContainsID reports whether id is currently present in the posting for key.
func (index *MultiKeyIndex[T, K]) ContainsID(key K, id uint64) bool {
	if index == nil {
		return false
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	posting, ok := index.postings[key]
	if !ok {
		return false
	}
	if posting.rest == nil {
		return posting.first == id
	}
	if posting.first == id || posting.rest.first == id {
		return true
	}
	for _, candidate := range posting.rest.rest {
		if candidate == id {
			return true
		}
	}
	return false
}

// Lookup returns values whose derived key equals key in stable ID order.
func (index *MultiKeyIndex[T, K]) Lookup(key K) []T {
	return index.LookupInto(key, nil)
}

// LookupInto resets dst and appends values whose derived key equals key. A
// caller-owned destination avoids result allocation on repeated lookups.
func (index *MultiKeyIndex[T, K]) LookupInto(key K, dst []T) []T {
	dst = dst[:0]
	if index == nil {
		return dst
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	posting, ok := index.postings[key]
	if !ok {
		return dst
	}
	if posting.rest == nil {
		if entry, exists := index.entries[posting.first]; exists {
			return append(dst, entry.value)
		}
		return dst
	}
	if entry, exists := index.entries[posting.first]; exists {
		dst = append(dst, entry.value)
	}
	if entry, exists := index.entries[posting.rest.first]; exists {
		dst = append(dst, entry.value)
	}
	for _, id := range posting.rest.rest {
		if entry, exists := index.entries[id]; exists {
			dst = append(dst, entry.value)
		}
	}
	return dst
}

// LookupIDs returns stable IDs whose derived key equals key.
func (index *MultiKeyIndex[T, K]) LookupIDs(key K) []uint64 {
	return index.LookupIDsInto(key, nil)
}

// LookupIDsInto resets dst and appends stable IDs whose derived key equals
// key. A caller-owned destination avoids result allocation on repeated
// lookups.
func (index *MultiKeyIndex[T, K]) LookupIDsInto(key K, dst []uint64) []uint64 {
	dst = dst[:0]
	if index == nil {
		return dst
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	posting, ok := index.postings[key]
	if !ok {
		return dst
	}
	return posting.values(dst)
}

// Len returns the number of indexed IDs, including records with no keys.
func (index *MultiKeyIndex[T, K]) Len() int {
	if index == nil {
		return 0
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	return len(index.entries)
}

// DistinctKeys returns the number of keys with at least one indexed ID.
func (index *MultiKeyIndex[T, K]) DistinctKeys() int {
	if index == nil {
		return 0
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	return len(index.postings)
}

// Clear removes all entries while retaining the extractor and key bound.
func (index *MultiKeyIndex[T, K]) Clear() {
	if index == nil {
		return
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	index.entries = nil
	index.postings = nil
}

func (index *MultiKeyIndex[T, K]) ensureInitializedLocked() {
	if index.entries == nil {
		index.entries = make(map[uint64]multiKeyIndexEntry[T, K])
	}
	if index.postings == nil {
		index.postings = make(map[K]u64PostingList)
	}
}

func (index *MultiKeyIndex[T, K]) removeKeyLocked(key K, id uint64) {
	posting, ok := index.postings[key]
	if !ok {
		return
	}
	next, removed, empty := posting.removeSorted(id)
	if !removed {
		return
	}
	if empty {
		delete(index.postings, key)
		return
	}
	index.postings[key] = next
}

func (index *MultiKeyIndex[T, K]) insertPostingLocked(key K, id uint64) {
	if posting, ok := index.postings[key]; ok {
		index.postings[key] = posting.insertSorted(id)
		return
	}
	index.postings[key] = newU64PostingList(id)
}

func multiKeyIndexKeySetsEqual[K comparable](left, right []K) bool {
	if len(left) != len(right) {
		return false
	}
	for _, leftKey := range left {
		found := false
		for _, rightKey := range right {
			if leftKey == rightKey {
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
