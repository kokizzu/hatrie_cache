package hatDataStructure

import (
	"errors"
	"sort"
	"sync"
)

var (
	// ErrHashIndexNil indicates that an operation was attempted on a nil index.
	ErrHashIndexNil = errors.New("hatDataStructure: hash index is nil")
	// ErrHashIndexExtractorRequired indicates that no key extractor was supplied.
	ErrHashIndexExtractorRequired = errors.New("hatDataStructure: hash index extractor is required")
	// ErrHashIndexDuplicateKey indicates that a unique index already owns a key.
	ErrHashIndexDuplicateKey = errors.New("hatDataStructure: hash index key already exists")
)

// HashIndexOptions controls the index mode and initial map sizing. Unique
// indexes reject a second ID for an existing key; non-unique indexes maintain
// a sorted posting list for every key.
type HashIndexOptions struct {
	Unique   bool
	Capacity int
}

// HashIndexEntry is one exact-match result with its stable ID and derived key.
type HashIndexEntry[T any, K comparable] struct {
	ID    uint64
	Key   K
	Value T
}

type hashIndexEntry[T any, K comparable] struct {
	key   K
	value T
}

// HashIndex is a typed exact-match secondary index. It uses a hash map for
// key lookup and keeps a reverse ID map so updates and deletes are exact.
type HashIndex[T any, K comparable] struct {
	mu          sync.RWMutex
	extractor   func(T) K
	unique      bool
	entries     map[uint64]hashIndexEntry[T, K]
	uniqueByKey map[K]uint64
	postings    map[K][]uint64
}

// NewHashIndex creates an empty typed hash index.
func NewHashIndex[T any, K comparable](extractor func(T) K, options HashIndexOptions) (*HashIndex[T, K], error) {
	if extractor == nil {
		return nil, ErrHashIndexExtractorRequired
	}
	if options.Capacity < 0 {
		options.Capacity = 0
	}
	index := &HashIndex[T, K]{
		extractor: extractor,
		unique:    options.Unique,
		entries:   make(map[uint64]hashIndexEntry[T, K], options.Capacity),
	}
	if options.Unique {
		index.uniqueByKey = make(map[K]uint64, options.Capacity)
	} else {
		index.postings = make(map[K][]uint64, options.Capacity)
	}
	return index, nil
}

// Upsert inserts or replaces a value for id. Unique-key conflicts are checked
// before any state changes, so a rejected update leaves the old posting intact.
func (index *HashIndex[T, K]) Upsert(id uint64, value T) error {
	if index == nil {
		return ErrHashIndexNil
	}
	key := index.extractor(value)
	index.mu.Lock()
	defer index.mu.Unlock()
	index.ensureInitializedLocked()
	current, exists := index.entries[id]
	if index.unique {
		if owner, occupied := index.uniqueByKey[key]; occupied && owner != id {
			return ErrHashIndexDuplicateKey
		}
	}
	if exists && current.key != key {
		index.removeKeyLocked(current.key, id)
	}
	if !exists {
		if index.unique {
			index.uniqueByKey[key] = id
		} else {
			index.postings[key] = insertHashIndexID(index.postings[key], id)
		}
	} else if current.key != key {
		if index.unique {
			index.uniqueByKey[key] = id
		} else {
			index.postings[key] = insertHashIndexID(index.postings[key], id)
		}
	}
	index.entries[id] = hashIndexEntry[T, K]{key: key, value: value}
	return nil
}

// Delete removes id and reports whether it was present.
func (index *HashIndex[T, K]) Delete(id uint64) bool {
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
	index.removeKeyLocked(entry.key, id)
	return true
}

// LookupOne returns the exact entry for key. In non-unique mode it returns the
// lowest stable ID, making the result deterministic; use Lookup for all hits.
func (index *HashIndex[T, K]) LookupOne(key K) (HashIndexEntry[T, K], bool) {
	if index == nil {
		return HashIndexEntry[T, K]{}, false
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	if index.unique {
		id, ok := index.uniqueByKey[key]
		if !ok {
			return HashIndexEntry[T, K]{}, false
		}
		entry := index.entries[id]
		return HashIndexEntry[T, K]{ID: id, Key: entry.key, Value: entry.value}, true
	}
	ids := index.postings[key]
	if len(ids) == 0 {
		return HashIndexEntry[T, K]{}, false
	}
	id := ids[0]
	entry := index.entries[id]
	return HashIndexEntry[T, K]{ID: id, Key: entry.key, Value: entry.value}, true
}

// Contains reports whether key has at least one indexed ID.
func (index *HashIndex[T, K]) Contains(key K) bool {
	if index == nil {
		return false
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	if index.unique {
		_, ok := index.uniqueByKey[key]
		return ok
	}
	return len(index.postings[key]) != 0
}

// Lookup returns values whose derived key equals key. Values are ordered by
// stable ID for both unique and non-unique indexes.
func (index *HashIndex[T, K]) Lookup(key K) []T {
	return index.LookupInto(key, nil)
}

// LookupInto resets dst and appends values whose derived key equals key. A
// caller-owned destination avoids result allocation on repeated lookups.
func (index *HashIndex[T, K]) LookupInto(key K, dst []T) []T {
	dst = dst[:0]
	if index == nil {
		return dst
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	if index.unique {
		id, ok := index.uniqueByKey[key]
		if !ok {
			return dst
		}
		return append(dst, index.entries[id].value)
	}
	for _, id := range index.postings[key] {
		dst = append(dst, index.entries[id].value)
	}
	return dst
}

// LookupIDs returns stable IDs whose derived key equals key.
func (index *HashIndex[T, K]) LookupIDs(key K) []uint64 {
	return index.LookupIDsInto(key, nil)
}

// LookupIDsInto resets dst and appends stable IDs whose derived key equals key.
func (index *HashIndex[T, K]) LookupIDsInto(key K, dst []uint64) []uint64 {
	dst = dst[:0]
	if index == nil {
		return dst
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	if index.unique {
		if id, ok := index.uniqueByKey[key]; ok {
			dst = append(dst, id)
		}
		return dst
	}
	return append(dst, index.postings[key]...)
}

// Len returns the number of indexed IDs.
func (index *HashIndex[T, K]) Len() int {
	if index == nil {
		return 0
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	return len(index.entries)
}

// DistinctKeys returns the number of keys with at least one indexed ID.
func (index *HashIndex[T, K]) DistinctKeys() int {
	if index == nil {
		return 0
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	if index.unique {
		return len(index.uniqueByKey)
	}
	return len(index.postings)
}

// Clear removes all entries while retaining the extractor and uniqueness mode.
func (index *HashIndex[T, K]) Clear() {
	if index == nil {
		return
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	index.entries = nil
	index.uniqueByKey = nil
	index.postings = nil
}

func (index *HashIndex[T, K]) ensureInitializedLocked() {
	if index.entries == nil {
		index.entries = make(map[uint64]hashIndexEntry[T, K])
	}
	if index.unique {
		if index.uniqueByKey == nil {
			index.uniqueByKey = make(map[K]uint64)
		}
		return
	}
	if index.postings == nil {
		index.postings = make(map[K][]uint64)
	}
}

func (index *HashIndex[T, K]) removeKeyLocked(key K, id uint64) {
	if index.unique {
		delete(index.uniqueByKey, key)
		return
	}
	ids := index.postings[key]
	position := sort.Search(len(ids), func(position int) bool { return ids[position] >= id })
	if position >= len(ids) || ids[position] != id {
		return
	}
	copy(ids[position:], ids[position+1:])
	ids[len(ids)-1] = 0
	ids = ids[:len(ids)-1]
	if len(ids) == 0 {
		delete(index.postings, key)
		return
	}
	index.postings[key] = ids
}

func insertHashIndexID(ids []uint64, id uint64) []uint64 {
	position := sort.Search(len(ids), func(position int) bool { return ids[position] >= id })
	if position < len(ids) && ids[position] == id {
		return ids
	}
	ids = append(ids, 0)
	copy(ids[position+1:], ids[position:])
	ids[position] = id
	return ids
}
