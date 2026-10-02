package hatDataStructure

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

var (
	// ErrMultikeyIndexNil indicates that an operation was attempted on a nil
	// generic multikey index.
	ErrMultikeyIndexNil = errors.New("hatDataStructure: multikey index is nil")
	// ErrMultikeyIndexKeyLimit indicates that one item would create too many
	// postings.
	ErrMultikeyIndexKeyLimit = errors.New("hatDataStructure: multikey index key limit exceeded")
	// ErrMultikeyIndexItemLimit indicates that a new item exceeds MaxItems.
	ErrMultikeyIndexItemLimit = errors.New("hatDataStructure: multikey index item limit exceeded")
	// ErrTypedMultikeyIndexNil indicates that an operation was attempted on a
	// nil extractor-backed multikey index.
	ErrTypedMultikeyIndexNil = errors.New("hatDataStructure: typed multikey index is nil")
	// ErrTypedMultikeyIndexExtractorRequired indicates that no item extractor
	// was supplied.
	ErrTypedMultikeyIndexExtractorRequired = errors.New("hatDataStructure: typed multikey index extractor is required")
)

// MultikeyIndexOptions bounds the number of distinct item keys and indexed
// items. A nonpositive limit means unlimited.
type MultikeyIndexOptions struct {
	MaxKeysPerItem int
	MaxItems       int
}

// MultikeyIndex maps comparable keys to sorted stable item IDs. It supports
// typed scalar, struct, and tuple-like comparable keys without converting them
// to strings. The reverse map makes replacement and deletion exact.
type MultikeyIndex[K comparable] struct {
	mu      sync.RWMutex
	options MultikeyIndexOptions
	byKey   map[K]u64PostingList
	byID    map[uint64][]K
}

// TypedMultikeyIndex extracts a comparable key slice from each item and keeps
// the underlying MultikeyIndex synchronized on every Upsert. It is the
// convenient tuple/array-facing API; callers that already have keys can use
// MultikeyIndex directly.
type TypedMultikeyIndex[T any, K comparable] struct {
	extractor func(T) []K
	index     *MultikeyIndex[K]
}

// NewTypedMultikeyIndex creates an extractor-backed multikey index.
func NewTypedMultikeyIndex[T any, K comparable](extractor func(T) []K, options MultikeyIndexOptions) (*TypedMultikeyIndex[T, K], error) {
	if extractor == nil {
		return nil, ErrTypedMultikeyIndexExtractorRequired
	}
	return &TypedMultikeyIndex[T, K]{
		extractor: extractor,
		index:     NewMultikeyIndex[K](options),
	}, nil
}

// Upsert extracts all keys from value and atomically replaces the item's
// previous postings.
func (index *TypedMultikeyIndex[T, K]) Upsert(id uint64, value T) error {
	if index == nil {
		return ErrTypedMultikeyIndexNil
	}
	return index.index.Set(id, index.extractor(value))
}

// Delete removes an item and all extracted postings.
func (index *TypedMultikeyIndex[T, K]) Delete(id uint64) bool {
	if index == nil {
		return false
	}
	return index.index.Delete(id)
}

// Lookup returns stable item IDs matching key and reuses dst when possible.
func (index *TypedMultikeyIndex[T, K]) Lookup(key K, dst []uint64) []uint64 {
	if index == nil {
		if dst != nil {
			return dst[:0]
		}
		return nil
	}
	return index.index.Lookup(key, dst)
}

// Contains reports whether id is indexed under key.
func (index *TypedMultikeyIndex[T, K]) Contains(key K, id uint64) bool {
	if index == nil {
		return false
	}
	return index.index.Contains(key, id)
}

// Len returns the number of indexed items.
func (index *TypedMultikeyIndex[T, K]) Len() int {
	if index == nil {
		return 0
	}
	return index.index.Len()
}

// KeyCount returns the number of distinct keys with postings.
func (index *TypedMultikeyIndex[T, K]) KeyCount() int {
	if index == nil {
		return 0
	}
	return index.index.KeyCount()
}

// Clear removes all indexed items while retaining the extractor and limits.
func (index *TypedMultikeyIndex[T, K]) Clear() {
	if index == nil {
		return
	}
	index.index.Clear()
}

// NewMultikeyIndex creates an empty bounded typed multikey index.
func NewMultikeyIndex[K comparable](options MultikeyIndexOptions) *MultikeyIndex[K] {
	return &MultikeyIndex[K]{
		options: options,
		byKey:   make(map[K]u64PostingList),
		byID:    make(map[uint64][]K),
	}
}

// Set replaces all keys associated with id. Input keys are deduplicated before
// mutation. Validation and capacity checks happen before existing state is
// changed, so rejected updates are atomic.
func (index *MultikeyIndex[K]) Set(id uint64, keys []K) error {
	if index == nil {
		return ErrMultikeyIndexNil
	}
	if index.options.MaxKeysPerItem > 0 && len(keys) > index.options.MaxKeysPerItem {
		return fmt.Errorf("%w: maximum %d", ErrMultikeyIndexKeyLimit, index.options.MaxKeysPerItem)
	}
	if index.sameKeysFast(id, keys) {
		return nil
	}
	normalized, err := normalizeMultikeyKeys(keys, index.options.MaxKeysPerItem)
	if err != nil {
		return err
	}

	index.mu.Lock()
	defer index.mu.Unlock()
	index.ensureInitializedLocked()
	old, exists := index.byID[id]
	if !exists && index.options.MaxItems > 0 && len(index.byID) >= index.options.MaxItems {
		return fmt.Errorf("%w: maximum %d", ErrMultikeyIndexItemLimit, index.options.MaxItems)
	}
	if multikeyKeysEqual(old, normalized) {
		return nil
	}
	for _, key := range old {
		index.removePostingLocked(key, id)
	}
	if len(normalized) == 0 {
		delete(index.byID, id)
		return nil
	}
	for _, key := range normalized {
		index.insertPostingLocked(key, id)
	}
	index.byID[id] = normalized
	return nil
}

func (index *MultikeyIndex[K]) sameKeysFast(id uint64, keys []K) bool {
	index.mu.RLock()
	defer index.mu.RUnlock()
	old, exists := index.byID[id]
	if !exists || len(old) != len(keys) {
		return false
	}
	for position, key := range keys {
		if old[position] != key {
			return false
		}
	}
	return true
}

// Delete removes id and all of its postings. It reports whether id existed.
func (index *MultikeyIndex[K]) Delete(id uint64) bool {
	if index == nil {
		return false
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	keys, exists := index.byID[id]
	if !exists {
		return false
	}
	for _, key := range keys {
		index.removePostingLocked(key, id)
	}
	delete(index.byID, id)
	return true
}

// Lookup appends sorted matching item IDs to dst and returns the resulting
// slice. A caller-owned destination avoids an allocation on repeated lookups.
func (index *MultikeyIndex[K]) Lookup(key K, dst []uint64) []uint64 {
	if index == nil {
		if dst != nil {
			return dst[:0]
		}
		return nil
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	posting, ok := index.byKey[key]
	if !ok {
		if dst != nil {
			return dst[:0]
		}
		return nil
	}
	return posting.values(dst[:0])
}

// Contains reports whether id is indexed under key.
func (index *MultikeyIndex[K]) Contains(key K, id uint64) bool {
	if index == nil {
		return false
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	posting, ok := index.byKey[key]
	if !ok {
		return false
	}
	if posting.first == id {
		return true
	}
	if posting.rest == nil {
		return false
	}
	if posting.rest.first == id {
		return true
	}
	position := sort.Search(len(posting.rest.rest), func(position int) bool { return posting.rest.rest[position] >= id })
	return position < len(posting.rest.rest) && posting.rest.rest[position] == id
}

// Len returns the number of items with at least one indexed key.
func (index *MultikeyIndex[K]) Len() int {
	if index == nil {
		return 0
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	return len(index.byID)
}

// KeyCount returns the number of distinct keys with at least one posting.
func (index *MultikeyIndex[K]) KeyCount() int {
	if index == nil {
		return 0
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	return len(index.byKey)
}

// Clear removes all items while retaining the configured limits and map
// capacity for reuse.
func (index *MultikeyIndex[K]) Clear() {
	if index == nil {
		return
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	for key := range index.byKey {
		delete(index.byKey, key)
	}
	for id := range index.byID {
		delete(index.byID, id)
	}
}

func (index *MultikeyIndex[K]) ensureInitializedLocked() {
	if index.byKey == nil {
		index.byKey = make(map[K]u64PostingList)
	}
	if index.byID == nil {
		index.byID = make(map[uint64][]K)
	}
}

func (index *MultikeyIndex[K]) removePostingLocked(key K, id uint64) {
	posting, ok := index.byKey[key]
	if !ok {
		return
	}
	next, removed, empty := posting.removeSorted(id)
	if !removed {
		return
	}
	if empty {
		delete(index.byKey, key)
		return
	}
	index.byKey[key] = next
}

func (index *MultikeyIndex[K]) insertPostingLocked(key K, id uint64) {
	if posting, ok := index.byKey[key]; ok {
		index.byKey[key] = posting.insertSorted(id)
		return
	}
	index.byKey[key] = newU64PostingList(id)
}

func normalizeMultikeyKeys[K comparable](keys []K, maxKeys int) ([]K, error) {
	if maxKeys > 0 && len(keys) > maxKeys {
		return nil, fmt.Errorf("%w: maximum %d", ErrMultikeyIndexKeyLimit, maxKeys)
	}
	if len(keys) == 0 {
		return nil, nil
	}
	normalized := make([]K, 0, len(keys))
	if len(keys) <= 8 {
		for _, key := range keys {
			duplicate := false
			for _, existing := range normalized {
				if existing == key {
					duplicate = true
					break
				}
			}
			if !duplicate {
				normalized = append(normalized, key)
			}
		}
		return normalized, nil
	}
	seen := make(map[K]struct{}, len(keys))
	for _, key := range keys {
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		normalized = append(normalized, key)
	}
	return normalized, nil
}

func multikeyKeysEqual[K comparable](left, right []K) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
