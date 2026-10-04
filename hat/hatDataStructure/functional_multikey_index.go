package hatDataStructure

import (
	"fmt"
	"sort"
	"sync"
)

// FunctionalMultikeyIndexOptions bounds the state held by a
// FunctionalMultikeyIndex. A nonpositive limit means unlimited.
type FunctionalMultikeyIndexOptions struct {
	MaxKeysPerItem int
	MaxItems       int
}

// FunctionalMultikeyIndex maps every comparable key extracted from a value to
// sorted item IDs. It keeps only keys and IDs, so callers can maintain this
// sidecar next to a row store or another value-bearing index.
type FunctionalMultikeyIndex[T any, K comparable] struct {
	mu        sync.RWMutex
	extractor func(T) []K
	options   FunctionalMultikeyIndexOptions
	byKey     map[K]u64PostingList
	byID      map[uint64][]K
}

// NewFunctionalMultikeyIndex creates an empty typed multikey index.
func NewFunctionalMultikeyIndex[T any, K comparable](extractor func(T) []K, options FunctionalMultikeyIndexOptions) (*FunctionalMultikeyIndex[T, K], error) {
	if extractor == nil {
		return nil, fmt.Errorf("functional multikey index extractor is nil")
	}
	return &FunctionalMultikeyIndex[T, K]{
		extractor: extractor,
		options:   options,
		byKey:     make(map[K]u64PostingList),
		byID:      make(map[uint64][]K),
	}, nil
}

// Upsert replaces all extracted keys associated with id. Input keys are
// deduplicated in first-seen order before mutation. Validation and capacity
// checks happen before any existing state is changed, so rejected updates are
// atomic.
func (index *FunctionalMultikeyIndex[T, K]) Upsert(id uint64, value T) error {
	if index == nil {
		return fmt.Errorf("functional multikey index is nil")
	}
	normalized, err := normalizeFunctionalMultikeyKeys(index.extractor(value), index.options.MaxKeysPerItem)
	if err != nil {
		return err
	}

	index.mu.Lock()
	defer index.mu.Unlock()
	old, exists := index.byID[id]
	if !exists && index.options.MaxItems > 0 && len(index.byID) >= index.options.MaxItems {
		return fmt.Errorf("functional multikey index item limit exceeded: maximum %d", index.options.MaxItems)
	}
	if functionalMultikeyKeysEqual(old, normalized) {
		return nil
	}
	for _, key := range old {
		index.removePosting(key, id)
	}
	if len(normalized) == 0 {
		delete(index.byID, id)
		return nil
	}
	for _, key := range normalized {
		index.insertPosting(key, id)
	}
	index.byID[id] = normalized
	return nil
}

// Delete removes id and all of its postings. It reports whether id existed.
func (index *FunctionalMultikeyIndex[T, K]) Delete(id uint64) bool {
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
		index.removePosting(key, id)
	}
	delete(index.byID, id)
	return true
}

// Lookup appends sorted matching item IDs to dst and returns the resulting
// slice. Passing a reusable destination avoids an allocation on the caller's
// hot path; a nil destination allocates only when matches exist.
func (index *FunctionalMultikeyIndex[T, K]) Lookup(key K, dst []uint64) []uint64 {
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
func (index *FunctionalMultikeyIndex[T, K]) Contains(key K, id uint64) bool {
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
func (index *FunctionalMultikeyIndex[T, K]) Len() int {
	if index == nil {
		return 0
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	return len(index.byID)
}

// KeyCount returns the number of distinct keys with at least one posting.
func (index *FunctionalMultikeyIndex[T, K]) KeyCount() int {
	if index == nil {
		return 0
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	return len(index.byKey)
}

func normalizeFunctionalMultikeyKeys[K comparable](keys []K, maxKeys int) ([]K, error) {
	if maxKeys > 0 && len(keys) > maxKeys {
		return nil, fmt.Errorf("functional multikey key limit exceeded: maximum %d", maxKeys)
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

func functionalMultikeyKeysEqual[K comparable](left, right []K) bool {
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

func (index *FunctionalMultikeyIndex[T, K]) removePosting(key K, id uint64) {
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

func (index *FunctionalMultikeyIndex[T, K]) insertPosting(key K, id uint64) {
	if posting, ok := index.byKey[key]; ok {
		index.byKey[key] = posting.insertSorted(id)
		return
	}
	index.byKey[key] = newU64PostingList(id)
}
