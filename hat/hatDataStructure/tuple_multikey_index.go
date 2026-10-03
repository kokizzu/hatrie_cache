package hatDataStructure

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

var (
	// ErrTupleMultikeyIndexNil indicates a method call on a nil index.
	ErrTupleMultikeyIndexNil = errors.New("hatDataStructure: tuple multikey index is nil")
	// ErrTupleMultikeyIndexExtractorRequired indicates that the constructor
	// received no tuple key extractor.
	ErrTupleMultikeyIndexExtractorRequired = errors.New("hatDataStructure: tuple multikey index extractor is required")
	// ErrTupleMultikeyIndexLimit indicates that a configured item or key bound
	// was exceeded.
	ErrTupleMultikeyIndexLimit = errors.New("hatDataStructure: tuple multikey index limit exceeded")
)

// TupleMultikeyIndexOptions bounds one tuple's distinct keys and the number
// of indexed tuples. A nonpositive limit means unlimited.
type TupleMultikeyIndexOptions struct {
	MaxKeysPerItem int
	MaxItems       int
}

// TupleMultikeyIndex maps comparable keys to sorted tuple IDs. The extractor
// lets callers flatten nested tuple/array fields without imposing a row
// representation or reflection cost on the index.
type TupleMultikeyIndex[T any, K comparable] struct {
	mu      sync.RWMutex
	extract func(T) ([]K, error)
	options TupleMultikeyIndexOptions
	byKey   map[K]u64PostingList
	byID    map[uint64][]K
}

// NewTupleMultikeyIndex creates an empty typed multikey index.
func NewTupleMultikeyIndex[T any, K comparable](extract func(T) ([]K, error), options TupleMultikeyIndexOptions) (*TupleMultikeyIndex[T, K], error) {
	if extract == nil {
		return nil, ErrTupleMultikeyIndexExtractorRequired
	}
	return &TupleMultikeyIndex[T, K]{
		extract: extract,
		options: options,
		byKey:   make(map[K]u64PostingList),
		byID:    make(map[uint64][]K),
	}, nil
}

// Upsert replaces every indexed key for id. Extraction, deduplication, and
// capacity validation happen before existing postings are changed, so a
// rejected update is atomic.
func (index *TupleMultikeyIndex[T, K]) Upsert(id uint64, tuple T) error {
	if index == nil {
		return ErrTupleMultikeyIndexNil
	}
	keys, err := index.extract(tuple)
	if err != nil {
		return err
	}
	normalized, err := normalizeTupleMultikeyKeys(keys, index.options.MaxKeysPerItem)
	if err != nil {
		return err
	}

	index.mu.Lock()
	defer index.mu.Unlock()
	old, exists := index.byID[id]
	if !exists && index.options.MaxItems > 0 && len(index.byID) >= index.options.MaxItems {
		return fmt.Errorf("%w: maximum %d items", ErrTupleMultikeyIndexLimit, index.options.MaxItems)
	}
	if tupleMultikeyKeysEqual(old, normalized) {
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

// Delete removes id and every posting associated with it.
func (index *TupleMultikeyIndex[T, K]) Delete(id uint64) bool {
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

// Lookup appends sorted matching IDs to dst. Supplying reusable capacity keeps
// the caller's lookup path allocation-free.
func (index *TupleMultikeyIndex[T, K]) Lookup(key K, dst []uint64) []uint64 {
	if index == nil {
		if dst != nil {
			return dst[:0]
		}
		return nil
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	posting, exists := index.byKey[key]
	if !exists {
		if dst != nil {
			return dst[:0]
		}
		return nil
	}
	return posting.values(dst[:0])
}

// Contains reports whether id is present under key.
func (index *TupleMultikeyIndex[T, K]) Contains(key K, id uint64) bool {
	if index == nil {
		return false
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	posting, exists := index.byKey[key]
	if !exists {
		return false
	}
	return tupleMultikeyPostingContains(posting, id)
}

// Len returns the number of tuples with at least one key.
func (index *TupleMultikeyIndex[T, K]) Len() int {
	if index == nil {
		return 0
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	return len(index.byID)
}

// KeyCount returns the number of distinct keys with at least one posting.
func (index *TupleMultikeyIndex[T, K]) KeyCount() int {
	if index == nil {
		return 0
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	return len(index.byKey)
}

// Clear removes all tuples and postings while retaining the configured
// extractor and bounds.
func (index *TupleMultikeyIndex[T, K]) Clear() {
	if index == nil {
		return
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	index.byKey = make(map[K]u64PostingList)
	index.byID = make(map[uint64][]K)
}

func normalizeTupleMultikeyKeys[K comparable](keys []K, maxKeys int) ([]K, error) {
	if maxKeys > 0 && len(keys) > maxKeys {
		return nil, fmt.Errorf("%w: maximum %d keys per item", ErrTupleMultikeyIndexLimit, maxKeys)
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

func tupleMultikeyKeysEqual[K comparable](left, right []K) bool {
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

func tupleMultikeyPostingContains(posting u64PostingList, id uint64) bool {
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

func (index *TupleMultikeyIndex[T, K]) removePosting(key K, id uint64) {
	posting, exists := index.byKey[key]
	if !exists {
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

func (index *TupleMultikeyIndex[T, K]) insertPosting(key K, id uint64) {
	if posting, exists := index.byKey[key]; exists {
		index.byKey[key] = posting.insertSorted(id)
		return
	}
	index.byKey[key] = newU64PostingList(id)
}
