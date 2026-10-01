package hatDataStructure

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

const MaxTupleMultikeyDimensions = 64

var (
	// ErrTupleMultikeyIndexNil reports a method call on a nil index.
	ErrTupleMultikeyIndexNil = errors.New("hatDataStructure: tuple multikey index is nil")
	// ErrTupleMultikeyCombinationLimit reports a row whose expanded tuple count
	// exceeds the configured safety bound.
	ErrTupleMultikeyCombinationLimit = errors.New("hatDataStructure: tuple multikey combination limit exceeded")
	// ErrTupleMultikeyDimensionLimit reports a row with too many tuple arrays.
	ErrTupleMultikeyDimensionLimit = errors.New("hatDataStructure: tuple multikey dimension limit exceeded")
)

// TupleMultikeyIndexOptions bounds the state held by a TupleMultikeyIndex.
// MaxCombinationsPerItem is the Cartesian-product limit for one item. A
// nonpositive value means unlimited, subject to integer and memory limits.
// MaxItems bounds the number of items with at least one expanded tuple.
type TupleMultikeyIndexOptions struct {
	MaxCombinationsPerItem int
	MaxItems               int
}

// TupleMultikeyIndex maps each expanded tuple to sorted item IDs. Set accepts
// one string slice per tuple dimension; every combination of one value from
// each dimension is indexed. Values within a dimension are deduplicated.
//
// For example, dimensions {"region-a", "region-b"} and {"hot", "cold"}
// create four lookup tuples. Empty dimensions create no tuples, which makes
// Set useful for clearing an item without a separate mutation path.
type TupleMultikeyIndex struct {
	mu      sync.RWMutex
	options TupleMultikeyIndexOptions
	byKey   map[string]u64PostingList
	byID    map[uint64][]string
}

// NewTupleMultikeyIndex creates an empty bounded tuple multikey index.
func NewTupleMultikeyIndex(options TupleMultikeyIndexOptions) *TupleMultikeyIndex {
	return &TupleMultikeyIndex{
		options: options,
		byKey:   make(map[string]u64PostingList),
		byID:    make(map[uint64][]string),
	}
}

// Set replaces all tuples associated with id. Validation and expansion happen
// before mutation, so an explosion-limit error leaves the previous state
// intact.
func (index *TupleMultikeyIndex) Set(id uint64, dimensions [][]string) error {
	if index == nil {
		return ErrTupleMultikeyIndexNil
	}
	normalized, err := normalizeTupleMultikeyKeys(dimensions, index.options.MaxCombinationsPerItem)
	if err != nil {
		return err
	}

	index.mu.Lock()
	defer index.mu.Unlock()
	old, exists := index.byID[id]
	if !exists && len(normalized) > 0 && index.options.MaxItems > 0 && len(index.byID) >= index.options.MaxItems {
		return fmt.Errorf("hatDataStructure: tuple multikey item limit exceeded: maximum %d", index.options.MaxItems)
	}
	if tupleMultikeyKeysEqual(old, normalized) {
		return nil
	}
	for _, key := range old {
		index.removeTuplePosting(key, id)
	}
	if len(normalized) == 0 {
		delete(index.byID, id)
		return nil
	}
	for _, key := range normalized {
		index.insertTuplePosting(key, id)
	}
	index.byID[id] = normalized
	return nil
}

// Delete removes id and all of its expanded tuple postings. It reports
// whether id existed.
func (index *TupleMultikeyIndex) Delete(id uint64) bool {
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
		index.removeTuplePosting(key, id)
	}
	delete(index.byID, id)
	return true
}

// Lookup appends sorted matching item IDs to dst and returns the resulting
// slice. Passing a reusable destination avoids allocation for a caller that
// already has enough capacity.
func (index *TupleMultikeyIndex) Lookup(values []string, dst []uint64) []uint64 {
	if index == nil {
		if dst != nil {
			return dst[:0]
		}
		return nil
	}
	key := encodeTupleMultikeyKey(values)
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

// Contains reports whether id is indexed under the complete tuple values.
func (index *TupleMultikeyIndex) Contains(values []string, id uint64) bool {
	if index == nil {
		return false
	}
	key := encodeTupleMultikeyKey(values)
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

// Len returns the number of items with at least one expanded tuple.
func (index *TupleMultikeyIndex) Len() int {
	if index == nil {
		return 0
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	return len(index.byID)
}

// KeyCount returns the number of distinct expanded tuples with at least one
// posting.
func (index *TupleMultikeyIndex) KeyCount() int {
	if index == nil {
		return 0
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	return len(index.byKey)
}

func normalizeTupleMultikeyKeys(dimensions [][]string, maxCombinations int) ([]string, error) {
	if len(dimensions) == 0 {
		return nil, nil
	}
	if len(dimensions) > MaxTupleMultikeyDimensions {
		return nil, fmt.Errorf("%w: maximum %d", ErrTupleMultikeyDimensionLimit, MaxTupleMultikeyDimensions)
	}
	normalizedDimensions := make([][]string, len(dimensions))
	combinations := 1
	for index, dimension := range dimensions {
		if len(dimension) == 0 {
			return nil, nil
		}
		unique := normalizeTupleMultikeyDimension(dimension)
		if len(unique) == 0 {
			return nil, nil
		}
		if combinations > int(^uint(0)>>1)/len(unique) {
			return nil, fmt.Errorf("%w: combination count overflows int", ErrTupleMultikeyCombinationLimit)
		}
		combinations *= len(unique)
		if maxCombinations > 0 && combinations > maxCombinations {
			return nil, fmt.Errorf("%w: maximum %d", ErrTupleMultikeyCombinationLimit, maxCombinations)
		}
		normalizedDimensions[index] = unique
	}

	keys := make([]string, 0, combinations)
	current := make([]string, len(normalizedDimensions))
	var expand func(int)
	expand = func(dimension int) {
		if dimension == len(normalizedDimensions) {
			keys = append(keys, encodeTupleMultikeyKey(current))
			return
		}
		for _, value := range normalizedDimensions[dimension] {
			current[dimension] = value
			expand(dimension + 1)
		}
	}
	expand(0)
	return keys, nil
}

func normalizeTupleMultikeyDimension(dimension []string) []string {
	if len(dimension) < 2 {
		return dimension
	}
	sortedUnique := true
	for index := 1; index < len(dimension); index++ {
		if dimension[index-1] >= dimension[index] {
			sortedUnique = false
			break
		}
	}
	if sortedUnique {
		return dimension
	}
	values := append([]string(nil), dimension...)
	sort.Strings(values)
	unique := values[:0]
	for _, value := range values {
		if len(unique) == 0 || unique[len(unique)-1] != value {
			unique = append(unique, value)
		}
	}
	return unique
}

func encodeTupleMultikeyKey(values []string) string {
	capacity := binary.MaxVarintLen64
	for _, value := range values {
		capacity += binary.MaxVarintLen64 + len(value)
	}
	var builder strings.Builder
	builder.Grow(capacity)
	var length [binary.MaxVarintLen64]byte
	count := binary.PutUvarint(length[:], uint64(len(values)))
	_, _ = builder.Write(length[:count])
	for _, value := range values {
		count = binary.PutUvarint(length[:], uint64(len(value)))
		_, _ = builder.Write(length[:count])
		_, _ = builder.WriteString(value)
	}
	return builder.String()
}

func tupleMultikeyKeysEqual(left, right []string) bool {
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

func (index *TupleMultikeyIndex) removeTuplePosting(key string, id uint64) {
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

func (index *TupleMultikeyIndex) insertTuplePosting(key string, id uint64) {
	if posting, ok := index.byKey[key]; ok {
		index.byKey[key] = posting.insertSorted(id)
		return
	}
	index.byKey[key] = newU64PostingList(id)
}
