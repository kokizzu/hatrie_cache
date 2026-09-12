package hatDataStructure

import "errors"

var (
	// ErrConditionalFunctionalIndexNil indicates that an operation was attempted on a nil index.
	ErrConditionalFunctionalIndexNil = errors.New("hatDataStructure: conditional functional index is nil")
	// ErrConditionalFunctionalIndexPredicateRequired indicates that no admission predicate was supplied.
	ErrConditionalFunctionalIndexPredicateRequired = errors.New("hatDataStructure: conditional functional index predicate is required")
)

// ConditionalFunctionalIndex indexes only values accepted by predicate. It
// derives comparable keys with extractor and delegates posting-list storage
// and lookup behavior to FunctionalIndex. A value that stops satisfying the
// predicate is removed from the index on replacement.
//
// The predicate is immutable after construction and is evaluated before the
// key extractor, so rejected values do not pay key-derivation cost or retain
// an index entry. The index is safe for concurrent readers and writers.
type ConditionalFunctionalIndex[T any, K comparable] struct {
	predicate func(T) bool
	index     *FunctionalIndex[T, K]
}

// NewConditionalFunctionalIndex creates an index that admits only values for
// which predicate returns true. Capacity is an initial sizing hint and may be
// zero. The extractor and predicate must both be non-nil.
func NewConditionalFunctionalIndex[T any, K comparable](extractor func(T) K, predicate func(T) bool, capacity int) (*ConditionalFunctionalIndex[T, K], error) {
	if predicate == nil {
		return nil, ErrConditionalFunctionalIndexPredicateRequired
	}
	index, err := NewFunctionalIndex(extractor, capacity)
	if err != nil {
		return nil, err
	}
	return &ConditionalFunctionalIndex[T, K]{
		predicate: predicate,
		index:     index,
	}, nil
}

// Upsert inserts or replaces a value for id when it is admitted. If the value
// is rejected, any existing value for id is removed and nil is returned.
func (index *ConditionalFunctionalIndex[T, K]) Upsert(id uint64, value T) error {
	if index == nil {
		return ErrConditionalFunctionalIndexNil
	}
	if !index.predicate(value) {
		index.index.Delete(id)
		return nil
	}
	return index.index.Upsert(id, value)
}

// Delete removes id and reports whether it was present in the admitted set.
func (index *ConditionalFunctionalIndex[T, K]) Delete(id uint64) bool {
	if index == nil {
		return false
	}
	return index.index.Delete(id)
}

// Lookup returns admitted values whose derived key equals key, in posting
// order.
func (index *ConditionalFunctionalIndex[T, K]) Lookup(key K) []T {
	if index == nil {
		return nil
	}
	return index.index.Lookup(key)
}

// LookupInto replaces dst with admitted values whose derived key equals key.
// It reuses dst's backing array when it has enough capacity.
func (index *ConditionalFunctionalIndex[T, K]) LookupInto(key K, dst []T) []T {
	if index == nil {
		return dst[:0]
	}
	return index.index.LookupInto(key, dst)
}

// LookupIDs returns stable admitted IDs whose derived key equals key, in
// posting order.
func (index *ConditionalFunctionalIndex[T, K]) LookupIDs(key K) []uint64 {
	if index == nil {
		return nil
	}
	return index.index.LookupIDs(key)
}

// LookupIDsInto replaces dst with stable admitted IDs whose derived key equals
// key. It reuses dst's backing array when it has enough capacity.
func (index *ConditionalFunctionalIndex[T, K]) LookupIDsInto(key K, dst []uint64) []uint64 {
	if index == nil {
		return dst[:0]
	}
	return index.index.LookupIDsInto(key, dst)
}

// Contains reports whether id is currently admitted under key.
func (index *ConditionalFunctionalIndex[T, K]) Contains(key K, id uint64) bool {
	if index == nil {
		return false
	}
	index.index.mu.RLock()
	defer index.index.mu.RUnlock()
	for _, candidate := range index.index.postings[key] {
		if candidate == id {
			return true
		}
	}
	return false
}

// Len returns the number of admitted IDs.
func (index *ConditionalFunctionalIndex[T, K]) Len() int {
	if index == nil {
		return 0
	}
	return index.index.Len()
}

// DistinctKeys returns the number of derived keys with at least one admitted
// ID.
func (index *ConditionalFunctionalIndex[T, K]) DistinctKeys() int {
	if index == nil {
		return 0
	}
	return index.index.DistinctKeys()
}

// Clear removes all admitted entries while retaining the predicate and
// extractor for reuse.
func (index *ConditionalFunctionalIndex[T, K]) Clear() {
	if index == nil {
		return
	}
	index.index.Clear()
}
