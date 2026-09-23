package hatDataStructure

import (
	"errors"
	"fmt"
)

var (
	// ErrMultiPartTreeIndexNil indicates an operation on a nil index.
	ErrMultiPartTreeIndexNil = errors.New("hatDataStructure: multi-part tree index is nil")
	// ErrMultiPartTreeIndexExtractorRequired indicates that no composite-key
	// extractor was supplied.
	ErrMultiPartTreeIndexExtractorRequired = errors.New("hatDataStructure: multi-part tree index extractor is required")
	// ErrMultiPartTreeIndexComparatorRequired indicates that no part comparator
	// was supplied.
	ErrMultiPartTreeIndexComparatorRequired = errors.New("hatDataStructure: multi-part tree index comparator is required")
	// ErrMultiPartTreeIndexPartCountInvalid indicates an unsupported key width.
	ErrMultiPartTreeIndexPartCountInvalid = errors.New("hatDataStructure: multi-part tree index part count is invalid")
	// ErrMultiPartTreeIndexKeyPartCount indicates a key with the wrong width.
	ErrMultiPartTreeIndexKeyPartCount = errors.New("hatDataStructure: multi-part tree index key part count is invalid")
	// ErrMultiPartTreeIndexPrefixTooLong indicates a prefix wider than the
	// configured composite key.
	ErrMultiPartTreeIndexPrefixTooLong = errors.New("hatDataStructure: multi-part tree index prefix is too long")
	// ErrMultiPartTreeIndexIteratorNil indicates an operation on a nil iterator.
	ErrMultiPartTreeIndexIteratorNil = errors.New("hatDataStructure: multi-part tree index iterator is nil")
)

// MultiPartTreeIndexEntry is one entry in composite-key order. Parts are
// schema ordered and must be treated as read-only by callers.
type MultiPartTreeIndexEntry[T any, K any] struct {
	ID    uint64
	Parts []K
	Value T
}

// MultiPartTreeIndex adds typed composite keys and prefix scans to the
// compact OrderedIndex core. Keys are ordered lexicographically by part and
// then by stable ID. Mutations retain OrderedIndex's iterator invalidation and
// copy-on-write behavior.
type MultiPartTreeIndex[T any, K any] struct {
	ordered   *OrderedIndex[T, []K]
	extractor func(T) []K
	compare   func(K, K) int
	partCount int
}

// MultiPartTreeIndexIterator walks an exact range or prefix result without
// allocating per returned entry. Prefix is copied when the iterator is made so
// callers may reuse their input slice after the constructor returns.
type MultiPartTreeIndexIterator[T any, K any] struct {
	inner       OrderedIndexIterator[T, []K]
	compare     func(K, K) int
	prefixInl   [4]K
	prefixExtra []K
	prefixLen   int
	prefixMode  bool
	done        bool
}

// NewMultiPartTreeIndex creates a composite ordered index with a fixed number
// of parts. The extractor must return exactly partCount values for every
// indexed record. capacity is only an initial entry hint.
func NewMultiPartTreeIndex[T any, K any](extractor func(T) []K, compare func(K, K) int, partCount, capacity int) (*MultiPartTreeIndex[T, K], error) {
	if extractor == nil {
		return nil, ErrMultiPartTreeIndexExtractorRequired
	}
	if compare == nil {
		return nil, ErrMultiPartTreeIndexComparatorRequired
	}
	if partCount <= 0 {
		return nil, ErrMultiPartTreeIndexPartCountInvalid
	}
	ordered, err := NewOrderedIndex(func(value T) []K { return extractor(value) }, multiPartTreeCompare(compare), capacity)
	if err != nil {
		return nil, err
	}
	return &MultiPartTreeIndex[T, K]{
		ordered:   ordered,
		extractor: extractor,
		compare:   compare,
		partCount: partCount,
	}, nil
}

// Upsert inserts or replaces one value. Replacements are reordered when any
// part changes; the extracted key is copied so the caller can reuse its slice.
func (index *MultiPartTreeIndex[T, K]) Upsert(id uint64, value T) error {
	if index == nil {
		return ErrMultiPartTreeIndexNil
	}
	parts := index.extractor(value)
	if err := index.validateKeyParts(parts); err != nil {
		return err
	}
	owned := append([]K(nil), parts...)
	return index.ordered.upsertKey(id, value, owned)
}

// Delete removes id and reports whether it existed.
func (index *MultiPartTreeIndex[T, K]) Delete(id uint64) bool {
	if index == nil {
		return false
	}
	return index.ordered.Delete(id)
}

// Clear removes all entries while retaining the key schema.
func (index *MultiPartTreeIndex[T, K]) Clear() {
	if index == nil {
		return
	}
	index.ordered.Clear()
}

// Len returns the number of indexed values.
func (index *MultiPartTreeIndex[T, K]) Len() int {
	if index == nil {
		return 0
	}
	return index.ordered.Len()
}

// Seek returns the first exact-width key greater than or equal to parts.
func (index *MultiPartTreeIndex[T, K]) Seek(parts []K) (MultiPartTreeIndexIterator[T, K], bool, error) {
	if index == nil {
		return MultiPartTreeIndexIterator[T, K]{}, false, ErrMultiPartTreeIndexNil
	}
	if err := index.validateKeyParts(parts); err != nil {
		return MultiPartTreeIndexIterator[T, K]{}, false, err
	}
	inner, found := index.ordered.Seek(parts)
	if !found {
		return MultiPartTreeIndexIterator[T, K]{}, false, nil
	}
	return MultiPartTreeIndexIterator[T, K]{inner: inner, compare: index.compare}, true, nil
}

// Prefix returns all entries whose leading parts equal prefix. An empty
// prefix matches the entire index. The seek is logarithmic and iteration is
// proportional to the matching range.
func (index *MultiPartTreeIndex[T, K]) Prefix(prefix []K) (MultiPartTreeIndexIterator[T, K], bool, error) {
	if index == nil {
		return MultiPartTreeIndexIterator[T, K]{}, false, ErrMultiPartTreeIndexNil
	}
	if len(prefix) > index.partCount {
		return MultiPartTreeIndexIterator[T, K]{}, false, fmt.Errorf("%w: got %d parts, want at most %d", ErrMultiPartTreeIndexPrefixTooLong, len(prefix), index.partCount)
	}
	inner, found := index.ordered.Seek(prefix)
	if !found {
		return MultiPartTreeIndexIterator[T, K]{}, false, nil
	}
	iterator := MultiPartTreeIndexIterator[T, K]{
		inner:      inner,
		compare:    index.compare,
		prefixMode: true,
	}
	iterator.setPrefix(prefix)
	return iterator, true, nil
}

// Range returns entries with exact-width composite keys between start and end
// inclusive. Bounds are located with binary search.
func (index *MultiPartTreeIndex[T, K]) Range(start, end []K) (MultiPartTreeIndexIterator[T, K], bool, error) {
	if index == nil {
		return MultiPartTreeIndexIterator[T, K]{}, false, ErrMultiPartTreeIndexNil
	}
	if err := index.validateKeyParts(start); err != nil {
		return MultiPartTreeIndexIterator[T, K]{}, false, err
	}
	if err := index.validateKeyParts(end); err != nil {
		return MultiPartTreeIndexIterator[T, K]{}, false, err
	}
	inner, found := index.ordered.Range(start, end)
	if !found {
		return MultiPartTreeIndexIterator[T, K]{}, false, nil
	}
	return MultiPartTreeIndexIterator[T, K]{inner: inner, compare: index.compare}, true, nil
}

// Next returns the next matching entry. Prefix iterators stop at the first
// key outside their prefix and release their underlying iterator.
func (iterator *MultiPartTreeIndexIterator[T, K]) Next() (MultiPartTreeIndexEntry[T, K], bool, error) {
	if iterator == nil {
		return MultiPartTreeIndexEntry[T, K]{}, false, ErrMultiPartTreeIndexIteratorNil
	}
	if iterator.done {
		return MultiPartTreeIndexEntry[T, K]{}, false, nil
	}
	entry, next, err := iterator.inner.Next()
	if err != nil {
		return MultiPartTreeIndexEntry[T, K]{}, false, err
	}
	if !next {
		iterator.done = true
		return MultiPartTreeIndexEntry[T, K]{}, false, nil
	}
	if iterator.prefixMode && !multiPartTreeHasPrefix(entry.Key, iterator.prefixParts(), iterator.compare) {
		iterator.inner.Close()
		iterator.done = true
		return MultiPartTreeIndexEntry[T, K]{}, false, nil
	}
	return MultiPartTreeIndexEntry[T, K]{ID: entry.ID, Parts: entry.Key, Value: entry.Value}, true, nil
}

func (iterator *MultiPartTreeIndexIterator[T, K]) setPrefix(prefix []K) {
	iterator.prefixLen = len(prefix)
	if len(prefix) <= len(iterator.prefixInl) {
		copy(iterator.prefixInl[:], prefix)
		return
	}
	iterator.prefixExtra = append([]K(nil), prefix...)
}

func (iterator *MultiPartTreeIndexIterator[T, K]) prefixParts() []K {
	if iterator.prefixExtra != nil {
		return iterator.prefixExtra
	}
	return iterator.prefixInl[:iterator.prefixLen]
}

// Close releases the underlying iterator when a caller stops before EOF.
func (iterator *MultiPartTreeIndexIterator[T, K]) Close() {
	if iterator == nil || iterator.done {
		return
	}
	iterator.inner.Close()
	iterator.done = true
}

func (index *MultiPartTreeIndex[T, K]) validateKeyParts(parts []K) error {
	if len(parts) != index.partCount {
		return fmt.Errorf("%w: got %d parts, want %d", ErrMultiPartTreeIndexKeyPartCount, len(parts), index.partCount)
	}
	return nil
}

func multiPartTreeCompare[K any](compare func(K, K) int) func([]K, []K) int {
	return func(left, right []K) int {
		limit := len(left)
		if len(right) < limit {
			limit = len(right)
		}
		for part := 0; part < limit; part++ {
			if result := compare(left[part], right[part]); result != 0 {
				return result
			}
		}
		if len(left) < len(right) {
			return -1
		}
		if len(left) > len(right) {
			return 1
		}
		return 0
	}
}

func multiPartTreeHasPrefix[K any](key, prefix []K, compare func(K, K) int) bool {
	if len(key) < len(prefix) {
		return false
	}
	for part := range prefix {
		if compare(key[part], prefix[part]) != 0 {
			return false
		}
	}
	return true
}
