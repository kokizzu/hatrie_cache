package hatDataStructure

import (
	"errors"
	"sort"
	"sync"
)

var (
	ErrMultiPartTreeIndexNil                 = errors.New("hatDataStructure: multi-part tree index is nil")
	ErrMultiPartTreeIndexComparatorRequired  = errors.New("hatDataStructure: multi-part tree index comparator is required")
	ErrMultiPartTreeIndexPartEmpty           = errors.New("hatDataStructure: multi-part tree index part is empty")
	ErrMultiPartTreeIndexIteratorNil         = errors.New("hatDataStructure: multi-part tree index iterator is nil")
	ErrMultiPartTreeIndexIteratorClosed      = errors.New("hatDataStructure: multi-part tree index iterator is closed")
	ErrMultiPartTreeIndexIteratorInvalidated = errors.New("hatDataStructure: multi-part tree index iterator is invalidated")
)

// TreeIndexEntry is one stable ID/value in a sorted index part. Equal keys
// are ordered by ID, which keeps merged scans deterministic across parts.
type TreeIndexEntry[T any, K any] struct {
	ID    uint64
	Key   K
	Value T
}

type multiPartTreeIndexPart[T any, K any] struct {
	id      uint64
	entries []TreeIndexEntry[T, K]
}

// MultiPartTreeIndex is a concurrent collection of immutable sorted parts.
// Each part is binary-searched independently and range scans merge only the
// selected slices, avoiding a full rebuild when a new analytical part arrives.
type MultiPartTreeIndex[T any, K any] struct {
	mu         sync.RWMutex
	compare    func(K, K) int
	parts      map[uint64]*multiPartTreeIndexPart[T, K]
	nextPartID uint64
	generation uint64
	length     int
}

// MultiPartTreeIndexIterator merges the selected ranges from immutable parts.
// A mutation of the parent index invalidates the iterator deterministically.
type MultiPartTreeIndexIterator[T any, K any] struct {
	index      *MultiPartTreeIndex[T, K]
	cursors    []multiPartTreeIndexCursor[T, K]
	heap       []int
	generation uint64
	closed     bool
	released   bool
}

type multiPartTreeIndexCursor[T any, K any] struct {
	partID   uint64
	entries  []TreeIndexEntry[T, K]
	position int
	limit    int
}

// NewMultiPartTreeIndex creates an empty multi-part index.
func NewMultiPartTreeIndex[T any, K any](compare func(K, K) int) (*MultiPartTreeIndex[T, K], error) {
	if compare == nil {
		return nil, ErrMultiPartTreeIndexComparatorRequired
	}
	return &MultiPartTreeIndex[T, K]{compare: compare, parts: make(map[uint64]*multiPartTreeIndexPart[T, K])}, nil
}

// AddPart copies and sorts one immutable part, returning its stable part ID.
func (index *MultiPartTreeIndex[T, K]) AddPart(entries []TreeIndexEntry[T, K]) (uint64, error) {
	if index == nil {
		return 0, ErrMultiPartTreeIndexNil
	}
	if len(entries) == 0 {
		return 0, ErrMultiPartTreeIndexPartEmpty
	}
	partEntries := append([]TreeIndexEntry[T, K](nil), entries...)
	sort.SliceStable(partEntries, func(left, right int) bool {
		return multiPartTreeIndexEntryLess(partEntries[left], partEntries[right], index.compare)
	})
	index.mu.Lock()
	defer index.mu.Unlock()
	index.nextPartID++
	partID := index.nextPartID
	index.parts[partID] = &multiPartTreeIndexPart[T, K]{id: partID, entries: partEntries}
	index.length += len(partEntries)
	index.generation++
	return partID, nil
}

// DeletePart removes a part and reports whether it was present.
func (index *MultiPartTreeIndex[T, K]) DeletePart(partID uint64) bool {
	if index == nil {
		return false
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	part, found := index.parts[partID]
	if !found {
		return false
	}
	delete(index.parts, partID)
	index.length -= len(part.entries)
	index.generation++
	return true
}

// Clear removes all parts while retaining the comparator.
func (index *MultiPartTreeIndex[T, K]) Clear() {
	if index == nil {
		return
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	index.parts = make(map[uint64]*multiPartTreeIndexPart[T, K])
	index.length = 0
	index.generation++
}

// Len returns the total number of entries across all parts.
func (index *MultiPartTreeIndex[T, K]) Len() int {
	if index == nil {
		return 0
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	return index.length
}

// PartCount returns the number of immutable parts.
func (index *MultiPartTreeIndex[T, K]) PartCount() int {
	if index == nil {
		return 0
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	return len(index.parts)
}

// Range returns a merged iterator over inclusive key bounds. Work before the
// first result is O(parts*log(part-size)); advancing is O(log(parts)).
func (index *MultiPartTreeIndex[T, K]) Range(start, end K) (MultiPartTreeIndexIterator[T, K], bool) {
	if index == nil {
		return MultiPartTreeIndexIterator[T, K]{}, false
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	if index.compare(start, end) > 0 || len(index.parts) == 0 {
		return MultiPartTreeIndexIterator[T, K]{}, false
	}
	iterator := MultiPartTreeIndexIterator[T, K]{
		index:      index,
		generation: index.generation,
	}
	for _, part := range index.parts {
		first := sort.Search(len(part.entries), func(position int) bool {
			return index.compare(part.entries[position].Key, start) >= 0
		})
		limit := sort.Search(len(part.entries), func(position int) bool {
			return index.compare(part.entries[position].Key, end) > 0
		})
		if first < limit {
			iterator.cursors = append(iterator.cursors, multiPartTreeIndexCursor[T, K]{
				partID:   part.id,
				entries:  part.entries,
				position: first,
				limit:    limit,
			})
		}
	}
	if len(iterator.cursors) == 0 {
		return MultiPartTreeIndexIterator[T, K]{}, false
	}
	iterator.heap = make([]int, len(iterator.cursors))
	for position := range iterator.heap {
		iterator.heap[position] = position
	}
	for position := len(iterator.heap)/2 - 1; position >= 0; position-- {
		iterator.siftDown(position)
	}
	return iterator, true
}

// Prefix is an inclusive range helper. Callers provide the smallest and
// largest composite keys sharing the desired prefix.
func (index *MultiPartTreeIndex[T, K]) Prefix(start, end K) (MultiPartTreeIndexIterator[T, K], bool) {
	return index.Range(start, end)
}

// Next returns the globally ordered next entry.
func (iterator *MultiPartTreeIndexIterator[T, K]) Next() (TreeIndexEntry[T, K], bool, error) {
	if iterator == nil || iterator.index == nil {
		return TreeIndexEntry[T, K]{}, false, ErrMultiPartTreeIndexIteratorNil
	}
	if iterator.closed {
		return TreeIndexEntry[T, K]{}, false, ErrMultiPartTreeIndexIteratorClosed
	}
	if iterator.released {
		return TreeIndexEntry[T, K]{}, false, nil
	}
	if iterator.generation != iterator.index.currentGeneration() {
		iterator.release()
		return TreeIndexEntry[T, K]{}, false, ErrMultiPartTreeIndexIteratorInvalidated
	}
	if len(iterator.heap) == 0 {
		iterator.release()
		return TreeIndexEntry[T, K]{}, false, nil
	}
	root := iterator.heap[0]
	cursor := &iterator.cursors[root]
	entry := cursor.entries[cursor.position]
	cursor.position++
	if cursor.position >= cursor.limit {
		last := len(iterator.heap) - 1
		iterator.heap[0] = iterator.heap[last]
		iterator.heap = iterator.heap[:last]
		if len(iterator.heap) > 0 {
			iterator.siftDown(0)
		}
	} else {
		iterator.siftDown(0)
	}
	return entry, true, nil
}

// Close releases the iterator and prevents resumption.
func (iterator *MultiPartTreeIndexIterator[T, K]) Close() {
	if iterator == nil || iterator.closed {
		return
	}
	iterator.closed = true
	iterator.release()
}

func (index *MultiPartTreeIndex[T, K]) currentGeneration() uint64 {
	index.mu.RLock()
	defer index.mu.RUnlock()
	return index.generation
}

func (iterator *MultiPartTreeIndexIterator[T, K]) release() {
	iterator.released = true
	iterator.heap = nil
	iterator.cursors = nil
}

func (iterator *MultiPartTreeIndexIterator[T, K]) siftDown(position int) {
	for {
		left := position*2 + 1
		if left >= len(iterator.heap) {
			return
		}
		right := left + 1
		best := left
		if right < len(iterator.heap) && iterator.lessCursor(right, left) {
			best = right
		}
		if !iterator.lessCursor(best, position) {
			return
		}
		iterator.heap[position], iterator.heap[best] = iterator.heap[best], iterator.heap[position]
		position = best
	}
}

func (iterator *MultiPartTreeIndexIterator[T, K]) lessCursor(left, right int) bool {
	leftCursor := iterator.cursors[iterator.heap[left]]
	rightCursor := iterator.cursors[iterator.heap[right]]
	leftEntry := leftCursor.entries[leftCursor.position]
	rightEntry := rightCursor.entries[rightCursor.position]
	comparison := iterator.index.compare(leftEntry.Key, rightEntry.Key)
	if comparison != 0 {
		return comparison < 0
	}
	if leftEntry.ID != rightEntry.ID {
		return leftEntry.ID < rightEntry.ID
	}
	return leftCursor.partID < rightCursor.partID
}

func multiPartTreeIndexEntryLess[T any, K any](left, right TreeIndexEntry[T, K], compare func(K, K) int) bool {
	comparison := compare(left.Key, right.Key)
	if comparison != 0 {
		return comparison < 0
	}
	return left.ID < right.ID
}
