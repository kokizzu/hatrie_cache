package hatDataStructure

import "sort"

// Last returns an iterator positioned at the greatest key. Its Next method
// walks entries in descending key order, with equal keys ordered by descending
// ID. The iterator has the same invalidation and Close requirements as First.
func (index *OrderedIndex[T, K]) Last() (OrderedIndexIterator[T, K], bool) {
	if index == nil {
		return OrderedIndexIterator[T, K]{}, false
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	if len(index.entries) == 0 {
		return OrderedIndexIterator[T, K]{}, false
	}
	index.active.Add(1)
	return OrderedIndexIterator[T, K]{
		index:      index,
		entries:    index.entries,
		generation: index.generation.Load(),
		position:   len(index.entries) - 1,
		reverse:    true,
	}, true
}

// SeekBefore returns an iterator at the greatest entry whose key is strictly
// less than key. The iterator walks in descending key order.
func (index *OrderedIndex[T, K]) SeekBefore(key K) (OrderedIndexIterator[T, K], bool) {
	return index.seekReverse(key, false)
}

// SeekBeforeOrEqual returns an iterator at the greatest entry whose key is
// less than or equal to key. The iterator walks in descending key order.
func (index *OrderedIndex[T, K]) SeekBeforeOrEqual(key K) (OrderedIndexIterator[T, K], bool) {
	return index.seekReverse(key, true)
}

func (index *OrderedIndex[T, K]) seekReverse(key K, inclusive bool) (OrderedIndexIterator[T, K], bool) {
	if index == nil {
		return OrderedIndexIterator[T, K]{}, false
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	position := sort.Search(len(index.entries), func(position int) bool {
		comparison := index.compare(index.entries[position].Key, key)
		if inclusive {
			return comparison > 0
		}
		return comparison >= 0
	}) - 1
	if position < 0 {
		return OrderedIndexIterator[T, K]{}, false
	}
	index.active.Add(1)
	return OrderedIndexIterator[T, K]{
		index:      index,
		entries:    index.entries,
		generation: index.generation.Load(),
		position:   position,
		reverse:    true,
	}, true
}

// LastSnapshotCursor returns a stable cursor positioned at the greatest key.
// Its Next method walks entries in descending key order, while mutations keep
// using copy-on-write so the cursor sees its original view.
func (index *OrderedIndex[T, K]) LastSnapshotCursor() (OrderedIndexSnapshotCursor[T, K], bool) {
	if index == nil {
		return OrderedIndexSnapshotCursor[T, K]{}, false
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	if len(index.entries) == 0 {
		return OrderedIndexSnapshotCursor[T, K]{}, false
	}
	index.active.Add(1)
	return OrderedIndexSnapshotCursor[T, K]{
		index:    index,
		entries:  index.entries,
		position: len(index.entries) - 1,
		reverse:  true,
	}, true
}

// SeekBefore positions a snapshot cursor at the greatest snapshot entry whose
// key is strictly less than key and changes it to descending traversal.
func (cursor *OrderedIndexSnapshotCursor[T, K]) SeekBefore(key K) error {
	return cursor.seekReverse(key, false)
}

// SeekBeforeOrEqual positions a snapshot cursor at the greatest snapshot entry
// whose key is less than or equal to key and changes it to descending
// traversal.
func (cursor *OrderedIndexSnapshotCursor[T, K]) SeekBeforeOrEqual(key K) error {
	return cursor.seekReverse(key, true)
}

func (cursor *OrderedIndexSnapshotCursor[T, K]) seekReverse(key K, inclusive bool) error {
	if cursor == nil || cursor.index == nil {
		return ErrOrderedIndexSnapshotCursorNil
	}
	if cursor.closed {
		return ErrOrderedIndexSnapshotCursorClosed
	}
	if cursor.released {
		return ErrOrderedIndexSnapshotCursorReleased
	}
	cursor.position = sort.Search(len(cursor.entries), func(position int) bool {
		comparison := cursor.index.compare(cursor.entries[position].Key, key)
		if inclusive {
			return comparison > 0
		}
		return comparison >= 0
	}) - 1
	cursor.reverse = true
	return nil
}
