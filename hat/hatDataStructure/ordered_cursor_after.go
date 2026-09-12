package hatDataStructure

import "sort"

// SeekAfterEntry positions a live iterator after the complete (key, ID)
// position. It preserves rows that share key with the continuation boundary.
func (index *OrderedIndex[T, K]) SeekAfterEntry(key K, id uint64) (OrderedIndexIterator[T, K], bool) {
	if index == nil {
		return OrderedIndexIterator[T, K]{}, false
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	position := sort.Search(len(index.entries), func(position int) bool {
		comparison := index.compare(index.entries[position].Key, key)
		return comparison > 0 || comparison == 0 && index.entries[position].ID > id
	})
	if position >= len(index.entries) {
		return OrderedIndexIterator[T, K]{}, false
	}
	index.active.Add(1)
	return OrderedIndexIterator[T, K]{index: index, entries: index.entries, generation: index.generation.Load(), position: position}, true
}

// SeekAfterEntry positions a stable snapshot cursor after the complete (key,
// ID) position. It is the duplicate-safe continuation primitive for tokens.
func (cursor *OrderedIndexSnapshotCursor[T, K]) SeekAfterEntry(key K, id uint64) error {
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
		return comparison > 0 || comparison == 0 && cursor.entries[position].ID > id
	})
	return nil
}
