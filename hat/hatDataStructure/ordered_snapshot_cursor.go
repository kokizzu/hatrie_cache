package hatDataStructure

import (
	"errors"
	"sort"
)

var (
	// ErrOrderedIndexSnapshotCursorNil indicates that an operation was attempted
	// on a nil or zero snapshot cursor.
	ErrOrderedIndexSnapshotCursorNil = errors.New("hatDataStructure: ordered index snapshot cursor is nil")
	// ErrOrderedIndexSnapshotCursorClosed indicates that an operation was
	// attempted after Close.
	ErrOrderedIndexSnapshotCursorClosed = errors.New("hatDataStructure: ordered index snapshot cursor is closed")
	// ErrOrderedIndexSnapshotCursorReleased indicates that a seek was attempted
	// after the cursor reached EOF and released its snapshot.
	ErrOrderedIndexSnapshotCursorReleased = errors.New("hatDataStructure: ordered index snapshot cursor is released")
)

// OrderedIndexSnapshotCursor walks a stable view of an OrderedIndex. Unlike
// OrderedIndexIterator, mutations after creation do not invalidate it. The
// cursor itself is not safe for concurrent use; the underlying index remains
// safe for concurrent readers and writers.
type OrderedIndexSnapshotCursor[T any, K any] struct {
	index    *OrderedIndex[T, K]
	entries  []OrderedIndexEntry[T, K]
	position int
	closed   bool
	released bool
}

// SnapshotCursor returns a zero-copy cursor over the current sorted entries.
// While the cursor is live, index mutations use copy-on-write and therefore
// cannot alter the cursor's view. The caller must Close the cursor when it
// stops before EOF. The bool is false for a nil or empty index.
func (index *OrderedIndex[T, K]) SnapshotCursor() (OrderedIndexSnapshotCursor[T, K], bool) {
	if index == nil {
		return OrderedIndexSnapshotCursor[T, K]{}, false
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	if len(index.entries) == 0 {
		return OrderedIndexSnapshotCursor[T, K]{}, false
	}
	index.active.Add(1)
	return OrderedIndexSnapshotCursor[T, K]{index: index, entries: index.entries}, true
}

// Next returns the next entry from the stable snapshot. Reaching EOF releases
// the snapshot's copy-on-write protection.
func (cursor *OrderedIndexSnapshotCursor[T, K]) Next() (OrderedIndexEntry[T, K], bool, error) {
	if cursor == nil || cursor.index == nil {
		return OrderedIndexEntry[T, K]{}, false, ErrOrderedIndexSnapshotCursorNil
	}
	if cursor.closed {
		return OrderedIndexEntry[T, K]{}, false, ErrOrderedIndexSnapshotCursorClosed
	}
	if cursor.released {
		return OrderedIndexEntry[T, K]{}, false, nil
	}
	if cursor.position >= len(cursor.entries) {
		cursor.release()
		return OrderedIndexEntry[T, K]{}, false, nil
	}
	entry := cursor.entries[cursor.position]
	cursor.position++
	return entry, true, nil
}

// Seek positions the cursor at the first snapshot entry whose key is greater
// than or equal to key.
func (cursor *OrderedIndexSnapshotCursor[T, K]) Seek(key K) error {
	return cursor.seek(key, false)
}

// SeekAfter positions the cursor at the first snapshot entry whose key is
// strictly greater than key.
func (cursor *OrderedIndexSnapshotCursor[T, K]) SeekAfter(key K) error {
	return cursor.seek(key, true)
}

// Close releases the cursor's snapshot protection. A closed cursor cannot be
// resumed.
func (cursor *OrderedIndexSnapshotCursor[T, K]) Close() {
	if cursor == nil || cursor.index == nil || cursor.closed {
		return
	}
	cursor.closed = true
	cursor.release()
}

func (cursor *OrderedIndexSnapshotCursor[T, K]) seek(key K, strict bool) error {
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
		if strict {
			return comparison > 0
		}
		return comparison >= 0
	})
	return nil
}

func (cursor *OrderedIndexSnapshotCursor[T, K]) release() {
	if cursor.released {
		return
	}
	cursor.index.active.Add(-1)
	cursor.released = true
}
