package hatDataStructure

import "errors"

var (
	// ErrPackedHashIndexNil indicates that an operation was attempted on a nil index.
	ErrPackedHashIndexNil = errors.New("hatDataStructure: packed hash index is nil")
	// ErrPackedHashIndexHashRequired indicates that no stable hash function was supplied.
	ErrPackedHashIndexHashRequired = errors.New("hatDataStructure: packed hash index hash function is required")
	// ErrPackedHashIndexDuplicateKey indicates that a build input contains a key more than once.
	ErrPackedHashIndexDuplicateKey = errors.New("hatDataStructure: packed hash index key already exists")
	// ErrPackedHashIndexTooLarge indicates that the slot count would overflow an int.
	ErrPackedHashIndexTooLarge = errors.New("hatDataStructure: packed hash index is too large")
)

// PackedHashIndex is an immutable exact-match index backed by a flat
// open-addressed table. It is intended for read-heavy snapshots: construction
// copies the supplied entries, while Lookup and Contains do not allocate or
// take a lock. The hash function must be deterministic for the lifetime of the
// index and should distribute keys uniformly.
type PackedHashIndex[T any, K comparable] struct {
	slots  []packedHashIndexSlot[T, K]
	used   []uint64
	hash   func(K) uint64
	length int
}

type packedHashIndexSlot[T any, K comparable] struct {
	hash  uint64
	entry HashIndexEntry[T, K]
}

// NewPackedHashIndex builds an immutable unique-key exact-match index. The
// input entries are copied; later changes to the input slice do not affect the
// index. A load factor of at most 75% leaves probe space for fast lookups.
func NewPackedHashIndex[T any, K comparable](hash func(K) uint64, entries []HashIndexEntry[T, K]) (*PackedHashIndex[T, K], error) {
	if hash == nil {
		return nil, ErrPackedHashIndexHashRequired
	}
	capacity, err := packedHashIndexCapacity(len(entries))
	if err != nil {
		return nil, err
	}
	index := &PackedHashIndex[T, K]{
		slots: make([]packedHashIndexSlot[T, K], capacity),
		used:  make([]uint64, (capacity+63)/64),
		hash:  hash,
	}
	for _, entry := range entries {
		if !index.insert(entry) {
			return nil, ErrPackedHashIndexDuplicateKey
		}
	}
	return index, nil
}

// Lookup returns the exact entry for key. The returned value is a copy of the
// immutable table entry.
func (index *PackedHashIndex[T, K]) Lookup(key K) (HashIndexEntry[T, K], bool) {
	if index == nil {
		return HashIndexEntry[T, K]{}, false
	}
	position, ok := index.find(key)
	if !ok {
		return HashIndexEntry[T, K]{}, false
	}
	return index.slots[position].entry, true
}

// Contains reports whether key is present.
func (index *PackedHashIndex[T, K]) Contains(key K) bool {
	if index == nil {
		return false
	}
	_, ok := index.find(key)
	return ok
}

// Len returns the number of distinct keys.
func (index *PackedHashIndex[T, K]) Len() int {
	if index == nil {
		return 0
	}
	return index.length
}

// Capacity returns the number of slots in the immutable table.
func (index *PackedHashIndex[T, K]) Capacity() int {
	if index == nil {
		return 0
	}
	return len(index.slots)
}

func (index *PackedHashIndex[T, K]) insert(entry HashIndexEntry[T, K]) bool {
	mask := uint64(len(index.slots) - 1)
	hash := index.hash(entry.Key)
	position := hash & mask
	for {
		slot := int(position)
		if !index.isUsed(slot) {
			index.setUsed(slot)
			index.slots[slot] = packedHashIndexSlot[T, K]{hash: hash, entry: entry}
			index.length++
			return true
		}
		current := index.slots[slot]
		if current.hash == hash && current.entry.Key == entry.Key {
			return false
		}
		position = (position + 1) & mask
	}
}

func (index *PackedHashIndex[T, K]) find(key K) (int, bool) {
	if len(index.slots) == 0 {
		return 0, false
	}
	hash := index.hash(key)
	mask := uint64(len(index.slots) - 1)
	position := hash & mask
	for {
		slot := int(position)
		if !index.isUsed(slot) {
			return 0, false
		}
		current := index.slots[slot]
		if current.hash == hash && current.entry.Key == key {
			return slot, true
		}
		position = (position + 1) & mask
	}
}

func (index *PackedHashIndex[T, K]) isUsed(slot int) bool {
	return index.used[slot>>6]&(uint64(1)<<uint(slot&63)) != 0
}

func (index *PackedHashIndex[T, K]) setUsed(slot int) {
	index.used[slot>>6] |= uint64(1) << uint(slot&63)
}

func packedHashIndexCapacity(length int) (int, error) {
	if length <= 0 {
		return 1, nil
	}
	maxInt := int(^uint(0) >> 1)
	capacity := 1
	for length > capacity*3/4 {
		if capacity > maxInt/2 {
			return 0, ErrPackedHashIndexTooLarge
		}
		capacity <<= 1
	}
	return capacity, nil
}
