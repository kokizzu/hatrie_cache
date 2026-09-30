package hatDataStructure

import (
	"errors"
	"math/bits"
	"sync"
)

// MaxBitsetIndexCapacity bounds the number of compact row slots in one index.
const MaxBitsetIndexCapacity = 1 << 26

var (
	// ErrBitsetIndexNil indicates that a method was called on a nil index.
	ErrBitsetIndexNil = errors.New("hatrie_cache: bitset index is nil")
	// ErrBitsetIndexCapacityInvalid indicates a zero, negative, or oversized capacity.
	ErrBitsetIndexCapacityInvalid = errors.New("hatrie_cache: bitset index capacity must be positive and bounded")
	// ErrBitsetIndexKeyCapacityInvalid indicates a negative map-sizing hint.
	ErrBitsetIndexKeyCapacityInvalid = errors.New("hatrie_cache: bitset index key capacity must be non-negative")
	// ErrBitsetIndexIDOutOfRange indicates a slot outside the configured range.
	ErrBitsetIndexIDOutOfRange = errors.New("hatrie_cache: bitset index slot is out of range")
)

// BitsetIndex is an exact secondary index for compact uint32 row slots. A key
// with one slot is stored inline and promotes to a packed bitmap only when it
// gains a second slot, keeping low-cardinality postings compact. Results are
// returned in ascending slot order.
type BitsetIndex[K comparable] struct {
	mu       sync.RWMutex
	capacity int
	keys     []K
	occupied []uint64
	postings map[K]bitsetPosting
	length   int
}

type bitsetPosting struct {
	words     []uint64
	singleton uint32
	count     uint32
}

// NewBitsetIndex allocates an index for capacity compact row slots. The slot
// range is [0, capacity). keyCapacity is only a map-sizing hint.
func NewBitsetIndex[K comparable](capacity, keyCapacity int) (*BitsetIndex[K], error) {
	if capacity <= 0 || capacity > MaxBitsetIndexCapacity {
		return nil, ErrBitsetIndexCapacityInvalid
	}
	if keyCapacity < 0 {
		return nil, ErrBitsetIndexKeyCapacityInvalid
	}
	var postings map[K]bitsetPosting
	if keyCapacity > 0 {
		postings = make(map[K]bitsetPosting, keyCapacity)
	}
	return &BitsetIndex[K]{
		capacity: capacity,
		keys:     make([]K, capacity),
		occupied: make([]uint64, (capacity+63)/64),
		postings: postings,
	}, nil
}

// Upsert assigns key to slot. Reusing a slot replaces its previous posting.
func (index *BitsetIndex[K]) Upsert(slot uint32, key K) error {
	if index == nil {
		return ErrBitsetIndexNil
	}
	if uint64(slot) >= uint64(index.capacity) {
		return ErrBitsetIndexIDOutOfRange
	}

	index.mu.Lock()
	defer index.mu.Unlock()
	if index.postings == nil {
		index.postings = make(map[K]bitsetPosting)
	}
	position := int(slot)
	wordIndex := int(slot >> 6)
	mask := uint64(1) << (slot & 63)
	if index.occupied[wordIndex]&mask != 0 {
		oldKey := index.keys[position]
		if oldKey == key {
			return nil
		}
		index.clearPostingLocked(oldKey, slot)
	} else {
		index.length++
	}
	index.keys[position] = key
	index.occupied[wordIndex] |= mask
	index.setPostingLocked(key, slot)
	return nil
}

// Delete removes a slot and reports whether it was present.
func (index *BitsetIndex[K]) Delete(slot uint32) bool {
	if index == nil || uint64(slot) >= uint64(index.capacity) {
		return false
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	position := int(slot)
	wordIndex := int(slot >> 6)
	mask := uint64(1) << (slot & 63)
	if index.occupied[wordIndex]&mask == 0 {
		return false
	}
	index.clearPostingLocked(index.keys[position], slot)
	var zero K
	index.keys[position] = zero
	index.occupied[wordIndex] &^= mask
	index.length--
	return true
}

// Contains reports exact membership for a slot/key pair.
func (index *BitsetIndex[K]) Contains(slot uint32, key K) bool {
	if index == nil || uint64(slot) >= uint64(index.capacity) {
		return false
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	position := int(slot)
	mask := uint64(1) << (slot & 63)
	return index.occupied[slot>>6]&mask != 0 && index.keys[position] == key
}

// LookupIDs returns all present slots for key in ascending order.
func (index *BitsetIndex[K]) LookupIDs(key K) []uint32 {
	return index.LookupIDsInto(key, nil)
}

// LookupIDsInto resets dst and appends all present slots for key. A caller can
// reuse dst capacity to avoid a result allocation on repeated lookups.
func (index *BitsetIndex[K]) LookupIDsInto(key K, dst []uint32) []uint32 {
	dst = dst[:0]
	if index == nil {
		return dst
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	posting, ok := index.postings[key]
	if !ok {
		return dst
	}
	if posting.count == 1 {
		return append(dst, posting.singleton)
	}
	for wordIndex, word := range posting.words {
		for word != 0 {
			bit := uint(bits.TrailingZeros64(word))
			dst = append(dst, uint32(wordIndex*64)+uint32(bit))
			word &= word - 1
		}
	}
	return dst
}

// Len returns the number of occupied slots.
func (index *BitsetIndex[K]) Len() int {
	if index == nil {
		return 0
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	return index.length
}

// DistinctKeys returns the number of keys with at least one occupied slot.
func (index *BitsetIndex[K]) DistinctKeys() int {
	if index == nil {
		return 0
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	return len(index.postings)
}

// Capacity returns the configured compact slot capacity.
func (index *BitsetIndex[K]) Capacity() int {
	if index == nil {
		return 0
	}
	return index.capacity
}

// Clear removes all slots and releases every key bitmap.
func (index *BitsetIndex[K]) Clear() {
	if index == nil {
		return
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	var zero K
	for position := range index.keys {
		index.keys[position] = zero
	}
	for wordIndex := range index.occupied {
		index.occupied[wordIndex] = 0
	}
	index.postings = nil
	index.length = 0
}

func (index *BitsetIndex[K]) setPostingLocked(key K, slot uint32) {
	posting, ok := index.postings[key]
	if !ok {
		index.postings[key] = bitsetPosting{singleton: slot, count: 1}
		return
	}
	if posting.count == 1 {
		if posting.singleton == slot {
			return
		}
		words := make([]uint64, (index.capacity+63)/64)
		words[posting.singleton>>6] |= uint64(1) << (posting.singleton & 63)
		words[slot>>6] |= uint64(1) << (slot & 63)
		index.postings[key] = bitsetPosting{words: words, count: 2}
		return
	}
	wordIndex := int(slot >> 6)
	mask := uint64(1) << (slot & 63)
	if posting.words[wordIndex]&mask != 0 {
		return
	}
	posting.words[wordIndex] |= mask
	posting.count++
	index.postings[key] = posting
}

func (index *BitsetIndex[K]) clearPostingLocked(key K, slot uint32) {
	posting, ok := index.postings[key]
	if !ok {
		return
	}
	if posting.count == 1 {
		if posting.singleton == slot {
			delete(index.postings, key)
		}
		return
	}
	wordIndex := int(slot >> 6)
	mask := uint64(1) << (slot & 63)
	if posting.words[wordIndex]&mask == 0 {
		return
	}
	posting.words[wordIndex] &^= mask
	if posting.count > 2 {
		posting.count--
		index.postings[key] = posting
		return
	}
	for wordIndex, word := range posting.words {
		if word == 0 {
			continue
		}
		remaining := uint32(wordIndex*64 + bits.TrailingZeros64(word))
		index.postings[key] = bitsetPosting{singleton: remaining, count: 1}
		return
	}
	delete(index.postings, key)
}
