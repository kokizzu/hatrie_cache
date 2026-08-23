// Package hatStorage provides reusable backing-storage primitives.
package hatStorage

import "math/bits"

// ReusableIndexes tracks vacant indexes in a compact bitmap plus reuse stack.
type ReusableIndexes struct {
	stack []int32
	bits  []uint64
	count int
}

func (indexes *ReusableIndexes) Len() int {
	if indexes == nil {
		return 0
	}
	return indexes.count
}
func (indexes *ReusableIndexes) Has(idx int32) bool {
	if indexes == nil || idx < 0 {
		return false
	}
	word, mask := indexBit(idx)
	return word < len(indexes.bits) && indexes.bits[word]&mask != 0
}
func (indexes *ReusableIndexes) Mark(idx int32) bool {
	if indexes == nil || idx < 0 {
		return false
	}
	word, mask := indexBit(idx)
	if word >= len(indexes.bits) {
		next := make([]uint64, word+1)
		copy(next, indexes.bits)
		indexes.bits = next
	}
	if indexes.bits[word]&mask != 0 {
		return false
	}
	indexes.bits[word] |= mask
	indexes.stack = append(indexes.stack, idx)
	indexes.count++
	return true
}
func (indexes *ReusableIndexes) Take() (int32, bool) {
	if indexes == nil {
		return 0, false
	}
	for len(indexes.stack) > 0 {
		last := len(indexes.stack) - 1
		idx := indexes.stack[last]
		indexes.stack[last] = 0
		indexes.stack = indexes.stack[:last]
		if indexes.Use(idx) {
			return idx, true
		}
	}
	return 0, false
}
func (indexes *ReusableIndexes) Use(idx int32) bool {
	if indexes == nil || idx < 0 {
		return false
	}
	word, mask := indexBit(idx)
	if word >= len(indexes.bits) || indexes.bits[word]&mask == 0 {
		return false
	}
	indexes.bits[word] &^= mask
	indexes.count--
	return true
}
func (indexes *ReusableIndexes) Compact(limit int) {
	if indexes == nil {
		return
	}
	if limit <= 0 {
		for i := range indexes.stack {
			indexes.stack[i] = 0
		}
		for i := range indexes.bits {
			indexes.bits[i] = 0
		}
		indexes.stack = nil
		indexes.bits = nil
		indexes.count = 0
		return
	}
	neededWords := (limit + 63) / 64
	if neededWords < len(indexes.bits) {
		for i := neededWords; i < len(indexes.bits); i++ {
			indexes.bits[i] = 0
		}
		indexes.bits = indexes.bits[:neededWords]
	}
	if neededWords == len(indexes.bits) && neededWords > 0 && limit%64 != 0 {
		indexes.bits[neededWords-1] &= (uint64(1) << uint(limit%64)) - 1
	}
	indexes.count = 0
	for _, word := range indexes.bits {
		indexes.count += bits.OnesCount64(word)
	}
	nextStack := indexes.stack[:0]
	for _, idx := range indexes.stack {
		if idx >= 0 && int(idx) < limit && indexes.Has(idx) {
			nextStack = append(nextStack, idx)
		}
	}
	for i := len(nextStack); i < len(indexes.stack); i++ {
		indexes.stack[i] = 0
	}
	indexes.stack = nextStack
	indexes.compactBackingSlices()
}
func (indexes *ReusableIndexes) compactBackingSlices() {
	if len(indexes.stack) == 0 {
		indexes.stack = nil
	} else if cap(indexes.stack) > 16 && len(indexes.stack)*4 < cap(indexes.stack) {
		next := make([]int32, len(indexes.stack))
		copy(next, indexes.stack)
		indexes.stack = next
	}
	if len(indexes.bits) == 0 {
		indexes.bits = nil
	} else if cap(indexes.bits) > 16 && len(indexes.bits)*4 < cap(indexes.bits) {
		next := make([]uint64, len(indexes.bits))
		copy(next, indexes.bits)
		indexes.bits = next
	}
}

// Clone returns an independent copy preserving live vacancies.
func (indexes *ReusableIndexes) Clone() ReusableIndexes {
	if indexes == nil || indexes.count == 0 {
		return ReusableIndexes{}
	}
	out := ReusableIndexes{stack: make([]int32, len(indexes.stack)), bits: make([]uint64, len(indexes.bits)), count: indexes.count}
	copy(out.stack, indexes.stack)
	copy(out.bits, indexes.bits)
	return out
}

// MetadataLengths returns the live stack and bitmap-word lengths.
func (indexes *ReusableIndexes) MetadataLengths() (int, int) {
	if indexes == nil {
		return 0, 0
	}
	return len(indexes.stack), len(indexes.bits)
}

// BackingBytes returns bytes retained by the metadata backing arrays.
func (indexes *ReusableIndexes) BackingBytes() uint64 {
	if indexes == nil {
		return 0
	}
	return uint64(cap(indexes.stack))*4 + uint64(cap(indexes.bits))*8
}
func indexBit(idx int32) (int, uint64) {
	value := int(idx)
	return value / 64, uint64(1) << uint(value%64)
}

// TrimReusableTail removes reusable entries at the tail and compacts metadata.
func TrimReusableTail[T any](values []T, indexes *ReusableIndexes) []T {
	var zero T
	trimmed := false
	for len(values) > 0 {
		idx := int32(len(values) - 1)
		if !indexes.Has(idx) {
			if trimmed {
				indexes.Compact(len(values))
			}
			return values
		}
		values[len(values)-1] = zero
		indexes.Use(idx)
		values = values[:len(values)-1]
		trimmed = true
	}
	if trimmed {
		indexes.Compact(0)
	}
	return values
}
