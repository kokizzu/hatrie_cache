package hatDataStructure

import (
	"errors"
	"math/bits"
)

var (
	ErrNullableBitmapInvalid         = errors.New("hatriecache: nullable bitmap length is invalid")
	ErrNullableBitmapIndexOutOfRange = errors.New("hatriecache: nullable bitmap index is out of range")
)

// NullableBitmap stores one null flag per row using one bit per position. It
// is not synchronized; callers that share a bitmap concurrently must provide
// their own synchronization.
type NullableBitmap struct {
	words  []uint64
	length int
}

// NewNullableBitmap allocates a bitmap with length valid row positions. New
// positions are valid (not null).
func NewNullableBitmap(length int) (*NullableBitmap, error) {
	if length < 0 {
		return nil, ErrNullableBitmapInvalid
	}
	return &NullableBitmap{words: make([]uint64, bitmapWordCount(length)), length: length}, nil
}

// Len returns the number of row positions.
func (bitmap *NullableBitmap) Len() int {
	if bitmap == nil {
		return 0
	}
	return bitmap.length
}

// SetNull marks one row as null.
func (bitmap *NullableBitmap) SetNull(index int) error {
	if err := bitmap.validateIndex(index); err != nil {
		return err
	}
	bitmap.words[index>>6] |= uint64(1) << (index & 63)
	return nil
}

// SetValid clears one row's null flag.
func (bitmap *NullableBitmap) SetValid(index int) error {
	if err := bitmap.validateIndex(index); err != nil {
		return err
	}
	bitmap.words[index>>6] &^= uint64(1) << (index & 63)
	return nil
}

// IsNull reports whether one row is marked null.
func (bitmap *NullableBitmap) IsNull(index int) (bool, error) {
	if err := bitmap.validateIndex(index); err != nil {
		return false, err
	}
	return bitmap.words[index>>6]&(uint64(1)<<(index&63)) != 0, nil
}

// CountNulls counts set null flags.
func (bitmap *NullableBitmap) CountNulls() int {
	if bitmap == nil {
		return 0
	}
	count := 0
	for index, word := range bitmap.words {
		if index == len(bitmap.words)-1 && bitmap.length&63 != 0 {
			word &= uint64(1)<<(bitmap.length&63) - 1
		}
		count += bits.OnesCount64(word)
	}
	return count
}

// Resize changes the number of row positions while preserving flags in the
// intersection of the old and new ranges. New positions are valid.
func (bitmap *NullableBitmap) Resize(length int) error {
	if bitmap == nil || length < 0 {
		return ErrNullableBitmapInvalid
	}
	requiredWords := bitmapWordCount(length)
	oldWords := len(bitmap.words)
	if requiredWords > oldWords {
		if requiredWords <= cap(bitmap.words) {
			bitmap.words = bitmap.words[:requiredWords]
			clearBitmapWords(bitmap.words[oldWords:])
		} else {
			bitmap.words = append(bitmap.words, make([]uint64, requiredWords-oldWords)...)
		}
	} else {
		bitmap.words = bitmap.words[:requiredWords]
	}
	bitmap.length = length
	bitmap.clearTail()
	return nil
}

func (bitmap *NullableBitmap) validateIndex(index int) error {
	if bitmap == nil || index < 0 || index >= bitmap.length {
		return ErrNullableBitmapIndexOutOfRange
	}
	return nil
}

func (bitmap *NullableBitmap) clearTail() {
	if len(bitmap.words) == 0 || bitmap.length&63 == 0 {
		return
	}
	bitmap.words[len(bitmap.words)-1] &= uint64(1)<<(bitmap.length&63) - 1
}

func bitmapWordCount(length int) int {
	words := length / 64
	if length&63 != 0 {
		words++
	}
	return words
}

func clearBitmapWords(words []uint64) {
	for index := range words {
		words[index] = 0
	}
}
