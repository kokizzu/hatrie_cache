package hatDataStructure

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math/bits"
)

const (
	persistentDeleteBitmapMagic   = "HTDB1"
	persistentDeleteBitmapVersion = byte(1)

	// MaxPersistentDeleteBitmapBytes bounds both the encoded snapshot and the
	// words admitted by DecodePersistentDeleteBitmap before allocation.
	MaxPersistentDeleteBitmapBytes = 16 << 20
)

var (
	// ErrPersistentDeleteBitmapInvalid reports a malformed, corrupt, or
	// internally inconsistent bitmap or snapshot.
	ErrPersistentDeleteBitmapInvalid = errors.New("persistent delete bitmap is invalid")
	// ErrPersistentDeleteBitmapOutOfRange reports a row outside the bitmap.
	ErrPersistentDeleteBitmapOutOfRange = errors.New("persistent delete bitmap row is out of range")
)

var persistentDeleteBitmapCRCTable = crc32.MakeTable(crc32.Castagnoli)

// PersistentDeleteBitmap stores one logical-delete bit per physical row.
// It is not safe for concurrent use; the owning table or part must provide
// synchronization around mutation and snapshot operations.
type PersistentDeleteBitmap struct {
	rows    uint64
	deleted uint64
	words   []uint64
}

// NewPersistentDeleteBitmap creates an empty bitmap for rows physical rows.
// The row count is bounded so a malformed or accidental configuration cannot
// request an unbounded allocation.
func NewPersistentDeleteBitmap(rows uint64) (*PersistentDeleteBitmap, error) {
	wordCount, err := persistentDeleteBitmapWordCount(rows)
	if err != nil {
		return nil, err
	}
	return &PersistentDeleteBitmap{
		rows:  rows,
		words: make([]uint64, int(wordCount)),
	}, nil
}

// Rows returns the physical row count represented by the bitmap.
func (bitmap *PersistentDeleteBitmap) Rows() uint64 {
	if bitmap == nil {
		return 0
	}
	return bitmap.rows
}

// Deleted returns the number of logically deleted rows.
func (bitmap *PersistentDeleteBitmap) Deleted() uint64 {
	if bitmap == nil {
		return 0
	}
	return bitmap.deleted
}

// Live returns the number of rows that remain visible.
func (bitmap *PersistentDeleteBitmap) Live() uint64 {
	if bitmap == nil {
		return 0
	}
	return bitmap.rows - bitmap.deleted
}

// Contains reports whether row is logically deleted. Out-of-range rows are
// never contained, which makes it suitable for read-side filtering.
func (bitmap *PersistentDeleteBitmap) Contains(row uint64) bool {
	if bitmap == nil || row >= bitmap.rows {
		return false
	}
	return bitmap.words[row/64]&(uint64(1)<<uint(row%64)) != 0
}

// Delete marks row as logically deleted. The returned bool is false when the
// row was already deleted.
func (bitmap *PersistentDeleteBitmap) Delete(row uint64) (bool, error) {
	if bitmap == nil {
		return false, ErrPersistentDeleteBitmapInvalid
	}
	if row >= bitmap.rows {
		return false, ErrPersistentDeleteBitmapOutOfRange
	}
	word := &bitmap.words[row/64]
	mask := uint64(1) << uint(row%64)
	if *word&mask != 0 {
		return false, nil
	}
	*word |= mask
	bitmap.deleted++
	return true, nil
}

// Undelete clears a logical delete. The returned bool is false when row was
// already live.
func (bitmap *PersistentDeleteBitmap) Undelete(row uint64) (bool, error) {
	if bitmap == nil {
		return false, ErrPersistentDeleteBitmapInvalid
	}
	if row >= bitmap.rows {
		return false, ErrPersistentDeleteBitmapOutOfRange
	}
	word := &bitmap.words[row/64]
	mask := uint64(1) << uint(row%64)
	if *word&mask == 0 {
		return false, nil
	}
	*word &^= mask
	bitmap.deleted--
	return true, nil
}

// MarshalBinary returns a deterministic HTDB1 snapshot. The payload uses one
// bit per row and ends with a CRC32C checksum. The returned bytes do not alias
// the bitmap.
func (bitmap *PersistentDeleteBitmap) MarshalBinary() ([]byte, error) {
	if bitmap == nil {
		return nil, ErrPersistentDeleteBitmapInvalid
	}
	wordCount, err := persistentDeleteBitmapWordCount(bitmap.rows)
	if err != nil || uint64(len(bitmap.words)) != wordCount {
		return nil, ErrPersistentDeleteBitmapInvalid
	}
	deleted, valid := persistentDeleteBitmapCount(bitmap.words, bitmap.rows)
	if !valid || deleted != bitmap.deleted {
		return nil, ErrPersistentDeleteBitmapInvalid
	}
	if wordCount > uint64((MaxPersistentDeleteBitmapBytes-len(persistentDeleteBitmapMagic)-1-4)/8) {
		return nil, ErrPersistentDeleteBitmapInvalid
	}
	capacity := len(persistentDeleteBitmapMagic) + 1 + 3*binary.MaxVarintLen64 + int(wordCount)*8 + 4
	encoded := make([]byte, 0, capacity)
	encoded = append(encoded, persistentDeleteBitmapMagic...)
	encoded = append(encoded, persistentDeleteBitmapVersion)
	encoded = persistentDeleteBitmapAppendUvarint(encoded, bitmap.rows)
	encoded = persistentDeleteBitmapAppendUvarint(encoded, bitmap.deleted)
	encoded = persistentDeleteBitmapAppendUvarint(encoded, wordCount)
	var wordBytes [8]byte
	for _, word := range bitmap.words {
		binary.LittleEndian.PutUint64(wordBytes[:], word)
		encoded = append(encoded, wordBytes[:]...)
	}
	return persistentDeleteBitmapAppendChecksum(encoded), nil
}

// UnmarshalBinary validates and atomically replaces bitmap with an HTDB1
// snapshot. A failed decode leaves the receiver unchanged.
func (bitmap *PersistentDeleteBitmap) UnmarshalBinary(encoded []byte) error {
	if bitmap == nil {
		return ErrPersistentDeleteBitmapInvalid
	}
	decoded, err := DecodePersistentDeleteBitmap(encoded)
	if err != nil {
		return err
	}
	*bitmap = *decoded
	return nil
}

// DecodePersistentDeleteBitmap validates and decodes an HTDB1 snapshot. It
// checks the size, checksum, dimensions, tail bits, and population count
// before allocating the packed words.
func DecodePersistentDeleteBitmap(encoded []byte) (*PersistentDeleteBitmap, error) {
	minimum := len(persistentDeleteBitmapMagic) + 1 + 1 + 1 + 1 + 4
	if len(encoded) < minimum || len(encoded) > MaxPersistentDeleteBitmapBytes {
		return nil, ErrPersistentDeleteBitmapInvalid
	}
	payloadLength := len(encoded) - 4
	storedChecksum := binary.LittleEndian.Uint32(encoded[payloadLength:])
	if storedChecksum != crc32.Checksum(encoded[:payloadLength], persistentDeleteBitmapCRCTable) {
		return nil, ErrPersistentDeleteBitmapInvalid
	}
	payload := encoded[:payloadLength]
	position := 0
	if !bytes.Equal(payload[position:position+len(persistentDeleteBitmapMagic)], []byte(persistentDeleteBitmapMagic)) {
		return nil, ErrPersistentDeleteBitmapInvalid
	}
	position += len(persistentDeleteBitmapMagic)
	if position >= len(payload) || payload[position] != persistentDeleteBitmapVersion {
		return nil, ErrPersistentDeleteBitmapInvalid
	}
	position++
	rows, ok := persistentDeleteBitmapReadUvarint(payload, &position)
	if !ok {
		return nil, ErrPersistentDeleteBitmapInvalid
	}
	deleted, ok := persistentDeleteBitmapReadUvarint(payload, &position)
	if !ok {
		return nil, ErrPersistentDeleteBitmapInvalid
	}
	wordCount, ok := persistentDeleteBitmapReadUvarint(payload, &position)
	if !ok {
		return nil, ErrPersistentDeleteBitmapInvalid
	}
	expectedWords, err := persistentDeleteBitmapWordCount(rows)
	if err != nil || wordCount != expectedWords || deleted > rows {
		return nil, ErrPersistentDeleteBitmapInvalid
	}
	if wordCount > uint64(len(payload)-position)/8 || int(wordCount)*8 != len(payload)-position {
		return nil, ErrPersistentDeleteBitmapInvalid
	}
	words := make([]uint64, int(wordCount))
	for index := range words {
		words[index] = binary.LittleEndian.Uint64(payload[position : position+8])
		position += 8
	}
	actualDeleted, valid := persistentDeleteBitmapCount(words, rows)
	if !valid || actualDeleted != deleted || position != len(payload) {
		return nil, ErrPersistentDeleteBitmapInvalid
	}
	return &PersistentDeleteBitmap{rows: rows, deleted: deleted, words: words}, nil
}

func persistentDeleteBitmapWordCount(rows uint64) (uint64, error) {
	if rows > ^uint64(0)-63 {
		return 0, ErrPersistentDeleteBitmapInvalid
	}
	wordCount := (rows + 63) / 64
	maxInt := uint64(^uint(0) >> 1)
	if wordCount > maxInt || wordCount > uint64((MaxPersistentDeleteBitmapBytes-len(persistentDeleteBitmapMagic)-1-4)/8) {
		return 0, ErrPersistentDeleteBitmapInvalid
	}
	return wordCount, nil
}

func persistentDeleteBitmapCount(words []uint64, rows uint64) (uint64, bool) {
	if rows == 0 {
		return 0, len(words) == 0
	}
	wordCount := (rows + 63) / 64
	if uint64(len(words)) != wordCount {
		return 0, false
	}
	deleted := uint64(0)
	for index, word := range words {
		if index == len(words)-1 && rows%64 != 0 {
			mask := (uint64(1) << uint(rows%64)) - 1
			if word&^mask != 0 {
				return 0, false
			}
		}
		deleted += uint64(bits.OnesCount64(word))
	}
	return deleted, deleted <= rows
}

func persistentDeleteBitmapAppendUvarint(encoded []byte, value uint64) []byte {
	var buffer [binary.MaxVarintLen64]byte
	length := binary.PutUvarint(buffer[:], value)
	return append(encoded, buffer[:length]...)
}

func persistentDeleteBitmapReadUvarint(encoded []byte, position *int) (uint64, bool) {
	if position == nil || *position < 0 || *position >= len(encoded) {
		return 0, false
	}
	value, length := binary.Uvarint(encoded[*position:])
	if length <= 0 {
		return 0, false
	}
	*position += length
	return value, true
}

func persistentDeleteBitmapAppendChecksum(encoded []byte) []byte {
	checksum := crc32.Checksum(encoded, persistentDeleteBitmapCRCTable)
	var buffer [4]byte
	binary.LittleEndian.PutUint32(buffer[:], checksum)
	return append(encoded, buffer[:]...)
}
