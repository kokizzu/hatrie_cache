package hatStorage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math/bits"

	"hatrie_cache/hat/hatDataStructure"
)

const (
	persistentDeleteBitmapMagic         = "PDB1"
	persistentDeleteBitmapHeaderBytes   = 20
	persistentDeleteBitmapChecksumBytes = 4
	persistentDeleteBitmapEncodingRuns  = 1
	persistentDeleteBitmapEncodingWords = 2
	persistentDeleteBitmapRunMaxRows    = 16 << 10
	// PersistentDeleteBitmapMaxBytes bounds one persisted stored-part bitmap.
	PersistentDeleteBitmapMaxBytes = 64 << 20
)

var (
	// ErrPersistentDeleteBitmapCorrupt reports malformed or checksum-invalid data.
	ErrPersistentDeleteBitmapCorrupt = errors.New("persistent delete bitmap is corrupt")
	// ErrPersistentDeleteBitmapTooLarge reports a bitmap over the bounded wire size.
	ErrPersistentDeleteBitmapTooLarge = errors.New("persistent delete bitmap is too large")
	// ErrPersistentDeleteBitmapNil reports a nil bitmap receiver.
	ErrPersistentDeleteBitmapNil = errors.New("persistent delete bitmap is nil")
)

var persistentDeleteBitmapCRCTable = crc32.MakeTable(crc32.Castagnoli)

// PersistentDeleteBitmap tracks deleted physical rows for one stored part.
// Mutations are intended to be serialized by the owning storage part; the
// bitmap itself does not add a lock to the hot row-update path.
type PersistentDeleteBitmap struct {
	rowCount     uint32
	deleted      hatDataStructure.RoaringBitmap
	deletedCount uint64
	denseWords   []uint64
}

// NewPersistentDeleteBitmap creates an empty bitmap for rowCount physical rows.
func NewPersistentDeleteBitmap(rowCount uint32) *PersistentDeleteBitmap {
	return &PersistentDeleteBitmap{
		rowCount: rowCount,
		deleted:  hatDataStructure.NewRoaringBitmap(),
	}
}

// RowCount returns the physical row bound.
func (bitmap *PersistentDeleteBitmap) RowCount() uint32 {
	if bitmap == nil {
		return 0
	}
	return bitmap.rowCount
}

// DeletedCount returns the number of marked rows.
func (bitmap *PersistentDeleteBitmap) DeletedCount() uint64 {
	if bitmap == nil {
		return 0
	}
	return bitmap.deletedCount
}

// Mark marks row and reports whether it was newly deleted.
func (bitmap *PersistentDeleteBitmap) Mark(row uint32) bool {
	if bitmap == nil || row >= bitmap.rowCount {
		return false
	}
	if bitmap.denseWords != nil {
		wordIndex := row / 64
		mask := uint64(1) << (row % 64)
		if bitmap.denseWords[wordIndex]&mask != 0 {
			return false
		}
		bitmap.denseWords[wordIndex] |= mask
		bitmap.deletedCount++
		return true
	}
	if bitmap.deleted.Add(row) == 0 {
		return false
	}
	bitmap.deletedCount++
	if bitmap.deletedCount > persistentDeleteBitmapRunMaxRows {
		bitmap.promoteDense()
	}
	return true
}

// Unmark clears row and reports whether it was previously marked.
func (bitmap *PersistentDeleteBitmap) Unmark(row uint32) bool {
	if bitmap == nil || row >= bitmap.rowCount {
		return false
	}
	if bitmap.denseWords != nil {
		wordIndex := row / 64
		mask := uint64(1) << (row % 64)
		if bitmap.denseWords[wordIndex]&mask == 0 {
			return false
		}
		bitmap.denseWords[wordIndex] &^= mask
		bitmap.deletedCount--
		return true
	}
	if bitmap.deleted.Remove(row) == 0 {
		return false
	}
	bitmap.deletedCount--
	return true
}

// Contains reports whether row is marked deleted.
func (bitmap *PersistentDeleteBitmap) Contains(row uint32) bool {
	if bitmap == nil || row >= bitmap.rowCount {
		return false
	}
	if bitmap.denseWords != nil {
		return bitmap.denseWords[row/64]&(uint64(1)<<(row%64)) != 0
	}
	return bitmap.deleted.Contains(row)
}

// VisitDeleted visits marked rows in ascending order. Returning false stops
// iteration and makes VisitDeleted return false.
func (bitmap *PersistentDeleteBitmap) VisitDeleted(visit func(uint32) bool) bool {
	if bitmap == nil || visit == nil {
		return false
	}
	if bitmap.denseWords != nil {
		return visitPersistentDeleteBitmapWords(bitmap.denseWords, visit)
	}
	completed := true
	bitmap.deleted.VisitContainers(func(key uint16, _ uint32, values []uint16, words []uint64) bool {
		for _, value := range values {
			if !visit(uint32(key)<<16 | uint32(value)) {
				completed = false
				return false
			}
		}
		for wordIndex, word := range words {
			for word != 0 {
				value := wordIndex*64 + bits.TrailingZeros64(word)
				if !visit(uint32(key)<<16 | uint32(value)) {
					completed = false
					return false
				}
				word &= word - 1
			}
		}
		return true
	})
	return completed
}

func (bitmap *PersistentDeleteBitmap) promoteDense() {
	wordCount := (uint64(bitmap.rowCount) + 63) / 64
	if wordCount*8 > PersistentDeleteBitmapMaxBytes {
		return
	}
	words := make([]uint64, int(wordCount))
	persistentDeleteBitmapDenseWords(words, bitmap.deleted, bitmap.rowCount)
	bitmap.denseWords = words
	bitmap.deleted = hatDataStructure.NewRoaringBitmap()
}

func visitPersistentDeleteBitmapWords(words []uint64, visit func(uint32) bool) bool {
	for wordIndex, word := range words {
		for word != 0 {
			row := uint32(wordIndex*64 + bits.TrailingZeros64(word))
			if !visit(row) {
				return false
			}
			word &= word - 1
		}
	}
	return true
}

// MarshalBinary encodes the bitmap as a bounded PDB1 frame. It selects
// run/delta encoding for sparse or contiguous deletes and dense words when
// that representation is smaller.
func (bitmap *PersistentDeleteBitmap) MarshalBinary() ([]byte, error) {
	if bitmap == nil {
		return nil, ErrPersistentDeleteBitmapNil
	}
	deletedCount := bitmap.deletedCount
	if deletedCount > uint64(^uint32(0)) {
		return nil, ErrPersistentDeleteBitmapTooLarge
	}
	wordCount := (uint64(bitmap.rowCount) + 63) / 64
	densePayloadSize := wordCount * 8
	denseSize := uint64(persistentDeleteBitmapHeaderBytes+persistentDeleteBitmapChecksumBytes) + densePayloadSize
	var runPayload []byte
	runCount := 0
	runSize := uint64(0)
	if deletedCount <= persistentDeleteBitmapRunMaxRows {
		values := bitmap.deleted.Values()
		runPayload, runCount = persistentDeleteBitmapRuns(values)
		runSize = uint64(persistentDeleteBitmapHeaderBytes + len(runPayload) + persistentDeleteBitmapChecksumBytes)
	}
	if runSize == 0 || (denseSize <= runSize && denseSize <= PersistentDeleteBitmapMaxBytes) {
		runSize = denseSize
	}
	if runSize > PersistentDeleteBitmapMaxBytes && denseSize > PersistentDeleteBitmapMaxBytes {
		return nil, ErrPersistentDeleteBitmapTooLarge
	}

	useDense := deletedCount > persistentDeleteBitmapRunMaxRows || denseSize < runSize
	if useDense {
		if densePayloadSize > uint64(PersistentDeleteBitmapMaxBytes) {
			return nil, ErrPersistentDeleteBitmapTooLarge
		}
		data := make([]byte, persistentDeleteBitmapHeaderBytes+int(densePayloadSize)+persistentDeleteBitmapChecksumBytes)
		copy(data[:4], persistentDeleteBitmapMagic)
		data[4] = persistentDeleteBitmapEncodingWords
		binary.LittleEndian.PutUint32(data[8:12], bitmap.rowCount)
		binary.LittleEndian.PutUint32(data[12:16], uint32(deletedCount))
		payload := data[persistentDeleteBitmapHeaderBytes : len(data)-persistentDeleteBitmapChecksumBytes]
		if bitmap.denseWords != nil {
			for index, word := range bitmap.denseWords {
				binary.LittleEndian.PutUint64(payload[index*8:], word)
			}
		} else {
			persistentDeleteBitmapDensePayload(payload, bitmap.deleted, bitmap.rowCount)
		}
		checksumOffset := len(data) - persistentDeleteBitmapChecksumBytes
		binary.LittleEndian.PutUint32(data[checksumOffset:], crc32.Checksum(data[:checksumOffset], persistentDeleteBitmapCRCTable))
		return data, nil
	}

	data := make([]byte, persistentDeleteBitmapHeaderBytes+len(runPayload)+persistentDeleteBitmapChecksumBytes)
	copy(data[:4], persistentDeleteBitmapMagic)
	data[4] = persistentDeleteBitmapEncodingRuns
	binary.LittleEndian.PutUint32(data[8:12], bitmap.rowCount)
	binary.LittleEndian.PutUint32(data[12:16], uint32(deletedCount))
	binary.LittleEndian.PutUint32(data[16:20], uint32(runCount))
	copy(data[persistentDeleteBitmapHeaderBytes:], runPayload)
	checksumOffset := len(data) - persistentDeleteBitmapChecksumBytes
	binary.LittleEndian.PutUint32(data[checksumOffset:], crc32.Checksum(data[:checksumOffset], persistentDeleteBitmapCRCTable))
	return data, nil
}

func persistentDeleteBitmapDensePayload(payload []byte, bitmap hatDataStructure.RoaringBitmap, rowCount uint32) {
	words := make([]uint64, len(payload)/8)
	persistentDeleteBitmapDenseWords(words, bitmap, rowCount)
	for index, word := range words {
		binary.LittleEndian.PutUint64(payload[index*8:], word)
	}
}

func persistentDeleteBitmapDenseWords(words []uint64, bitmap hatDataStructure.RoaringBitmap, rowCount uint32) {
	bitmap.VisitContainers(func(key uint16, _ uint32, values []uint16, containerWords []uint64) bool {
		baseWord := uint64(key) * 1024
		for wordIndex, word := range containerWords {
			wordOffset := baseWord + uint64(wordIndex)
			if wordOffset >= uint64(len(words)) {
				break
			}
			words[wordOffset] = word
		}
		for _, value := range values {
			row := uint64(key)<<16 | uint64(value)
			if row >= uint64(rowCount) {
				continue
			}
			words[row/64] |= uint64(1) << (row % 64)
		}
		return true
	})
}

// UnmarshalBinary replaces bitmap with a validated PDB1 frame.
func (bitmap *PersistentDeleteBitmap) UnmarshalBinary(data []byte) error {
	if bitmap == nil {
		return ErrPersistentDeleteBitmapNil
	}
	decoded, err := DecodePersistentDeleteBitmap(data)
	if err != nil {
		return err
	}
	*bitmap = *decoded
	return nil
}

// DecodePersistentDeleteBitmap validates and decodes one PDB1 frame.
func DecodePersistentDeleteBitmap(data []byte) (*PersistentDeleteBitmap, error) {
	minimum := persistentDeleteBitmapHeaderBytes + persistentDeleteBitmapChecksumBytes
	if len(data) < minimum || len(data) > PersistentDeleteBitmapMaxBytes {
		return nil, persistentDeleteBitmapCorrupt("invalid length")
	}
	if string(data[:4]) != persistentDeleteBitmapMagic || data[5] != 0 || data[6] != 0 || data[7] != 0 {
		return nil, persistentDeleteBitmapCorrupt("invalid header")
	}
	checksumOffset := len(data) - persistentDeleteBitmapChecksumBytes
	wantChecksum := binary.LittleEndian.Uint32(data[checksumOffset:])
	if gotChecksum := crc32.Checksum(data[:checksumOffset], persistentDeleteBitmapCRCTable); gotChecksum != wantChecksum {
		return nil, persistentDeleteBitmapCorrupt("checksum mismatch")
	}

	rowCount := binary.LittleEndian.Uint32(data[8:12])
	deletedCount := uint64(binary.LittleEndian.Uint32(data[12:16]))
	runCount := binary.LittleEndian.Uint32(data[16:20])
	bitmap := NewPersistentDeleteBitmap(rowCount)
	payload := data[persistentDeleteBitmapHeaderBytes:checksumOffset]
	switch data[4] {
	case persistentDeleteBitmapEncodingRuns:
		if err := decodePersistentDeleteBitmapRuns(bitmap, payload, runCount, deletedCount); err != nil {
			return nil, err
		}
	case persistentDeleteBitmapEncodingWords:
		if runCount != 0 {
			return nil, persistentDeleteBitmapCorrupt("dense frame has runs")
		}
		if err := decodePersistentDeleteBitmapWords(bitmap, payload, deletedCount); err != nil {
			return nil, err
		}
	default:
		return nil, persistentDeleteBitmapCorrupt("unknown encoding")
	}
	return bitmap, nil
}

func persistentDeleteBitmapRuns(values []uint32) ([]byte, int) {
	payload := make([]byte, 0, len(values)*2)
	previousEnd := uint64(0)
	runCount := 0
	for index := 0; index < len(values); {
		start := uint64(values[index])
		end := start + 1
		index++
		for index < len(values) && uint64(values[index]) == end {
			end++
			index++
		}
		payload = appendPersistentDeleteBitmapUvarint(payload, start-previousEnd)
		payload = appendPersistentDeleteBitmapUvarint(payload, end-start-1)
		previousEnd = end
		runCount++
	}
	return payload, runCount
}

func decodePersistentDeleteBitmapRuns(bitmap *PersistentDeleteBitmap, payload []byte, runCount uint32, deletedCount uint64) error {
	offset := 0
	previousEnd := uint64(0)
	decodedCount := uint64(0)
	for run := uint32(0); run < runCount; run++ {
		gap, err := readPersistentDeleteBitmapUvarint(payload, &offset)
		if err != nil {
			return err
		}
		lengthMinusOne, err := readPersistentDeleteBitmapUvarint(payload, &offset)
		if err != nil {
			return err
		}
		if previousEnd > uint64(bitmap.rowCount) || gap > uint64(bitmap.rowCount)-previousEnd {
			return persistentDeleteBitmapCorrupt("run start out of range")
		}
		start := previousEnd + gap
		length := lengthMinusOne + 1
		if start >= uint64(bitmap.rowCount) || length > uint64(bitmap.rowCount)-start {
			return persistentDeleteBitmapCorrupt("run end out of range")
		}
		end := start + length
		if run > 0 && gap == 0 {
			return persistentDeleteBitmapCorrupt("adjacent runs")
		}
		for row := start; row < end; row++ {
			bitmap.Mark(uint32(row))
		}
		decodedCount += length
		if decodedCount > deletedCount {
			return persistentDeleteBitmapCorrupt("delete count overflow")
		}
		previousEnd = end
	}
	if offset != len(payload) || decodedCount != deletedCount {
		return persistentDeleteBitmapCorrupt("run payload mismatch")
	}
	return nil
}

func decodePersistentDeleteBitmapWords(bitmap *PersistentDeleteBitmap, payload []byte, deletedCount uint64) error {
	wordCount := (uint64(bitmap.rowCount) + 63) / 64
	if uint64(len(payload)) != wordCount*8 {
		return persistentDeleteBitmapCorrupt("dense payload length mismatch")
	}
	decodedCount := uint64(0)
	for index := uint64(0); index < wordCount; index++ {
		word := binary.LittleEndian.Uint64(payload[index*8 : index*8+8])
		if index == wordCount-1 && bitmap.rowCount%64 != 0 {
			validMask := (uint64(1) << (bitmap.rowCount % 64)) - 1
			if word&^validMask != 0 {
				return persistentDeleteBitmapCorrupt("dense row out of range")
			}
		}
		decodedCount += uint64(bits.OnesCount64(word))
		for word != 0 {
			row := index*64 + uint64(bits.TrailingZeros64(word))
			bitmap.deleted.Add(uint32(row))
			word &= word - 1
		}
	}
	if decodedCount != deletedCount {
		return persistentDeleteBitmapCorrupt("dense delete count mismatch")
	}
	bitmap.deletedCount = decodedCount
	return nil
}

func appendPersistentDeleteBitmapUvarint(dst []byte, value uint64) []byte {
	var encoded [10]byte
	length := binary.PutUvarint(encoded[:], value)
	return append(dst, encoded[:length]...)
}

func readPersistentDeleteBitmapUvarint(data []byte, offset *int) (uint64, error) {
	if *offset >= len(data) {
		return 0, persistentDeleteBitmapCorrupt("truncated varint")
	}
	value, length := binary.Uvarint(data[*offset:])
	if length <= 0 {
		return 0, persistentDeleteBitmapCorrupt("invalid varint")
	}
	*offset += length
	return value, nil
}

func persistentDeleteBitmapCorrupt(detail string) error {
	return errors.Join(ErrPersistentDeleteBitmapCorrupt, errors.New(detail))
}
