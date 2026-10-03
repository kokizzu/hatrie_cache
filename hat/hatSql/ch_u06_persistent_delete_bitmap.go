package hatSql

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math/bits"
)

const (
	typedTableDeleteBitmapStateMagic   = "HTDB1"
	typedTableDeleteBitmapStateVersion = byte(1)
	// MaxTypedTableDeleteBitmapBytes bounds the compact bitmap state before
	// restore can allocate its word slice.
	MaxTypedTableDeleteBitmapBytes = MaxTypedTablePatchStateBytes
)

var (
	// ErrTypedTableDeleteBitmapStateUnsupported reports a table without patch parts.
	ErrTypedTableDeleteBitmapStateUnsupported = errors.New("typed table delete bitmap state is unsupported")
	// ErrTypedTableDeleteBitmapStateInvalid reports malformed or mismatched state.
	ErrTypedTableDeleteBitmapStateInvalid = errors.New("typed table delete bitmap state is invalid")
)

// MarshalDeleteBitmap returns a compact CRC-protected logical-delete snapshot.
// Unlike MarshalPatchState, it identifies physical rows by an order
// fingerprint instead of copying every row key into the payload.
func (table *TypedTable) MarshalDeleteBitmap() ([]byte, error) {
	if table == nil {
		return nil, fmt.Errorf("typed table is nil")
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	if table.patchParts == nil {
		return nil, ErrTypedTableDeleteBitmapStateUnsupported
	}
	if len(table.schema.Name) > int(^uint32(0)) || len(table.keys) > int(^uint32(0)) {
		return nil, fmt.Errorf("%w: table name or row count overflows format", ErrTypedTableDeleteBitmapStateInvalid)
	}
	rowCount := len(table.keys)
	wordCount := (rowCount + typedTableDeleteBitmapWordBits - 1) / typedTableDeleteBitmapWordBits
	deletedCount := 0
	for index := 0; index < wordCount; index++ {
		word := typedTableDeleteBitmapWord(table.patchParts.deleted, index, rowCount)
		deletedCount += bits.OnesCount64(word)
	}
	if deletedCount > rowCount {
		return nil, fmt.Errorf("%w: deleted row count is invalid", ErrTypedTableDeleteBitmapStateInvalid)
	}
	if len(table.schema.Name) > int(^uint32(0)) || uint64(wordCount) > uint64(^uint32(0)) {
		return nil, fmt.Errorf("%w: bitmap dimensions overflow format", ErrTypedTableDeleteBitmapStateInvalid)
	}
	size := len(typedTableDeleteBitmapStateMagic) + 4 + 4 + len(table.schema.Name) + 4 + 4 + 4 + sha256.Size + 4
	if !addTypedTablePatchStateSize(&size, wordCount*8) || size > MaxTypedTableDeleteBitmapBytes-4 {
		return nil, fmt.Errorf("%w: snapshot exceeds %d bytes", ErrTypedTableDeleteBitmapStateInvalid, MaxTypedTableDeleteBitmapBytes)
	}
	fingerprint, err := table.deleteBitmapFingerprintLocked()
	if err != nil {
		return nil, err
	}
	encoded := make([]byte, 0, size+4)
	encoded = append(encoded, typedTableDeleteBitmapStateMagic...)
	encoded = append(encoded, typedTableDeleteBitmapStateVersion, 0, 0, 0)
	encoded = appendUint32(encoded, uint32(len(table.schema.Name)))
	encoded = append(encoded, table.schema.Name...)
	encoded = appendUint32(encoded, uint32(rowCount))
	encoded = appendUint32(encoded, uint32(deletedCount))
	encoded = appendUint32(encoded, uint32(wordCount))
	encoded = append(encoded, fingerprint[:]...)
	for index := 0; index < wordCount; index++ {
		encoded = appendUint64(encoded, typedTableDeleteBitmapWord(table.patchParts.deleted, index, rowCount))
	}
	return appendUint32(encoded, crc32.ChecksumIEEE(encoded)), nil
}

// RestoreDeleteBitmap validates and atomically installs a compact logical
// delete snapshot for the table's current physical row order.
func (table *TypedTable) RestoreDeleteBitmap(encoded []byte) error {
	if table == nil {
		return fmt.Errorf("typed table is nil")
	}
	table.mu.RLock()
	patchEnabled := table.patchParts != nil
	table.mu.RUnlock()
	if !patchEnabled {
		return ErrTypedTableDeleteBitmapStateUnsupported
	}
	minimum := len(typedTableDeleteBitmapStateMagic) + 4 + 4 + 4 + 4 + 4 + sha256.Size + 4
	if len(encoded) < minimum || len(encoded) > MaxTypedTableDeleteBitmapBytes {
		return fmt.Errorf("%w: snapshot length is invalid", ErrTypedTableDeleteBitmapStateInvalid)
	}
	payloadLength := len(encoded) - 4
	if binary.LittleEndian.Uint32(encoded[payloadLength:]) != crc32.ChecksumIEEE(encoded[:payloadLength]) {
		return fmt.Errorf("%w: checksum mismatch", ErrTypedTableDeleteBitmapStateInvalid)
	}
	payload := encoded[:payloadLength]
	position := 0
	if !bytes.Equal(payload[position:position+len(typedTableDeleteBitmapStateMagic)], []byte(typedTableDeleteBitmapStateMagic)) {
		return fmt.Errorf("%w: magic mismatch", ErrTypedTableDeleteBitmapStateInvalid)
	}
	position += len(typedTableDeleteBitmapStateMagic)
	if position+4 > len(payload) || payload[position] != typedTableDeleteBitmapStateVersion {
		return fmt.Errorf("%w: unsupported version", ErrTypedTableDeleteBitmapStateInvalid)
	}
	position += 4
	tableName, ok := readTypedTablePatchStateString(payload, &position)
	if !ok {
		return fmt.Errorf("%w: table name is truncated", ErrTypedTableDeleteBitmapStateInvalid)
	}
	rowCount, ok := readTypedTablePatchStateUint32(payload, &position)
	if !ok {
		return fmt.Errorf("%w: row count is truncated", ErrTypedTableDeleteBitmapStateInvalid)
	}
	deletedCount, ok := readTypedTablePatchStateUint32(payload, &position)
	if !ok {
		return fmt.Errorf("%w: deleted row count is truncated", ErrTypedTableDeleteBitmapStateInvalid)
	}
	wordCount, ok := readTypedTablePatchStateUint32(payload, &position)
	if !ok {
		return fmt.Errorf("%w: word count is truncated", ErrTypedTableDeleteBitmapStateInvalid)
	}
	expectedWords := (uint64(rowCount) + typedTableDeleteBitmapWordBits - 1) / typedTableDeleteBitmapWordBits
	if uint64(wordCount) != expectedWords || uint64(deletedCount) > uint64(rowCount) {
		return fmt.Errorf("%w: bitmap dimensions are invalid", ErrTypedTableDeleteBitmapStateInvalid)
	}
	if len(payload)-position < sha256.Size {
		return fmt.Errorf("%w: row fingerprint is truncated", ErrTypedTableDeleteBitmapStateInvalid)
	}
	var expectedFingerprint [sha256.Size]byte
	copy(expectedFingerprint[:], payload[position:position+sha256.Size])
	position += sha256.Size
	if uint64(wordCount) > uint64(len(payload)-position)/8 {
		return fmt.Errorf("%w: bitmap is truncated", ErrTypedTableDeleteBitmapStateInvalid)
	}
	words := make([]uint64, int(wordCount))
	actualDeleted := 0
	for index := range words {
		words[index] = binary.LittleEndian.Uint64(payload[position : position+8])
		position += 8
		if index == len(words)-1 && rowCount%typedTableDeleteBitmapWordBits != 0 {
			mask := (uint64(1) << uint(rowCount%typedTableDeleteBitmapWordBits)) - 1
			if words[index]&^mask != 0 {
				return fmt.Errorf("%w: bitmap contains rows outside the table", ErrTypedTableDeleteBitmapStateInvalid)
			}
		}
		actualDeleted += bits.OnesCount64(words[index])
	}
	if actualDeleted != int(deletedCount) || position != len(payload) {
		return fmt.Errorf("%w: bitmap count or trailing bytes are invalid", ErrTypedTableDeleteBitmapStateInvalid)
	}

	table.mu.Lock()
	defer table.mu.Unlock()
	if table.patchParts == nil {
		return ErrTypedTableDeleteBitmapStateUnsupported
	}
	if table.schema.Name != tableName || len(table.keys) != int(rowCount) {
		return fmt.Errorf("%w: schema or row count mismatch", ErrTypedTableDeleteBitmapStateInvalid)
	}
	currentFingerprint, err := table.deleteBitmapFingerprintLocked()
	if err != nil {
		return err
	}
	if currentFingerprint != expectedFingerprint {
		return fmt.Errorf("%w: physical row order mismatch", ErrTypedTableDeleteBitmapStateInvalid)
	}
	table.patchParts.deleted.words = words
	table.patchParts.deletedCount = actualDeleted
	table.patchParts.mergeScheduled = false
	return nil
}

func typedTableDeleteBitmapFingerprint(keys []string) ([sha256.Size]byte, error) {
	hasher := sha256.New()
	var length [4]byte
	for index, key := range keys {
		if len(key) > int(^uint32(0)) {
			return [sha256.Size]byte{}, fmt.Errorf("%w: key %d exceeds fingerprint format", ErrTypedTableDeleteBitmapStateInvalid, index)
		}
		binary.LittleEndian.PutUint32(length[:], uint32(len(key)))
		if _, err := hasher.Write(length[:]); err != nil {
			return [sha256.Size]byte{}, err
		}
		if _, err := hasher.Write([]byte(key)); err != nil {
			return [sha256.Size]byte{}, err
		}
	}
	var fingerprint [sha256.Size]byte
	hasher.Sum(fingerprint[:0])
	return fingerprint, nil
}

func (table *TypedTable) deleteBitmapFingerprintLocked() ([sha256.Size]byte, error) {
	if table.deleteFPValid {
		return table.deleteFP, nil
	}
	fingerprint, err := typedTableDeleteBitmapFingerprint(table.keys)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	table.deleteFP = fingerprint
	table.deleteFPValid = true
	return fingerprint, nil
}

func (table *TypedTable) invalidateTypedTableDeleteBitmapFingerprintLocked() {
	if table == nil {
		return
	}
	table.deleteFPValid = false
}

func typedTableDeleteBitmapWord(bitmap typedTableDeleteBitmap, index, rows int) uint64 {
	if index < 0 || index >= len(bitmap.words) {
		return 0
	}
	word := bitmap.words[index]
	if rows > 0 && index == (rows-1)/typedTableDeleteBitmapWordBits && rows%typedTableDeleteBitmapWordBits != 0 {
		word &= (uint64(1) << uint(rows%typedTableDeleteBitmapWordBits)) - 1
	}
	return word
}
