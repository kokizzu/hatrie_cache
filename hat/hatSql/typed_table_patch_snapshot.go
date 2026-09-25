package hatSql

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math/bits"

	"github.com/cespare/xxhash/v2"
	"hatrie_cache/hat/hatMerkle"
)

const (
	// Version 2 replaces the serialized physical key list with a deterministic
	// 128-bit key-order fingerprint. Version 1 remains readable for upgrades.
	typedTablePatchStateMagic          = "HTDP1"
	typedTablePatchStateVersion        = byte(2)
	typedTablePatchStateLegacyVersion  = byte(1)
	typedTablePatchStateKeyDigestBytes = 16
	typedTablePatchStateKeyDigestSeedA = 0x9e3779b185ebca87
	typedTablePatchStateKeyDigestSeedB = 0xc2b2ae3d27d4eb4f

	// MaxTypedTablePatchStateBytes bounds encoded logical-delete snapshots
	// before they can allocate memory during restore.
	MaxTypedTablePatchStateBytes = 16 << 20
)

var (
	ErrTypedTablePatchStateUnsupported = errors.New("typed table patch state is unsupported")
	ErrTypedTablePatchStateInvalid     = errors.New("typed table patch state is invalid")
)

// MarshalPatchState returns a deterministic CRC-protected snapshot of the
// logical-delete bitmap. Row data is intentionally excluded; restore the same
// physical rows first, then restore this state before serving reads.
func (table *TypedTable) MarshalPatchState() ([]byte, error) {
	if table == nil {
		return nil, fmt.Errorf("typed table is nil")
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	if table.patchParts == nil {
		return nil, ErrTypedTablePatchStateUnsupported
	}
	if len(table.keys) > int(^uint32(0)) || len(table.schema.Name) > int(^uint32(0)) {
		return nil, fmt.Errorf("%w: row or table name count overflows format", ErrTypedTablePatchStateInvalid)
	}
	rowCount := len(table.keys)
	wordCount := (rowCount + typedTableDeleteBitmapWordBits - 1) / typedTableDeleteBitmapWordBits
	deletedCount := 0
	for index := 0; index < wordCount; index++ {
		word := uint64(0)
		if index < len(table.patchParts.deleted.words) {
			word = table.patchParts.deleted.words[index]
		}
		if index == wordCount-1 && rowCount%typedTableDeleteBitmapWordBits != 0 {
			word &= (uint64(1) << uint(rowCount%typedTableDeleteBitmapWordBits)) - 1
		}
		deletedCount += bits.OnesCount64(word)
	}
	if deletedCount > rowCount {
		return nil, fmt.Errorf("%w: deleted row count is invalid", ErrTypedTablePatchStateInvalid)
	}

	size := len(typedTablePatchStateMagic) + 4 + 4 + 4 + 4 + 4 + len(table.schema.Name) + 4
	for index, key := range table.keys {
		if len(key) > int(^uint32(0)) {
			return nil, fmt.Errorf("%w: key %d exceeds snapshot limit", ErrTypedTablePatchStateInvalid, index)
		}
	}
	if !addTypedTablePatchStateSize(&size, typedTablePatchStateKeyDigestBytes) ||
		!addTypedTablePatchStateSize(&size, wordCount*8) {
		return nil, fmt.Errorf("%w: snapshot exceeds %d bytes", ErrTypedTablePatchStateInvalid, MaxTypedTablePatchStateBytes)
	}
	if size > MaxTypedTablePatchStateBytes {
		return nil, fmt.Errorf("%w: snapshot exceeds %d bytes", ErrTypedTablePatchStateInvalid, MaxTypedTablePatchStateBytes)
	}

	encoded := make([]byte, 0, size+4)
	encoded = append(encoded, typedTablePatchStateMagic...)
	encoded = append(encoded, typedTablePatchStateVersion, 0, 0, 0)
	encoded = appendUint32(encoded, uint32(len(table.schema.Name)))
	encoded = append(encoded, table.schema.Name...)
	encoded = appendUint32(encoded, uint32(rowCount))
	encoded = appendUint32(encoded, uint32(deletedCount))
	encoded = appendUint32(encoded, uint32(wordCount))
	keyDigest := table.typedTablePatchStateKeyDigestLocked()
	encoded = append(encoded, keyDigest[:]...)
	for index := 0; index < wordCount; index++ {
		word := uint64(0)
		if index < len(table.patchParts.deleted.words) {
			word = table.patchParts.deleted.words[index]
		}
		if index == wordCount-1 && rowCount%typedTableDeleteBitmapWordBits != 0 {
			word &= (uint64(1) << uint(rowCount%typedTableDeleteBitmapWordBits)) - 1
		}
		encoded = appendUint64(encoded, word)
	}
	return appendUint32(encoded, crc32.ChecksumIEEE(encoded)), nil
}

// MarshalPatchStateWithManifest returns one delete snapshot together with
// immutable-part metadata that identifies the exact bytes. Counts are read
// from that same snapshot so concurrent table mutations cannot produce a
// descriptor for a different bitmap.
func (table *TypedTable) MarshalPatchStateWithManifest() ([]byte, hatMerkle.PartDeleteBitmap, error) {
	encoded, err := table.MarshalPatchState()
	if err != nil {
		return nil, hatMerkle.PartDeleteBitmap{}, err
	}
	rowCount, deletedCount, err := typedTablePatchStateCounts(encoded)
	if err != nil {
		return nil, hatMerkle.PartDeleteBitmap{}, err
	}
	bitmap, err := hatMerkle.BuildPartDeleteBitmap(encoded, uint64(rowCount), uint64(deletedCount))
	if err != nil {
		return nil, hatMerkle.PartDeleteBitmap{}, err
	}
	return encoded, bitmap, nil
}

func typedTablePatchStateCounts(encoded []byte) (uint32, uint32, error) {
	if len(encoded) < 4 {
		return 0, 0, fmt.Errorf("%w: snapshot is truncated", ErrTypedTablePatchStateInvalid)
	}
	payload := encoded[:len(encoded)-4]
	position := 0
	if len(payload) < len(typedTablePatchStateMagic)+4 ||
		!bytes.Equal(payload[:len(typedTablePatchStateMagic)], []byte(typedTablePatchStateMagic)) {
		return 0, 0, fmt.Errorf("%w: snapshot header is invalid", ErrTypedTablePatchStateInvalid)
	}
	position += len(typedTablePatchStateMagic) + 4
	tableNameLength, ok := readTypedTablePatchStateUint32(payload, &position)
	if !ok || uint64(tableNameLength) > uint64(len(payload)-position) {
		return 0, 0, fmt.Errorf("%w: table name is truncated", ErrTypedTablePatchStateInvalid)
	}
	position += int(tableNameLength)
	rowCount, ok := readTypedTablePatchStateUint32(payload, &position)
	if !ok {
		return 0, 0, fmt.Errorf("%w: row count is truncated", ErrTypedTablePatchStateInvalid)
	}
	deletedCount, ok := readTypedTablePatchStateUint32(payload, &position)
	if !ok {
		return 0, 0, fmt.Errorf("%w: deleted row count is truncated", ErrTypedTablePatchStateInvalid)
	}
	return rowCount, deletedCount, nil
}

// RestorePatchState validates and atomically installs a logical-delete
// snapshot for the table's current physical rows. The physical key order must
// match exactly because bitmap positions are physical row positions.
func (table *TypedTable) RestorePatchState(encoded []byte) error {
	if table == nil {
		return fmt.Errorf("typed table is nil")
	}
	table.mu.RLock()
	patchEnabled := table.patchParts != nil
	table.mu.RUnlock()
	if !patchEnabled {
		return ErrTypedTablePatchStateUnsupported
	}
	minimum := len(typedTablePatchStateMagic) + 4 + 4 + 4 + 4 + 4 + 4
	if len(encoded) > MaxTypedTablePatchStateBytes || len(encoded) < minimum {
		return fmt.Errorf("%w: snapshot length is invalid", ErrTypedTablePatchStateInvalid)
	}
	payloadLength := len(encoded) - 4
	if binary.LittleEndian.Uint32(encoded[payloadLength:]) != crc32.ChecksumIEEE(encoded[:payloadLength]) {
		return fmt.Errorf("%w: checksum mismatch", ErrTypedTablePatchStateInvalid)
	}
	payload := encoded[:payloadLength]
	position := 0
	if !bytes.Equal(payload[position:position+len(typedTablePatchStateMagic)], []byte(typedTablePatchStateMagic)) {
		return fmt.Errorf("%w: magic mismatch", ErrTypedTablePatchStateInvalid)
	}
	position += len(typedTablePatchStateMagic)
	if position+4 > len(payload) || (payload[position] != typedTablePatchStateVersion && payload[position] != typedTablePatchStateLegacyVersion) {
		return fmt.Errorf("%w: unsupported version", ErrTypedTablePatchStateInvalid)
	}
	version := payload[position]
	position += 4
	tableName, ok := readTypedTablePatchStateString(payload, &position)
	if !ok {
		return fmt.Errorf("%w: table name is truncated", ErrTypedTablePatchStateInvalid)
	}
	rowCount, ok := readTypedTablePatchStateUint32(payload, &position)
	if !ok {
		return fmt.Errorf("%w: row count is truncated", ErrTypedTablePatchStateInvalid)
	}
	deletedCount, ok := readTypedTablePatchStateUint32(payload, &position)
	if !ok {
		return fmt.Errorf("%w: deleted row count is truncated", ErrTypedTablePatchStateInvalid)
	}
	wordCount, ok := readTypedTablePatchStateUint32(payload, &position)
	if !ok {
		return fmt.Errorf("%w: word count is truncated", ErrTypedTablePatchStateInvalid)
	}
	expectedWords := (uint64(rowCount) + typedTableDeleteBitmapWordBits - 1) / typedTableDeleteBitmapWordBits
	if uint64(wordCount) != expectedWords || uint64(deletedCount) > uint64(rowCount) {
		return fmt.Errorf("%w: bitmap dimensions are invalid", ErrTypedTablePatchStateInvalid)
	}
	if version == typedTablePatchStateLegacyVersion && uint64(rowCount) > uint64(len(payload)-position)/4 {
		return fmt.Errorf("%w: row count exceeds encoded keys", ErrTypedTablePatchStateInvalid)
	}
	if version == typedTablePatchStateVersion && len(payload)-position < typedTablePatchStateKeyDigestBytes {
		return fmt.Errorf("%w: key digest is truncated", ErrTypedTablePatchStateInvalid)
	}
	table.mu.Lock()
	defer table.mu.Unlock()
	if table.patchParts == nil {
		return ErrTypedTablePatchStateUnsupported
	}
	if table.schema.Name != tableName || len(table.keys) != int(rowCount) {
		return fmt.Errorf("%w: schema or row count mismatch", ErrTypedTablePatchStateInvalid)
	}
	if version == typedTablePatchStateLegacyVersion {
		for index, key := range table.keys {
			if !readTypedTablePatchStateKey(payload, &position, key) {
				return fmt.Errorf("%w: physical key order mismatch at row %d", ErrTypedTablePatchStateInvalid, index)
			}
		}
	} else {
		var expectedDigest [typedTablePatchStateKeyDigestBytes]byte
		copy(expectedDigest[:], payload[position:position+typedTablePatchStateKeyDigestBytes])
		position += typedTablePatchStateKeyDigestBytes
		actualDigest := table.typedTablePatchStateKeyDigestLocked()
		if !bytes.Equal(expectedDigest[:], actualDigest[:]) {
			return fmt.Errorf("%w: physical key digest mismatch", ErrTypedTablePatchStateInvalid)
		}
	}
	if uint64(wordCount) > uint64(len(payload)-position)/8 {
		return fmt.Errorf("%w: bitmap is truncated", ErrTypedTablePatchStateInvalid)
	}
	words := make([]uint64, int(wordCount))
	actualDeleted := 0
	for index := range words {
		words[index] = binary.LittleEndian.Uint64(payload[position : position+8])
		position += 8
		if index == len(words)-1 && rowCount%typedTableDeleteBitmapWordBits != 0 {
			mask := (uint64(1) << uint(rowCount%typedTableDeleteBitmapWordBits)) - 1
			if words[index]&^mask != 0 {
				return fmt.Errorf("%w: bitmap contains rows outside the table", ErrTypedTablePatchStateInvalid)
			}
		}
		actualDeleted += bits.OnesCount64(words[index])
	}
	if actualDeleted != int(deletedCount) || position != len(payload) {
		return fmt.Errorf("%w: bitmap count or trailing bytes are invalid", ErrTypedTablePatchStateInvalid)
	}
	table.patchParts.deleted.words = words
	table.patchParts.deletedCount = actualDeleted
	table.patchParts.mergeScheduled = false
	return nil
}

func addTypedTablePatchStateSize(size *int, extra int) bool {
	if size == nil || extra < 0 || *size > MaxTypedTablePatchStateBytes-extra {
		return false
	}
	*size += extra
	return true
}

func typedTablePatchStateKeyDigest(keys []string) [typedTablePatchStateKeyDigestBytes]byte {
	// The fingerprint is for compact layout identity, not authentication. The
	// enclosing snapshot CRC still detects accidental corruption.
	left := xxhash.NewWithSeed(typedTablePatchStateKeyDigestSeedA)
	right := xxhash.NewWithSeed(typedTablePatchStateKeyDigestSeedB)
	var length [4]byte
	for _, key := range keys {
		binary.LittleEndian.PutUint32(length[:], uint32(len(key)))
		_, _ = left.Write(length[:])
		_, _ = right.Write(length[:])
		_, _ = left.WriteString(key)
		_, _ = right.WriteString(key)
	}
	var result [typedTablePatchStateKeyDigestBytes]byte
	binary.LittleEndian.PutUint64(result[:8], left.Sum64())
	binary.LittleEndian.PutUint64(result[8:], right.Sum64())
	return result
}

func (table *TypedTable) typedTablePatchStateKeyDigestLocked() [typedTablePatchStateKeyDigestBytes]byte {
	state := table.patchParts
	if state == nil {
		return typedTablePatchStateKeyDigest(table.keys)
	}
	state.keyDigestMu.Lock()
	defer state.keyDigestMu.Unlock()
	if state.keyDigestValid && state.keyDigestGen == table.patchStateKeyLayoutGeneration {
		return state.keyDigest
	}
	state.keyDigest = typedTablePatchStateKeyDigest(table.keys)
	state.keyDigestGen = table.patchStateKeyLayoutGeneration
	state.keyDigestValid = true
	return state.keyDigest
}

func readTypedTablePatchStateUint32(encoded []byte, position *int) (uint32, bool) {
	if position == nil || *position < 0 || len(encoded)-*position < 4 {
		return 0, false
	}
	value := binary.LittleEndian.Uint32(encoded[*position : *position+4])
	*position += 4
	return value, true
}

func readTypedTablePatchStateString(encoded []byte, position *int) (string, bool) {
	length, ok := readTypedTablePatchStateUint32(encoded, position)
	if !ok || uint64(length) > uint64(len(encoded)-*position) {
		return "", false
	}
	end := *position + int(length)
	value := string(encoded[*position:end])
	*position = end
	return value, true
}

func readTypedTablePatchStateKey(encoded []byte, position *int, expected string) bool {
	length, ok := readTypedTablePatchStateUint32(encoded, position)
	if !ok || uint64(length) > uint64(len(encoded)-*position) {
		return false
	}
	end := *position + int(length)
	matched := len(expected) == int(length) && bytes.Equal(encoded[*position:end], []byte(expected))
	*position = end
	return matched
}
