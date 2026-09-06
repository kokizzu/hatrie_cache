package hatCodec

import (
	"encoding/binary"
	"errors"
	"math/bits"
)

var (
	ErrBitPackedNumericInvalid  = errors.New("hatriecache: bit-packed numeric encoding is invalid")
	ErrBitPackedNumericTooLarge = errors.New("hatriecache: bit-packed numeric encoding is too large")
)

// EncodeBitPackedUint64 stores a count, the minimum required bit width, and a
// tightly packed little-endian bit stream. It is useful when a numeric column
// has a much smaller range than uint64.
func EncodeBitPackedUint64(values []uint64) ([]byte, error) {
	if len(values) == 0 {
		return []byte{}, nil
	}
	maximum := uint64(0)
	for _, value := range values {
		if value > maximum {
			maximum = value
		}
	}
	width := bits.Len64(maximum)
	count := uint64(len(values))
	if width != 0 && count > (^uint64(0)-7)/uint64(width) {
		return nil, ErrBitPackedNumericTooLarge
	}
	totalBits := count * uint64(width)
	payloadBytes := (totalBits + 7) / 8
	maxInt := uint64(^uint(0) >> 1)
	if payloadBytes > maxInt-11 {
		return nil, ErrBitPackedNumericTooLarge
	}

	var scratch [binary.MaxVarintLen64]byte
	countBytes := binary.PutUvarint(scratch[:], count)
	encoded := make([]byte, countBytes+1+int(payloadBytes))
	copy(encoded, scratch[:countBytes])
	encoded[countBytes] = byte(width)
	payload := encoded[countBytes+1:]
	for index, value := range values {
		bitOffset := uint64(index) * uint64(width)
		for bit := 0; bit < width; bit++ {
			if value&(uint64(1)<<uint(bit)) == 0 {
				continue
			}
			payload[(bitOffset+uint64(bit))/8] |= byte(uint64(1) << uint((bitOffset+uint64(bit))&7))
		}
	}
	return encoded, nil
}

// DecodeBitPackedUint64 decodes the format produced by EncodeBitPackedUint64.
// It rejects truncated, overflowing, non-canonical, and trailing payloads.
func DecodeBitPackedUint64(encoded []byte) ([]uint64, error) {
	if len(encoded) == 0 {
		return []uint64{}, nil
	}
	count, offset := binary.Uvarint(encoded)
	if offset <= 0 || count == 0 || count > uint64(^uint(0)>>1) || offset >= len(encoded) {
		return nil, ErrBitPackedNumericInvalid
	}
	width := int(encoded[offset])
	offset++
	if width > 64 {
		return nil, ErrBitPackedNumericInvalid
	}
	if width != 0 && count > (^uint64(0)-7)/uint64(width) {
		return nil, ErrBitPackedNumericInvalid
	}
	totalBits := count * uint64(width)
	payloadBytes := (totalBits + 7) / 8
	if payloadBytes > uint64(len(encoded)-offset) || int(payloadBytes) != len(encoded)-offset {
		return nil, ErrBitPackedNumericInvalid
	}
	payload := encoded[offset:]
	if totalBits&7 != 0 && payload[len(payload)-1]&^byte((uint64(1)<<uint(totalBits&7))-1) != 0 {
		return nil, ErrBitPackedNumericInvalid
	}
	values := make([]uint64, int(count))
	for index := range values {
		bitOffset := uint64(index) * uint64(width)
		var value uint64
		for bit := 0; bit < width; bit++ {
			position := bitOffset + uint64(bit)
			if payload[position/8]&(byte(uint64(1)<<uint(position&7))) != 0 {
				value |= uint64(1) << uint(bit)
			}
		}
		values[index] = value
	}
	return values, nil
}
