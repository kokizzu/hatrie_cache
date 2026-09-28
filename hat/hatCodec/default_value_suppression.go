package hatCodec

import (
	"encoding/binary"
	"errors"
	"math/bits"
)

var (
	ErrCompactUint64Invalid  = errors.New("hatriecache: compact uint64 encoding is invalid")
	ErrCompactUint64TooLarge = errors.New("hatriecache: compact uint64 encoding is too large")
)

// CompactUint64Encoding identifies the representation selected for a compact
// unsigned integer block.
type CompactUint64Encoding uint8

const (
	CompactUint64EncodingRaw CompactUint64Encoding = iota
	CompactUint64EncodingBitPacked
	CompactUint64EncodingDefaultSuppressed
)

const (
	compactUint64Magic      = "HCS1"
	compactUint64Header     = len(compactUint64Magic) + 1
	maxCompactUint64Values  = uint64(1 << 27)
	minCompactDefaultValues = 8
)

// SelectCompactUint64Encoding chooses the smallest representation. Ties keep
// the raw format, then the existing bit-packed format, for compatibility.
func SelectCompactUint64Encoding(values []uint64) CompactUint64Encoding {
	if len(values) == 0 || uint64(len(values)) > maxCompactUint64Values {
		return CompactUint64EncodingRaw
	}
	rawBytes, rawOK := compactUint64RawPayloadSize(len(values))
	packedBytes, packedOK := compactUint64BitPackedPayloadSize(values)
	suppressedBytes, suppressedOK := compactUint64DefaultPayloadSize(values)

	bestEncoding := CompactUint64EncodingRaw
	bestBytes := rawBytes
	bestOK := rawOK
	if packedOK && (!bestOK || packedBytes < bestBytes) {
		bestEncoding = CompactUint64EncodingBitPacked
		bestBytes = packedBytes
		bestOK = true
	}
	if suppressedOK && (!bestOK || suppressedBytes < bestBytes) {
		bestEncoding = CompactUint64EncodingDefaultSuppressed
	}
	return bestEncoding
}

// EncodeCompactUint64 writes a bounded, versioned block using the smallest
// exact representation among raw, bit-packed, and zero-suppressed values.
func EncodeCompactUint64(values []uint64) ([]byte, error) {
	if uint64(len(values)) > maxCompactUint64Values {
		return nil, ErrCompactUint64TooLarge
	}
	choice := SelectCompactUint64Encoding(values)
	var payloadSize int
	switch choice {
	case CompactUint64EncodingRaw:
		payloadSize, _ = compactUint64RawPayloadSize(len(values))
	case CompactUint64EncodingBitPacked:
		var ok bool
		payloadSize, ok = compactUint64BitPackedPayloadSize(values)
		if !ok {
			return nil, ErrCompactUint64TooLarge
		}
	case CompactUint64EncodingDefaultSuppressed:
		var ok bool
		payloadSize, ok = compactUint64DefaultPayloadSize(values)
		if !ok {
			return nil, ErrCompactUint64TooLarge
		}
	default:
		return nil, ErrCompactUint64Invalid
	}
	if payloadSize > int(^uint(0)>>1)-compactUint64Header {
		return nil, ErrCompactUint64TooLarge
	}
	encoded := make([]byte, compactUint64Header+payloadSize)
	copy(encoded, compactUint64Magic)
	encoded[len(compactUint64Magic)] = byte(choice)
	offset := compactUint64Header
	switch choice {
	case CompactUint64EncodingRaw:
		offset += binary.PutUvarint(encoded[offset:], uint64(len(values)))
		for _, value := range values {
			binary.LittleEndian.PutUint64(encoded[offset:], value)
			offset += 8
		}
	case CompactUint64EncodingBitPacked:
		width := compactUint64BitWidth(values)
		offset += binary.PutUvarint(encoded[offset:], uint64(len(values)))
		encoded[offset] = byte(width)
		offset++
		compactUint64EncodeBitPackedValues(encoded[offset:], values, width)
	case CompactUint64EncodingDefaultSuppressed:
		offset += binary.PutUvarint(encoded[offset:], uint64(len(values)))
		bitmapBytes := (len(values) + 7) / 8
		bitmap := encoded[offset : offset+bitmapBytes]
		offset += bitmapBytes
		for index, value := range values {
			if value == 0 {
				continue
			}
			bitmap[index/8] |= 1 << uint(index%8)
			offset += binary.PutUvarint(encoded[offset:], value)
		}
	}
	return encoded, nil
}

// DecodeCompactUint64 validates and decodes one compact unsigned integer
// block. It bounds the row count before allocating the output slice.
func DecodeCompactUint64(encoded []byte) ([]uint64, error) {
	if len(encoded) < compactUint64Header || string(encoded[:len(compactUint64Magic)]) != compactUint64Magic {
		return nil, ErrCompactUint64Invalid
	}
	payload := encoded[compactUint64Header:]
	switch CompactUint64Encoding(encoded[len(compactUint64Magic)]) {
	case CompactUint64EncodingRaw:
		return decodeCompactUint64Raw(payload)
	case CompactUint64EncodingBitPacked:
		if len(payload) == 0 {
			return nil, ErrCompactUint64Invalid
		}
		values, err := DecodeBitPackedUint64(payload)
		if err != nil {
			return nil, ErrCompactUint64Invalid
		}
		return values, nil
	case CompactUint64EncodingDefaultSuppressed:
		return decodeCompactUint64DefaultSuppressed(payload)
	default:
		return nil, ErrCompactUint64Invalid
	}
}

func compactUint64RawPayloadSize(count int) (int, bool) {
	if count < 0 || uint64(count) > maxCompactUint64Values {
		return 0, false
	}
	var scratch [binary.MaxVarintLen64]byte
	countBytes := binary.PutUvarint(scratch[:], uint64(count))
	total := uint64(countBytes) + uint64(count)*8
	if total > uint64(^uint(0)>>1) {
		return 0, false
	}
	return int(total), true
}

func compactUint64BitPackedPayloadSize(values []uint64) (int, bool) {
	if len(values) == 0 || uint64(len(values)) > maxCompactUint64Values {
		return 0, false
	}
	width := uint64(compactUint64BitWidth(values))
	count := uint64(len(values))
	if width != 0 && count > (^uint64(0)-7)/width {
		return 0, false
	}
	var scratch [binary.MaxVarintLen64]byte
	countBytes := binary.PutUvarint(scratch[:], count)
	payloadBytes := (count*width + 7) / 8
	total := uint64(countBytes+1) + payloadBytes
	if total > uint64(^uint(0)>>1) {
		return 0, false
	}
	return int(total), true
}

func compactUint64BitWidth(values []uint64) int {
	maximum := uint64(0)
	for _, value := range values {
		if value > maximum {
			maximum = value
		}
	}
	return bits.Len64(maximum)
}

func compactUint64EncodeBitPackedValues(destination []byte, values []uint64, width int) {
	switch width {
	case 0:
		return
	case 8:
		for index, value := range values {
			destination[index] = byte(value)
		}
	case 16:
		for index, value := range values {
			binary.LittleEndian.PutUint16(destination[index*2:], uint16(value))
		}
	case 32:
		for index, value := range values {
			binary.LittleEndian.PutUint32(destination[index*4:], uint32(value))
		}
	case 64:
		for index, value := range values {
			binary.LittleEndian.PutUint64(destination[index*8:], value)
		}
	default:
		for index, value := range values {
			bitOffset := index * width
			byteOffset := bitOffset / 8
			bitShift := bitOffset % 8
			remaining := width
			for remaining > 0 {
				available := 8 - bitShift
				if available > remaining {
					available = remaining
				}
				mask := (uint64(1) << uint(available)) - 1
				destination[byteOffset] |= byte(value&mask) << uint(bitShift)
				value >>= uint(available)
				remaining -= available
				byteOffset++
				bitShift = 0
			}
		}
	}
}

func compactUint64DefaultPayloadSize(values []uint64) (int, bool) {
	if len(values) == 0 || uint64(len(values)) > maxCompactUint64Values {
		return 0, false
	}
	defaultCount := 0
	for _, value := range values {
		if value == 0 {
			defaultCount++
		}
	}
	if defaultCount < minCompactDefaultValues {
		return 0, false
	}
	var scratch [binary.MaxVarintLen64]byte
	total := uint64(binary.PutUvarint(scratch[:], uint64(len(values))))
	total += uint64((len(values) + 7) / 8)
	maxInt := uint64(^uint(0) >> 1)
	for _, value := range values {
		if value == 0 {
			continue
		}
		valueBytes := binary.PutUvarint(scratch[:], value)
		if total > maxInt-uint64(valueBytes) {
			return 0, false
		}
		total += uint64(valueBytes)
	}
	if total > maxInt {
		return 0, false
	}
	return int(total), true
}

func decodeCompactUint64Raw(payload []byte) ([]uint64, error) {
	count, offset, err := readCompactUint64Count(payload)
	if err != nil || count > maxCompactUint64Values {
		return nil, ErrCompactUint64Invalid
	}
	remaining := payload[offset:]
	if len(remaining)%8 != 0 || count != uint64(len(remaining)/8) {
		return nil, ErrCompactUint64Invalid
	}
	values := make([]uint64, int(count))
	for index := range values {
		values[index] = binary.LittleEndian.Uint64(remaining[index*8:])
	}
	return values, nil
}

func decodeCompactUint64DefaultSuppressed(payload []byte) ([]uint64, error) {
	count, offset, err := readCompactUint64Count(payload)
	if err != nil || count > maxCompactUint64Values {
		return nil, ErrCompactUint64Invalid
	}
	if count > (^uint64(0)-7)/8 {
		return nil, ErrCompactUint64Invalid
	}
	bitmapBytes := (count + 7) / 8
	if bitmapBytes > uint64(len(payload)-offset) {
		return nil, ErrCompactUint64Invalid
	}
	bitmapOffset := offset
	offset += int(bitmapBytes)
	if count%8 != 0 && bitmapBytes > 0 {
		paddingMask := byte(0xff << uint(count%8))
		if payload[bitmapOffset+int(bitmapBytes)-1]&paddingMask != 0 {
			return nil, ErrCompactUint64Invalid
		}
	}
	values := make([]uint64, int(count))
	var scratch [binary.MaxVarintLen64]byte
	for index := uint64(0); index < count; index++ {
		if payload[bitmapOffset+int(index/8)]&(1<<uint(index%8)) == 0 {
			continue
		}
		value, width := binary.Uvarint(payload[offset:])
		if width <= 0 || value == 0 || binary.PutUvarint(scratch[:], value) != width {
			return nil, ErrCompactUint64Invalid
		}
		values[int(index)] = value
		offset += width
	}
	if offset != len(payload) {
		return nil, ErrCompactUint64Invalid
	}
	return values, nil
}

func readCompactUint64Count(payload []byte) (uint64, int, error) {
	count, width := binary.Uvarint(payload)
	if width <= 0 {
		return 0, 0, ErrCompactUint64Invalid
	}
	return count, width, nil
}
