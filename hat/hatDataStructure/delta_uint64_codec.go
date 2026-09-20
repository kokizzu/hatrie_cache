package hatDataStructure

import (
	"encoding/binary"
	"errors"
)

const (
	uint64CodecHeaderSize = 5
	uint64CodecMaxValues  = 1 << 24
)

var uint64CodecMagic = [4]byte{'H', 'D', 'U', '1'}

var (
	// ErrUint64CodecHeader reports an invalid or truncated codec header.
	ErrUint64CodecHeader = errors.New("hatDataStructure: invalid uint64 codec header")
	// ErrUint64CodecEncoding reports an unknown payload encoding.
	ErrUint64CodecEncoding = errors.New("hatDataStructure: unknown uint64 codec encoding")
	// ErrUint64CodecNotMonotone reports a decreasing input to delta encoding.
	ErrUint64CodecNotMonotone = errors.New("hatDataStructure: uint64 delta input is not monotone")
	// ErrUint64CodecTooManyValues prevents unbounded decoder allocation.
	ErrUint64CodecTooManyValues = errors.New("hatDataStructure: uint64 codec value count is too large")
	// ErrUint64CodecTruncated reports a payload shorter than its declared data.
	ErrUint64CodecTruncated = errors.New("hatDataStructure: truncated uint64 codec payload")
	// ErrUint64CodecOverflow reports an integer overflow while decoding.
	ErrUint64CodecOverflow = errors.New("hatDataStructure: uint64 codec value overflows")
	// ErrUint64CodecTrailing reports bytes after a complete payload.
	ErrUint64CodecTrailing = errors.New("hatDataStructure: uint64 codec payload has trailing bytes")
)

// Uint64Encoding identifies the physical encoding selected for a uint64
// payload.
type Uint64Encoding uint8

const (
	// Uint64EncodingRaw stores fixed-width little-endian uint64 values.
	Uint64EncodingRaw Uint64Encoding = iota
	// Uint64EncodingDelta stores the first value and unsigned deltas as varints.
	Uint64EncodingDelta
)

func (encoding Uint64Encoding) String() string {
	switch encoding {
	case Uint64EncodingRaw:
		return "raw"
	case Uint64EncodingDelta:
		return "delta"
	default:
		return "unknown"
	}
}

// EncodeUint64 chooses delta encoding only when the input is nondecreasing
// and the resulting payload is smaller than the raw fixed-width form. The
// adaptive fallback keeps unordered or high-entropy values from expanding.
func EncodeUint64(values []uint64) ([]byte, Uint64Encoding, error) {
	if len(values) > uint64CodecMaxValues {
		return nil, Uint64EncodingRaw, ErrUint64CodecTooManyValues
	}
	deltaSize, monotone := uint64DeltaEncodedSize(values)
	rawSize := uint64RawEncodedSize(len(values))
	if monotone && deltaSize < rawSize {
		return encodeUint64DeltaPayload(values, deltaSize), Uint64EncodingDelta, nil
	}
	return encodeUint64RawPayload(values, rawSize), Uint64EncodingRaw, nil
}

// EncodeUint64Delta encodes a nondecreasing uint64 slice using a compact
// first-value-plus-delta varint representation.
func EncodeUint64Delta(values []uint64) ([]byte, error) {
	if len(values) > uint64CodecMaxValues {
		return nil, ErrUint64CodecTooManyValues
	}
	size, monotone := uint64DeltaEncodedSize(values)
	if !monotone {
		return nil, ErrUint64CodecNotMonotone
	}
	return encodeUint64DeltaPayload(values, size), nil
}

// DecodeUint64 validates and decodes a payload produced by EncodeUint64 or
// EncodeUint64Delta. It returns the physical encoding found in the header.
func DecodeUint64(payload []byte) ([]uint64, Uint64Encoding, error) {
	if len(payload) < uint64CodecHeaderSize {
		return nil, Uint64EncodingRaw, ErrUint64CodecHeader
	}
	if payload[0] != uint64CodecMagic[0] || payload[1] != uint64CodecMagic[1] || payload[2] != uint64CodecMagic[2] || payload[3] != uint64CodecMagic[3] {
		return nil, Uint64EncodingRaw, ErrUint64CodecHeader
	}
	encoding := Uint64Encoding(payload[4])
	if encoding != Uint64EncodingRaw && encoding != Uint64EncodingDelta {
		return nil, Uint64EncodingRaw, ErrUint64CodecEncoding
	}
	offset := uint64CodecHeaderSize
	count, next, err := readUint64CodecVarint(payload, offset)
	if err != nil {
		return nil, encoding, err
	}
	offset = next
	if count > uint64CodecMaxValues {
		return nil, encoding, ErrUint64CodecTooManyValues
	}
	if count == 0 {
		if offset != len(payload) {
			return nil, encoding, ErrUint64CodecTrailing
		}
		return nil, encoding, nil
	}

	values := make([]uint64, int(count))
	if encoding == Uint64EncodingRaw {
		bytesNeeded := int(count) * 8
		if len(payload)-offset < bytesNeeded {
			return nil, encoding, ErrUint64CodecTruncated
		}
		if len(payload)-offset > bytesNeeded {
			return nil, encoding, ErrUint64CodecTrailing
		}
		for index := range values {
			values[index] = binary.LittleEndian.Uint64(payload[offset+index*8:])
		}
		return values, encoding, nil
	}

	first, next, err := readUint64CodecVarint(payload, offset)
	if err != nil {
		return nil, encoding, err
	}
	values[0] = first
	offset = next
	for index := 1; index < len(values); index++ {
		delta, next, err := readUint64CodecVarint(payload, offset)
		if err != nil {
			return nil, encoding, err
		}
		if delta > ^uint64(0)-values[index-1] {
			return nil, encoding, ErrUint64CodecOverflow
		}
		values[index] = values[index-1] + delta
		offset = next
	}
	if offset != len(payload) {
		return nil, encoding, ErrUint64CodecTrailing
	}
	return values, encoding, nil
}

func uint64RawEncodedSize(count int) int {
	return uint64CodecHeaderSize + uint64CodecVarintSize(uint64(count)) + count*8
}

func uint64DeltaEncodedSize(values []uint64) (int, bool) {
	size := uint64CodecHeaderSize + uint64CodecVarintSize(uint64(len(values)))
	if len(values) == 0 {
		return size, true
	}
	size += uint64CodecVarintSize(values[0])
	previous := values[0]
	for _, value := range values[1:] {
		if value < previous {
			return 0, false
		}
		size += uint64CodecVarintSize(value - previous)
		previous = value
	}
	return size, true
}

func encodeUint64RawPayload(values []uint64, size int) []byte {
	payload := make([]byte, size)
	offset := writeUint64CodecHeader(payload, Uint64EncodingRaw, len(values))
	for _, value := range values {
		binary.LittleEndian.PutUint64(payload[offset:], value)
		offset += 8
	}
	return payload
}

func encodeUint64DeltaPayload(values []uint64, size int) []byte {
	payload := make([]byte, size)
	offset := writeUint64CodecHeader(payload, Uint64EncodingDelta, len(values))
	if len(values) == 0 {
		return payload
	}
	offset += binary.PutUvarint(payload[offset:], values[0])
	previous := values[0]
	for _, value := range values[1:] {
		offset += binary.PutUvarint(payload[offset:], value-previous)
		previous = value
	}
	return payload
}

func writeUint64CodecHeader(payload []byte, encoding Uint64Encoding, count int) int {
	copy(payload, uint64CodecMagic[:])
	payload[4] = byte(encoding)
	return uint64CodecHeaderSize + binary.PutUvarint(payload[uint64CodecHeaderSize:], uint64(count))
}

func readUint64CodecVarint(payload []byte, offset int) (uint64, int, error) {
	value, size := binary.Uvarint(payload[offset:])
	if size == 0 {
		return 0, offset, ErrUint64CodecTruncated
	}
	if size < 0 {
		return 0, offset, ErrUint64CodecOverflow
	}
	return value, offset + size, nil
}

func uint64CodecVarintSize(value uint64) int {
	size := 1
	for value >= 0x80 {
		value >>= 7
		size++
	}
	return size
}
