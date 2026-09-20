package hatDataStructure

import (
	"encoding/binary"
	"errors"
	"math"
	mathbits "math/bits"
)

const (
	float64CodecHeaderSize          = 5
	float64CodecMaxValues           = 1 << 24
	float64GorillaSampleTransitions = 256
)

var float64CodecMagic = [4]byte{'H', 'G', 'F', '1'}

var (
	// ErrFloat64CodecHeader reports an invalid or truncated codec header.
	ErrFloat64CodecHeader = errors.New("hatDataStructure: invalid float64 codec header")
	// ErrFloat64CodecEncoding reports an unknown payload encoding.
	ErrFloat64CodecEncoding = errors.New("hatDataStructure: unknown float64 codec encoding")
	// ErrFloat64CodecTooManyValues prevents unbounded decoder allocation.
	ErrFloat64CodecTooManyValues = errors.New("hatDataStructure: float64 codec value count is too large")
	// ErrFloat64CodecTruncated reports a bitstream shorter than its declared data.
	ErrFloat64CodecTruncated = errors.New("hatDataStructure: truncated float64 codec payload")
	// ErrFloat64CodecOverflow reports a malformed varint or bit-field overflow.
	ErrFloat64CodecOverflow = errors.New("hatDataStructure: float64 codec value overflows")
	// ErrFloat64CodecWindow reports an invalid Gorilla significant-bit window.
	ErrFloat64CodecWindow = errors.New("hatDataStructure: invalid float64 Gorilla window")
	// ErrFloat64CodecTrailing reports bytes or nonzero padding after a payload.
	ErrFloat64CodecTrailing = errors.New("hatDataStructure: float64 codec payload has trailing data")
)

// Float64Encoding identifies the physical encoding selected for a float64
// payload.
type Float64Encoding uint8

const (
	// Float64EncodingRaw stores IEEE-754 bits as fixed-width little-endian words.
	Float64EncodingRaw Float64Encoding = iota
	// Float64EncodingGorilla stores XOR transitions in a compact bitstream.
	Float64EncodingGorilla
)

func (encoding Float64Encoding) String() string {
	switch encoding {
	case Float64EncodingRaw:
		return "raw"
	case Float64EncodingGorilla:
		return "gorilla"
	default:
		return "unknown"
	}
}

// EncodeFloat64 chooses Gorilla XOR encoding only when it is smaller than raw
// fixed-width storage. This preserves the raw size for high-entropy values.
func EncodeFloat64(values []float64) ([]byte, Float64Encoding, error) {
	if len(values) > float64CodecMaxValues {
		return nil, Float64EncodingRaw, ErrFloat64CodecTooManyValues
	}
	rawSize := float64RawEncodedSize(len(values))
	if !float64GorillaLooksPromising(values) {
		return encodeFloat64RawPayload(values, rawSize), Float64EncodingRaw, nil
	}
	gorillaSize := float64GorillaEncodedSize(values)
	if gorillaSize < rawSize {
		payload, err := encodeFloat64GorillaPayload(values, gorillaSize)
		return payload, Float64EncodingGorilla, err
	}
	return encodeFloat64RawPayload(values, rawSize), Float64EncodingRaw, nil
}

func float64GorillaLooksPromising(values []float64) bool {
	if len(values) < 2 {
		return false
	}
	transitions := len(values) - 1
	if transitions > float64GorillaSampleTransitions {
		transitions = float64GorillaSampleTransitions
	}
	bitCount := 64
	window := float64GorillaWindow{}
	previous := math.Float64bits(values[0])
	for index := 1; index <= transitions; index++ {
		current := math.Float64bits(values[index])
		bitCount += float64GorillaStepBits(previous, current, &window)
		previous = current
	}
	return bitCount < 64*(transitions+1)
}

// EncodeFloat64Gorilla forces Gorilla XOR encoding for a non-empty or empty
// float64 slice. It is useful when a versioned format has already selected the
// encoding and wants the compact representation even if it is not smaller.
func EncodeFloat64Gorilla(values []float64) ([]byte, error) {
	if len(values) > float64CodecMaxValues {
		return nil, ErrFloat64CodecTooManyValues
	}
	return encodeFloat64GorillaPayload(values, float64GorillaEncodedSize(values))
}

// DecodeFloat64 validates and decodes a payload produced by EncodeFloat64 or
// EncodeFloat64Gorilla. It preserves the exact IEEE-754 bit pattern, including
// signed zero and NaN payload bits.
func DecodeFloat64(payload []byte) ([]float64, Float64Encoding, error) {
	if len(payload) < float64CodecHeaderSize {
		return nil, Float64EncodingRaw, ErrFloat64CodecHeader
	}
	if payload[0] != float64CodecMagic[0] || payload[1] != float64CodecMagic[1] || payload[2] != float64CodecMagic[2] || payload[3] != float64CodecMagic[3] {
		return nil, Float64EncodingRaw, ErrFloat64CodecHeader
	}
	encoding := Float64Encoding(payload[4])
	if encoding != Float64EncodingRaw && encoding != Float64EncodingGorilla {
		return nil, Float64EncodingRaw, ErrFloat64CodecEncoding
	}
	offset := float64CodecHeaderSize
	count, next, err := readFloat64CodecVarint(payload, offset)
	if err != nil {
		return nil, encoding, err
	}
	offset = next
	if count > float64CodecMaxValues {
		return nil, encoding, ErrFloat64CodecTooManyValues
	}
	var bitCount uint64
	if encoding == Float64EncodingGorilla {
		bitCount, next, err = readFloat64CodecVarint(payload, offset)
		if err != nil {
			return nil, encoding, err
		}
		offset = next
		expectedBytes := bitCount / 8
		if bitCount%8 != 0 {
			expectedBytes++
		}
		availableBytes := uint64(len(payload) - offset)
		if expectedBytes > availableBytes {
			return nil, encoding, ErrFloat64CodecTruncated
		}
		if expectedBytes < availableBytes {
			return nil, encoding, ErrFloat64CodecTrailing
		}
		if count == 0 && bitCount != 0 {
			return nil, encoding, ErrFloat64CodecOverflow
		}
		if count > 0 && bitCount < 64 {
			return nil, encoding, ErrFloat64CodecTruncated
		}
	}
	if count == 0 {
		if offset != len(payload) {
			return nil, encoding, ErrFloat64CodecTrailing
		}
		return nil, encoding, nil
	}

	values := make([]float64, int(count))
	if encoding == Float64EncodingRaw {
		bytesNeeded := int(count) * 8
		if len(payload)-offset < bytesNeeded {
			return nil, encoding, ErrFloat64CodecTruncated
		}
		if len(payload)-offset > bytesNeeded {
			return nil, encoding, ErrFloat64CodecTrailing
		}
		for index := range values {
			bits := binary.LittleEndian.Uint64(payload[offset+index*8:])
			values[index] = math.Float64frombits(bits)
		}
		return values, encoding, nil
	}

	reader := float64BitReader{data: payload[offset:], validBits: bitCount}
	previous, err := reader.Read(64)
	if err != nil {
		return nil, encoding, err
	}
	values[0] = math.Float64frombits(previous)
	window := float64GorillaWindow{}
	for index := 1; index < len(values); index++ {
		control, err := reader.Read(1)
		if err != nil {
			return nil, encoding, err
		}
		var xor uint64
		if control == 1 {
			reuse, err := reader.Read(1)
			if err != nil {
				return nil, encoding, err
			}
			if reuse == 0 {
				if !window.set {
					return nil, encoding, ErrFloat64CodecWindow
				}
				significant := 64 - window.leading - window.trailing
				value, err := reader.Read(significant)
				if err != nil {
					return nil, encoding, err
				}
				xor = value << window.trailing
			} else {
				leading, err := reader.Read(6)
				if err != nil {
					return nil, encoding, err
				}
				significantMinusOne, err := reader.Read(6)
				if err != nil {
					return nil, encoding, err
				}
				significant := int(significantMinusOne) + 1
				if leading > 63 || int(leading)+significant > 64 {
					return nil, encoding, ErrFloat64CodecWindow
				}
				trailing := 64 - int(leading) - significant
				value, err := reader.Read(significant)
				if err != nil {
					return nil, encoding, err
				}
				window = float64GorillaWindow{leading: int(leading), trailing: trailing, set: true}
				xor = value << trailing
			}
		}
		previous ^= xor
		values[index] = math.Float64frombits(previous)
	}
	if err := reader.Exhausted(); err != nil {
		return nil, encoding, err
	}
	return values, encoding, nil
}

func float64RawEncodedSize(count int) int {
	return float64CodecHeaderSize + uint64CodecVarintSize(uint64(count)) + count*8
}

func float64GorillaEncodedSize(values []float64) int {
	bitCount := float64GorillaBitCount(values)
	return float64CodecHeaderSize + uint64CodecVarintSize(uint64(len(values))) + uint64CodecVarintSize(uint64(bitCount)) + (bitCount+7)/8
}

func float64GorillaBitCount(values []float64) int {
	if len(values) == 0 {
		return 0
	}
	bitCount := 64
	window := float64GorillaWindow{}
	previous := math.Float64bits(values[0])
	for _, value := range values[1:] {
		current := math.Float64bits(value)
		bitCount += float64GorillaStepBits(previous, current, &window)
		previous = current
	}
	return bitCount
}

func encodeFloat64RawPayload(values []float64, size int) []byte {
	payload := make([]byte, size)
	offset := writeFloat64CodecHeader(payload, Float64EncodingRaw, len(values))
	for _, value := range values {
		binary.LittleEndian.PutUint64(payload[offset:], math.Float64bits(value))
		offset += 8
	}
	return payload
}

func encodeFloat64GorillaPayload(values []float64, size int) ([]byte, error) {
	payload := make([]byte, size)
	bitCount := float64GorillaBitCount(values)
	offset := writeFloat64GorillaHeader(payload, len(values), bitCount)
	writer := float64BitWriter{data: payload[offset:offset]}
	if len(values) > 0 {
		previous := math.Float64bits(values[0])
		writer.Write(previous, 64)
		window := float64GorillaWindow{}
		for _, value := range values[1:] {
			current := math.Float64bits(value)
			float64WriteGorillaStep(&writer, previous, current, &window)
			previous = current
		}
	}
	writer.Flush()
	if offset+len(writer.data) != len(payload) {
		return nil, ErrFloat64CodecOverflow
	}
	return payload, nil
}

func writeFloat64CodecHeader(payload []byte, encoding Float64Encoding, count int) int {
	copy(payload, float64CodecMagic[:])
	payload[4] = byte(encoding)
	return float64CodecHeaderSize + binary.PutUvarint(payload[float64CodecHeaderSize:], uint64(count))
}

func writeFloat64GorillaHeader(payload []byte, count, bitCount int) int {
	offset := writeFloat64CodecHeader(payload, Float64EncodingGorilla, count)
	return offset + binary.PutUvarint(payload[offset:], uint64(bitCount))
}

type float64GorillaWindow struct {
	leading  int
	trailing int
	set      bool
}

func float64GorillaStepBits(previous, current uint64, window *float64GorillaWindow) int {
	xor := previous ^ current
	if xor == 0 {
		return 1
	}
	leading := mathbits.LeadingZeros64(xor)
	trailing := mathbits.TrailingZeros64(xor)
	if window.set && leading >= window.leading && trailing >= window.trailing {
		return 2 + (64 - window.leading - window.trailing)
	}
	significant := 64 - leading - trailing
	window.leading = leading
	window.trailing = trailing
	window.set = true
	return 14 + significant
}

func float64WriteGorillaStep(writer *float64BitWriter, previous, current uint64, window *float64GorillaWindow) {
	xor := previous ^ current
	if xor == 0 {
		writer.Write(0, 1)
		return
	}
	leading := mathbits.LeadingZeros64(xor)
	trailing := mathbits.TrailingZeros64(xor)
	if window.set && leading >= window.leading && trailing >= window.trailing {
		writer.Write(0b10, 2)
		writer.Write(xor>>window.trailing, 64-window.leading-window.trailing)
		return
	}
	significant := 64 - leading - trailing
	writer.Write(0b11, 2)
	writer.Write(uint64(leading), 6)
	writer.Write(uint64(significant-1), 6)
	writer.Write(xor>>trailing, significant)
	window.leading = leading
	window.trailing = trailing
	window.set = true
}

type float64BitWriter struct {
	data   []byte
	buffer uint64
	bits   uint8
}

func (writer *float64BitWriter) Write(value uint64, count int) {
	for count > 0 {
		take := int(64 - writer.bits)
		if take > count {
			take = count
		}
		shift := count - take
		part := value
		if take < 64 {
			part = (value >> shift) & ((uint64(1) << take) - 1)
		}
		writer.buffer = (writer.buffer << take) | part
		writer.bits += uint8(take)
		count -= take
		if writer.bits == 64 {
			var encoded [8]byte
			binary.BigEndian.PutUint64(encoded[:], writer.buffer)
			writer.data = append(writer.data, encoded[:]...)
			writer.buffer = 0
			writer.bits = 0
		}
	}
}

func (writer *float64BitWriter) Flush() {
	if writer.bits == 0 {
		return
	}
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], writer.buffer<<(64-writer.bits))
	writer.data = append(writer.data, encoded[:(int(writer.bits)+7)/8]...)
	writer.buffer = 0
	writer.bits = 0
}

type float64BitReader struct {
	data       []byte
	byteOffset int
	buffer     uint64
	bits       uint8
	validBits  uint64
	readBits   uint64
}

func (reader *float64BitReader) Read(count int) (uint64, error) {
	if count < 0 || count > 64 {
		return 0, ErrFloat64CodecOverflow
	}
	if reader.readBits+uint64(count) > reader.validBits {
		return 0, ErrFloat64CodecTruncated
	}
	requested := count
	var value uint64
	for count > 0 {
		if reader.bits == 0 && !reader.fill() {
			return 0, ErrFloat64CodecTruncated
		}
		take := count
		if take > int(reader.bits) {
			take = int(reader.bits)
		}
		part := reader.buffer >> (64 - take)
		value = (value << take) | part
		reader.buffer <<= take
		reader.bits -= uint8(take)
		count -= take
	}
	reader.readBits += uint64(requested)
	return value, nil
}

func (reader *float64BitReader) fill() bool {
	if reader.byteOffset >= len(reader.data) {
		return false
	}
	count := len(reader.data) - reader.byteOffset
	if count > 8 {
		count = 8
	}
	var value uint64
	for index := 0; index < count; index++ {
		value = value<<8 | uint64(reader.data[reader.byteOffset+index])
	}
	reader.byteOffset += count
	reader.buffer = value << (64 - count*8)
	reader.bits = uint8(count * 8)
	return true
}

func (reader *float64BitReader) Exhausted() error {
	if reader.readBits != reader.validBits || reader.byteOffset != len(reader.data) || reader.buffer != 0 {
		return ErrFloat64CodecTrailing
	}
	return nil
}

func readFloat64CodecVarint(payload []byte, offset int) (uint64, int, error) {
	value, size := binary.Uvarint(payload[offset:])
	if size == 0 {
		return 0, offset, ErrFloat64CodecTruncated
	}
	if size < 0 {
		return 0, offset, ErrFloat64CodecOverflow
	}
	return value, offset + size, nil
}
