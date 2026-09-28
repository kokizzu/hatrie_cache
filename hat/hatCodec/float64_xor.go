package hatCodec

import (
	"encoding/binary"
	"errors"
	"math"
	"math/bits"
)

const (
	float64XORVersion byte = 1
	float64XORModeRaw byte = 0
	float64XORModeXOR byte = 1
	float64XORHeader       = 6
	float64XORProbe        = 32
	float64XORMaxInt       = int(^uint(0) >> 1)
)

var errFloat64XORFrame = errors.New("hatCodec: invalid float64 XOR frame")

// EncodeFloat64XOR encodes a float64 block with significant XOR bytes and
// falls back to a fixed-width raw frame when the compact frame is not smaller.
func EncodeFloat64XOR(values []float64) []byte {
	rawSize := float64XORRawSize(len(values))
	if !float64XORHasEarlyCompression(values) {
		return encodeFloat64XORRaw(values, rawSize)
	}
	xorSize := float64XOREncodedSize(values)
	if xorSize >= rawSize || xorSize > uint64(float64XORMaxInt) {
		return encodeFloat64XORRaw(values, rawSize)
	}

	frame := make([]byte, 0, int(xorSize))
	frame = appendFloat64XORHeader(frame, float64XORModeXOR, len(values))
	if len(values) == 0 {
		return frame
	}
	previous := math.Float64bits(values[0])
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], previous)
	frame = append(frame, encoded[:]...)
	for _, value := range values[1:] {
		current := math.Float64bits(value)
		delta := current ^ previous
		if delta == 0 {
			frame = append(frame, 0)
			previous = current
			continue
		}

		leading, trailing, significantBytes := float64XORSpan(delta)
		frame = append(frame, 1, byte(leading<<4|(significantBytes-1)))
		shift := uint(trailing * 8)
		for i := 0; i < significantBytes; i++ {
			frame = append(frame, byte(delta>>shift))
			shift += 8
		}
		previous = current
	}
	return frame
}

// DecodeFloat64XOR decodes an HGF1 frame and reuses dst when its capacity is
// sufficient.
func DecodeFloat64XOR(frame []byte, dst []float64) ([]float64, error) {
	if len(frame) < float64XORHeader ||
		frame[0] != 'H' || frame[1] != 'G' || frame[2] != 'F' || frame[3] != '1' ||
		frame[4] != float64XORVersion {
		return nil, errFloat64XORFrame
	}
	mode := frame[5]
	if mode != float64XORModeRaw && mode != float64XORModeXOR {
		return nil, errFloat64XORFrame
	}
	countValue, countBytes, ok := decodeFloat64XORUvarint(frame[float64XORHeader:])
	if !ok || countValue > uint64(float64XORMaxInt) {
		return nil, errFloat64XORFrame
	}
	count := int(countValue)
	offset := float64XORHeader + countBytes

	switch mode {
	case float64XORModeRaw:
		payloadSize := uint64(count) * 8
		if payloadSize/8 != uint64(count) || payloadSize != uint64(len(frame)-offset) {
			return nil, errFloat64XORFrame
		}
		dst = float64XORDestination(dst, count)
		for i := range dst {
			dst[i] = math.Float64frombits(binary.LittleEndian.Uint64(frame[offset+i*8:]))
		}
		return dst, nil
	case float64XORModeXOR:
		if cap(dst) < count {
			if _, ok := decodeFloat64XORValues(frame, offset, count, nil); !ok {
				return nil, errFloat64XORFrame
			}
		}
		dst = float64XORDestination(dst, count)
		if _, ok := decodeFloat64XORValues(frame, offset, count, dst); !ok {
			return nil, errFloat64XORFrame
		}
		return dst, nil
	}
	return nil, errFloat64XORFrame
}

func encodeFloat64XORRaw(values []float64, size uint64) []byte {
	frame := make([]byte, 0, int(size))
	frame = appendFloat64XORHeader(frame, float64XORModeRaw, len(values))
	for _, value := range values {
		var encoded [8]byte
		binary.LittleEndian.PutUint64(encoded[:], math.Float64bits(value))
		frame = append(frame, encoded[:]...)
	}
	return frame
}

func appendFloat64XORHeader(frame []byte, mode byte, count int) []byte {
	frame = append(frame, 'H', 'G', 'F', '1', float64XORVersion, mode)
	return appendFloat64XORUvarint(frame, uint64(count))
}

func appendFloat64XORUvarint(dst []byte, value uint64) []byte {
	var encoded [10]byte
	n := binary.PutUvarint(encoded[:], value)
	return append(dst, encoded[:n]...)
}

func decodeFloat64XORUvarint(data []byte) (uint64, int, bool) {
	var value uint64
	for i, encoded := range data {
		if i == 10 {
			return 0, 0, false
		}
		if i == 9 && encoded > 1 {
			return 0, 0, false
		}
		if encoded < 0x80 {
			if i > 0 && encoded == 0 {
				return 0, 0, false
			}
			value |= uint64(encoded) << (7 * i)
			return value, i + 1, true
		}
		if i == 9 {
			return 0, 0, false
		}
		value |= uint64(encoded&0x7f) << (7 * i)
	}
	return 0, 0, false
}

func decodeFloat64XORValues(frame []byte, offset, count int, dst []float64) (int, bool) {
	if count == 0 {
		return offset, offset == len(frame)
	}
	if len(frame)-offset < 8 {
		return 0, false
	}
	previous := binary.LittleEndian.Uint64(frame[offset:])
	offset += 8
	if dst != nil {
		dst[0] = math.Float64frombits(previous)
	}
	for i := 1; i < count; i++ {
		if offset >= len(frame) {
			return 0, false
		}
		token := frame[offset]
		offset++
		var current uint64
		switch token {
		case 0:
			current = previous
		case 1:
			if offset >= len(frame) {
				return 0, false
			}
			descriptor := frame[offset]
			offset++
			leading := int(descriptor >> 4)
			significantBytes := int(descriptor&0x0f) + 1
			if leading > 8 || significantBytes > 8-leading || significantBytes > len(frame)-offset {
				return 0, false
			}
			var delta uint64
			for byteIndex := 0; byteIndex < significantBytes; byteIndex++ {
				delta |= uint64(frame[offset+byteIndex]) << uint(8*byteIndex)
			}
			offset += significantBytes
			trailing := 8 - leading - significantBytes
			current = previous ^ (delta << uint(8*trailing))
		default:
			return 0, false
		}
		if dst != nil {
			dst[i] = math.Float64frombits(current)
		}
		previous = current
	}
	return offset, offset == len(frame)
}

func float64XORDestination(dst []float64, count int) []float64 {
	if cap(dst) < count {
		return make([]float64, count)
	}
	return dst[:count]
}

func float64XORSpan(delta uint64) (leading, trailing, significantBytes int) {
	leading = bits.LeadingZeros64(delta) / 8
	trailing = bits.TrailingZeros64(delta) / 8
	significantBytes = 8 - leading - trailing
	return leading, trailing, significantBytes
}

func float64XORHasEarlyCompression(values []float64) bool {
	if len(values) < 2 {
		return false
	}
	limit := len(values)
	if limit > float64XORProbe+1 {
		limit = float64XORProbe + 1
	}
	rawSize := uint64(limit) * 8
	compactSize := uint64(8)
	previous := math.Float64bits(values[0])
	for i := 1; i < limit; i++ {
		delta := math.Float64bits(values[i]) ^ previous
		if delta == 0 {
			compactSize++
		} else {
			_, _, significantBytes := float64XORSpan(delta)
			compactSize += uint64(2 + significantBytes)
		}
		previous = math.Float64bits(values[i])
	}
	return compactSize*5 <= rawSize*4
}

func float64XOREncodedSize(values []float64) uint64 {
	size := uint64(float64XORHeader) + float64XORUvarintSize(uint64(len(values)))
	if len(values) == 0 {
		return size
	}
	size = float64XORAddSize(size, 8)
	previous := math.Float64bits(values[0])
	for _, value := range values[1:] {
		current := math.Float64bits(value)
		delta := current ^ previous
		if delta == 0 {
			size = float64XORAddSize(size, 1)
		} else {
			_, _, significantBytes := float64XORSpan(delta)
			size = float64XORAddSize(size, uint64(2+significantBytes))
		}
		previous = current
	}
	return size
}

func float64XORRawSize(count int) uint64 {
	size := uint64(float64XORHeader) + float64XORUvarintSize(uint64(count))
	return float64XORAddSize(size, uint64(count)*8)
}

func float64XORUvarintSize(value uint64) uint64 {
	size := uint64(1)
	for value >= 0x80 {
		value >>= 7
		size++
	}
	return size
}

func float64XORAddSize(left, right uint64) uint64 {
	if ^uint64(0)-left < right {
		return ^uint64(0)
	}
	return left + right
}
