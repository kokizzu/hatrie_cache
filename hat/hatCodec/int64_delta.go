package hatCodec

import (
	"encoding/binary"
	"errors"
)

const (
	int64DeltaVersion   byte = 1
	int64DeltaModeRaw   byte = 0
	int64DeltaModeDelta byte = 1
	int64DeltaModeByte  byte = 2
	int64DeltaHeader         = 6
	int64DeltaProbe          = 32
	int64DeltaMaxInt         = int(^uint(0) >> 1)
)

var errInt64DeltaFrame = errors.New("hatCodec: invalid int64 delta frame")

// EncodeInt64Delta encodes a signed integer block with ZigZag delta varints
// when a measured delta frame is smaller than the fixed-width raw frame.
func EncodeInt64Delta(values []int64) []byte {
	rawSize := int64DeltaRawSize(len(values))
	if !int64DeltaHasEarlyCompression(values) {
		return encodeInt64DeltaRaw(values, rawSize)
	}
	if int64DeltaHasByteDeltas(values) {
		byteSize := int64DeltaByteSize(len(values))
		if byteSize < rawSize {
			return encodeInt64DeltaByte(values, byteSize)
		}
	}
	deltaSize := int64DeltaEncodedSize(values)
	if deltaSize >= rawSize || deltaSize > uint64(int64DeltaMaxInt) {
		return encodeInt64DeltaRaw(values, rawSize)
	}

	frame := make([]byte, 0, int(deltaSize))
	frame = appendInt64DeltaHeader(frame, int64DeltaModeDelta, len(values))
	if len(values) == 0 {
		return frame
	}
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], uint64(values[0]))
	frame = append(frame, encoded[:]...)
	previous := uint64(values[0])
	for _, value := range values[1:] {
		current := uint64(value)
		delta := int64(current - previous)
		frame = appendInt64DeltaUvarint(frame, int64DeltaZigZagEncode(delta))
		previous = current
	}
	return frame
}

// DecodeInt64Delta decodes an HID1 frame and reuses dst when its capacity is
// sufficient.
func DecodeInt64Delta(frame []byte, dst []int64) ([]int64, error) {
	if len(frame) < int64DeltaHeader ||
		frame[0] != 'H' || frame[1] != 'I' || frame[2] != 'D' || frame[3] != '1' ||
		frame[4] != int64DeltaVersion {
		return nil, errInt64DeltaFrame
	}
	mode := frame[5]
	if mode != int64DeltaModeRaw && mode != int64DeltaModeDelta && mode != int64DeltaModeByte {
		return nil, errInt64DeltaFrame
	}
	countValue, countBytes, ok := decodeInt64DeltaUvarint(frame[int64DeltaHeader:])
	if !ok || countValue > uint64(int64DeltaMaxInt) {
		return nil, errInt64DeltaFrame
	}
	count := int(countValue)
	offset := int64DeltaHeader + countBytes

	switch mode {
	case int64DeltaModeRaw:
		payloadSize := uint64(count) * 8
		if payloadSize/8 != uint64(count) || payloadSize != uint64(len(frame)-offset) {
			return nil, errInt64DeltaFrame
		}
		dst = int64DeltaDestination(dst, count)
		for i := range dst {
			dst[i] = int64(binary.LittleEndian.Uint64(frame[offset+i*8:]))
		}
		return dst, nil
	case int64DeltaModeDelta:
		if cap(dst) < count {
			if _, ok := decodeInt64DeltaValues(frame, offset, count, nil); !ok {
				return nil, errInt64DeltaFrame
			}
		}
		dst = int64DeltaDestination(dst, count)
		if _, ok := decodeInt64DeltaValues(frame, offset, count, dst); !ok {
			return nil, errInt64DeltaFrame
		}
		return dst, nil
	case int64DeltaModeByte:
		if count == 0 {
			if offset != len(frame) {
				return nil, errInt64DeltaFrame
			}
			return int64DeltaDestination(dst, 0), nil
		}
		if len(frame)-offset != 8+count-1 {
			return nil, errInt64DeltaFrame
		}
		dst = int64DeltaDestination(dst, count)
		previous := binary.LittleEndian.Uint64(frame[offset:])
		dst[0] = int64(previous)
		for i := 1; i < count; i++ {
			previous += uint64(int64(int8(frame[offset+8+i-1])))
			dst[i] = int64(previous)
		}
		return dst, nil
	}
	return nil, errInt64DeltaFrame
}

func encodeInt64DeltaRaw(values []int64, size uint64) []byte {
	frame := make([]byte, 0, int(size))
	frame = appendInt64DeltaHeader(frame, int64DeltaModeRaw, len(values))
	for _, value := range values {
		var encoded [8]byte
		binary.LittleEndian.PutUint64(encoded[:], uint64(value))
		frame = append(frame, encoded[:]...)
	}
	return frame
}

func encodeInt64DeltaByte(values []int64, size uint64) []byte {
	frame := make([]byte, 0, int(size))
	frame = appendInt64DeltaHeader(frame, int64DeltaModeByte, len(values))
	if len(values) == 0 {
		return frame
	}
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], uint64(values[0]))
	frame = append(frame, encoded[:]...)
	previous := uint64(values[0])
	for _, value := range values[1:] {
		current := uint64(value)
		frame = append(frame, byte(int8(int64(current-previous))))
		previous = current
	}
	return frame
}

func appendInt64DeltaHeader(frame []byte, mode byte, count int) []byte {
	frame = append(frame, 'H', 'I', 'D', '1', int64DeltaVersion, mode)
	return appendInt64DeltaUvarint(frame, uint64(count))
}

func appendInt64DeltaUvarint(dst []byte, value uint64) []byte {
	if value < 0x80 {
		return append(dst, byte(value))
	}
	var encoded [10]byte
	n := binary.PutUvarint(encoded[:], value)
	return append(dst, encoded[:n]...)
}

func decodeInt64DeltaUvarint(data []byte) (uint64, int, bool) {
	if len(data) == 0 {
		return 0, 0, false
	}
	if data[0] < 0x80 {
		return uint64(data[0]), 1, true
	}
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

func decodeInt64DeltaValues(frame []byte, offset, count int, dst []int64) (int, bool) {
	if count == 0 {
		return offset, offset == len(frame)
	}
	if len(frame)-offset < 8 {
		return 0, false
	}
	previous := binary.LittleEndian.Uint64(frame[offset:])
	offset += 8
	if dst != nil {
		dst[0] = int64(previous)
	}
	for i := 1; i < count; i++ {
		encoded, consumed, ok := decodeInt64DeltaUvarint(frame[offset:])
		if !ok {
			return 0, false
		}
		offset += consumed
		current := previous + uint64(int64(int64DeltaZigZagDecode(encoded)))
		if dst != nil {
			dst[i] = int64(current)
		}
		previous = current
	}
	return offset, offset == len(frame)
}

func int64DeltaDestination(dst []int64, count int) []int64 {
	if cap(dst) < count {
		return make([]int64, count)
	}
	return dst[:count]
}

func int64DeltaZigZagEncode(value int64) uint64 {
	unsigned := uint64(value)
	return (unsigned << 1) ^ uint64(value>>63)
}

func int64DeltaZigZagDecode(value uint64) int64 {
	unsigned := (value >> 1) ^ uint64(-int64(value&1))
	return int64(unsigned)
}

func int64DeltaHasEarlyCompression(values []int64) bool {
	if len(values) < 2 {
		return false
	}
	limit := len(values)
	if limit > int64DeltaProbe+1 {
		limit = int64DeltaProbe + 1
	}
	rawSize := uint64(limit) * 8
	compactSize := uint64(8)
	previous := uint64(values[0])
	for i := 1; i < limit; i++ {
		current := uint64(values[i])
		compactSize += int64DeltaUvarintSize(int64DeltaZigZagEncode(int64(current - previous)))
		previous = current
	}
	return compactSize*5 <= rawSize*4
}

func int64DeltaHasByteDeltas(values []int64) bool {
	if len(values) < 2 {
		return false
	}
	previous := uint64(values[0])
	for _, value := range values[1:] {
		current := uint64(value)
		delta := int64(current - previous)
		if delta < -128 || delta > 127 {
			return false
		}
		previous = current
	}
	return true
}

func int64DeltaEncodedSize(values []int64) uint64 {
	size := uint64(int64DeltaHeader) + int64DeltaUvarintSize(uint64(len(values)))
	if len(values) == 0 {
		return size
	}
	size = int64DeltaAddSize(size, 8)
	previous := uint64(values[0])
	for _, value := range values[1:] {
		current := uint64(value)
		size = int64DeltaAddSize(size, int64DeltaUvarintSize(int64DeltaZigZagEncode(int64(current-previous))))
		previous = current
	}
	return size
}

func int64DeltaRawSize(count int) uint64 {
	size := uint64(int64DeltaHeader) + int64DeltaUvarintSize(uint64(count))
	return int64DeltaAddSize(size, uint64(count)*8)
}

func int64DeltaByteSize(count int) uint64 {
	size := uint64(int64DeltaHeader) + int64DeltaUvarintSize(uint64(count))
	if count == 0 {
		return size
	}
	return int64DeltaAddSize(size, uint64(8+count-1))
}

func int64DeltaUvarintSize(value uint64) uint64 {
	size := uint64(1)
	for value >= 0x80 {
		value >>= 7
		size++
	}
	return size
}

func int64DeltaAddSize(left, right uint64) uint64 {
	if ^uint64(0)-left < right {
		return ^uint64(0)
	}
	return left + right
}
