package hatCodec

import (
	"encoding/binary"
	"errors"
)

const (
	runLengthUint64Version byte = 1
	runLengthUint64RawMode byte = 0
	runLengthUint64RLEMode byte = 1
	runLengthUint64Probe        = 64
	runLengthUint64Header       = 6
)

var errRunLengthUint64Frame = errors.New("hatCodec: invalid run-length uint64 frame")

// EncodeRunLengthUint64 encodes a uint64 block with a compact run-length mode
// when that mode is smaller than the fixed-width raw representation.
func EncodeRunLengthUint64(values []uint64) []byte {
	rawSize := runLengthUint64RawSize(len(values))
	if !runLengthUint64HasEarlyRun(values) {
		return encodeRunLengthUint64Raw(values, rawSize)
	}
	rleSize, hasRuns := runLengthUint64EncodedSize(values)
	if !hasRuns || rleSize >= rawSize || rleSize > uint64(maxInt) {
		return encodeRunLengthUint64Raw(values, rawSize)
	}

	frame := make([]byte, 0, int(rleSize))
	frame = appendRunLengthUint64Header(frame, runLengthUint64RLEMode, len(values))
	for i := 0; i < len(values); {
		runEnd := runLengthUint64RunEnd(values, i)
		if runEnd-i >= 2 {
			frame = appendRunLengthUint64Uvarint(frame, uint64(runEnd-i)<<1|1)
			frame = appendRunLengthUint64Uvarint(frame, values[i])
			i = runEnd
			continue
		}

		literalStart := i
		i = runEnd
		for i < len(values) {
			runEnd = runLengthUint64RunEnd(values, i)
			if runEnd-i >= 2 {
				break
			}
			i = runEnd
		}
		frame = appendRunLengthUint64Uvarint(frame, uint64(i-literalStart)<<1)
		for _, value := range values[literalStart:i] {
			var encoded [8]byte
			binary.LittleEndian.PutUint64(encoded[:], value)
			frame = append(frame, encoded[:]...)
		}
	}
	return frame
}

// DecodeRunLengthUint64 decodes an HCR1 uint64 block into dst when its
// capacity is sufficient, otherwise it allocates a destination block.
func DecodeRunLengthUint64(frame []byte, dst []uint64) ([]uint64, error) {
	if len(frame) < runLengthUint64Header ||
		frame[0] != 'H' || frame[1] != 'C' || frame[2] != 'R' || frame[3] != '1' {
		return nil, errRunLengthUint64Frame
	}
	if frame[4] != runLengthUint64Version {
		return nil, errRunLengthUint64Frame
	}
	mode := frame[5]
	if mode != runLengthUint64RawMode && mode != runLengthUint64RLEMode {
		return nil, errRunLengthUint64Frame
	}

	countValue, consumed, ok := decodeRunLengthUint64Uvarint(frame[runLengthUint64Header:])
	if !ok || countValue > uint64(maxInt) {
		return nil, errRunLengthUint64Frame
	}
	count := int(countValue)
	offset := runLengthUint64Header + consumed

	switch mode {
	case runLengthUint64RawMode:
		payloadSize := uint64(count) * 8
		if payloadSize/8 != uint64(count) || uint64(len(frame)-offset) != payloadSize {
			return nil, errRunLengthUint64Frame
		}
		dst = runLengthUint64Destination(dst, count)
		for i := range dst {
			dst[i] = binary.LittleEndian.Uint64(frame[offset+i*8:])
		}
		return dst, nil
	case runLengthUint64RLEMode:
		if cap(dst) < count {
			if _, ok := decodeRunLengthUint64RLE(frame, offset, count, nil); !ok {
				return nil, errRunLengthUint64Frame
			}
		}
		dst = runLengthUint64Destination(dst, count)
		if _, ok := decodeRunLengthUint64RLE(frame, offset, count, dst); !ok {
			return nil, errRunLengthUint64Frame
		}
		return dst, nil
	}
	return nil, errRunLengthUint64Frame
}

func runLengthUint64Destination(dst []uint64, count int) []uint64 {
	if cap(dst) < count {
		return make([]uint64, count)
	}
	return dst[:count]
}

func decodeRunLengthUint64RLE(frame []byte, offset, count int, dst []uint64) (int, bool) {
	decoded := 0
	for decoded < count {
		token, tokenBytes, ok := decodeRunLengthUint64Uvarint(frame[offset:])
		if !ok || token == 0 {
			return 0, false
		}
		offset += tokenBytes
		items := token >> 1
		if items == 0 || items > uint64(count-decoded) {
			return 0, false
		}
		if token&1 == 1 {
			value, valueBytes, ok := decodeRunLengthUint64Uvarint(frame[offset:])
			if !ok {
				return 0, false
			}
			offset += valueBytes
			if dst != nil {
				for i := uint64(0); i < items; i++ {
					dst[decoded] = value
					decoded++
				}
			} else {
				decoded += int(items)
			}
			continue
		}

		literalBytes := items * 8
		if literalBytes/8 != items || literalBytes > uint64(len(frame)-offset) {
			return 0, false
		}
		if dst != nil {
			for i := uint64(0); i < items; i++ {
				dst[decoded] = binary.LittleEndian.Uint64(frame[offset:])
				offset += 8
				decoded++
			}
		} else {
			offset += int(literalBytes)
			decoded += int(items)
		}
	}
	return offset, offset == len(frame)
}

func encodeRunLengthUint64Raw(values []uint64, size uint64) []byte {
	frame := make([]byte, 0, int(size))
	frame = appendRunLengthUint64Header(frame, runLengthUint64RawMode, len(values))
	for _, value := range values {
		var encoded [8]byte
		binary.LittleEndian.PutUint64(encoded[:], value)
		frame = append(frame, encoded[:]...)
	}
	return frame
}

func appendRunLengthUint64Header(frame []byte, mode byte, count int) []byte {
	frame = append(frame, 'H', 'C', 'R', '1', runLengthUint64Version, mode)
	return appendRunLengthUint64Uvarint(frame, uint64(count))
}

func appendRunLengthUint64Uvarint(dst []byte, value uint64) []byte {
	var encoded [10]byte
	n := binary.PutUvarint(encoded[:], value)
	return append(dst, encoded[:n]...)
}

func decodeRunLengthUint64Uvarint(data []byte) (uint64, int, bool) {
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

func runLengthUint64RunEnd(values []uint64, start int) int {
	end := start + 1
	for end < len(values) && values[end] == values[start] {
		end++
	}
	return end
}

func runLengthUint64HasEarlyRun(values []uint64) bool {
	probeLength := len(values)
	if probeLength > runLengthUint64Probe {
		probeLength = runLengthUint64Probe
	}
	for i := 1; i < probeLength; i++ {
		if values[i] == values[i-1] {
			return true
		}
	}
	return false
}

func runLengthUint64EncodedSize(values []uint64) (uint64, bool) {
	size := uint64(runLengthUint64Header) + runLengthUint64UvarintSize(uint64(len(values)))
	hasRuns := false
	for i := 0; i < len(values); {
		runEnd := runLengthUint64RunEnd(values, i)
		if runEnd-i >= 2 {
			hasRuns = true
			size = runLengthUint64AddSize(size, runLengthUint64UvarintSize(uint64(runEnd-i)<<1|1))
			size = runLengthUint64AddSize(size, runLengthUint64UvarintSize(values[i]))
			i = runEnd
			continue
		}

		literalStart := i
		i = runEnd
		for i < len(values) {
			runEnd = runLengthUint64RunEnd(values, i)
			if runEnd-i >= 2 {
				break
			}
			i = runEnd
		}
		size = runLengthUint64AddSize(size, runLengthUint64UvarintSize(uint64(i-literalStart)<<1))
		size = runLengthUint64AddSize(size, uint64(i-literalStart)*8)
	}
	return size, hasRuns
}

func runLengthUint64RawSize(count int) uint64 {
	size := uint64(runLengthUint64Header) + runLengthUint64UvarintSize(uint64(count))
	return runLengthUint64AddSize(size, uint64(count)*8)
}

func runLengthUint64UvarintSize(value uint64) uint64 {
	size := uint64(1)
	for value >= 0x80 {
		value >>= 7
		size++
	}
	return size
}

func runLengthUint64AddSize(left, right uint64) uint64 {
	if ^uint64(0)-left < right {
		return ^uint64(0)
	}
	return left + right
}

const maxInt = int(^uint(0) >> 1)
