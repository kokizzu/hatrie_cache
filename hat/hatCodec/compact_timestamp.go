package hatCodec

import (
	"encoding/binary"
	"errors"
)

var (
	ErrCompactTimestampInvalid  = errors.New("hatriecache: compact timestamp block is invalid")
	ErrCompactTimestampTooLarge = errors.New("hatriecache: compact timestamp block is too large")
)

// CompactTimestampEncoding identifies the representation selected for an
// int64 timestamp block.
type CompactTimestampEncoding uint8

const (
	CompactTimestampEncodingRaw CompactTimestampEncoding = iota
	CompactTimestampEncodingDoubleDelta
)

const (
	compactTimestampMagic     = "HTD1"
	compactTimestampHeader    = len(compactTimestampMagic) + 1
	maxCompactTimestampValues = uint64(1 << 20)
	maxCompactTimestampBytes  = uint64(64 << 20)
)

// SelectCompactTimestampEncoding chooses double-delta only when its complete
// payload is strictly smaller than fixed-width raw timestamps.
func SelectCompactTimestampEncoding(values []int64) CompactTimestampEncoding {
	if len(values) == 0 || uint64(len(values)) > maxCompactTimestampValues {
		return CompactTimestampEncodingRaw
	}
	if !compactTimestampLooksCompressible(values) {
		return CompactTimestampEncodingRaw
	}
	rawBytes, rawOK := compactTimestampRawPayloadSize(len(values))
	deltaBytes, deltaOK := compactTimestampPayloadSize(values)
	if deltaOK && rawOK && deltaBytes < rawBytes {
		return CompactTimestampEncodingDoubleDelta
	}
	return CompactTimestampEncodingRaw
}

// EncodeCompactTimestamps writes a deterministic raw or double-delta block.
// Differences use modular int64 arithmetic so the representation is exact for
// arbitrary signed timestamp-like values, not only monotonic timestamps.
func EncodeCompactTimestamps(values []int64) ([]byte, error) {
	if uint64(len(values)) > maxCompactTimestampValues {
		return nil, ErrCompactTimestampTooLarge
	}
	choice := SelectCompactTimestampEncoding(values)
	payloadBytes, ok := compactTimestampRawPayloadSize(len(values))
	if choice == CompactTimestampEncodingDoubleDelta {
		payloadBytes, ok = compactTimestampPayloadSize(values)
	}
	if !ok || uint64(payloadBytes)+uint64(compactTimestampHeader) > maxCompactTimestampBytes || payloadBytes > int(^uint(0)>>1)-compactTimestampHeader {
		return nil, ErrCompactTimestampTooLarge
	}
	encoded := make([]byte, compactTimestampHeader+payloadBytes)
	copy(encoded, compactTimestampMagic)
	encoded[len(compactTimestampMagic)] = byte(choice)
	offset := compactTimestampHeader
	if choice == CompactTimestampEncodingRaw {
		offset += binary.PutUvarint(encoded[offset:], uint64(len(values)))
		for _, value := range values {
			binary.LittleEndian.PutUint64(encoded[offset:], uint64(value))
			offset += 8
		}
		return encoded[:offset], nil
	}
	offset += binary.PutUvarint(encoded[offset:], uint64(len(values)))
	if len(values) == 0 {
		return encoded[:offset], nil
	}
	offset += binary.PutUvarint(encoded[offset:], compactTimestampZigZag(values[0]))
	if len(values) == 1 {
		return encoded[:offset], nil
	}
	previous := values[0]
	delta := compactTimestampDifference(values[1], previous)
	offset += binary.PutUvarint(encoded[offset:], compactTimestampZigZag(delta))
	previousDelta := delta
	previous = values[1]
	for index := 2; index < len(values); index++ {
		delta = compactTimestampDifference(values[index], previous)
		deltaDelta := compactTimestampDifference(delta, previousDelta)
		if deltaDelta == 0 {
			encoded[offset] = 0
			offset++
		} else {
			offset += binary.PutUvarint(encoded[offset:], compactTimestampZigZag(deltaDelta))
		}
		previousDelta = delta
		previous = values[index]
	}
	return encoded[:offset], nil
}

// DecodeCompactTimestamps validates and decodes one timestamp block before
// allocating the result slice.
func DecodeCompactTimestamps(encoded []byte) ([]int64, error) {
	if len(encoded) < compactTimestampHeader || string(encoded[:len(compactTimestampMagic)]) != compactTimestampMagic || uint64(len(encoded)) > maxCompactTimestampBytes {
		return nil, ErrCompactTimestampInvalid
	}
	payload := encoded[compactTimestampHeader:]
	switch CompactTimestampEncoding(encoded[len(compactTimestampMagic)]) {
	case CompactTimestampEncodingRaw:
		return decodeCompactTimestampRaw(payload)
	case CompactTimestampEncodingDoubleDelta:
		return decodeCompactTimestampDoubleDelta(payload)
	default:
		return nil, ErrCompactTimestampInvalid
	}
}

func compactTimestampRawPayloadSize(count int) (int, bool) {
	if count < 0 || uint64(count) > maxCompactTimestampValues {
		return 0, false
	}
	var scratch [binary.MaxVarintLen64]byte
	countBytes := binary.PutUvarint(scratch[:], uint64(count))
	total := uint64(countBytes) + uint64(count)*8
	if total > maxCompactTimestampBytes || total > uint64(^uint(0)>>1) {
		return 0, false
	}
	return int(total), true
}

func compactTimestampPayloadSize(values []int64) (int, bool) {
	if len(values) == 0 || uint64(len(values)) > maxCompactTimestampValues {
		return 0, false
	}
	var scratch [binary.MaxVarintLen64]byte
	total := uint64(binary.PutUvarint(scratch[:], uint64(len(values))))
	total += uint64(binary.PutUvarint(scratch[:], compactTimestampZigZag(values[0])))
	if len(values) > 1 {
		firstDelta := compactTimestampDifference(values[1], values[0])
		total += uint64(binary.PutUvarint(scratch[:], compactTimestampZigZag(firstDelta)))
		previous := values[1]
		previousDelta := firstDelta
		for index := 2; index < len(values); index++ {
			delta := compactTimestampDifference(values[index], previous)
			deltaDelta := compactTimestampDifference(delta, previousDelta)
			total += uint64(binary.PutUvarint(scratch[:], compactTimestampZigZag(deltaDelta)))
			previous = values[index]
			previousDelta = delta
		}
	}
	if total > maxCompactTimestampBytes || total > uint64(^uint(0)>>1) {
		return 0, false
	}
	return int(total), true
}

func compactTimestampLooksCompressible(values []int64) bool {
	if len(values) < 3 {
		return true
	}
	sampleCount := len(values) - 2
	if sampleCount > 32 {
		sampleCount = 32
	}
	var scratch [binary.MaxVarintLen64]byte
	var sampleBytes uint64
	for sample := 0; sample < sampleCount; sample++ {
		index := 2 + sample*(len(values)-2)/sampleCount
		previousDelta := compactTimestampDifference(values[index-1], values[index-2])
		delta := compactTimestampDifference(values[index], values[index-1])
		deltaDelta := compactTimestampDifference(delta, previousDelta)
		sampleBytes += uint64(binary.PutUvarint(scratch[:], compactTimestampZigZag(deltaDelta)))
	}
	return sampleBytes < uint64(sampleCount*8)
}

func decodeCompactTimestampRaw(payload []byte) ([]int64, error) {
	count, offset, err := readCompactTimestampUvarint(payload)
	if err != nil || count > maxCompactTimestampValues {
		return nil, ErrCompactTimestampInvalid
	}
	remaining := payload[offset:]
	if len(remaining)%8 != 0 || count != uint64(len(remaining)/8) {
		return nil, ErrCompactTimestampInvalid
	}
	values := make([]int64, int(count))
	for index := range values {
		values[index] = int64(binary.LittleEndian.Uint64(remaining[index*8:]))
	}
	return values, nil
}

func decodeCompactTimestampDoubleDelta(payload []byte) ([]int64, error) {
	count, offset, err := readCompactTimestampUvarint(payload)
	if err != nil || count > maxCompactTimestampValues {
		return nil, ErrCompactTimestampInvalid
	}
	values := make([]int64, int(count))
	if count == 0 {
		if offset != len(payload) {
			return nil, ErrCompactTimestampInvalid
		}
		return values, nil
	}
	first, width, ok := readCompactTimestampUvarintAt(payload, offset)
	if !ok {
		return nil, ErrCompactTimestampInvalid
	}
	offset += width
	values[0] = compactTimestampUnzigZag(first)
	if count == 1 {
		if offset != len(payload) {
			return nil, ErrCompactTimestampInvalid
		}
		return values, nil
	}
	firstDelta, width, ok := readCompactTimestampUvarintAt(payload, offset)
	if !ok {
		return nil, ErrCompactTimestampInvalid
	}
	offset += width
	delta := compactTimestampUnzigZag(firstDelta)
	values[1] = int64(uint64(values[0]) + uint64(delta))
	previousDelta := delta
	for index := 2; index < len(values); index++ {
		if offset >= len(payload) {
			return nil, ErrCompactTimestampInvalid
		}
		if payload[offset] == 0 {
			offset++
			delta = previousDelta
		} else {
			deltaDelta, width, ok := readCompactTimestampUvarintAt(payload, offset)
			if !ok {
				return nil, ErrCompactTimestampInvalid
			}
			offset += width
			delta = int64(uint64(previousDelta) + uint64(compactTimestampUnzigZag(deltaDelta)))
		}
		values[index] = int64(uint64(values[index-1]) + uint64(delta))
		previousDelta = delta
	}
	if offset != len(payload) {
		return nil, ErrCompactTimestampInvalid
	}
	return values, nil
}

func compactTimestampDifference(value, previous int64) int64 {
	return int64(uint64(value) - uint64(previous))
}

func compactTimestampZigZag(value int64) uint64 {
	return uint64(value)<<1 ^ uint64(value>>63)
}

func compactTimestampUnzigZag(value uint64) int64 {
	return int64(value>>1) ^ -int64(value&1)
}

func readCompactTimestampUvarint(payload []byte) (uint64, int, error) {
	value, width, ok := readCompactTimestampUvarintAt(payload, 0)
	if !ok {
		return 0, 0, ErrCompactTimestampInvalid
	}
	return value, width, nil
}

func readCompactTimestampUvarintAt(payload []byte, offset int) (uint64, int, bool) {
	if offset < 0 || offset >= len(payload) {
		return 0, 0, false
	}
	if payload[offset] == 0 {
		return 0, 1, true
	}
	value, width := binary.Uvarint(payload[offset:])
	if width <= 0 {
		return 0, 0, false
	}
	var scratch [binary.MaxVarintLen64]byte
	if binary.PutUvarint(scratch[:], value) != width {
		return 0, 0, false
	}
	return value, width, true
}
