package hatCodec

import (
	"encoding/binary"
	"errors"
)

var (
	ErrLowCardinalityStringInvalid  = errors.New("hatriecache: low-cardinality string block is invalid")
	ErrLowCardinalityStringTooLarge = errors.New("hatriecache: low-cardinality string block is too large")
)

// LowCardinalityStringEncoding identifies the representation selected for a
// string block.
type LowCardinalityStringEncoding uint8

const (
	LowCardinalityStringEncodingRaw LowCardinalityStringEncoding = iota
	LowCardinalityStringEncodingDictionary
)

const (
	lowCardinalityStringMagic       = "HLD1"
	lowCardinalityStringHeader      = len(lowCardinalityStringMagic) + 1
	maxLowCardinalityStringValues   = uint64(1 << 20)
	maxLowCardinalityStringBytes    = uint64(64 << 20)
	maxLowCardinalityStringValueLen = uint64(1 << 20)
	maxLowCardinalityDictionary     = uint64(1 << 16)
)

type lowCardinalityStringPlan struct {
	dictionary       []string
	indexes          []uint64
	rawPayloadBytes  uint64
	dictPayloadBytes uint64
}

// SelectLowCardinalityStringEncoding chooses a dictionary only when its full
// framed payload is smaller than the length-prefixed raw representation.
func SelectLowCardinalityStringEncoding(values []string) LowCardinalityStringEncoding {
	plan, err := buildLowCardinalityStringPlan(values)
	if err != nil || plan.dictPayloadBytes >= plan.rawPayloadBytes {
		return LowCardinalityStringEncodingRaw
	}
	return LowCardinalityStringEncodingDictionary
}

// EncodeLowCardinalityStrings writes a deterministic, bounded string block.
// Dictionary entries are emitted in first-seen order and repeated values are
// represented by compact uvarint indexes.
func EncodeLowCardinalityStrings(values []string) ([]byte, error) {
	plan, err := buildLowCardinalityStringPlan(values)
	if err != nil {
		return nil, err
	}
	choice := LowCardinalityStringEncodingRaw
	payloadBytes := plan.rawPayloadBytes
	if plan.dictPayloadBytes < plan.rawPayloadBytes {
		choice = LowCardinalityStringEncodingDictionary
		payloadBytes = plan.dictPayloadBytes
	}
	if payloadBytes > uint64(^uint(0)>>1)-uint64(lowCardinalityStringHeader) {
		return nil, ErrLowCardinalityStringTooLarge
	}
	encoded := make([]byte, lowCardinalityStringHeader+int(payloadBytes))
	copy(encoded, lowCardinalityStringMagic)
	encoded[len(lowCardinalityStringMagic)] = byte(choice)
	offset := lowCardinalityStringHeader
	switch choice {
	case LowCardinalityStringEncodingRaw:
		offset += binary.PutUvarint(encoded[offset:], uint64(len(values)))
		for _, value := range values {
			offset += binary.PutUvarint(encoded[offset:], uint64(len(value)))
			offset += copy(encoded[offset:], value)
		}
	case LowCardinalityStringEncodingDictionary:
		offset += binary.PutUvarint(encoded[offset:], uint64(len(values)))
		offset += binary.PutUvarint(encoded[offset:], uint64(len(plan.dictionary)))
		for _, value := range plan.dictionary {
			offset += binary.PutUvarint(encoded[offset:], uint64(len(value)))
			offset += copy(encoded[offset:], value)
		}
		for _, index := range plan.indexes {
			offset += binary.PutUvarint(encoded[offset:], index)
		}
	}
	return encoded[:offset], nil
}

// DecodeLowCardinalityStrings validates and decodes one string block. The
// decoder bounds all counts and byte lengths before allocating output state.
func DecodeLowCardinalityStrings(encoded []byte) ([]string, error) {
	if len(encoded) < lowCardinalityStringHeader || string(encoded[:len(lowCardinalityStringMagic)]) != lowCardinalityStringMagic {
		return nil, ErrLowCardinalityStringInvalid
	}
	payload := encoded[lowCardinalityStringHeader:]
	switch LowCardinalityStringEncoding(encoded[len(lowCardinalityStringMagic)]) {
	case LowCardinalityStringEncodingRaw:
		return decodeLowCardinalityRaw(payload)
	case LowCardinalityStringEncodingDictionary:
		return decodeLowCardinalityDictionary(payload)
	default:
		return nil, ErrLowCardinalityStringInvalid
	}
}

func buildLowCardinalityStringPlan(values []string) (lowCardinalityStringPlan, error) {
	if uint64(len(values)) > maxLowCardinalityStringValues {
		return lowCardinalityStringPlan{}, ErrLowCardinalityStringTooLarge
	}
	var scratch [binary.MaxVarintLen64]byte
	rawBytes := uint64(binary.PutUvarint(scratch[:], uint64(len(values))))
	stringBytes := uint64(0)
	for _, value := range values {
		valueBytes := uint64(len(value))
		if valueBytes > maxLowCardinalityStringValueLen || stringBytes > maxLowCardinalityStringBytes-valueBytes {
			return lowCardinalityStringPlan{}, ErrLowCardinalityStringTooLarge
		}
		stringBytes += valueBytes
		lengthBytes := uint64(binary.PutUvarint(scratch[:], valueBytes))
		if rawBytes > maxLowCardinalityStringBytes-lengthBytes-valueBytes {
			return lowCardinalityStringPlan{}, ErrLowCardinalityStringTooLarge
		}
		rawBytes += lengthBytes + valueBytes
	}
	if !shouldAttemptLowCardinalityDictionary(values) {
		return lowCardinalityStringPlan{rawPayloadBytes: rawBytes, dictPayloadBytes: rawBytes}, nil
	}
	dictionary := make([]string, 0)
	indexes := make([]uint64, len(values))
	lookup := make(map[string]uint64)
	for index, value := range values {
		if existing, ok := lookup[value]; ok {
			indexes[index] = existing
			continue
		}
		if uint64(len(dictionary)) >= maxLowCardinalityDictionary {
			return lowCardinalityStringPlan{}, ErrLowCardinalityStringTooLarge
		}
		existing := uint64(len(dictionary))
		lookup[value] = existing
		dictionary = append(dictionary, value)
		indexes[index] = existing
	}
	dictBytes := uint64(binary.PutUvarint(scratch[:], uint64(len(values))))
	dictBytes += uint64(binary.PutUvarint(scratch[:], uint64(len(dictionary))))
	for _, value := range dictionary {
		valueBytes := uint64(len(value))
		lengthBytes := uint64(binary.PutUvarint(scratch[:], valueBytes))
		if dictBytes > maxLowCardinalityStringBytes-lengthBytes-valueBytes {
			return lowCardinalityStringPlan{}, ErrLowCardinalityStringTooLarge
		}
		dictBytes += lengthBytes + valueBytes
	}
	for _, index := range indexes {
		indexBytes := uint64(binary.PutUvarint(scratch[:], index))
		if dictBytes > maxLowCardinalityStringBytes-indexBytes {
			return lowCardinalityStringPlan{}, ErrLowCardinalityStringTooLarge
		}
		dictBytes += indexBytes
	}
	return lowCardinalityStringPlan{
		dictionary:       dictionary,
		indexes:          indexes,
		rawPayloadBytes:  rawBytes,
		dictPayloadBytes: dictBytes,
	}, nil
}

func shouldAttemptLowCardinalityDictionary(values []string) bool {
	if len(values) < 2 {
		return false
	}
	sampleCount := len(values)
	if sampleCount > 64 {
		sampleCount = 64
	}
	var sample [64]string
	distinct := 0
	for index := 0; index < sampleCount; index++ {
		value := values[index*len(values)/sampleCount]
		seen := false
		for previous := 0; previous < distinct; previous++ {
			if sample[previous] == value {
				seen = true
				break
			}
		}
		if seen {
			continue
		}
		sample[distinct] = value
		distinct++
		if distinct*2 >= sampleCount {
			return false
		}
	}
	return distinct > 0
}

func decodeLowCardinalityRaw(payload []byte) ([]string, error) {
	count, offset, err := readLowCardinalityUvarint(payload)
	if err != nil || count > maxLowCardinalityStringValues {
		return nil, ErrLowCardinalityStringInvalid
	}
	values := make([]string, int(count))
	var totalBytes uint64
	for index := range values {
		length, width, ok := readLowCardinalityUvarintAt(payload, offset)
		if !ok || length > maxLowCardinalityStringValueLen || length > uint64(len(payload)-offset-width) {
			return nil, ErrLowCardinalityStringInvalid
		}
		offset += width
		if totalBytes > maxLowCardinalityStringBytes-length {
			return nil, ErrLowCardinalityStringInvalid
		}
		totalBytes += length
		values[index] = string(payload[offset : offset+int(length)])
		offset += int(length)
	}
	if offset != len(payload) {
		return nil, ErrLowCardinalityStringInvalid
	}
	return values, nil
}

func decodeLowCardinalityDictionary(payload []byte) ([]string, error) {
	count, offset, err := readLowCardinalityUvarint(payload)
	if err != nil || count > maxLowCardinalityStringValues {
		return nil, ErrLowCardinalityStringInvalid
	}
	dictionaryCount, width, ok := readLowCardinalityUvarintAt(payload, offset)
	if !ok || dictionaryCount == 0 || dictionaryCount > maxLowCardinalityDictionary || dictionaryCount > count {
		if count == 0 && dictionaryCount == 0 && ok {
			offset += width
			if offset == len(payload) {
				return []string{}, nil
			}
		}
		return nil, ErrLowCardinalityStringInvalid
	}
	offset += width
	dictionary := make([]string, int(dictionaryCount))
	seen := make(map[string]struct{}, int(dictionaryCount))
	var totalBytes uint64
	for index := range dictionary {
		length, valueWidth, valueOK := readLowCardinalityUvarintAt(payload, offset)
		if !valueOK || length > maxLowCardinalityStringValueLen || length > uint64(len(payload)-offset-valueWidth) {
			return nil, ErrLowCardinalityStringInvalid
		}
		offset += valueWidth
		if totalBytes > maxLowCardinalityStringBytes-length {
			return nil, ErrLowCardinalityStringInvalid
		}
		totalBytes += length
		value := string(payload[offset : offset+int(length)])
		if _, duplicate := seen[value]; duplicate {
			return nil, ErrLowCardinalityStringInvalid
		}
		seen[value] = struct{}{}
		dictionary[index] = value
		offset += int(length)
	}
	values := make([]string, int(count))
	for index := range values {
		valueIndex, valueWidth, valueOK := readLowCardinalityUvarintAt(payload, offset)
		if !valueOK || valueIndex >= dictionaryCount {
			return nil, ErrLowCardinalityStringInvalid
		}
		offset += valueWidth
		values[index] = dictionary[valueIndex]
	}
	if offset != len(payload) {
		return nil, ErrLowCardinalityStringInvalid
	}
	return values, nil
}

func readLowCardinalityUvarint(payload []byte) (uint64, int, error) {
	value, width, ok := readLowCardinalityUvarintAt(payload, 0)
	if !ok {
		return 0, 0, ErrLowCardinalityStringInvalid
	}
	return value, width, nil
}

func readLowCardinalityUvarintAt(payload []byte, offset int) (uint64, int, bool) {
	if offset < 0 || offset >= len(payload) {
		return 0, 0, false
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
