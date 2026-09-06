package hatCodec

import (
	"encoding/binary"
	"errors"
	"math"
	"math/bits"
)

var ErrGorillaFloatInvalid = errors.New("hatriecache: gorilla float encoding is invalid")

const (
	gorillaFloatSame   = byte(0)
	gorillaFloatWindow = byte(1)
)

// EncodeGorillaFloat64 encodes float64 bit patterns using XOR deltas. Repeated
// values use one control byte; changing values store the significant XOR
// window rather than all eight bytes.
func EncodeGorillaFloat64(values []float64) ([]byte, error) {
	if len(values) == 0 {
		return []byte{}, nil
	}
	encoded := make([]byte, 0, len(values))
	var scratch [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(scratch[:], uint64(len(values)))
	encoded = append(encoded, scratch[:n]...)
	var first [8]byte
	binary.LittleEndian.PutUint64(first[:], math.Float64bits(values[0]))
	encoded = append(encoded, first[:]...)
	previous := math.Float64bits(values[0])
	for _, value := range values[1:] {
		current := math.Float64bits(value)
		xor := previous ^ current
		if xor == 0 {
			encoded = append(encoded, gorillaFloatSame)
			previous = current
			continue
		}
		leading := bits.LeadingZeros64(xor)
		trailing := bits.TrailingZeros64(xor)
		significant := 64 - leading - trailing
		payloadBytes := (significant + 7) / 8
		encoded = append(encoded, gorillaFloatWindow, byte(leading), byte(significant))
		shifted := xor >> trailing
		for index := 0; index < payloadBytes; index++ {
			encoded = append(encoded, byte(shifted>>uint(8*index)))
		}
		previous = current
	}
	return encoded, nil
}

// DecodeGorillaFloat64 decodes the format produced by EncodeGorillaFloat64.
// Truncated, overflowing, non-canonical, and trailing payloads are rejected.
func DecodeGorillaFloat64(encoded []byte) ([]float64, error) {
	if len(encoded) == 0 {
		return []float64{}, nil
	}
	count, offset := binary.Uvarint(encoded)
	if offset <= 0 || count > uint64(len(encoded)) {
		return nil, ErrGorillaFloatInvalid
	}
	if count == 0 {
		if offset != len(encoded) {
			return nil, ErrGorillaFloatInvalid
		}
		return []float64{}, nil
	}
	if len(encoded)-offset < 8 {
		return nil, ErrGorillaFloatInvalid
	}
	previous := binary.LittleEndian.Uint64(encoded[offset : offset+8])
	offset += 8
	values := make([]float64, int(count))
	values[0] = math.Float64frombits(previous)
	for index := 1; index < int(count); index++ {
		if offset >= len(encoded) {
			return nil, ErrGorillaFloatInvalid
		}
		control := encoded[offset]
		offset++
		switch control {
		case gorillaFloatSame:
		case gorillaFloatWindow:
			if len(encoded)-offset < 2 {
				return nil, ErrGorillaFloatInvalid
			}
			leading := int(encoded[offset])
			significant := int(encoded[offset+1])
			offset += 2
			if leading > 63 || significant < 1 || significant > 64 || leading+significant > 64 {
				return nil, ErrGorillaFloatInvalid
			}
			payloadBytes := (significant + 7) / 8
			if len(encoded)-offset < payloadBytes {
				return nil, ErrGorillaFloatInvalid
			}
			var shifted uint64
			for payloadIndex := 0; payloadIndex < payloadBytes; payloadIndex++ {
				shifted |= uint64(encoded[offset+payloadIndex]) << uint(8*payloadIndex)
			}
			if significant < 64 && shifted>>uint(significant) != 0 {
				return nil, ErrGorillaFloatInvalid
			}
			offset += payloadBytes
			xor := shifted << uint(64-leading-significant)
			if xor == 0 {
				return nil, ErrGorillaFloatInvalid
			}
			previous ^= xor
		default:
			return nil, ErrGorillaFloatInvalid
		}
		values[index] = math.Float64frombits(previous)
	}
	if offset != len(encoded) {
		return nil, ErrGorillaFloatInvalid
	}
	return values, nil
}
