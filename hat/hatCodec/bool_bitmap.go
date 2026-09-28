package hatCodec

import (
	"encoding/binary"
	"errors"
)

const (
	boolBitmapVersion byte = 1
	boolBitmapHeader       = 5
	boolBitmapMaxInt       = int(^uint(0) >> 1)
)

var errBoolBitmapFrame = errors.New("hatCodec: invalid bool bitmap frame")

var boolBitmapLookup [256][8]bool

func init() {
	for value := range boolBitmapLookup {
		for bit := range boolBitmapLookup[value] {
			boolBitmapLookup[value][bit] = value&(1<<uint(bit)) != 0
		}
	}
}

// EncodeBoolBitmap packs each boolean into one bit in an HCB1 frame. Bits are
// ordered least-significant first within each payload byte.
func EncodeBoolBitmap(values []bool) []byte {
	payloadSize := len(values) / 8
	if len(values)&7 != 0 {
		payloadSize++
	}
	frame := make([]byte, 0, boolBitmapHeader+binary.MaxVarintLen64+payloadSize)
	frame = append(frame, 'H', 'C', 'B', '1', boolBitmapVersion)
	frame = appendBoolBitmapUvarint(frame, uint64(len(values)))
	payloadOffset := len(frame)
	frame = append(frame, make([]byte, payloadSize)...)
	for byteIndex := 0; byteIndex < payloadSize; byteIndex++ {
		start := byteIndex * 8
		end := start + 8
		if end > len(values) {
			end = len(values)
		}
		var packed byte
		for bit := start; bit < end; bit++ {
			if values[bit] {
				packed |= 1 << uint(bit-start)
			}
		}
		frame[payloadOffset+byteIndex] = packed
	}
	return frame
}

// DecodeBoolBitmap decodes an HCB1 frame and reuses dst when its capacity is
// sufficient.
func DecodeBoolBitmap(frame []byte, dst []bool) ([]bool, error) {
	if len(frame) < boolBitmapHeader ||
		frame[0] != 'H' || frame[1] != 'C' || frame[2] != 'B' || frame[3] != '1' ||
		frame[4] != boolBitmapVersion {
		return nil, errBoolBitmapFrame
	}
	countValue, countBytes, ok := decodeBoolBitmapUvarint(frame[boolBitmapHeader:])
	if !ok || countValue > uint64(boolBitmapMaxInt) {
		return nil, errBoolBitmapFrame
	}
	payloadSize, ok := boolBitmapPayloadSize(countValue)
	if !ok {
		return nil, errBoolBitmapFrame
	}
	payloadOffset := boolBitmapHeader + countBytes
	if payloadSize != uint64(len(frame)-payloadOffset) {
		return nil, errBoolBitmapFrame
	}
	if countValue&7 != 0 && payloadSize > 0 {
		usedBits := byte(countValue & 7)
		paddingMask := byte(0xff << usedBits)
		if frame[len(frame)-1]&paddingMask != 0 {
			return nil, errBoolBitmapFrame
		}
	}

	count := int(countValue)
	if cap(dst) < count {
		dst = make([]bool, count)
	} else {
		dst = dst[:count]
	}
	fullBytes := count / 8
	for byteIndex := 0; byteIndex < fullBytes; byteIndex++ {
		start := byteIndex * 8
		copy(dst[start:start+8], boolBitmapLookup[frame[payloadOffset+byteIndex]][:])
	}
	for i := fullBytes * 8; i < count; i++ {
		dst[i] = frame[payloadOffset+i/8]&(1<<uint(i&7)) != 0
	}
	return dst, nil
}

func appendBoolBitmapUvarint(dst []byte, value uint64) []byte {
	var encoded [10]byte
	n := binary.PutUvarint(encoded[:], value)
	return append(dst, encoded[:n]...)
}

func decodeBoolBitmapUvarint(data []byte) (uint64, int, bool) {
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

func boolBitmapPayloadSize(count uint64) (uint64, bool) {
	if count > ^uint64(0)-7 {
		return 0, false
	}
	return (count + 7) / 8, true
}
