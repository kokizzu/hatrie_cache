package hatSql

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

var sqlRowBinaryAdaptiveMagic = [4]byte{'H', 'S', 'A', '1'}

// SQLRowBinaryAdaptiveCodec identifies the payload selected by
// EncodeSQLRowBinaryAdaptive.
type SQLRowBinaryAdaptiveCodec uint8

const (
	SQLRowBinaryAdaptiveCodecLegacy SQLRowBinaryAdaptiveCodec = iota + 1
	SQLRowBinaryAdaptiveCodecDelta
	SQLRowBinaryAdaptiveCodecDoubleDelta
)

// EncodeSQLRowBinaryAdaptive chooses the smallest of the legacy, first-order
// delta, and second-order delta payloads. The explicit envelope identifies the
// selected codec and preserves compatibility with callers using the legacy
// functions.
func EncodeSQLRowBinaryAdaptive(columns []SQLRowBinaryColumn, rows []SQLRow) ([]byte, error) {
	if err := validateSQLRowBinaryColumns(columns); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	legacy, err := EncodeSQLRowBinary(columns, rows)
	if err != nil {
		return nil, err
	}
	delta, err := EncodeSQLRowBinaryDelta(columns, rows)
	if err != nil {
		return nil, err
	}
	doubleDelta, err := EncodeSQLRowBinaryDoubleDelta(columns, rows)
	if err != nil {
		return nil, err
	}
	codec := SQLRowBinaryAdaptiveCodecLegacy
	selected := legacy
	if len(delta) < len(selected) {
		codec = SQLRowBinaryAdaptiveCodecDelta
		selected = delta
	}
	if len(doubleDelta) < len(selected) {
		codec = SQLRowBinaryAdaptiveCodecDoubleDelta
		selected = doubleDelta
	}
	encoded := make([]byte, 0, len(selected)+1+len(sqlRowBinaryAdaptiveMagic)+binary.MaxVarintLen64)
	encoded = append(encoded, sqlRowBinaryAdaptiveMagic[:]...)
	encoded = append(encoded, byte(codec))
	encoded = appendSQLRowBinaryDeltaUvarint(encoded, uint64(len(selected)))
	return append(encoded, selected...), nil
}

// DecodeSQLRowBinaryAdaptive decodes an explicit adaptive envelope. Legacy
// payloads should continue to use DecodeSQLRowBinary; requiring the envelope
// here avoids ambiguous format-marker collisions in arbitrary legacy bytes.
func DecodeSQLRowBinaryAdaptive(columns []SQLRowBinaryColumn, encoded []byte) ([]SQLRow, error) {
	if err := validateSQLRowBinaryColumns(columns); err != nil {
		return nil, err
	}
	if len(encoded) == 0 {
		return nil, nil
	}
	if len(encoded) < len(sqlRowBinaryAdaptiveMagic)+1 || !bytes.Equal(encoded[:len(sqlRowBinaryAdaptiveMagic)], sqlRowBinaryAdaptiveMagic[:]) {
		return nil, fmt.Errorf("RowBinary adaptive header is invalid or truncated")
	}
	codec := SQLRowBinaryAdaptiveCodec(encoded[len(sqlRowBinaryAdaptiveMagic)])
	offset := len(sqlRowBinaryAdaptiveMagic) + 1
	payloadLength, err := readSQLRowBinaryDeltaUvarint(encoded, &offset, "adaptive payload length")
	if err != nil {
		return nil, err
	}
	if payloadLength > uint64(len(encoded)-offset) {
		return nil, fmt.Errorf("RowBinary adaptive payload length %d exceeds remaining input", payloadLength)
	}
	payloadEnd := offset + int(payloadLength)
	if payloadEnd != len(encoded) {
		return nil, fmt.Errorf("RowBinary adaptive envelope has %d trailing bytes", len(encoded)-payloadEnd)
	}
	payload := encoded[offset:payloadEnd]
	switch codec {
	case SQLRowBinaryAdaptiveCodecLegacy:
		return DecodeSQLRowBinary(columns, payload)
	case SQLRowBinaryAdaptiveCodecDelta, SQLRowBinaryAdaptiveCodecDoubleDelta:
		return DecodeSQLRowBinaryDelta(columns, payload)
	default:
		return nil, fmt.Errorf("RowBinary adaptive codec %d is unsupported", codec)
	}
}
