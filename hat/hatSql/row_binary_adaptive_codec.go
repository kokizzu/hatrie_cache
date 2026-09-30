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
	return encodeSQLRowBinaryAdaptive(nil, columns, rows)
}

// EncodeSQLRowBinaryAdaptiveInto chooses the same adaptive codec as
// EncodeSQLRowBinaryAdaptive and writes the HSA1 envelope into dst when it has
// enough capacity. The candidate payloads retain the existing codec behavior;
// only the final selected envelope is reused.
func EncodeSQLRowBinaryAdaptiveInto(dst []byte, columns []SQLRowBinaryColumn, rows []SQLRow) ([]byte, error) {
	return encodeSQLRowBinaryAdaptive(dst, columns, rows)
}

// SQLRowBinaryAdaptiveEncoder retains candidate payload and delta scratch
// buffers for repeated adaptive encodes. Its zero value is ready for use; one
// encoder must not be used concurrently by multiple goroutines.
type SQLRowBinaryAdaptiveEncoder struct {
	legacy       []byte
	delta        []byte
	doubleDelta  []byte
	deltaScratch sqlRowBinaryDeltaScratch
}

// Reset releases retained candidate and scratch buffers. It is useful after a
// workload with unusually large rows so the encoder does not keep its high
// water mark.
func (encoder *SQLRowBinaryAdaptiveEncoder) Reset() {
	if encoder == nil {
		return
	}
	*encoder = SQLRowBinaryAdaptiveEncoder{}
}

// EncodeInto reuses the encoder's candidate buffers and writes the selected
// HSA1 envelope into dst. It preserves the stateless adaptive selection and
// wire format while avoiding candidate allocations after warm-up.
func (encoder *SQLRowBinaryAdaptiveEncoder) EncodeInto(dst []byte, columns []SQLRowBinaryColumn, rows []SQLRow) ([]byte, error) {
	if encoder == nil {
		return EncodeSQLRowBinaryAdaptiveInto(dst, columns, rows)
	}
	if err := validateSQLRowBinaryColumns(columns); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return dst[:0], nil
	}
	if len(rows) > maxSQLRowBinaryRows {
		return nil, fmt.Errorf("RowBinary row count %d exceeds limit %d", len(rows), maxSQLRowBinaryRows)
	}
	var err error
	encoder.legacy, err = encodeSQLRowBinaryValidated(encoder.legacy, columns, rows)
	if err != nil {
		return nil, err
	}
	encoder.delta, err = encodeSQLRowBinaryDeltaValidated(encoder.delta, columns, rows, false, &encoder.deltaScratch)
	if err != nil {
		return nil, err
	}
	encoder.doubleDelta, err = encodeSQLRowBinaryDeltaValidated(encoder.doubleDelta, columns, rows, true, &encoder.deltaScratch)
	if err != nil {
		return nil, err
	}
	codec := SQLRowBinaryAdaptiveCodecLegacy
	selected := encoder.legacy
	if len(encoder.delta) < len(selected) {
		codec = SQLRowBinaryAdaptiveCodecDelta
		selected = encoder.delta
	}
	if len(encoder.doubleDelta) < len(selected) {
		codec = SQLRowBinaryAdaptiveCodecDoubleDelta
		selected = encoder.doubleDelta
	}
	capacity := len(selected) + 1 + len(sqlRowBinaryAdaptiveMagic) + binary.MaxVarintLen64
	if cap(dst) < capacity {
		dst = make([]byte, 0, capacity)
	} else {
		dst = dst[:0]
	}
	dst = append(dst, sqlRowBinaryAdaptiveMagic[:]...)
	dst = append(dst, byte(codec))
	dst = appendSQLRowBinaryDeltaUvarint(dst, uint64(len(selected)))
	return append(dst, selected...), nil
}

func encodeSQLRowBinaryAdaptive(dst []byte, columns []SQLRowBinaryColumn, rows []SQLRow) ([]byte, error) {
	if err := validateSQLRowBinaryColumns(columns); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return dst[:0], nil
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
	capacity := len(selected) + 1 + len(sqlRowBinaryAdaptiveMagic) + binary.MaxVarintLen64
	if cap(dst) < capacity {
		dst = make([]byte, 0, capacity)
	} else {
		dst = dst[:0]
	}
	encoded := dst
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
