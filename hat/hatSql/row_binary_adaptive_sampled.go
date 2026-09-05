package hatSql

import (
	"encoding/binary"
	"fmt"
)

// EncodeSQLRowBinaryAdaptiveSampled chooses a RowBinary adaptive codec from a
// prefix of rows, then encodes the complete batch with that codec. A positive
// sampleRows value is required for non-empty batches; values larger than the
// batch are clamped. This opt-in variant avoids materializing all three full
// candidate payloads, at the cost of potentially choosing a less compact codec
// for data whose shape changes after the sampled prefix.
func EncodeSQLRowBinaryAdaptiveSampled(columns []SQLRowBinaryColumn, rows []SQLRow, sampleRows int) ([]byte, error) {
	if err := validateSQLRowBinaryColumns(columns); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	if sampleRows <= 0 {
		return nil, fmt.Errorf("RowBinary adaptive sample row count %d must be positive", sampleRows)
	}
	if len(rows) > maxSQLRowBinaryRows {
		return nil, fmt.Errorf("RowBinary adaptive row count %d exceeds limit %d", len(rows), maxSQLRowBinaryRows)
	}

	sampleCount := sampleRows
	if sampleCount > len(rows) {
		sampleCount = len(rows)
	}
	sample := rows[:sampleCount]
	legacy, err := EncodeSQLRowBinary(columns, sample)
	if err != nil {
		return nil, err
	}
	delta, err := EncodeSQLRowBinaryDelta(columns, sample)
	if err != nil {
		return nil, err
	}
	doubleDelta, err := EncodeSQLRowBinaryDoubleDelta(columns, sample)
	if err != nil {
		return nil, err
	}

	codec := SQLRowBinaryAdaptiveCodecLegacy
	if len(delta) < len(legacy) {
		codec = SQLRowBinaryAdaptiveCodecDelta
	}
	selectedSample := legacy
	if codec == SQLRowBinaryAdaptiveCodecDelta {
		selectedSample = delta
	}
	if len(doubleDelta) < len(selectedSample) {
		codec = SQLRowBinaryAdaptiveCodecDoubleDelta
	}

	selected, err := encodeSQLRowBinaryAdaptiveCodec(columns, rows, codec)
	if err != nil {
		return nil, err
	}
	encoded := make([]byte, 0, len(selected)+1+len(sqlRowBinaryAdaptiveMagic)+binary.MaxVarintLen64)
	encoded = append(encoded, sqlRowBinaryAdaptiveMagic[:]...)
	encoded = append(encoded, byte(codec))
	encoded = appendSQLRowBinaryDeltaUvarint(encoded, uint64(len(selected)))
	return append(encoded, selected...), nil
}

func encodeSQLRowBinaryAdaptiveCodec(columns []SQLRowBinaryColumn, rows []SQLRow, codec SQLRowBinaryAdaptiveCodec) ([]byte, error) {
	switch codec {
	case SQLRowBinaryAdaptiveCodecLegacy:
		return EncodeSQLRowBinary(columns, rows)
	case SQLRowBinaryAdaptiveCodecDelta:
		return EncodeSQLRowBinaryDelta(columns, rows)
	case SQLRowBinaryAdaptiveCodecDoubleDelta:
		return EncodeSQLRowBinaryDoubleDelta(columns, rows)
	default:
		return nil, fmt.Errorf("RowBinary adaptive codec %d is unsupported", codec)
	}
}
