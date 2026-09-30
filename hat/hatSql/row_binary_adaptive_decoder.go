package hatSql

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// SQLRowBinaryAdaptiveDecoder reuses delta scratch while DecodeInto reuses the
// caller's row slice and row maps. Its zero value is ready for use; one decoder
// must not be used concurrently by multiple goroutines.
type SQLRowBinaryAdaptiveDecoder struct {
	deltaScratch sqlRowBinaryDeltaScratch
}

// Reset releases retained delta scratch. Row maps remain owned by the caller's
// destination slice and are released when that slice is released.
func (decoder *SQLRowBinaryAdaptiveDecoder) Reset() {
	if decoder == nil {
		return
	}
	*decoder = SQLRowBinaryAdaptiveDecoder{}
}

// DecodeSQLRowBinaryAdaptiveInto decodes an HSA1 stream into dst, reusing row
// maps when the caller passes the result from a previous call. It preserves the
// existing allocating DecodeSQLRowBinaryAdaptive API and wire format.
func DecodeSQLRowBinaryAdaptiveInto(dst []SQLRow, columns []SQLRowBinaryColumn, encoded []byte) ([]SQLRow, error) {
	return decodeSQLRowBinaryAdaptiveInto(dst, columns, encoded, nil)
}

// DecodeInto decodes an HSA1 stream while retaining delta scratch between
// calls. Pass the returned slice as dst[:0] on the next call for zero warm-call
// output allocations. One decoder must not be used concurrently.
func (decoder *SQLRowBinaryAdaptiveDecoder) DecodeInto(dst []SQLRow, columns []SQLRowBinaryColumn, encoded []byte) ([]SQLRow, error) {
	if decoder == nil {
		return DecodeSQLRowBinaryAdaptiveInto(dst, columns, encoded)
	}
	return decodeSQLRowBinaryAdaptiveInto(dst, columns, encoded, &decoder.deltaScratch)
}

func decodeSQLRowBinaryAdaptiveInto(dst []SQLRow, columns []SQLRowBinaryColumn, encoded []byte, scratch *sqlRowBinaryDeltaScratch) ([]SQLRow, error) {
	if err := validateSQLRowBinaryColumns(columns); err != nil {
		return nil, err
	}
	if len(encoded) == 0 {
		return dst[:0], nil
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
		return decodeSQLRowBinaryInto(dst, columns, payload)
	case SQLRowBinaryAdaptiveCodecDelta, SQLRowBinaryAdaptiveCodecDoubleDelta:
		return decodeSQLRowBinaryDeltaInto(dst, columns, payload, codec == SQLRowBinaryAdaptiveCodecDoubleDelta, scratch)
	default:
		return nil, fmt.Errorf("RowBinary adaptive codec %d is unsupported", codec)
	}
}

func decodeSQLRowBinaryInto(dst []SQLRow, columns []SQLRowBinaryColumn, encoded []byte) ([]SQLRow, error) {
	if len(encoded) == 0 {
		return dst[:0], nil
	}
	rows := dst[:0]
	offset := 0
	for offset < len(encoded) {
		if len(rows) >= maxSQLRowBinaryRows {
			return nil, fmt.Errorf("RowBinary row count exceeds limit %d", maxSQLRowBinaryRows)
		}
		rows = appendSQLRowBinaryDecodeRow(rows, columns)
		rowIndex := len(rows) - 1
		for _, column := range columns {
			if column.Nullable {
				if offset >= len(encoded) {
					return nil, fmt.Errorf("RowBinary row %d column %q is missing its NULL marker", rowIndex, column.Name)
				}
				marker := encoded[offset]
				offset++
				switch marker {
				case 0:
				case 1:
					rows[rowIndex][column.Name] = nil
					continue
				default:
					return nil, fmt.Errorf("RowBinary row %d column %q has invalid NULL marker %d", rowIndex, column.Name, marker)
				}
			}
			value, next, err := decodeSQLRowBinaryValueInto(column.Type, encoded, offset, rowIndex, column.Name, rows[rowIndex][column.Name])
			if err != nil {
				return nil, err
			}
			rows[rowIndex][column.Name] = value
			offset = next
		}
	}
	return rows, nil
}

func decodeSQLRowBinaryDeltaInto(dst []SQLRow, columns []SQLRowBinaryColumn, encoded []byte, doubleDelta bool, scratch *sqlRowBinaryDeltaScratch) ([]SQLRow, error) {
	if len(encoded) == 0 {
		return dst[:0], nil
	}
	if len(encoded) < len(sqlRowBinaryDeltaMagic) {
		return nil, fmt.Errorf("RowBinary delta header is truncated")
	}
	switch {
	case bytes.Equal(encoded[:len(sqlRowBinaryDeltaMagic)], sqlRowBinaryDeltaMagic[:]):
	case bytes.Equal(encoded[:len(sqlRowBinaryDoubleDeltaMagic)], sqlRowBinaryDoubleDeltaMagic[:]):
		doubleDelta = true
	default:
		return nil, fmt.Errorf("RowBinary delta has an invalid format marker")
	}
	offset := len(sqlRowBinaryDeltaMagic)
	rowCount, err := readSQLRowBinaryDeltaUvarint(encoded, &offset, "row count")
	if err != nil {
		return nil, err
	}
	if rowCount > maxSQLRowBinaryRows {
		return nil, fmt.Errorf("RowBinary delta row count %d exceeds limit %d", rowCount, maxSQLRowBinaryRows)
	}
	rows := resizeSQLRowBinaryDecodeRows(dst, int(rowCount), columns)
	if scratch == nil {
		scratch = &sqlRowBinaryDeltaScratch{}
	}
	scratch.reset(len(columns))
	previous := scratch.previous
	previousDelta := scratch.previousDelta
	seen := scratch.seen
	for rowIndex := range rows {
		for columnIndex, column := range columns {
			if column.Nullable {
				marker, markerErr := readSQLRowBinaryDeltaByte(encoded, &offset, rowIndex, column.Name, "NULL marker")
				if markerErr != nil {
					return nil, markerErr
				}
				switch marker {
				case 0:
				case 1:
					rows[rowIndex][column.Name] = nil
					continue
				default:
					return nil, fmt.Errorf("RowBinary delta row %d column %q has invalid NULL marker %d", rowIndex, column.Name, marker)
				}
			}
			if sqlRowBinaryDeltaType(column.Type) {
				encodedDelta, deltaErr := readSQLRowBinaryDeltaUvarint(encoded, &offset, "value delta")
				if deltaErr != nil {
					return nil, fmt.Errorf("RowBinary delta row %d column %q: %w", rowIndex, column.Name, deltaErr)
				}
				valueDelta := sqlRowBinaryDeltaUnZigZag(encodedDelta)
				if doubleDelta && seen[columnIndex] {
					valueDelta += previousDelta[columnIndex]
				}
				current := previous[columnIndex] + valueDelta
				value, valueErr := sqlRowBinaryDeltaDecodedValue(column.Type, current, rowIndex, column.Name)
				if valueErr != nil {
					return nil, valueErr
				}
				rows[rowIndex][column.Name] = value
				previousDelta[columnIndex] = valueDelta
				previous[columnIndex] = current
				seen[columnIndex] = true
				continue
			}
			value, next, valueErr := decodeSQLRowBinaryDeltaValueInto(column.Type, encoded, offset, rowIndex, column.Name, rows[rowIndex][column.Name])
			if valueErr != nil {
				return nil, valueErr
			}
			rows[rowIndex][column.Name] = value
			offset = next
		}
	}
	if offset != len(encoded) {
		return nil, fmt.Errorf("RowBinary delta has %d trailing bytes", len(encoded)-offset)
	}
	return rows, nil
}

func decodeSQLRowBinaryValueInto(kind SQLRowBinaryType, encoded []byte, offset, row int, column string, previous interface{}) (interface{}, int, error) {
	if kind != SQLRowBinaryString && kind != SQLRowBinaryBytes && kind != SQLRowBinaryJSON {
		return decodeSQLRowBinaryValue(kind, encoded, offset, row, column)
	}
	value, next, err := decodeSQLRowBinaryBytes(encoded, offset, row, column)
	if err != nil {
		return nil, offset, err
	}
	switch kind {
	case SQLRowBinaryString:
		if old, ok := previous.(string); ok && sqlRowBinaryStringMatchesBytes(old, value) {
			return old, next, nil
		}
		return string(value), next, nil
	case SQLRowBinaryJSON:
		if old, ok := previous.(json.RawMessage); ok && bytes.Equal(old, value) {
			return old, next, nil
		}
		return json.RawMessage(value), next, nil
	default:
		if old, ok := previous.([]byte); ok && cap(old) >= len(value) {
			old = old[:len(value)]
			copy(old, value)
			return old, next, nil
		}
		return append([]byte(nil), value...), next, nil
	}
}

func decodeSQLRowBinaryDeltaValueInto(kind SQLRowBinaryType, encoded []byte, offset, row int, column string, previous interface{}) (interface{}, int, error) {
	if kind != SQLRowBinaryString && kind != SQLRowBinaryBytes && kind != SQLRowBinaryJSON {
		return decodeSQLRowBinaryDeltaValue(kind, encoded, offset, row, column)
	}
	value, next, err := decodeSQLRowBinaryDeltaBytes(encoded, offset, row, column)
	if err != nil {
		return nil, offset, err
	}
	switch kind {
	case SQLRowBinaryString:
		if old, ok := previous.(string); ok && sqlRowBinaryStringMatchesBytes(old, value) {
			return old, next, nil
		}
		return string(value), next, nil
	case SQLRowBinaryJSON:
		if old, ok := previous.(json.RawMessage); ok && bytes.Equal(old, value) {
			return old, next, nil
		}
		return json.RawMessage(value), next, nil
	default:
		if old, ok := previous.([]byte); ok && cap(old) >= len(value) {
			old = old[:len(value)]
			copy(old, value)
			return old, next, nil
		}
		copyValue := make([]byte, len(value))
		copy(copyValue, value)
		return copyValue, next, nil
	}
}

func sqlRowBinaryStringMatchesBytes(value string, encoded []byte) bool {
	if len(value) != len(encoded) {
		return false
	}
	for index, byteValue := range encoded {
		if value[index] != byteValue {
			return false
		}
	}
	return true
}

func appendSQLRowBinaryDecodeRow(rows []SQLRow, columns []SQLRowBinaryColumn) []SQLRow {
	if len(rows) < cap(rows) {
		rows = rows[:len(rows)+1]
	} else {
		rows = append(rows, nil)
	}
	rowIndex := len(rows) - 1
	row := rows[rowIndex]
	if row == nil {
		rows[rowIndex] = make(SQLRow, len(columns))
		return rows
	}
	if len(row) <= len(columns) {
		return rows
	}
	for key := range row {
		if !sqlRowBinaryDecodeColumnExists(columns, key) {
			delete(row, key)
		}
	}
	return rows
}

func resizeSQLRowBinaryDecodeRows(dst []SQLRow, rowCount int, columns []SQLRowBinaryColumn) []SQLRow {
	if cap(dst) < rowCount {
		dst = make([]SQLRow, rowCount)
	} else {
		dst = dst[:rowCount]
	}
	for rowIndex := range dst {
		row := dst[rowIndex]
		if row == nil {
			dst[rowIndex] = make(SQLRow, len(columns))
			continue
		}
		if len(row) <= len(columns) {
			continue
		}
		for key := range row {
			if !sqlRowBinaryDecodeColumnExists(columns, key) {
				delete(row, key)
			}
		}
	}
	return dst
}

func sqlRowBinaryDecodeColumnExists(columns []SQLRowBinaryColumn, name string) bool {
	for _, column := range columns {
		if column.Name == name {
			return true
		}
	}
	return false
}
