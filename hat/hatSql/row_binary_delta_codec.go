package hatSql

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"time"
)

var (
	sqlRowBinaryDeltaMagic       = [4]byte{'H', 'S', 'D', '1'}
	sqlRowBinaryDoubleDeltaMagic = [4]byte{'H', 'S', 'D', '2'}
)

// EncodeSQLRowBinaryDelta encodes rows using signed delta varints for integer
// and time columns. Other column types retain the RowBinary representation.
// The format is opt-in and requires the same ordered schema when decoding.
func EncodeSQLRowBinaryDelta(columns []SQLRowBinaryColumn, rows []SQLRow) ([]byte, error) {
	return encodeSQLRowBinaryDelta(columns, rows, false)
}

// EncodeSQLRowBinaryDoubleDelta encodes rows using second-order delta varints
// for integer and time columns. It is most effective for regularly advancing
// counters and timestamps; use EncodeSQLRowBinaryDelta for irregular data.
func EncodeSQLRowBinaryDoubleDelta(columns []SQLRowBinaryColumn, rows []SQLRow) ([]byte, error) {
	return encodeSQLRowBinaryDelta(columns, rows, true)
}

func encodeSQLRowBinaryDelta(columns []SQLRowBinaryColumn, rows []SQLRow, doubleDelta bool) ([]byte, error) {
	if err := validateSQLRowBinaryColumns(columns); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	if len(rows) > maxSQLRowBinaryRows {
		return nil, fmt.Errorf("RowBinary delta row count %d exceeds limit %d", len(rows), maxSQLRowBinaryRows)
	}
	magic := sqlRowBinaryDeltaMagic
	if doubleDelta {
		magic = sqlRowBinaryDoubleDeltaMagic
	}
	encoded := make([]byte, 0, len(rows)*len(columns))
	encoded = append(encoded, magic[:]...)
	encoded = appendSQLRowBinaryDeltaUvarint(encoded, uint64(len(rows)))
	previous := make([]uint64, len(columns))
	previousDelta := make([]uint64, len(columns))
	seen := make([]bool, len(columns))
	for rowIndex, row := range rows {
		for columnIndex, column := range columns {
			value := interface{}(nil)
			if row != nil {
				value = row[column.Name]
			}
			if value == nil {
				if !column.Nullable {
					return nil, fmt.Errorf("RowBinary delta row %d column %q is NULL but not nullable", rowIndex, column.Name)
				}
				encoded = append(encoded, 1)
				continue
			}
			if column.Nullable {
				encoded = append(encoded, 0)
			}
			if sqlRowBinaryDeltaType(column.Type) {
				current, err := sqlRowBinaryDeltaValue(column.Type, value, rowIndex, column.Name)
				if err != nil {
					return nil, err
				}
				delta := current - previous[columnIndex]
				encodedDelta := delta
				if doubleDelta {
					encodedDelta -= previousDelta[columnIndex]
				}
				encoded = appendSQLRowBinaryDeltaUvarint(encoded, sqlRowBinaryDeltaZigZag(encodedDelta))
				previousDelta[columnIndex] = delta
				previous[columnIndex] = current
				seen[columnIndex] = true
				continue
			}
			var err error
			encoded, err = appendSQLRowBinaryDeltaValue(encoded, column.Type, value, rowIndex, column.Name)
			if err != nil {
				return nil, err
			}
		}
	}
	return encoded, nil
}

// DecodeSQLRowBinaryDelta decodes both the first-order and second-order
// RowBinary delta formats. It rejects malformed headers, truncated values,
// oversized row counts, and trailing bytes before returning any rows.
func DecodeSQLRowBinaryDelta(columns []SQLRowBinaryColumn, encoded []byte) ([]SQLRow, error) {
	if err := validateSQLRowBinaryColumns(columns); err != nil {
		return nil, err
	}
	if len(encoded) == 0 {
		return nil, nil
	}
	if len(encoded) < len(sqlRowBinaryDeltaMagic) {
		return nil, fmt.Errorf("RowBinary delta header is truncated")
	}
	doubleDelta := false
	switch {
	case string(encoded[:len(sqlRowBinaryDeltaMagic)]) == string(sqlRowBinaryDeltaMagic[:]):
	case string(encoded[:len(sqlRowBinaryDoubleDeltaMagic)]) == string(sqlRowBinaryDoubleDeltaMagic[:]):
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
	rows := make([]SQLRow, int(rowCount))
	previous := make([]uint64, len(columns))
	previousDelta := make([]uint64, len(columns))
	seen := make([]bool, len(columns))
	for rowIndex := range rows {
		row := make(SQLRow, len(columns))
		for columnIndex, column := range columns {
			if column.Nullable {
				marker, markerErr := readSQLRowBinaryDeltaByte(encoded, &offset, rowIndex, column.Name, "NULL marker")
				if markerErr != nil {
					return nil, markerErr
				}
				switch marker {
				case 0:
				case 1:
					row[column.Name] = nil
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
				row[column.Name] = value
				previousDelta[columnIndex] = valueDelta
				previous[columnIndex] = current
				seen[columnIndex] = true
				continue
			}
			value, next, valueErr := decodeSQLRowBinaryDeltaValue(column.Type, encoded, offset, rowIndex, column.Name)
			if valueErr != nil {
				return nil, valueErr
			}
			row[column.Name] = value
			offset = next
		}
		rows[rowIndex] = row
	}
	if offset != len(encoded) {
		return nil, fmt.Errorf("RowBinary delta has %d trailing bytes", len(encoded)-offset)
	}
	return rows, nil
}

func sqlRowBinaryDeltaType(kind SQLRowBinaryType) bool {
	switch kind {
	case SQLRowBinaryInt64, SQLRowBinaryUint64, SQLRowBinaryDate, SQLRowBinaryDateTime, SQLRowBinaryDuration:
		return true
	default:
		return false
	}
}

func sqlRowBinaryDeltaValue(kind SQLRowBinaryType, value interface{}, row int, column string) (uint64, error) {
	switch kind {
	case SQLRowBinaryInt64:
		converted, ok := sqlRowBinaryInt64(value)
		if !ok {
			return 0, fmt.Errorf("RowBinary delta row %d column %q expects int64, got %T", row, column, value)
		}
		return uint64(converted), nil
	case SQLRowBinaryUint64:
		converted, ok := sqlRowBinaryUint64(value)
		if !ok {
			return 0, fmt.Errorf("RowBinary delta row %d column %q expects uint64, got %T", row, column, value)
		}
		return converted, nil
	case SQLRowBinaryDate:
		converted, ok := value.(time.Time)
		if !ok {
			return 0, fmt.Errorf("RowBinary delta row %d column %q expects time.Time, got %T", row, column, value)
		}
		utc := converted.UTC()
		midnight := time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)
		days := midnight.Unix() / (24 * 60 * 60)
		if days < math.MinInt32 || days > math.MaxInt32 {
			return 0, fmt.Errorf("RowBinary delta row %d column %q date is out of range", row, column)
		}
		return uint64(days), nil
	case SQLRowBinaryDateTime:
		converted, ok := value.(time.Time)
		if !ok {
			return 0, fmt.Errorf("RowBinary delta row %d column %q expects time.Time, got %T", row, column, value)
		}
		return uint64(converted.UnixNano()), nil
	case SQLRowBinaryDuration:
		converted, ok := sqlRowBinaryDuration(value)
		if !ok {
			return 0, fmt.Errorf("RowBinary delta row %d column %q expects time.Duration, got %T", row, column, value)
		}
		return uint64(converted), nil
	default:
		return 0, fmt.Errorf("RowBinary delta column %q has unsupported type %d", column, kind)
	}
}

func sqlRowBinaryDeltaDecodedValue(kind SQLRowBinaryType, bits uint64, row int, column string) (interface{}, error) {
	switch kind {
	case SQLRowBinaryInt64:
		return int64(bits), nil
	case SQLRowBinaryUint64:
		return bits, nil
	case SQLRowBinaryDate:
		days := int64(bits)
		if days < math.MinInt32 || days > math.MaxInt32 {
			return nil, fmt.Errorf("RowBinary delta row %d column %q date is out of range", row, column)
		}
		return time.Unix(days*24*60*60, 0).UTC(), nil
	case SQLRowBinaryDateTime:
		return time.Unix(0, int64(bits)).UTC(), nil
	case SQLRowBinaryDuration:
		return time.Duration(int64(bits)), nil
	default:
		return nil, fmt.Errorf("RowBinary delta column %q has unsupported type %d", column, kind)
	}
}

func appendSQLRowBinaryDeltaValue(destination []byte, kind SQLRowBinaryType, value interface{}, row int, column string) ([]byte, error) {
	switch kind {
	case SQLRowBinaryFloat64:
		converted, ok := sqlRowBinaryFloat64(value)
		if !ok {
			return nil, fmt.Errorf("RowBinary delta row %d column %q expects float64, got %T", row, column, value)
		}
		var fixed [8]byte
		binary.LittleEndian.PutUint64(fixed[:], math.Float64bits(converted))
		return append(destination, fixed[:]...), nil
	case SQLRowBinaryBool:
		converted, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("RowBinary delta row %d column %q expects bool, got %T", row, column, value)
		}
		if converted {
			return append(destination, 1), nil
		}
		return append(destination, 0), nil
	case SQLRowBinaryString:
		converted, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("RowBinary delta row %d column %q expects string, got %T", row, column, value)
		}
		return appendSQLRowBinaryBytes(destination, []byte(converted)), nil
	case SQLRowBinaryBytes:
		converted, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("RowBinary delta row %d column %q expects []byte, got %T", row, column, value)
		}
		return appendSQLRowBinaryBytes(destination, converted), nil
	case SQLRowBinaryUUID:
		converted, ok := value.([16]byte)
		if !ok {
			return nil, fmt.Errorf("RowBinary delta row %d column %q expects [16]byte, got %T", row, column, value)
		}
		return append(destination, converted[:]...), nil
	case SQLRowBinaryJSON:
		converted, ok := value.(json.RawMessage)
		if !ok {
			return nil, fmt.Errorf("RowBinary delta row %d column %q expects json.RawMessage, got %T", row, column, value)
		}
		return appendSQLRowBinaryBytes(destination, converted), nil
	default:
		return nil, fmt.Errorf("RowBinary delta column %q has unsupported type %d", column, kind)
	}
}

func decodeSQLRowBinaryDeltaValue(kind SQLRowBinaryType, encoded []byte, offset, row int, column string) (interface{}, int, error) {
	switch kind {
	case SQLRowBinaryFloat64:
		value, next, err := readSQLRowBinaryDeltaFixed(encoded, offset, 8, row, column)
		if err != nil {
			return nil, offset, err
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(value)), next, nil
	case SQLRowBinaryBool:
		value, next, err := readSQLRowBinaryDeltaFixed(encoded, offset, 1, row, column)
		if err != nil {
			return nil, offset, err
		}
		switch value[0] {
		case 0:
			return false, next, nil
		case 1:
			return true, next, nil
		default:
			return nil, offset, fmt.Errorf("RowBinary delta row %d column %q has invalid bool value %d", row, column, value[0])
		}
	case SQLRowBinaryString, SQLRowBinaryBytes, SQLRowBinaryJSON:
		value, next, err := decodeSQLRowBinaryDeltaBytes(encoded, offset, row, column)
		if err != nil {
			return nil, offset, err
		}
		switch kind {
		case SQLRowBinaryString:
			return string(value), next, nil
		case SQLRowBinaryJSON:
			return json.RawMessage(value), next, nil
		default:
			copyValue := make([]byte, len(value))
			copy(copyValue, value)
			return copyValue, next, nil
		}
	case SQLRowBinaryUUID:
		value, next, err := readSQLRowBinaryDeltaFixed(encoded, offset, 16, row, column)
		if err != nil {
			return nil, offset, err
		}
		var uuid [16]byte
		copy(uuid[:], value)
		return uuid, next, nil
	default:
		return nil, offset, fmt.Errorf("RowBinary delta column %q has unsupported type %d", column, kind)
	}
}

func decodeSQLRowBinaryDeltaBytes(encoded []byte, offset, row int, column string) ([]byte, int, error) {
	if offset < 0 || offset >= len(encoded) {
		return nil, offset, fmt.Errorf("RowBinary delta row %d column %q has truncated length prefix", row, column)
	}
	length, size := binary.Uvarint(encoded[offset:])
	if size <= 0 {
		return nil, offset, fmt.Errorf("RowBinary delta row %d column %q has invalid length prefix", row, column)
	}
	offset += size
	if length > uint64(len(encoded)-offset) {
		return nil, offset, fmt.Errorf("RowBinary delta row %d column %q length %d exceeds remaining payload", row, column, length)
	}
	end := offset + int(length)
	return encoded[offset:end], end, nil
}

func readSQLRowBinaryDeltaFixed(encoded []byte, offset, size, row int, column string) ([]byte, int, error) {
	if size < 0 || offset < 0 || offset > len(encoded) || size > len(encoded)-offset {
		return nil, offset, fmt.Errorf("RowBinary delta row %d column %q is truncated", row, column)
	}
	return encoded[offset : offset+size], offset + size, nil
}

func readSQLRowBinaryDeltaByte(encoded []byte, offset *int, row int, column, label string) (byte, error) {
	if offset == nil || *offset < 0 || *offset >= len(encoded) {
		return 0, fmt.Errorf("RowBinary delta row %d column %q is missing its %s", row, column, label)
	}
	value := encoded[*offset]
	*offset++
	return value, nil
}

func appendSQLRowBinaryDeltaUvarint(destination []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(encoded[:], value)
	return append(destination, encoded[:n]...)
}

func readSQLRowBinaryDeltaUvarint(encoded []byte, offset *int, label string) (uint64, error) {
	if offset == nil || *offset < 0 || *offset >= len(encoded) {
		return 0, fmt.Errorf("RowBinary delta %s is truncated", label)
	}
	value, size := binary.Uvarint(encoded[*offset:])
	if size <= 0 {
		return 0, fmt.Errorf("RowBinary delta %s is invalid", label)
	}
	*offset += size
	return value, nil
}

func sqlRowBinaryDeltaZigZag(value uint64) uint64 {
	return (value << 1) ^ uint64(int64(value)>>63)
}

func sqlRowBinaryDeltaUnZigZag(value uint64) uint64 {
	if value&1 == 0 {
		return value >> 1
	}
	return ^(value >> 1)
}
