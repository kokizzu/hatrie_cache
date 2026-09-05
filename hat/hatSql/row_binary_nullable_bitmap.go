package hatSql

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"time"
)

var sqlRowBinaryNullableBitmapMagic = [4]byte{'H', 'S', 'B', '1'}

// EncodeSQLRowBinaryBitmap encodes RowBinary rows with one bitmap per row for
// nullable columns. The schema remains out of band; non-null values retain
// the existing RowBinary representation.
func EncodeSQLRowBinaryBitmap(columns []SQLRowBinaryColumn, rows []SQLRow) ([]byte, error) {
	if err := validateSQLRowBinaryColumns(columns); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	if len(rows) > maxSQLRowBinaryRows {
		return nil, fmt.Errorf("RowBinary bitmap row count %d exceeds limit %d", len(rows), maxSQLRowBinaryRows)
	}
	nullableBits, bitmapBytes, _ := sqlRowBinaryNullableBitmapLayout(columns)
	encoded := make([]byte, 0, len(sqlRowBinaryNullableBitmapMagic)+len(rows)*(bitmapBytes+len(columns)))
	encoded = append(encoded, sqlRowBinaryNullableBitmapMagic[:]...)
	for rowIndex, row := range rows {
		bitmapStart := len(encoded)
		encoded = append(encoded, make([]byte, bitmapBytes)...)
		for columnIndex, column := range columns {
			value := interface{}(nil)
			if row != nil {
				value = row[column.Name]
			}
			if value == nil {
				if !column.Nullable {
					return nil, fmt.Errorf("RowBinary bitmap row %d column %q is NULL but not nullable", rowIndex, column.Name)
				}
				bit := nullableBits[columnIndex]
				encoded[bitmapStart+bit/8] |= byte(1 << uint(bit%8))
				continue
			}
			var err error
			encoded, err = appendSQLRowBinaryValue(encoded, column.Type, value, rowIndex, column.Name)
			if err != nil {
				return nil, err
			}
		}
	}
	return encoded, nil
}

// DecodeSQLRowBinaryBitmap decodes the nullable-bitmap RowBinary format and
// rejects invalid markers, unused bitmap bits, truncated values, and trailing
// malformed rows.
func DecodeSQLRowBinaryBitmap(columns []SQLRowBinaryColumn, encoded []byte) ([]SQLRow, error) {
	if err := validateSQLRowBinaryColumns(columns); err != nil {
		return nil, err
	}
	if len(encoded) == 0 {
		return nil, nil
	}
	if len(encoded) < len(sqlRowBinaryNullableBitmapMagic) || string(encoded[:len(sqlRowBinaryNullableBitmapMagic)]) != string(sqlRowBinaryNullableBitmapMagic[:]) {
		return nil, fmt.Errorf("RowBinary bitmap format marker is invalid or truncated")
	}
	nullableBits, bitmapBytes, nullableCount := sqlRowBinaryNullableBitmapLayout(columns)
	offset := len(sqlRowBinaryNullableBitmapMagic)
	if offset == len(encoded) {
		return nil, nil
	}
	rows := make([]SQLRow, 0)
	for offset < len(encoded) {
		if len(rows) >= maxSQLRowBinaryRows {
			return nil, fmt.Errorf("RowBinary bitmap row count exceeds limit %d", maxSQLRowBinaryRows)
		}
		if len(encoded)-offset < bitmapBytes {
			return nil, fmt.Errorf("RowBinary bitmap row %d is missing its NULL bitmap", len(rows))
		}
		bitmap := encoded[offset : offset+bitmapBytes]
		offset += bitmapBytes
		if err := validateSQLRowBinaryNullableBitmap(bitmap, nullableCount, len(rows)); err != nil {
			return nil, err
		}
		row := make(SQLRow, len(columns))
		for columnIndex, column := range columns {
			if bit := nullableBits[columnIndex]; bit >= 0 && bitmap[bit/8]&(byte(1)<<uint(bit%8)) != 0 {
				row[column.Name] = nil
				continue
			}
			value, next, err := decodeSQLRowBinaryBitmapValue(column.Type, encoded, offset, len(rows), column.Name)
			if err != nil {
				return nil, err
			}
			row[column.Name] = value
			offset = next
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func sqlRowBinaryNullableBitmapLayout(columns []SQLRowBinaryColumn) ([]int, int, int) {
	bits := make([]int, len(columns))
	nullableCount := 0
	for index, column := range columns {
		bits[index] = -1
		if column.Nullable {
			bits[index] = nullableCount
			nullableCount++
		}
	}
	return bits, (nullableCount + 7) / 8, nullableCount
}

func validateSQLRowBinaryNullableBitmap(bitmap []byte, nullableCount, row int) error {
	if nullableCount == 0 || nullableCount%8 == 0 {
		return nil
	}
	unusedMask := byte(^(uint8(1<<(uint(nullableCount%8))) - 1))
	if bitmap[len(bitmap)-1]&unusedMask != 0 {
		return fmt.Errorf("RowBinary bitmap row %d has nonzero unused NULL bitmap bits", row)
	}
	return nil
}

func decodeSQLRowBinaryBitmapValue(kind SQLRowBinaryType, encoded []byte, offset, row int, column string) (interface{}, int, error) {
	switch kind {
	case SQLRowBinaryInt64:
		value, next, err := readSQLRowBinaryBitmapFixed(encoded, offset, 8, row, column)
		if err != nil {
			return nil, offset, err
		}
		return int64(binary.LittleEndian.Uint64(value)), next, nil
	case SQLRowBinaryUint64:
		value, next, err := readSQLRowBinaryBitmapFixed(encoded, offset, 8, row, column)
		if err != nil {
			return nil, offset, err
		}
		return binary.LittleEndian.Uint64(value), next, nil
	case SQLRowBinaryFloat64:
		value, next, err := readSQLRowBinaryBitmapFixed(encoded, offset, 8, row, column)
		if err != nil {
			return nil, offset, err
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(value)), next, nil
	case SQLRowBinaryBool:
		value, next, err := readSQLRowBinaryBitmapFixed(encoded, offset, 1, row, column)
		if err != nil {
			return nil, offset, err
		}
		switch value[0] {
		case 0:
			return false, next, nil
		case 1:
			return true, next, nil
		default:
			return nil, offset, fmt.Errorf("RowBinary bitmap row %d column %q has invalid bool value %d", row, column, value[0])
		}
	case SQLRowBinaryString, SQLRowBinaryBytes, SQLRowBinaryJSON:
		value, next, err := decodeSQLRowBinaryBitmapBytes(encoded, offset, row, column)
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
	case SQLRowBinaryDate:
		value, next, err := readSQLRowBinaryBitmapFixed(encoded, offset, 4, row, column)
		if err != nil {
			return nil, offset, err
		}
		days := int64(int32(binary.LittleEndian.Uint32(value)))
		return time.Unix(days*24*60*60, 0).UTC(), next, nil
	case SQLRowBinaryDateTime:
		value, next, err := readSQLRowBinaryBitmapFixed(encoded, offset, 8, row, column)
		if err != nil {
			return nil, offset, err
		}
		return time.Unix(0, int64(binary.LittleEndian.Uint64(value))).UTC(), next, nil
	case SQLRowBinaryDuration:
		value, next, err := readSQLRowBinaryBitmapFixed(encoded, offset, 8, row, column)
		if err != nil {
			return nil, offset, err
		}
		return time.Duration(int64(binary.LittleEndian.Uint64(value))), next, nil
	case SQLRowBinaryUUID:
		value, next, err := readSQLRowBinaryBitmapFixed(encoded, offset, 16, row, column)
		if err != nil {
			return nil, offset, err
		}
		var uuid [16]byte
		copy(uuid[:], value)
		return uuid, next, nil
	default:
		return nil, offset, fmt.Errorf("RowBinary bitmap column %q has unsupported type %d", column, kind)
	}
}

func readSQLRowBinaryBitmapFixed(encoded []byte, offset, size, row int, column string) ([]byte, int, error) {
	if size < 0 || offset < 0 || offset > len(encoded) || size > len(encoded)-offset {
		return nil, offset, fmt.Errorf("RowBinary bitmap row %d column %q is truncated", row, column)
	}
	return encoded[offset : offset+size], offset + size, nil
}

func decodeSQLRowBinaryBitmapBytes(encoded []byte, offset, row int, column string) ([]byte, int, error) {
	if offset < 0 || offset >= len(encoded) {
		return nil, offset, fmt.Errorf("RowBinary bitmap row %d column %q has truncated length prefix", row, column)
	}
	length, size := binary.Uvarint(encoded[offset:])
	if size <= 0 {
		return nil, offset, fmt.Errorf("RowBinary bitmap row %d column %q has invalid length prefix", row, column)
	}
	offset += size
	if length > uint64(len(encoded)-offset) {
		return nil, offset, fmt.Errorf("RowBinary bitmap row %d column %q length %d exceeds remaining payload", row, column, length)
	}
	end := offset + int(length)
	return encoded[offset:end], end, nil
}
