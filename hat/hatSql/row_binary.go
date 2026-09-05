package hatSql

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"time"
)

// SQLRowBinaryType identifies the physical representation of one schema
// column in a RowBinary stream.
type SQLRowBinaryType uint8

const (
	SQLRowBinaryInt64 SQLRowBinaryType = iota + 1
	SQLRowBinaryUint64
	SQLRowBinaryFloat64
	SQLRowBinaryBool
	SQLRowBinaryString
	SQLRowBinaryBytes
	SQLRowBinaryDate
	SQLRowBinaryDateTime
	SQLRowBinaryDuration
	SQLRowBinaryUUID
	SQLRowBinaryJSON
)

// SQLRowBinaryColumn describes one schema-ordered RowBinary field. The
// payload does not include column names or types; both sides must use the
// same ordered schema.
type SQLRowBinaryColumn struct {
	Name     string
	Type     SQLRowBinaryType
	Nullable bool
}

const maxSQLRowBinaryRows = 1_000_000

// EncodeSQLRowBinary encodes rows as a schema-aware RowBinary stream. Fixed
// width values use little-endian bytes and strings/bytes/JSON use an unsigned
// varint length followed by their contents. Empty or missing map fields are
// NULL only when the schema marks the column nullable.
func EncodeSQLRowBinary(columns []SQLRowBinaryColumn, rows []SQLRow) ([]byte, error) {
	if err := validateSQLRowBinaryColumns(columns); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	if len(rows) > maxSQLRowBinaryRows {
		return nil, fmt.Errorf("RowBinary row count %d exceeds limit %d", len(rows), maxSQLRowBinaryRows)
	}
	encoded := make([]byte, 0)
	for rowIndex, row := range rows {
		for _, column := range columns {
			value := interface{}(nil)
			if row != nil {
				value = row[column.Name]
			}
			if value == nil {
				if !column.Nullable {
					return nil, fmt.Errorf("RowBinary row %d column %q is NULL but not nullable", rowIndex, column.Name)
				}
				encoded = append(encoded, 1)
				continue
			}
			if column.Nullable {
				encoded = append(encoded, 0)
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

// DecodeSQLRowBinary decodes a complete schema-aware RowBinary stream. The
// decoder consumes rows until the input is exhausted and rejects partial rows,
// invalid nullable markers, oversized lengths, and trailing malformed data.
func DecodeSQLRowBinary(columns []SQLRowBinaryColumn, encoded []byte) ([]SQLRow, error) {
	if err := validateSQLRowBinaryColumns(columns); err != nil {
		return nil, err
	}
	if len(encoded) == 0 {
		return nil, nil
	}
	rows := make([]SQLRow, 0)
	offset := 0
	for offset < len(encoded) {
		if len(rows) >= maxSQLRowBinaryRows {
			return nil, fmt.Errorf("RowBinary row count exceeds limit %d", maxSQLRowBinaryRows)
		}
		row := make(SQLRow, len(columns))
		for _, column := range columns {
			if column.Nullable {
				if offset >= len(encoded) {
					return nil, fmt.Errorf("RowBinary row %d column %q is missing its NULL marker", len(rows), column.Name)
				}
				marker := encoded[offset]
				offset++
				switch marker {
				case 0:
				case 1:
					row[column.Name] = nil
					continue
				default:
					return nil, fmt.Errorf("RowBinary row %d column %q has invalid NULL marker %d", len(rows), column.Name, marker)
				}
			}
			value, next, err := decodeSQLRowBinaryValue(column.Type, encoded, offset, len(rows), column.Name)
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

func validateSQLRowBinaryColumns(columns []SQLRowBinaryColumn) error {
	if len(columns) == 0 {
		return fmt.Errorf("RowBinary schema must contain at least one column")
	}
	seen := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		if column.Name == "" {
			return fmt.Errorf("RowBinary column name is required")
		}
		if _, ok := seen[column.Name]; ok {
			return fmt.Errorf("RowBinary column %q is duplicated", column.Name)
		}
		seen[column.Name] = struct{}{}
		if !validSQLRowBinaryType(column.Type) {
			return fmt.Errorf("RowBinary column %q has unsupported type %d", column.Name, column.Type)
		}
	}
	return nil
}

func validSQLRowBinaryType(kind SQLRowBinaryType) bool {
	return kind >= SQLRowBinaryInt64 && kind <= SQLRowBinaryJSON
}

func appendSQLRowBinaryValue(destination []byte, kind SQLRowBinaryType, value interface{}, row int, column string) ([]byte, error) {
	switch kind {
	case SQLRowBinaryInt64:
		converted, ok := sqlRowBinaryInt64(value)
		if !ok {
			return nil, fmt.Errorf("RowBinary row %d column %q expects int64, got %T", row, column, value)
		}
		var encoded [8]byte
		binary.LittleEndian.PutUint64(encoded[:], uint64(converted))
		return append(destination, encoded[:]...), nil
	case SQLRowBinaryUint64:
		converted, ok := sqlRowBinaryUint64(value)
		if !ok {
			return nil, fmt.Errorf("RowBinary row %d column %q expects uint64, got %T", row, column, value)
		}
		var encoded [8]byte
		binary.LittleEndian.PutUint64(encoded[:], converted)
		return append(destination, encoded[:]...), nil
	case SQLRowBinaryFloat64:
		converted, ok := sqlRowBinaryFloat64(value)
		if !ok {
			return nil, fmt.Errorf("RowBinary row %d column %q expects float64, got %T", row, column, value)
		}
		var encoded [8]byte
		binary.LittleEndian.PutUint64(encoded[:], math.Float64bits(converted))
		return append(destination, encoded[:]...), nil
	case SQLRowBinaryBool:
		converted, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("RowBinary row %d column %q expects bool, got %T", row, column, value)
		}
		if converted {
			return append(destination, 1), nil
		}
		return append(destination, 0), nil
	case SQLRowBinaryString:
		converted, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("RowBinary row %d column %q expects string, got %T", row, column, value)
		}
		return appendSQLRowBinaryBytes(destination, []byte(converted)), nil
	case SQLRowBinaryBytes:
		converted, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("RowBinary row %d column %q expects []byte, got %T", row, column, value)
		}
		return appendSQLRowBinaryBytes(destination, converted), nil
	case SQLRowBinaryDate:
		converted, ok := value.(time.Time)
		if !ok {
			return nil, fmt.Errorf("RowBinary row %d column %q expects time.Time, got %T", row, column, value)
		}
		midnight := time.Date(converted.UTC().Year(), converted.UTC().Month(), converted.UTC().Day(), 0, 0, 0, 0, time.UTC)
		days := midnight.Unix() / (24 * 60 * 60)
		if days < math.MinInt32 || days > math.MaxInt32 {
			return nil, fmt.Errorf("RowBinary row %d column %q date is out of range", row, column)
		}
		var encoded [4]byte
		binary.LittleEndian.PutUint32(encoded[:], uint32(int32(days)))
		return append(destination, encoded[:]...), nil
	case SQLRowBinaryDateTime:
		converted, ok := value.(time.Time)
		if !ok {
			return nil, fmt.Errorf("RowBinary row %d column %q expects time.Time, got %T", row, column, value)
		}
		var encoded [8]byte
		binary.LittleEndian.PutUint64(encoded[:], uint64(converted.UnixNano()))
		return append(destination, encoded[:]...), nil
	case SQLRowBinaryDuration:
		converted, ok := sqlRowBinaryDuration(value)
		if !ok {
			return nil, fmt.Errorf("RowBinary row %d column %q expects time.Duration, got %T", row, column, value)
		}
		var encoded [8]byte
		binary.LittleEndian.PutUint64(encoded[:], uint64(converted))
		return append(destination, encoded[:]...), nil
	case SQLRowBinaryUUID:
		converted, ok := value.([16]byte)
		if !ok {
			return nil, fmt.Errorf("RowBinary row %d column %q expects [16]byte, got %T", row, column, value)
		}
		return append(destination, converted[:]...), nil
	case SQLRowBinaryJSON:
		converted, ok := value.(json.RawMessage)
		if !ok {
			return nil, fmt.Errorf("RowBinary row %d column %q expects json.RawMessage, got %T", row, column, value)
		}
		return appendSQLRowBinaryBytes(destination, converted), nil
	default:
		return nil, fmt.Errorf("RowBinary column %q has unsupported type %d", column, kind)
	}
}

func appendSQLRowBinaryBytes(destination, value []byte) []byte {
	var length [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(length[:], uint64(len(value)))
	destination = append(destination, length[:n]...)
	return append(destination, value...)
}

func decodeSQLRowBinaryValue(kind SQLRowBinaryType, encoded []byte, offset, row int, column string) (interface{}, int, error) {
	readFixed := func(size int) ([]byte, int, error) {
		if size < 0 || len(encoded)-offset < size {
			return nil, offset, fmt.Errorf("RowBinary row %d column %q is truncated", row, column)
		}
		return encoded[offset : offset+size], offset + size, nil
	}
	switch kind {
	case SQLRowBinaryInt64:
		value, next, err := readFixed(8)
		if err != nil {
			return nil, offset, err
		}
		return int64(binary.LittleEndian.Uint64(value)), next, nil
	case SQLRowBinaryUint64:
		value, next, err := readFixed(8)
		if err != nil {
			return nil, offset, err
		}
		return binary.LittleEndian.Uint64(value), next, nil
	case SQLRowBinaryFloat64:
		value, next, err := readFixed(8)
		if err != nil {
			return nil, offset, err
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(value)), next, nil
	case SQLRowBinaryBool:
		value, next, err := readFixed(1)
		if err != nil {
			return nil, offset, err
		}
		switch value[0] {
		case 0:
			return false, next, nil
		case 1:
			return true, next, nil
		default:
			return nil, offset, fmt.Errorf("RowBinary row %d column %q has invalid bool value %d", row, column, value[0])
		}
	case SQLRowBinaryString, SQLRowBinaryBytes, SQLRowBinaryJSON:
		value, next, err := decodeSQLRowBinaryBytes(encoded, offset, row, column)
		if err != nil {
			return nil, offset, err
		}
		switch kind {
		case SQLRowBinaryString:
			return string(value), next, nil
		case SQLRowBinaryJSON:
			return json.RawMessage(value), next, nil
		default:
			return append([]byte(nil), value...), next, nil
		}
	case SQLRowBinaryDate:
		value, next, err := readFixed(4)
		if err != nil {
			return nil, offset, err
		}
		days := int64(int32(binary.LittleEndian.Uint32(value)))
		return time.Unix(days*24*60*60, 0).UTC(), next, nil
	case SQLRowBinaryDateTime:
		value, next, err := readFixed(8)
		if err != nil {
			return nil, offset, err
		}
		return time.Unix(0, int64(binary.LittleEndian.Uint64(value))).UTC(), next, nil
	case SQLRowBinaryDuration:
		value, next, err := readFixed(8)
		if err != nil {
			return nil, offset, err
		}
		return time.Duration(int64(binary.LittleEndian.Uint64(value))), next, nil
	case SQLRowBinaryUUID:
		value, next, err := readFixed(16)
		if err != nil {
			return nil, offset, err
		}
		var uuid [16]byte
		copy(uuid[:], value)
		return uuid, next, nil
	default:
		return nil, offset, fmt.Errorf("RowBinary column %q has unsupported type %d", column, kind)
	}
}

func decodeSQLRowBinaryBytes(encoded []byte, offset, row int, column string) ([]byte, int, error) {
	length, size := binary.Uvarint(encoded[offset:])
	if size <= 0 {
		return nil, offset, fmt.Errorf("RowBinary row %d column %q has invalid length prefix", row, column)
	}
	offset += size
	if length > uint64(len(encoded)-offset) {
		return nil, offset, fmt.Errorf("RowBinary row %d column %q length %d exceeds remaining payload", row, column, length)
	}
	end := offset + int(length)
	return encoded[offset:end], end, nil
}

func sqlRowBinaryInt64(value interface{}) (int64, bool) {
	switch value := value.(type) {
	case int:
		return int64(value), true
	case int8:
		return int64(value), true
	case int16:
		return int64(value), true
	case int32:
		return int64(value), true
	case int64:
		return value, true
	case uint:
		if uint64(value) > math.MaxInt64 {
			return 0, false
		}
		return int64(value), true
	case uint8:
		return int64(value), true
	case uint16:
		return int64(value), true
	case uint32:
		return int64(value), true
	case uint64:
		if value > math.MaxInt64 {
			return 0, false
		}
		return int64(value), true
	default:
		return 0, false
	}
}

func sqlRowBinaryUint64(value interface{}) (uint64, bool) {
	switch value := value.(type) {
	case uint:
		return uint64(value), true
	case uint8:
		return uint64(value), true
	case uint16:
		return uint64(value), true
	case uint32:
		return uint64(value), true
	case uint64:
		return value, true
	case int:
		if value < 0 {
			return 0, false
		}
		return uint64(value), true
	case int8:
		if value < 0 {
			return 0, false
		}
		return uint64(value), true
	case int16:
		if value < 0 {
			return 0, false
		}
		return uint64(value), true
	case int32:
		if value < 0 {
			return 0, false
		}
		return uint64(value), true
	case int64:
		if value < 0 {
			return 0, false
		}
		return uint64(value), true
	default:
		return 0, false
	}
}

func sqlRowBinaryFloat64(value interface{}) (float64, bool) {
	switch value := value.(type) {
	case float32:
		return float64(value), true
	case float64:
		return value, true
	default:
		return 0, false
	}
}

func sqlRowBinaryDuration(value interface{}) (time.Duration, bool) {
	switch value := value.(type) {
	case time.Duration:
		return value, true
	case int64:
		return time.Duration(value), true
	default:
		return 0, false
	}
}
