package hatSql

import (
	"encoding/binary"
	"fmt"
	"math"
)

// SQLRowBinaryColumnReadStats describes the bytes consumed by one schema
// column while analyzing a RowBinary stream. Bytes include nullable markers.
type SQLRowBinaryColumnReadStats struct {
	Name   string
	Bytes  int
	Values int
	Nulls  int
}

// SQLRowBinaryReadStats reports per-column read amplification without
// materializing row maps. Bytes is the complete encoded payload size and the
// sum of column Bytes equals it for a valid stream.
type SQLRowBinaryReadStats struct {
	Rows    int
	Bytes   int
	Columns []SQLRowBinaryColumnReadStats
}

// AnalyzeSQLRowBinaryRead validates a complete RowBinary stream and reports
// how many bytes, values, and NULLs each schema column consumed. It performs
// no row or variable-length value allocations, making it suitable for
// diagnostics around a normal decode path.
func AnalyzeSQLRowBinaryRead(columns []SQLRowBinaryColumn, encoded []byte) (SQLRowBinaryReadStats, error) {
	if err := validateSQLRowBinaryColumns(columns); err != nil {
		return SQLRowBinaryReadStats{}, err
	}
	stats := SQLRowBinaryReadStats{Bytes: len(encoded), Columns: make([]SQLRowBinaryColumnReadStats, len(columns))}
	for index, column := range columns {
		stats.Columns[index].Name = column.Name
	}
	if len(encoded) == 0 {
		return stats, nil
	}
	offset := 0
	for offset < len(encoded) {
		if stats.Rows >= maxSQLRowBinaryRows {
			return SQLRowBinaryReadStats{}, fmt.Errorf("RowBinary read row count exceeds limit %d", maxSQLRowBinaryRows)
		}
		for index, column := range columns {
			start := offset
			if column.Nullable {
				if offset >= len(encoded) {
					return SQLRowBinaryReadStats{}, fmt.Errorf("RowBinary read row %d column %q is missing its NULL marker", stats.Rows, column.Name)
				}
				marker := encoded[offset]
				offset++
				switch marker {
				case 0:
				case 1:
					stats.Columns[index].Nulls++
					stats.Columns[index].Bytes += offset - start
					continue
				default:
					return SQLRowBinaryReadStats{}, fmt.Errorf("RowBinary read row %d column %q has invalid NULL marker %d", stats.Rows, column.Name, marker)
				}
			}
			stats.Columns[index].Values++
			next, err := skipSQLRowBinaryValue(column.Type, encoded, offset, stats.Rows, column.Name)
			if err != nil {
				return SQLRowBinaryReadStats{}, err
			}
			if err := validateSQLRowBinaryEncodedFixedValue(column, encoded, offset, stats.Rows); err != nil {
				return SQLRowBinaryReadStats{}, err
			}
			offset = next
			stats.Columns[index].Bytes += offset - start
		}
		stats.Rows++
	}
	return stats, nil
}

func validateSQLRowBinaryEncodedFixedValue(column SQLRowBinaryColumn, encoded []byte, offset, row int) error {
	var size int
	switch column.Type {
	case SQLRowBinaryEnum8:
		size = 1
	case SQLRowBinaryEnum16:
		size = 2
	case SQLRowBinaryDecimal128:
		size = 16
	case SQLRowBinaryDecimal256:
		size = 32
	default:
		return nil
	}
	if offset < 0 || size > len(encoded)-offset {
		return fmt.Errorf("RowBinary read row %d column %q is truncated", row, column.Name)
	}
	if column.Type == SQLRowBinaryEnum8 || column.Type == SQLRowBinaryEnum16 {
		var code uint64
		if size == 1 {
			code = uint64(encoded[offset])
		} else {
			code = uint64(binary.LittleEndian.Uint16(encoded[offset : offset+size]))
		}
		if code >= uint64(len(column.EnumValues)) {
			return fmt.Errorf("RowBinary read row %d column %q enum code %d exceeds %d labels", row, column.Name, code, len(column.EnumValues))
		}
	} else if column.Type == SQLRowBinaryDecimal128 || column.Type == SQLRowBinaryDecimal256 {
		value, _, err := decodeSQLRowBinaryValue(column.Type, encoded, offset, row, column.Name)
		if err != nil {
			return err
		}
		if err := validateSQLRowBinaryDecodedValue(column, value, row); err != nil {
			return err
		}
	}
	return nil
}

func skipSQLRowBinaryValue(kind SQLRowBinaryType, encoded []byte, offset, row int, column string) (int, error) {
	switch kind {
	case SQLRowBinaryInt64, SQLRowBinaryUint64, SQLRowBinaryFloat64, SQLRowBinaryDateTime, SQLRowBinaryDuration:
		return skipSQLRowBinaryFixed(encoded, offset, 8, row, column)
	case SQLRowBinaryEnum8:
		return skipSQLRowBinaryFixed(encoded, offset, 1, row, column)
	case SQLRowBinaryEnum16:
		return skipSQLRowBinaryFixed(encoded, offset, 2, row, column)
	case SQLRowBinaryBool:
		if offset < 0 || offset >= len(encoded) {
			return offset, fmt.Errorf("RowBinary read row %d column %q is truncated", row, column)
		}
		if encoded[offset] > 1 {
			return offset, fmt.Errorf("RowBinary read row %d column %q has invalid bool value %d", row, column, encoded[offset])
		}
		return offset + 1, nil
	case SQLRowBinaryString, SQLRowBinaryBytes, SQLRowBinaryJSON:
		return skipSQLRowBinaryBytes(encoded, offset, row, column)
	case SQLRowBinaryDate:
		value, next, err := readSQLRowBinaryFixed(encoded, offset, 4, row, column)
		if err != nil {
			return offset, err
		}
		days := int64(int32(binary.LittleEndian.Uint32(value)))
		if days < math.MinInt32 || days > math.MaxInt32 {
			return offset, fmt.Errorf("RowBinary read row %d column %q date is out of range", row, column)
		}
		return next, nil
	case SQLRowBinaryUUID:
		return skipSQLRowBinaryFixed(encoded, offset, 16, row, column)
	case SQLRowBinaryIPv4:
		return skipSQLRowBinaryFixed(encoded, offset, 4, row, column)
	case SQLRowBinaryIPv6:
		return skipSQLRowBinaryFixed(encoded, offset, 16, row, column)
	case SQLRowBinaryDecimal128:
		return skipSQLRowBinaryFixed(encoded, offset, 16, row, column)
	case SQLRowBinaryDecimal256:
		return skipSQLRowBinaryFixed(encoded, offset, 32, row, column)
	default:
		return offset, fmt.Errorf("RowBinary read column %q has unsupported type %d", column, kind)
	}
}

func skipSQLRowBinaryFixed(encoded []byte, offset, size, row int, column string) (int, error) {
	if size < 0 || offset < 0 || offset > len(encoded) || size > len(encoded)-offset {
		return offset, fmt.Errorf("RowBinary read row %d column %q is truncated", row, column)
	}
	return offset + size, nil
}

func readSQLRowBinaryFixed(encoded []byte, offset, size, row int, column string) ([]byte, int, error) {
	if size < 0 || offset < 0 || offset > len(encoded) || size > len(encoded)-offset {
		return nil, offset, fmt.Errorf("RowBinary read row %d column %q is truncated", row, column)
	}
	return encoded[offset : offset+size], offset + size, nil
}

func skipSQLRowBinaryBytes(encoded []byte, offset, row int, column string) (int, error) {
	if offset < 0 || offset >= len(encoded) {
		return offset, fmt.Errorf("RowBinary read row %d column %q has truncated length prefix", row, column)
	}
	length, size := binary.Uvarint(encoded[offset:])
	if size <= 0 {
		return offset, fmt.Errorf("RowBinary read row %d column %q has invalid length prefix", row, column)
	}
	offset += size
	if length > uint64(len(encoded)-offset) {
		return offset, fmt.Errorf("RowBinary read row %d column %q length %d exceeds remaining payload", row, column, length)
	}
	return offset + int(length), nil
}
