package hatSql

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"time"
)

const (
	sqlRowBinaryStatsHeader      = "HBS1"
	maxSQLRowBinaryStatsMetadata = 64 << 20
)

// SQLRowBinaryColumnStats describes exact counts and, when the physical type
// is orderable, the observed non-NULL minimum and maximum for one column.
// JSON columns expose counts but intentionally do not expose min/max.
type SQLRowBinaryColumnStats struct {
	Name       string
	NullCount  uint64
	ValueCount uint64
	HasMinMax  bool
	Min        interface{}
	Max        interface{}
}

// BuildSQLRowBinaryColumnStats computes exact per-column metadata for rows.
// NaN values count as present values but are excluded from float min/max.
func BuildSQLRowBinaryColumnStats(columns []SQLRowBinaryColumn, rows []SQLRow) ([]SQLRowBinaryColumnStats, error) {
	if err := validateSQLRowBinaryColumns(columns); err != nil {
		return nil, err
	}
	if len(rows) > maxSQLRowBinaryRows {
		return nil, fmt.Errorf("RowBinary stats row count %d exceeds limit %d", len(rows), maxSQLRowBinaryRows)
	}
	stats := make([]SQLRowBinaryColumnStats, len(columns))
	for columnIndex, column := range columns {
		stats[columnIndex].Name = column.Name
		for rowIndex, row := range rows {
			value := interface{}(nil)
			if row != nil {
				value = row[column.Name]
			}
			if value == nil {
				if !column.Nullable {
					return nil, fmt.Errorf("RowBinary stats row %d column %q is NULL but not nullable", rowIndex, column.Name)
				}
				stats[columnIndex].NullCount++
				continue
			}
			if err := accumulateSQLRowBinaryColumnStats(&stats[columnIndex], column, value, rowIndex); err != nil {
				return nil, err
			}
		}
	}
	return stats, nil
}

// EncodeSQLRowBinaryWithStats writes an opt-in HBS1 envelope containing exact
// column statistics and an ordinary RowBinary payload.
func EncodeSQLRowBinaryWithStats(columns []SQLRowBinaryColumn, rows []SQLRow) ([]byte, error) {
	stats, payload, err := encodeSQLRowBinaryRowsAndStats(columns, rows)
	if err != nil {
		return nil, err
	}
	metadata := make([]byte, 0)
	for index, column := range columns {
		columnStats := stats[index]
		metadata = appendSQLRowBinaryStatsUvarint(metadata, columnStats.NullCount)
		metadata = appendSQLRowBinaryStatsUvarint(metadata, columnStats.ValueCount)
		if columnStats.HasMinMax {
			metadata = append(metadata, 1)
			metadata, err = appendSQLRowBinaryValue(metadata, column.Type, columnStats.Min, -1, column.Name)
			if err != nil {
				return nil, err
			}
			metadata, err = appendSQLRowBinaryValue(metadata, column.Type, columnStats.Max, -1, column.Name)
			if err != nil {
				return nil, err
			}
		} else {
			metadata = append(metadata, 0)
		}
		if len(metadata) > maxSQLRowBinaryStatsMetadata {
			return nil, fmt.Errorf("RowBinary stats metadata exceeds limit %d", maxSQLRowBinaryStatsMetadata)
		}
	}
	encoded := make([]byte, 0, len(sqlRowBinaryStatsHeader)+len(metadata)+len(payload))
	encoded = append(encoded, sqlRowBinaryStatsHeader...)
	encoded = appendSQLRowBinaryStatsUvarint(encoded, uint64(len(rows)))
	encoded = appendSQLRowBinaryStatsUvarint(encoded, uint64(len(metadata)))
	encoded = append(encoded, metadata...)
	encoded = appendSQLRowBinaryStatsUvarint(encoded, uint64(len(payload)))
	encoded = append(encoded, payload...)
	return encoded, nil
}

func encodeSQLRowBinaryRowsAndStats(columns []SQLRowBinaryColumn, rows []SQLRow) ([]SQLRowBinaryColumnStats, []byte, error) {
	if err := validateSQLRowBinaryColumns(columns); err != nil {
		return nil, nil, err
	}
	if len(rows) > maxSQLRowBinaryRows {
		return nil, nil, fmt.Errorf("RowBinary stats row count %d exceeds limit %d", len(rows), maxSQLRowBinaryRows)
	}
	stats := make([]SQLRowBinaryColumnStats, len(columns))
	for index, column := range columns {
		stats[index].Name = column.Name
	}
	payload := make([]byte, 0)
	for rowIndex, row := range rows {
		for columnIndex, column := range columns {
			value := interface{}(nil)
			if row != nil {
				value = row[column.Name]
			}
			if value == nil {
				if !column.Nullable {
					return nil, nil, fmt.Errorf("RowBinary stats row %d column %q is NULL but not nullable", rowIndex, column.Name)
				}
				stats[columnIndex].NullCount++
				payload = append(payload, 1)
				continue
			}
			if column.Nullable {
				payload = append(payload, 0)
			}
			if err := accumulateSQLRowBinaryColumnStats(&stats[columnIndex], column, value, rowIndex); err != nil {
				return nil, nil, err
			}
			var err error
			payload, err = appendSQLRowBinaryValue(payload, column.Type, value, rowIndex, column.Name)
			if err != nil {
				return nil, nil, err
			}
		}
	}
	return stats, payload, nil
}

// DecodeSQLRowBinaryWithStats validates and decodes an HBS1 envelope. It
// recomputes metadata from the decoded rows and rejects stale statistics.
func DecodeSQLRowBinaryWithStats(columns []SQLRowBinaryColumn, encoded []byte) ([]SQLRow, []SQLRowBinaryColumnStats, error) {
	if err := validateSQLRowBinaryColumns(columns); err != nil {
		return nil, nil, err
	}
	if len(encoded) < len(sqlRowBinaryStatsHeader) || string(encoded[:len(sqlRowBinaryStatsHeader)]) != sqlRowBinaryStatsHeader {
		return nil, nil, fmt.Errorf("invalid RowBinary stats header")
	}
	offset := len(sqlRowBinaryStatsHeader)
	rowCount, err := readSQLRowBinaryStatsUvarint(encoded, &offset, "row count")
	if err != nil {
		return nil, nil, err
	}
	if rowCount > maxSQLRowBinaryRows {
		return nil, nil, fmt.Errorf("RowBinary stats row count %d exceeds limit %d", rowCount, maxSQLRowBinaryRows)
	}
	metadataLength, err := readSQLRowBinaryStatsUvarint(encoded, &offset, "metadata length")
	if err != nil {
		return nil, nil, err
	}
	if metadataLength > maxSQLRowBinaryStatsMetadata || metadataLength > uint64(len(encoded)-offset) {
		return nil, nil, fmt.Errorf("RowBinary stats metadata length %d is invalid", metadataLength)
	}
	metadataEnd := offset + int(metadataLength)
	metadata := encoded[offset:metadataEnd]
	metadataOffset := 0
	stats := make([]SQLRowBinaryColumnStats, len(columns))
	for index, column := range columns {
		stats[index].Name = column.Name
		nullCount, err := readSQLRowBinaryStatsUvarint(metadata, &metadataOffset, "NULL count")
		if err != nil {
			return nil, nil, fmt.Errorf("RowBinary stats column %q: %w", column.Name, err)
		}
		valueCount, err := readSQLRowBinaryStatsUvarint(metadata, &metadataOffset, "value count")
		if err != nil {
			return nil, nil, fmt.Errorf("RowBinary stats column %q: %w", column.Name, err)
		}
		if nullCount > rowCount || valueCount > rowCount-nullCount {
			return nil, nil, fmt.Errorf("RowBinary stats column %q counts exceed row count", column.Name)
		}
		if nullCount+valueCount != rowCount {
			return nil, nil, fmt.Errorf("RowBinary stats column %q counts do not equal row count", column.Name)
		}
		if metadataOffset >= len(metadata) {
			return nil, nil, fmt.Errorf("RowBinary stats column %q is missing its min/max marker", column.Name)
		}
		marker := metadata[metadataOffset]
		metadataOffset++
		stats[index].NullCount = nullCount
		stats[index].ValueCount = valueCount
		switch marker {
		case 0:
		case 1:
			if !sqlRowBinaryStatsSupportsMinMax(column.Type) {
				return nil, nil, fmt.Errorf("RowBinary stats column %q does not support min/max", column.Name)
			}
			min, next, err := decodeSQLRowBinaryValue(column.Type, metadata, metadataOffset, -1, column.Name)
			if err != nil {
				return nil, nil, err
			}
			metadataOffset = next
			max, next, err := decodeSQLRowBinaryValue(column.Type, metadata, metadataOffset, -1, column.Name)
			if err != nil {
				return nil, nil, err
			}
			metadataOffset = next
			if column.Type == SQLRowBinaryFloat64 && (math.IsNaN(min.(float64)) || math.IsNaN(max.(float64))) {
				return nil, nil, fmt.Errorf("RowBinary stats column %q contains NaN min/max", column.Name)
			}
			if compareSQLRowBinaryStatsValues(column.Type, min, max) > 0 {
				return nil, nil, fmt.Errorf("RowBinary stats column %q has min greater than max", column.Name)
			}
			stats[index].HasMinMax = true
			stats[index].Min = min
			stats[index].Max = max
		default:
			return nil, nil, fmt.Errorf("RowBinary stats column %q has invalid min/max marker %d", column.Name, marker)
		}
	}
	if metadataOffset != len(metadata) {
		return nil, nil, fmt.Errorf("RowBinary stats metadata has %d trailing bytes", len(metadata)-metadataOffset)
	}
	offset = metadataEnd
	payloadLength, err := readSQLRowBinaryStatsUvarint(encoded, &offset, "payload length")
	if err != nil {
		return nil, nil, err
	}
	if payloadLength > uint64(len(encoded)-offset) {
		return nil, nil, fmt.Errorf("RowBinary stats payload length %d exceeds remaining input", payloadLength)
	}
	payloadEnd := offset + int(payloadLength)
	payload := encoded[offset:payloadEnd]
	if payloadEnd != len(encoded) {
		return nil, nil, fmt.Errorf("RowBinary stats envelope has %d trailing bytes", len(encoded)-payloadEnd)
	}
	rows, err := DecodeSQLRowBinary(columns, payload)
	if err != nil {
		return nil, nil, err
	}
	if uint64(len(rows)) != rowCount {
		return nil, nil, fmt.Errorf("RowBinary stats row count %d does not match payload rows %d", rowCount, len(rows))
	}
	expected, err := BuildSQLRowBinaryColumnStats(columns, rows)
	if err != nil {
		return nil, nil, err
	}
	for index := range stats {
		if !equalSQLRowBinaryColumnStats(columns[index].Type, stats[index], expected[index]) {
			return nil, nil, fmt.Errorf("RowBinary stats column %q does not match payload", columns[index].Name)
		}
	}
	return rows, stats, nil
}

func accumulateSQLRowBinaryColumnStats(stats *SQLRowBinaryColumnStats, column SQLRowBinaryColumn, value interface{}, row int) error {
	stats.ValueCount++
	normalized, orderable, err := normalizeSQLRowBinaryStatsValue(column.Type, value, row, column.Name)
	if err != nil {
		return err
	}
	if !orderable {
		return nil
	}
	if column.Type == SQLRowBinaryFloat64 && math.IsNaN(normalized.(float64)) {
		return nil
	}
	if !stats.HasMinMax {
		stats.HasMinMax = true
		retained := cloneSQLRowBinaryStatsValue(column.Type, normalized)
		stats.Min = retained
		stats.Max = retained
		return nil
	}
	if compareSQLRowBinaryStatsValues(column.Type, normalized, stats.Min) < 0 {
		stats.Min = cloneSQLRowBinaryStatsValue(column.Type, normalized)
	}
	if compareSQLRowBinaryStatsValues(column.Type, normalized, stats.Max) > 0 {
		stats.Max = cloneSQLRowBinaryStatsValue(column.Type, normalized)
	}
	return nil
}

func normalizeSQLRowBinaryStatsValue(kind SQLRowBinaryType, value interface{}, row int, column string) (interface{}, bool, error) {
	switch kind {
	case SQLRowBinaryInt64:
		converted, ok := sqlRowBinaryInt64(value)
		if !ok {
			return nil, false, fmt.Errorf("RowBinary stats row %d column %q expects int64, got %T", row, column, value)
		}
		return converted, true, nil
	case SQLRowBinaryUint64:
		converted, ok := sqlRowBinaryUint64(value)
		if !ok {
			return nil, false, fmt.Errorf("RowBinary stats row %d column %q expects uint64, got %T", row, column, value)
		}
		return converted, true, nil
	case SQLRowBinaryFloat64:
		converted, ok := sqlRowBinaryFloat64(value)
		if !ok {
			return nil, false, fmt.Errorf("RowBinary stats row %d column %q expects float64, got %T", row, column, value)
		}
		return converted, true, nil
	case SQLRowBinaryBool:
		converted, ok := value.(bool)
		if !ok {
			return nil, false, fmt.Errorf("RowBinary stats row %d column %q expects bool, got %T", row, column, value)
		}
		return converted, true, nil
	case SQLRowBinaryString:
		converted, ok := value.(string)
		if !ok {
			return nil, false, fmt.Errorf("RowBinary stats row %d column %q expects string, got %T", row, column, value)
		}
		return converted, true, nil
	case SQLRowBinaryBytes:
		converted, ok := value.([]byte)
		if !ok {
			return nil, false, fmt.Errorf("RowBinary stats row %d column %q expects []byte, got %T", row, column, value)
		}
		return converted, true, nil
	case SQLRowBinaryDate:
		converted, ok := value.(time.Time)
		if !ok {
			return nil, false, fmt.Errorf("RowBinary stats row %d column %q expects time.Time, got %T", row, column, value)
		}
		utc := converted.UTC()
		midnight := time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)
		days := midnight.Unix() / (24 * 60 * 60)
		if days < math.MinInt32 || days > math.MaxInt32 {
			return nil, false, fmt.Errorf("RowBinary stats row %d column %q date is out of range", row, column)
		}
		return time.Unix(days*24*60*60, 0).UTC(), true, nil
	case SQLRowBinaryDateTime:
		converted, ok := value.(time.Time)
		if !ok {
			return nil, false, fmt.Errorf("RowBinary stats row %d column %q expects time.Time, got %T", row, column, value)
		}
		return time.Unix(0, converted.UnixNano()).UTC(), true, nil
	case SQLRowBinaryDuration:
		converted, ok := sqlRowBinaryDuration(value)
		if !ok {
			return nil, false, fmt.Errorf("RowBinary stats row %d column %q expects time.Duration, got %T", row, column, value)
		}
		return converted, true, nil
	case SQLRowBinaryUUID:
		converted, ok := value.([16]byte)
		if !ok {
			return nil, false, fmt.Errorf("RowBinary stats row %d column %q expects [16]byte, got %T", row, column, value)
		}
		return converted, true, nil
	case SQLRowBinaryJSON:
		if _, ok := value.(json.RawMessage); !ok {
			return nil, false, fmt.Errorf("RowBinary stats row %d column %q expects json.RawMessage, got %T", row, column, value)
		}
		return nil, false, nil
	default:
		return nil, false, fmt.Errorf("RowBinary stats column %q has unsupported type %d", column, kind)
	}
}

func cloneSQLRowBinaryStatsValue(kind SQLRowBinaryType, value interface{}) interface{} {
	if kind != SQLRowBinaryBytes {
		return value
	}
	return append([]byte(nil), value.([]byte)...)
}

func sqlRowBinaryStatsSupportsMinMax(kind SQLRowBinaryType) bool {
	return kind >= SQLRowBinaryInt64 && kind <= SQLRowBinaryUUID
}

func compareSQLRowBinaryStatsValues(kind SQLRowBinaryType, left, right interface{}) int {
	switch kind {
	case SQLRowBinaryInt64:
		leftValue, rightValue := left.(int64), right.(int64)
		if leftValue < rightValue {
			return -1
		}
		if leftValue > rightValue {
			return 1
		}
	case SQLRowBinaryUint64:
		leftValue, rightValue := left.(uint64), right.(uint64)
		if leftValue < rightValue {
			return -1
		}
		if leftValue > rightValue {
			return 1
		}
	case SQLRowBinaryFloat64:
		leftValue, rightValue := left.(float64), right.(float64)
		if leftValue < rightValue {
			return -1
		}
		if leftValue > rightValue {
			return 1
		}
	case SQLRowBinaryBool:
		leftValue, rightValue := left.(bool), right.(bool)
		if !leftValue && rightValue {
			return -1
		}
		if leftValue && !rightValue {
			return 1
		}
	case SQLRowBinaryString:
		leftValue, rightValue := left.(string), right.(string)
		if leftValue < rightValue {
			return -1
		}
		if leftValue > rightValue {
			return 1
		}
	case SQLRowBinaryBytes:
		leftValue, rightValue := left.([]byte), right.([]byte)
		return compareSQLRowBinaryBytes(leftValue, rightValue)
	case SQLRowBinaryDate, SQLRowBinaryDateTime:
		leftValue, rightValue := left.(time.Time), right.(time.Time)
		if leftValue.Before(rightValue) {
			return -1
		}
		if leftValue.After(rightValue) {
			return 1
		}
	case SQLRowBinaryDuration:
		leftValue, rightValue := left.(time.Duration), right.(time.Duration)
		if leftValue < rightValue {
			return -1
		}
		if leftValue > rightValue {
			return 1
		}
	case SQLRowBinaryUUID:
		leftValue, rightValue := left.([16]byte), right.([16]byte)
		for index := range leftValue {
			if leftValue[index] < rightValue[index] {
				return -1
			}
			if leftValue[index] > rightValue[index] {
				return 1
			}
		}
	}
	return 0
}

func compareSQLRowBinaryBytes(left, right []byte) int {
	limit := len(left)
	if len(right) < limit {
		limit = len(right)
	}
	for index := 0; index < limit; index++ {
		if left[index] < right[index] {
			return -1
		}
		if left[index] > right[index] {
			return 1
		}
	}
	if len(left) < len(right) {
		return -1
	}
	if len(left) > len(right) {
		return 1
	}
	return 0
}

func equalSQLRowBinaryColumnStats(kind SQLRowBinaryType, left, right SQLRowBinaryColumnStats) bool {
	if left.Name != right.Name || left.NullCount != right.NullCount || left.ValueCount != right.ValueCount || left.HasMinMax != right.HasMinMax {
		return false
	}
	if !left.HasMinMax {
		return true
	}
	return compareSQLRowBinaryStatsValues(kind, left.Min, right.Min) == 0 && compareSQLRowBinaryStatsValues(kind, left.Max, right.Max) == 0
}

func appendSQLRowBinaryStatsUvarint(destination []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(encoded[:], value)
	return append(destination, encoded[:n]...)
}

func readSQLRowBinaryStatsUvarint(encoded []byte, offset *int, label string) (uint64, error) {
	if *offset >= len(encoded) {
		return 0, fmt.Errorf("RowBinary stats %s is truncated", label)
	}
	value, size := binary.Uvarint(encoded[*offset:])
	if size <= 0 {
		return 0, fmt.Errorf("RowBinary stats %s is invalid", label)
	}
	*offset += size
	return value, nil
}
