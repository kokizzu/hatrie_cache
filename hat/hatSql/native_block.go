package hatSql

import (
	"encoding/binary"
	"fmt"
)

var sqlNativeBlockMagic = [4]byte{'H', 'N', 'B', '1'}

const (
	sqlNativeBlockVersion      byte = 1
	sqlNativeBlockFlagComplete byte = 1 << 0
	maxSQLNativeBlockColumns        = 4096
	maxSQLNativeBlockBytes          = 256 << 20
)

// SQLNativeBlockOptions carries stream metadata that travels with one typed
// columnar block. Progress is caller-defined and can represent a source
// frontier, byte offset, or row watermark.
type SQLNativeBlockOptions struct {
	Sequence uint64
	Progress uint64
	Complete bool
}

// SQLNativeBlock is a self-describing, column-framed wire block. Columns are
// encoded independently, which lets a receiver validate or skip one complete
// column without decoding every row value first.
type SQLNativeBlock struct {
	Columns  []SQLRowBinaryColumn
	Rows     []SQLRow
	Sequence uint64
	Progress uint64
	Complete bool
}

// EncodeSQLNativeBlock encodes a bounded, self-describing columnar block.
// Nullable columns use one validity bitmap followed by only non-NULL values,
// avoiding one marker byte per value used by the legacy RowBinary stream.
func EncodeSQLNativeBlock(columns []SQLRowBinaryColumn, rows []SQLRow, options SQLNativeBlockOptions) ([]byte, error) {
	if err := validateSQLNativeBlockSchema(columns); err != nil {
		return nil, err
	}
	if len(rows) > maxSQLRowBinaryRows {
		return nil, fmt.Errorf("native block row count %d exceeds limit %d", len(rows), maxSQLRowBinaryRows)
	}

	encoded := make([]byte, 0, 32+len(columns)*16)
	encoded = append(encoded, sqlNativeBlockMagic[:]...)
	encoded = append(encoded, sqlNativeBlockVersion)
	flags := byte(0)
	if options.Complete {
		flags |= sqlNativeBlockFlagComplete
	}
	encoded = append(encoded, flags)
	encoded = appendSQLNativeBlockUvarint(encoded, options.Sequence)
	encoded = appendSQLNativeBlockUvarint(encoded, options.Progress)
	encoded = appendSQLNativeBlockUvarint(encoded, uint64(len(rows)))
	encoded = appendSQLNativeBlockUvarint(encoded, uint64(len(columns)))

	for _, column := range columns {
		encoded = appendSQLNativeBlockString(encoded, column.Name)
		encoded = append(encoded, byte(column.Type))
		if column.Nullable {
			encoded = append(encoded, 1)
		} else {
			encoded = append(encoded, 0)
		}
		encoded = appendSQLNativeBlockUvarint(encoded, uint64(len(column.EnumValues)))
		for _, enumValue := range column.EnumValues {
			encoded = appendSQLNativeBlockString(encoded, enumValue)
		}
		encoded = append(encoded, column.DecimalScale, column.DecimalPrecision)

		payload, err := encodeSQLNativeBlockColumn(column, rows)
		if err != nil {
			return nil, err
		}
		encoded = appendSQLNativeBlockUvarint(encoded, uint64(len(payload)))
		encoded = append(encoded, payload...)
		if len(encoded) > maxSQLNativeBlockBytes {
			return nil, fmt.Errorf("native block exceeds %d bytes", maxSQLNativeBlockBytes)
		}
	}
	return encoded, nil
}

// DecodeSQLNativeBlock decodes one complete native block and rejects unknown
// flags, truncated metadata, invalid validity bits, malformed values, and
// trailing bytes.
func DecodeSQLNativeBlock(encoded []byte) (SQLNativeBlock, error) {
	if len(encoded) > maxSQLNativeBlockBytes {
		return SQLNativeBlock{}, fmt.Errorf("native block exceeds %d bytes", maxSQLNativeBlockBytes)
	}
	minimum := len(sqlNativeBlockMagic) + 2
	if len(encoded) < minimum || string(encoded[:len(sqlNativeBlockMagic)]) != string(sqlNativeBlockMagic[:]) {
		return SQLNativeBlock{}, fmt.Errorf("native block header is invalid or truncated")
	}
	offset := len(sqlNativeBlockMagic)
	version := encoded[offset]
	offset++
	if version != sqlNativeBlockVersion {
		return SQLNativeBlock{}, fmt.Errorf("native block version %d is unsupported", version)
	}
	flags := encoded[offset]
	offset++
	if flags & ^sqlNativeBlockFlagComplete != 0 {
		return SQLNativeBlock{}, fmt.Errorf("native block flags %d are unsupported", flags)
	}
	sequence, err := readSQLNativeBlockUvarint(encoded, &offset, "sequence")
	if err != nil {
		return SQLNativeBlock{}, err
	}
	progress, err := readSQLNativeBlockUvarint(encoded, &offset, "progress")
	if err != nil {
		return SQLNativeBlock{}, err
	}
	rowCount, err := readSQLNativeBlockUvarint(encoded, &offset, "row count")
	if err != nil {
		return SQLNativeBlock{}, err
	}
	if rowCount > maxSQLRowBinaryRows {
		return SQLNativeBlock{}, fmt.Errorf("native block row count %d exceeds limit %d", rowCount, maxSQLRowBinaryRows)
	}
	columnCount, err := readSQLNativeBlockUvarint(encoded, &offset, "column count")
	if err != nil {
		return SQLNativeBlock{}, err
	}
	if columnCount > maxSQLNativeBlockColumns {
		return SQLNativeBlock{}, fmt.Errorf("native block column count %d exceeds limit %d", columnCount, maxSQLNativeBlockColumns)
	}

	columns := make([]SQLRowBinaryColumn, int(columnCount))
	payloads := make([][]byte, int(columnCount))
	for index := range columns {
		name, err := readSQLNativeBlockString(encoded, &offset, "column name")
		if err != nil {
			return SQLNativeBlock{}, err
		}
		if len(name) == 0 {
			return SQLNativeBlock{}, fmt.Errorf("native block column %d has an empty name", index)
		}
		if offset >= len(encoded) {
			return SQLNativeBlock{}, fmt.Errorf("native block column %d type is truncated", index)
		}
		kind := SQLRowBinaryType(encoded[offset])
		offset++
		if offset >= len(encoded) {
			return SQLNativeBlock{}, fmt.Errorf("native block column %d nullable flag is truncated", index)
		}
		nullableByte := encoded[offset]
		offset++
		if nullableByte > 1 {
			return SQLNativeBlock{}, fmt.Errorf("native block column %q nullable flag %d is invalid", name, nullableByte)
		}
		enumCount, err := readSQLNativeBlockUvarint(encoded, &offset, "enum count")
		if err != nil {
			return SQLNativeBlock{}, err
		}
		if enumCount > maxSQLNativeBlockColumns {
			return SQLNativeBlock{}, fmt.Errorf("native block column %q enum count %d is too large", name, enumCount)
		}
		var enumValues []string
		if enumCount > 0 {
			enumValues = make([]string, int(enumCount))
		}
		for enumIndex := range enumValues {
			enumValues[enumIndex], err = readSQLNativeBlockString(encoded, &offset, "enum value")
			if err != nil {
				return SQLNativeBlock{}, err
			}
		}
		if len(encoded)-offset < 2 {
			return SQLNativeBlock{}, fmt.Errorf("native block column %q decimal metadata is truncated", name)
		}
		column := SQLRowBinaryColumn{
			Name:             name,
			Type:             kind,
			Nullable:         nullableByte == 1,
			EnumValues:       enumValues,
			DecimalScale:     encoded[offset],
			DecimalPrecision: encoded[offset+1],
		}
		offset += 2
		payloadLength, err := readSQLNativeBlockUvarint(encoded, &offset, "column payload length")
		if err != nil {
			return SQLNativeBlock{}, err
		}
		if payloadLength > uint64(len(encoded)-offset) {
			return SQLNativeBlock{}, fmt.Errorf("native block column %q payload length %d exceeds remaining input", name, payloadLength)
		}
		end := offset + int(payloadLength)
		payloads[index] = encoded[offset:end]
		offset = end
		columns[index] = column
	}
	if offset != len(encoded) {
		return SQLNativeBlock{}, fmt.Errorf("native block has %d trailing bytes", len(encoded)-offset)
	}
	if err := validateSQLNativeBlockSchema(columns); err != nil {
		return SQLNativeBlock{}, err
	}

	rows := make([]SQLRow, int(rowCount))
	for index := range rows {
		rows[index] = make(SQLRow, len(columns))
	}
	for index, column := range columns {
		if err := decodeSQLNativeBlockColumn(column, payloads[index], rows); err != nil {
			return SQLNativeBlock{}, err
		}
	}
	return SQLNativeBlock{
		Columns:  columns,
		Rows:     rows,
		Sequence: sequence,
		Progress: progress,
		Complete: flags&sqlNativeBlockFlagComplete != 0,
	}, nil
}

func validateSQLNativeBlockSchema(columns []SQLRowBinaryColumn) error {
	if len(columns) > maxSQLNativeBlockColumns {
		return fmt.Errorf("native block column count %d exceeds limit %d", len(columns), maxSQLNativeBlockColumns)
	}
	return validateSQLRowBinaryColumns(columns)
}

func encodeSQLNativeBlockColumn(column SQLRowBinaryColumn, rows []SQLRow) ([]byte, error) {
	payload := make([]byte, 0)
	if column.Nullable {
		bitmapBytes := (len(rows) + 7) / 8
		payload = make([]byte, bitmapBytes)
		for rowIndex, row := range rows {
			if row != nil && row[column.Name] != nil {
				payload[rowIndex/8] |= 1 << uint(rowIndex%8)
			}
		}
	}
	for rowIndex, row := range rows {
		value := interface{}(nil)
		if row != nil {
			value = row[column.Name]
		}
		if value == nil {
			if !column.Nullable {
				return nil, fmt.Errorf("native block row %d column %q is NULL but not nullable", rowIndex, column.Name)
			}
			continue
		}
		var err error
		payload, err = appendSQLRowBinaryColumnValue(payload, column, value, rowIndex)
		if err != nil {
			return nil, err
		}
	}
	return payload, nil
}

func decodeSQLNativeBlockColumn(column SQLRowBinaryColumn, payload []byte, rows []SQLRow) error {
	offset := 0
	bitmapBytes := 0
	if column.Nullable {
		bitmapBytes = (len(rows) + 7) / 8
		if len(payload) < bitmapBytes {
			return fmt.Errorf("native block column %q validity bitmap is truncated", column.Name)
		}
		if len(rows)%8 != 0 && bitmapBytes > 0 {
			unused := payload[bitmapBytes-1] &^ byte((1<<uint(len(rows)%8))-1)
			if unused != 0 {
				return fmt.Errorf("native block column %q validity bitmap has trailing bits", column.Name)
			}
		}
		offset = bitmapBytes
	}
	for rowIndex := range rows {
		if column.Nullable && payload[rowIndex/8]&(1<<uint(rowIndex%8)) == 0 {
			rows[rowIndex][column.Name] = nil
			continue
		}
		value, next, err := decodeSQLRowBinaryValue(column.Type, payload, offset, rowIndex, column.Name)
		if err != nil {
			return err
		}
		rows[rowIndex][column.Name] = value
		offset = next
	}
	if offset != len(payload) {
		return fmt.Errorf("native block column %q has %d trailing payload bytes", column.Name, len(payload)-offset)
	}
	return nil
}

func appendSQLNativeBlockUvarint(destination []byte, value uint64) []byte {
	var buffer [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buffer[:], value)
	return append(destination, buffer[:n]...)
}

func appendSQLNativeBlockString(destination []byte, value string) []byte {
	destination = appendSQLNativeBlockUvarint(destination, uint64(len(value)))
	return append(destination, value...)
}

func readSQLNativeBlockUvarint(encoded []byte, offset *int, label string) (uint64, error) {
	if *offset >= len(encoded) {
		return 0, fmt.Errorf("native block %s is truncated", label)
	}
	value, size := binary.Uvarint(encoded[*offset:])
	if size <= 0 {
		return 0, fmt.Errorf("native block %s varint is invalid", label)
	}
	*offset += size
	return value, nil
}

func readSQLNativeBlockString(encoded []byte, offset *int, label string) (string, error) {
	length, err := readSQLNativeBlockUvarint(encoded, offset, label+" length")
	if err != nil {
		return "", err
	}
	if length > uint64(len(encoded)-*offset) {
		return "", fmt.Errorf("native block %s length %d exceeds remaining input", label, length)
	}
	end := *offset + int(length)
	value := string(encoded[*offset:end])
	*offset = end
	return value, nil
}
