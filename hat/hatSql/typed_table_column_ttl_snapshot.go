package hatSql

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

const (
	typedTableColumnTTLStateMagic   = "HCTTL1"
	typedTableColumnTTLStateVersion = 1
)

type typedTableColumnTTLStateSnapshotColumn struct {
	name     string
	lifetime int64
}

// MarshalColumnTTLState returns a deterministic CRC-protected snapshot of
// processing-time column deadlines. Row data is intentionally excluded; restore
// rows first, then restore this state before starting maintenance.
func (table *TypedTable) MarshalColumnTTLState() ([]byte, error) {
	if table == nil {
		return nil, fmt.Errorf("typed table is nil")
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	columns := table.processingColumnTTLIndexesLocked()
	if len(columns) == 0 {
		return nil, ErrTypedTableTTLStateUnsupported
	}
	if len(table.keys) > int(^uint32(0)) || len(columns) > int(^uint32(0)) || len(table.schema.Name) > int(^uint32(0)) {
		return nil, fmt.Errorf("%w: row, column, or table name count overflows format", ErrTypedTableTTLStateInvalid)
	}
	size := len(typedTableColumnTTLStateMagic) + 1 + 3 + 4 + len(table.schema.Name) + 4 + 4
	for _, column := range columns {
		name := table.schema.Columns[column].Name
		if len(name) > int(^uint32(0)) {
			return nil, fmt.Errorf("%w: column name overflows format", ErrTypedTableTTLStateInvalid)
		}
		size += 4 + len(name) + 8
	}
	if size > MaxTypedTableTTLStateBytes-4 {
		return nil, fmt.Errorf("%w: snapshot exceeds %d bytes", ErrTypedTableTTLStateInvalid, MaxTypedTableTTLStateBytes)
	}
	for _, key := range table.keys {
		if len(key) > int(^uint32(0)) {
			return nil, fmt.Errorf("%w: row key overflows format", ErrTypedTableTTLStateInvalid)
		}
		size += 4 + len(key) + 8*len(columns)
		if size > MaxTypedTableTTLStateBytes-4 {
			return nil, fmt.Errorf("%w: snapshot exceeds %d bytes", ErrTypedTableTTLStateInvalid, MaxTypedTableTTLStateBytes)
		}
	}
	encoded := make([]byte, 0, size+4)
	encoded = append(encoded, typedTableColumnTTLStateMagic...)
	encoded = append(encoded, typedTableColumnTTLStateVersion, 0, 0, 0)
	encoded = appendUint32(encoded, uint32(len(table.schema.Name)))
	encoded = append(encoded, table.schema.Name...)
	encoded = appendUint32(encoded, uint32(len(table.keys)))
	encoded = appendUint32(encoded, uint32(len(columns)))
	for _, column := range columns {
		name := table.schema.Columns[column].Name
		encoded = appendUint32(encoded, uint32(len(name)))
		encoded = append(encoded, name...)
		encoded = appendUint64(encoded, uint64(table.columnTTLs[column].options.Lifetime))
	}
	for row, key := range table.keys {
		encoded = appendUint32(encoded, uint32(len(key)))
		encoded = append(encoded, key...)
		for _, column := range columns {
			if row >= len(table.columnTTLs[column].deadlines) {
				return nil, fmt.Errorf("%w: invalid deadline row %d", ErrTypedTableTTLStateInvalid, row)
			}
			encoded = appendUint64(encoded, uint64(table.columnTTLs[column].deadlines[row]))
		}
	}
	return appendUint32(encoded, crc32.ChecksumIEEE(encoded)), nil
}

// RestoreColumnTTLState validates and atomically installs processing-time
// column deadlines for the current physical rows. Column and row order may
// differ from the snapshot, but names, lifetimes, and key coverage must match.
func (table *TypedTable) RestoreColumnTTLState(encoded []byte) error {
	if table == nil {
		return fmt.Errorf("typed table is nil")
	}
	if len(encoded) > MaxTypedTableTTLStateBytes || len(encoded) < len(typedTableColumnTTLStateMagic)+1+3+4+4+4+4 {
		return fmt.Errorf("%w: snapshot length is invalid", ErrTypedTableTTLStateInvalid)
	}
	payloadLength := len(encoded) - 4
	if binary.LittleEndian.Uint32(encoded[payloadLength:]) != crc32.ChecksumIEEE(encoded[:payloadLength]) {
		return fmt.Errorf("%w: checksum mismatch", ErrTypedTableTTLStateInvalid)
	}
	payload := encoded[:payloadLength]
	position := 0
	if !bytes.Equal(payload[position:position+len(typedTableColumnTTLStateMagic)], []byte(typedTableColumnTTLStateMagic)) {
		return fmt.Errorf("%w: magic mismatch", ErrTypedTableTTLStateInvalid)
	}
	position += len(typedTableColumnTTLStateMagic)
	if position+4 > len(payload) || payload[position] != typedTableColumnTTLStateVersion {
		return fmt.Errorf("%w: unsupported version", ErrTypedTableTTLStateInvalid)
	}
	position += 4
	tableName, ok := readTTLStateString(payload, &position)
	if !ok {
		return fmt.Errorf("%w: table name is truncated", ErrTypedTableTTLStateInvalid)
	}
	rowCount, ok := readTTLStateUint32(payload, &position)
	if !ok {
		return fmt.Errorf("%w: row count is truncated", ErrTypedTableTTLStateInvalid)
	}
	columnCount, ok := readTTLStateUint32(payload, &position)
	if !ok || columnCount == 0 {
		return fmt.Errorf("%w: column count is invalid", ErrTypedTableTTLStateInvalid)
	}
	table.mu.RLock()
	expectedTableName := table.schema.Name
	expectedRowCount := len(table.keys)
	expectedColumnCount := len(table.processingColumnTTLIndexesLocked())
	table.mu.RUnlock()
	if tableName != expectedTableName || int(rowCount) != expectedRowCount || int(columnCount) != expectedColumnCount {
		return fmt.Errorf("%w: schema, row count, or column count mismatch", ErrTypedTableTTLStateInvalid)
	}
	if uint64(columnCount) > uint64(len(payload)-position)/12 {
		return fmt.Errorf("%w: column count is invalid", ErrTypedTableTTLStateInvalid)
	}
	columns := make([]typedTableColumnTTLStateSnapshotColumn, int(columnCount))
	for index := range columns {
		name, ok := readTTLStateString(payload, &position)
		if !ok || position+8 > len(payload) {
			return fmt.Errorf("%w: column %d is truncated", ErrTypedTableTTLStateInvalid, index)
		}
		lifetime := int64(binary.LittleEndian.Uint64(payload[position : position+8]))
		position += 8
		columns[index] = typedTableColumnTTLStateSnapshotColumn{name: name, lifetime: lifetime}
	}
	minimumRowBytes := uint64(4) + uint64(columnCount)*8
	if minimumRowBytes == 0 || uint64(rowCount) > uint64(len(payload)-position)/minimumRowBytes {
		return fmt.Errorf("%w: row count is invalid", ErrTypedTableTTLStateInvalid)
	}
	keys := make([]string, int(rowCount))
	deadlines := make([]int64, int(rowCount)*int(columnCount))
	for row := range keys {
		key, ok := readTTLStateString(payload, &position)
		if !ok || uint64(len(payload)-position) < uint64(columnCount)*8 {
			return fmt.Errorf("%w: row %d is truncated", ErrTypedTableTTLStateInvalid, row)
		}
		keys[row] = key
		for column := 0; column < int(columnCount); column++ {
			deadlines[row*int(columnCount)+column] = int64(binary.LittleEndian.Uint64(payload[position : position+8]))
			position += 8
		}
	}
	if position != len(payload) {
		return fmt.Errorf("%w: trailing bytes", ErrTypedTableTTLStateInvalid)
	}

	table.mu.Lock()
	defer table.mu.Unlock()
	if table.schema.Name != tableName || len(keys) != len(table.keys) {
		return fmt.Errorf("%w: schema or row count mismatch", ErrTypedTableTTLStateInvalid)
	}
	expectedColumns := table.processingColumnTTLIndexesLocked()
	if len(columns) != len(expectedColumns) {
		return fmt.Errorf("%w: column count mismatch", ErrTypedTableTTLStateInvalid)
	}
	columnIndexes := make([]int, len(columns))
	seenColumns := make(map[string]struct{}, len(columns))
	for encodedColumn, column := range columns {
		if _, duplicate := seenColumns[column.name]; duplicate {
			return fmt.Errorf("%w: duplicate column %q", ErrTypedTableTTLStateInvalid, column.name)
		}
		seenColumns[column.name] = struct{}{}
		index, exists := table.byName[column.name]
		if !exists || table.columnTTLs[index] == nil || table.columnTTLs[index].options.Mode != TypedTableTTLProcessingTime || int64(table.columnTTLs[index].options.Lifetime) != column.lifetime {
			return fmt.Errorf("%w: column %q configuration mismatch", ErrTypedTableTTLStateInvalid, column.name)
		}
		columnIndexes[encodedColumn] = index
	}
	if len(seenColumns) != len(expectedColumns) {
		return fmt.Errorf("%w: snapshot does not cover every processing-time column", ErrTypedTableTTLStateInvalid)
	}
	seenKeys := make(map[string]struct{}, len(keys))
	rowPositions := make([]int, len(keys))
	for row, key := range keys {
		if _, duplicate := seenKeys[key]; duplicate {
			return fmt.Errorf("%w: duplicate key %q", ErrTypedTableTTLStateInvalid, key)
		}
		position, exists := table.positions[key]
		if !exists {
			return fmt.Errorf("%w: key %q is not in table", ErrTypedTableTTLStateInvalid, key)
		}
		seenKeys[key] = struct{}{}
		rowPositions[row] = position
	}
	installed := make([][]int64, len(table.columnTTLs))
	for encodedColumn, index := range columnIndexes {
		installed[index] = make([]int64, len(keys))
		for row := range keys {
			installed[index][rowPositions[row]] = deadlines[row*int(columnCount)+encodedColumn]
		}
	}
	if len(seenKeys) != len(table.keys) {
		return fmt.Errorf("%w: snapshot does not cover every row", ErrTypedTableTTLStateInvalid)
	}
	for _, index := range expectedColumns {
		table.columnTTLs[index].deadlines = installed[index]
	}
	return nil
}

func (table *TypedTable) processingColumnTTLIndexesLocked() []int {
	if table == nil || table.columnTTLs == nil {
		return nil
	}
	columns := make([]int, 0)
	for index, state := range table.columnTTLs {
		if state != nil && state.options.Mode == TypedTableTTLProcessingTime {
			columns = append(columns, index)
		}
	}
	return columns
}
