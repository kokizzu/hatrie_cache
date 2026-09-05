package hatSql

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
)

const (
	sqlRowBinaryDictionaryHeader        = "HDB1"
	maxSQLRowBinaryDictionaryEntries    = 1_000_000
	maxSQLRowBinaryDictionaryValueBytes = 64 << 20
	maxSQLRowBinaryDictionaryBytes      = 1 << 30
)

type sqlRowBinaryDictionary struct {
	index     map[string]uint64
	byteIndex map[uint32][]uint64
	values    []string
	bytes     uint64
}

// SQLRowBinaryDictionaryEncoder retains selected string-like values so later
// schema-compatible batches can send dictionary ids instead of repeating the
// values. It is not safe for concurrent use.
type SQLRowBinaryDictionaryEncoder struct {
	columns           []SQLRowBinaryColumn
	dictionaryColumns []bool
	dictionaries      []sqlRowBinaryDictionary
}

// SQLRowBinaryDictionaryDecoder retains dictionary values received from prior
// batches. It is not safe for concurrent use.
type SQLRowBinaryDictionaryDecoder struct {
	columns           []SQLRowBinaryColumn
	dictionaryColumns []bool
	dictionaries      []sqlRowBinaryDictionary
}

// NewSQLRowBinaryDictionaryEncoder creates a stateful encoder. Dictionary
// columns must be named SQLRowBinaryString, SQLRowBinaryBytes, or
// SQLRowBinaryJSON columns in columns. An empty dictionaryColumns selection is
// valid and encodes all values without dictionary substitution.
func NewSQLRowBinaryDictionaryEncoder(columns []SQLRowBinaryColumn, dictionaryColumns []string) (*SQLRowBinaryDictionaryEncoder, error) {
	selected, err := validateSQLRowBinaryDictionarySelection(columns, dictionaryColumns)
	if err != nil {
		return nil, err
	}
	encoder := &SQLRowBinaryDictionaryEncoder{
		columns:           append([]SQLRowBinaryColumn(nil), columns...),
		dictionaryColumns: selected,
		dictionaries:      make([]sqlRowBinaryDictionary, len(columns)),
	}
	for index, isDictionary := range selected {
		if isDictionary {
			encoder.dictionaries[index] = newSQLRowBinaryDictionary(columns[index].Type)
		}
	}
	return encoder, nil
}

// NewSQLRowBinaryDictionaryDecoder creates a stateful decoder for the same
// ordered schema and dictionary selection used by the encoder.
func NewSQLRowBinaryDictionaryDecoder(columns []SQLRowBinaryColumn, dictionaryColumns []string) (*SQLRowBinaryDictionaryDecoder, error) {
	selected, err := validateSQLRowBinaryDictionarySelection(columns, dictionaryColumns)
	if err != nil {
		return nil, err
	}
	return &SQLRowBinaryDictionaryDecoder{
		columns:           append([]SQLRowBinaryColumn(nil), columns...),
		dictionaryColumns: selected,
		dictionaries:      make([]sqlRowBinaryDictionary, len(columns)),
	}, nil
}

// Reset discards all retained encoder dictionaries while keeping their
// capacity available for reuse.
func (e *SQLRowBinaryDictionaryEncoder) Reset() {
	if e == nil {
		return
	}
	for index, isDictionary := range e.dictionaryColumns {
		if !isDictionary {
			continue
		}
		dictionary := &e.dictionaries[index]
		clear(dictionary.index)
		clear(dictionary.byteIndex)
		dictionary.values = dictionary.values[:0]
		dictionary.bytes = 0
	}
}

// Reset discards all retained decoder dictionaries while keeping their
// capacity available for reuse.
func (d *SQLRowBinaryDictionaryDecoder) Reset() {
	if d == nil {
		return
	}
	for index, isDictionary := range d.dictionaryColumns {
		if !isDictionary {
			continue
		}
		dictionary := &d.dictionaries[index]
		clear(dictionary.index)
		clear(dictionary.byteIndex)
		dictionary.values = dictionary.values[:0]
		dictionary.bytes = 0
	}
}

// Encode encodes one batch and retains any new dictionary values for later
// batches. A failed call does not change the retained dictionaries.
func (e *SQLRowBinaryDictionaryEncoder) Encode(rows []SQLRow) ([]byte, error) {
	if e == nil {
		return nil, fmt.Errorf("RowBinary dictionary encoder is nil")
	}
	if len(rows) > maxSQLRowBinaryRows {
		return nil, fmt.Errorf("RowBinary dictionary row count %d exceeds limit %d", len(rows), maxSQLRowBinaryRows)
	}
	pending := make([]sqlRowBinaryDictionary, len(e.columns))
	rowPayload := make([]byte, 0)
	for rowIndex, row := range rows {
		for columnIndex, column := range e.columns {
			value := interface{}(nil)
			if row != nil {
				value = row[column.Name]
			}
			if value == nil {
				if !column.Nullable {
					return nil, fmt.Errorf("RowBinary dictionary row %d column %q is NULL but not nullable", rowIndex, column.Name)
				}
				rowPayload = append(rowPayload, 1)
				continue
			}
			if column.Nullable {
				rowPayload = append(rowPayload, 0)
			}
			if e.dictionaryColumns[columnIndex] {
				_, id, ok, err := sqlRowBinaryDictionaryLookup(&e.dictionaries[columnIndex], column.Type, value, rowIndex, column.Name)
				if err != nil {
					return nil, err
				}
				dictionary := &e.dictionaries[columnIndex]
				if !ok {
					_, pendingID, pendingOK, err := sqlRowBinaryDictionaryLookup(&pending[columnIndex], column.Type, value, rowIndex, column.Name)
					if err != nil {
						return nil, err
					}
					if pendingOK {
						id = uint64(len(dictionary.values)) + pendingID
					} else {
						key, err := sqlRowBinaryDictionaryKey(column.Type, value, rowIndex, column.Name)
						if err != nil {
							return nil, err
						}
						pendingID = addSQLRowBinaryDictionaryValue(&pending[columnIndex], column.Type, key)
						id = uint64(len(dictionary.values)) + pendingID
					}
				}
				rowPayload = appendSQLRowBinaryDictionaryUvarint(rowPayload, id)
				continue
			}
			var err error
			rowPayload, err = appendSQLRowBinaryValue(rowPayload, column.Type, value, rowIndex, column.Name)
			if err != nil {
				return nil, err
			}
		}
	}
	if err := validateSQLRowBinaryDictionaryGrowth(e.columns, e.dictionaries, e.dictionaryColumns, pending); err != nil {
		return nil, err
	}
	encoded := make([]byte, 0, len(sqlRowBinaryDictionaryHeader)+len(rowPayload))
	encoded = append(encoded, sqlRowBinaryDictionaryHeader...)
	encoded = appendSQLRowBinaryDictionaryUvarint(encoded, uint64(len(rows)))
	for index, isDictionary := range e.dictionaryColumns {
		if !isDictionary {
			continue
		}
		values := pending[index].values
		encoded = appendSQLRowBinaryDictionaryUvarint(encoded, uint64(len(values)))
		for _, value := range values {
			encoded = appendSQLRowBinaryDictionaryString(encoded, value)
		}
	}
	encoded = append(encoded, rowPayload...)
	for index, isDictionary := range e.dictionaryColumns {
		if !isDictionary {
			continue
		}
		dictionary := &e.dictionaries[index]
		for _, value := range pending[index].values {
			addSQLRowBinaryDictionaryValue(dictionary, e.columns[index].Type, value)
		}
	}
	return encoded, nil
}

// Decode decodes one batch and retains its dictionary additions for later
// batches. A failed call does not change the retained dictionaries.
func (d *SQLRowBinaryDictionaryDecoder) Decode(encoded []byte) ([]SQLRow, error) {
	if d == nil {
		return nil, fmt.Errorf("RowBinary dictionary decoder is nil")
	}
	if len(encoded) < len(sqlRowBinaryDictionaryHeader) || string(encoded[:len(sqlRowBinaryDictionaryHeader)]) != sqlRowBinaryDictionaryHeader {
		return nil, fmt.Errorf("invalid RowBinary dictionary header")
	}
	offset := len(sqlRowBinaryDictionaryHeader)
	rowCount, err := readSQLRowBinaryDictionaryUvarint(encoded, &offset, "row count")
	if err != nil {
		return nil, err
	}
	if rowCount > maxSQLRowBinaryRows {
		return nil, fmt.Errorf("RowBinary dictionary row count %d exceeds limit %d", rowCount, maxSQLRowBinaryRows)
	}
	pending := make([][]string, len(d.columns))
	pendingBytes := make([]uint64, len(d.columns))
	for index, isDictionary := range d.dictionaryColumns {
		if !isDictionary {
			continue
		}
		additionCount, err := readSQLRowBinaryDictionaryUvarint(encoded, &offset, "dictionary entry count")
		if err != nil {
			return nil, err
		}
		dictionary := &d.dictionaries[index]
		if additionCount > maxSQLRowBinaryDictionaryEntries || uint64(len(dictionary.values)) > maxSQLRowBinaryDictionaryEntries-additionCount {
			return nil, fmt.Errorf("RowBinary dictionary column %q exceeds %d entries", d.columns[index].Name, maxSQLRowBinaryDictionaryEntries)
		}
		if additionCount > 0 {
			pending[index] = make([]string, 0, int(additionCount))
		}
		for entryIndex := uint64(0); entryIndex < additionCount; entryIndex++ {
			value, next, err := decodeSQLRowBinaryDictionaryString(encoded, offset)
			if err != nil {
				return nil, fmt.Errorf("RowBinary dictionary column %q entry %d: %w", d.columns[index].Name, entryIndex, err)
			}
			offset = next
			pending[index] = append(pending[index], value)
			pendingBytes[index] += uint64(len(value))
			if len(value) > maxSQLRowBinaryDictionaryValueBytes || d.dictionaries[index].bytes+pendingBytes[index] > maxSQLRowBinaryDictionaryBytes {
				return nil, fmt.Errorf("RowBinary dictionary column %q exceeds byte limit", d.columns[index].Name)
			}
		}
	}
	rows := make([]SQLRow, 0, int(rowCount))
	for rowIndex := uint64(0); rowIndex < rowCount; rowIndex++ {
		row := make(SQLRow, len(d.columns))
		for columnIndex, column := range d.columns {
			if column.Nullable {
				if offset >= len(encoded) {
					return nil, fmt.Errorf("RowBinary dictionary row %d column %q is missing its NULL marker", rowIndex, column.Name)
				}
				marker := encoded[offset]
				offset++
				switch marker {
				case 0:
				case 1:
					row[column.Name] = nil
					continue
				default:
					return nil, fmt.Errorf("RowBinary dictionary row %d column %q has invalid NULL marker %d", rowIndex, column.Name, marker)
				}
			}
			if d.dictionaryColumns[columnIndex] {
				id, err := readSQLRowBinaryDictionaryUvarint(encoded, &offset, "dictionary id")
				if err != nil {
					return nil, fmt.Errorf("RowBinary dictionary row %d column %q: %w", rowIndex, column.Name, err)
				}
				dictionary := &d.dictionaries[columnIndex]
				if id >= uint64(len(dictionary.values))+uint64(len(pending[columnIndex])) {
					return nil, fmt.Errorf("RowBinary dictionary row %d column %q references unknown id %d", rowIndex, column.Name, id)
				}
				value := ""
				if id < uint64(len(dictionary.values)) {
					value = dictionary.values[id]
				} else {
					value = pending[columnIndex][id-uint64(len(dictionary.values))]
				}
				row[column.Name] = sqlRowBinaryDictionaryValue(column.Type, value)
				continue
			}
			value, next, err := decodeSQLRowBinaryValue(column.Type, encoded, offset, int(rowIndex), column.Name)
			if err != nil {
				return nil, err
			}
			row[column.Name] = value
			offset = next
		}
		rows = append(rows, row)
	}
	if offset != len(encoded) {
		return nil, fmt.Errorf("RowBinary dictionary batch has %d trailing bytes", len(encoded)-offset)
	}
	for index, isDictionary := range d.dictionaryColumns {
		if !isDictionary {
			continue
		}
		dictionary := &d.dictionaries[index]
		dictionary.values = append(dictionary.values, pending[index]...)
		dictionary.bytes += pendingBytes[index]
	}
	return rows, nil
}

func validateSQLRowBinaryDictionarySelection(columns []SQLRowBinaryColumn, dictionaryColumns []string) ([]bool, error) {
	if err := validateSQLRowBinaryColumns(columns); err != nil {
		return nil, err
	}
	columnIndexes := make(map[string]int, len(columns))
	for index, column := range columns {
		columnIndexes[column.Name] = index
	}
	selected := make([]bool, len(columns))
	for _, name := range dictionaryColumns {
		index, ok := columnIndexes[name]
		if !ok {
			return nil, fmt.Errorf("RowBinary dictionary column %q is not in the schema", name)
		}
		if selected[index] {
			return nil, fmt.Errorf("RowBinary dictionary column %q is selected more than once", name)
		}
		switch columns[index].Type {
		case SQLRowBinaryString, SQLRowBinaryBytes, SQLRowBinaryJSON:
		default:
			return nil, fmt.Errorf("RowBinary dictionary column %q has unsupported type %d", name, columns[index].Type)
		}
		selected[index] = true
	}
	return selected, nil
}

func validateSQLRowBinaryDictionaryGrowth(columns []SQLRowBinaryColumn, dictionaries []sqlRowBinaryDictionary, selected []bool, pending []sqlRowBinaryDictionary) error {
	for index, isDictionary := range selected {
		if !isDictionary {
			continue
		}
		if len(dictionaries[index].values) > maxSQLRowBinaryDictionaryEntries-len(pending[index].values) {
			return fmt.Errorf("RowBinary dictionary column %q exceeds %d entries", columns[index].Name, maxSQLRowBinaryDictionaryEntries)
		}
		bytes := dictionaries[index].bytes
		for _, value := range pending[index].values {
			if len(value) > maxSQLRowBinaryDictionaryValueBytes || bytes > maxSQLRowBinaryDictionaryBytes-uint64(len(value)) {
				return fmt.Errorf("RowBinary dictionary column %q exceeds byte limit", columns[index].Name)
			}
			bytes += uint64(len(value))
		}
	}
	return nil
}

func newSQLRowBinaryDictionary(kind SQLRowBinaryType) sqlRowBinaryDictionary {
	dictionary := sqlRowBinaryDictionary{}
	switch kind {
	case SQLRowBinaryString:
		dictionary.index = make(map[string]uint64)
	case SQLRowBinaryBytes, SQLRowBinaryJSON:
		dictionary.byteIndex = make(map[uint32][]uint64)
	}
	return dictionary
}

func sqlRowBinaryDictionaryLookup(dictionary *sqlRowBinaryDictionary, kind SQLRowBinaryType, value interface{}, row int, column string) (string, uint64, bool, error) {
	switch kind {
	case SQLRowBinaryString:
		converted, ok := value.(string)
		if !ok {
			return "", 0, false, fmt.Errorf("RowBinary dictionary row %d column %q expects string, got %T", row, column, value)
		}
		id, found := dictionary.index[converted]
		return converted, id, found, nil
	case SQLRowBinaryBytes, SQLRowBinaryJSON:
		converted, ok := sqlRowBinaryDictionaryBytes(kind, value)
		if !ok {
			want := "[]byte"
			if kind == SQLRowBinaryJSON {
				want = "json.RawMessage"
			}
			return "", 0, false, fmt.Errorf("RowBinary dictionary row %d column %q expects %s, got %T", row, column, want, value)
		}
		hash := crc32.ChecksumIEEE(converted)
		for _, id := range dictionary.byteIndex[hash] {
			if sqlRowBinaryDictionaryBytesEqual(dictionary.values[id], converted) {
				return "", id, true, nil
			}
		}
		return "", 0, false, nil
	default:
		return "", 0, false, fmt.Errorf("RowBinary dictionary column %q has unsupported type %d", column, kind)
	}
}

func sqlRowBinaryDictionaryBytes(kind SQLRowBinaryType, value interface{}) ([]byte, bool) {
	switch kind {
	case SQLRowBinaryBytes:
		converted, ok := value.([]byte)
		return converted, ok
	case SQLRowBinaryJSON:
		converted, ok := value.(json.RawMessage)
		return converted, ok
	default:
		return nil, false
	}
}

func sqlRowBinaryDictionaryBytesEqual(value string, other []byte) bool {
	if len(value) != len(other) {
		return false
	}
	for index := range other {
		if value[index] != other[index] {
			return false
		}
	}
	return true
}

func addSQLRowBinaryDictionaryValue(dictionary *sqlRowBinaryDictionary, kind SQLRowBinaryType, value string) uint64 {
	id := uint64(len(dictionary.values))
	dictionary.values = append(dictionary.values, value)
	dictionary.bytes += uint64(len(value))
	switch kind {
	case SQLRowBinaryString:
		if dictionary.index == nil {
			dictionary.index = make(map[string]uint64)
		}
		dictionary.index[value] = id
	case SQLRowBinaryBytes, SQLRowBinaryJSON:
		if dictionary.byteIndex == nil {
			dictionary.byteIndex = make(map[uint32][]uint64)
		}
		hash := crc32.ChecksumIEEE([]byte(value))
		dictionary.byteIndex[hash] = append(dictionary.byteIndex[hash], id)
	}
	return id
}

func sqlRowBinaryDictionaryKey(kind SQLRowBinaryType, value interface{}, row int, column string) (string, error) {
	switch kind {
	case SQLRowBinaryString:
		converted, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("RowBinary dictionary row %d column %q expects string, got %T", row, column, value)
		}
		return converted, nil
	case SQLRowBinaryBytes:
		converted, ok := value.([]byte)
		if !ok {
			return "", fmt.Errorf("RowBinary dictionary row %d column %q expects []byte, got %T", row, column, value)
		}
		return string(converted), nil
	case SQLRowBinaryJSON:
		converted, ok := value.(json.RawMessage)
		if !ok {
			return "", fmt.Errorf("RowBinary dictionary row %d column %q expects json.RawMessage, got %T", row, column, value)
		}
		return string(converted), nil
	default:
		return "", fmt.Errorf("RowBinary dictionary column %q has unsupported type %d", column, kind)
	}
}

func sqlRowBinaryDictionaryValue(kind SQLRowBinaryType, value string) interface{} {
	switch kind {
	case SQLRowBinaryString:
		return value
	case SQLRowBinaryBytes:
		return append([]byte(nil), value...)
	case SQLRowBinaryJSON:
		return json.RawMessage(append([]byte(nil), value...))
	default:
		return nil
	}
}

func appendSQLRowBinaryDictionaryUvarint(destination []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(encoded[:], value)
	return append(destination, encoded[:n]...)
}

func appendSQLRowBinaryDictionaryString(destination []byte, value string) []byte {
	destination = appendSQLRowBinaryDictionaryUvarint(destination, uint64(len(value)))
	return append(destination, value...)
}

func readSQLRowBinaryDictionaryUvarint(encoded []byte, offset *int, label string) (uint64, error) {
	if *offset >= len(encoded) {
		return 0, fmt.Errorf("RowBinary dictionary %s is truncated", label)
	}
	value, size := binary.Uvarint(encoded[*offset:])
	if size <= 0 {
		return 0, fmt.Errorf("RowBinary dictionary %s is invalid", label)
	}
	*offset += size
	return value, nil
}

func decodeSQLRowBinaryDictionaryString(encoded []byte, offset int) (string, int, error) {
	length, err := readSQLRowBinaryDictionaryUvarint(encoded, &offset, "dictionary value length")
	if err != nil {
		return "", offset, err
	}
	if length > maxSQLRowBinaryDictionaryValueBytes {
		return "", offset, fmt.Errorf("dictionary value length %d exceeds limit", length)
	}
	if length > uint64(len(encoded)-offset) {
		return "", offset, fmt.Errorf("dictionary value length %d exceeds remaining payload", length)
	}
	end := offset + int(length)
	return string(encoded[offset:end]), end, nil
}
