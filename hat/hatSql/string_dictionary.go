package hatSql

import (
	"encoding/binary"
	"fmt"
)

var sqlStringDictionaryMagic = [4]byte{'H', 'S', 'D', 'C'}

const maxSQLStringDictionaryBytes = 64 << 20

// EncodeSQLStringDictionary encodes a string column as a dictionary followed
// by unsigned dictionary IDs. Dictionary order is first-seen order, making
// output deterministic for the same input sequence.
func EncodeSQLStringDictionary(values []string) ([]byte, error) {
	if len(values) == 0 {
		return nil, nil
	}
	if len(values) > maxSQLRowBinaryRows {
		return nil, fmt.Errorf("string dictionary value count %d exceeds limit %d", len(values), maxSQLRowBinaryRows)
	}
	dictionary := make([]string, 0)
	indexes := make(map[string]uint64)
	ids := make([]uint64, len(values))
	dictionaryBytes := 0
	for index, value := range values {
		id, exists := indexes[value]
		if !exists {
			if dictionaryBytes > maxSQLStringDictionaryBytes-len(value) {
				return nil, fmt.Errorf("string dictionary bytes exceed limit %d", maxSQLStringDictionaryBytes)
			}
			id = uint64(len(dictionary))
			indexes[value] = id
			dictionary = append(dictionary, value)
			dictionaryBytes += len(value)
		}
		ids[index] = id
	}
	encoded := make([]byte, 0, len(sqlStringDictionaryMagic)+len(values)*2+dictionaryBytes)
	encoded = append(encoded, sqlStringDictionaryMagic[:]...)
	encoded = appendSQLRowBinaryDeltaUvarint(encoded, uint64(len(values)))
	encoded = appendSQLRowBinaryDeltaUvarint(encoded, uint64(len(dictionary)))
	for _, value := range dictionary {
		encoded = appendSQLRowBinaryDeltaUvarint(encoded, uint64(len(value)))
		encoded = append(encoded, value...)
	}
	for _, id := range ids {
		encoded = appendSQLRowBinaryDeltaUvarint(encoded, id)
	}
	return encoded, nil
}

// DecodeSQLStringDictionary decodes a complete dictionary string column and
// rejects invalid headers, oversized dictionaries, invalid IDs, truncation,
// duplicate dictionary entries, and trailing bytes.
func DecodeSQLStringDictionary(encoded []byte) ([]string, error) {
	if len(encoded) == 0 {
		return nil, nil
	}
	if len(encoded) < len(sqlStringDictionaryMagic) || string(encoded[:len(sqlStringDictionaryMagic)]) != string(sqlStringDictionaryMagic[:]) {
		return nil, fmt.Errorf("string dictionary format marker is invalid or truncated")
	}
	offset := len(sqlStringDictionaryMagic)
	valueCount, err := readSQLStringDictionaryUvarint(encoded, &offset, "value count")
	if err != nil {
		return nil, err
	}
	dictionaryCount, err := readSQLStringDictionaryUvarint(encoded, &offset, "dictionary count")
	if err != nil {
		return nil, err
	}
	if valueCount > maxSQLRowBinaryRows {
		return nil, fmt.Errorf("string dictionary value count %d exceeds limit %d", valueCount, maxSQLRowBinaryRows)
	}
	if dictionaryCount > valueCount {
		return nil, fmt.Errorf("string dictionary count %d exceeds value count %d", dictionaryCount, valueCount)
	}
	if valueCount > 0 && dictionaryCount == 0 {
		return nil, fmt.Errorf("string dictionary has values but no dictionary entries")
	}
	dictionary := make([]string, int(dictionaryCount))
	seen := make(map[string]struct{}, int(dictionaryCount))
	dictionaryBytes := 0
	for index := range dictionary {
		length, lengthErr := readSQLStringDictionaryUvarint(encoded, &offset, "dictionary entry length")
		if lengthErr != nil {
			return nil, lengthErr
		}
		if length > uint64(len(encoded)-offset) {
			return nil, fmt.Errorf("string dictionary entry %d length %d exceeds remaining input", index, length)
		}
		if length > uint64(maxSQLStringDictionaryBytes-dictionaryBytes) {
			return nil, fmt.Errorf("string dictionary bytes exceed limit %d", maxSQLStringDictionaryBytes)
		}
		end := offset + int(length)
		value := string(encoded[offset:end])
		if _, exists := seen[value]; exists {
			return nil, fmt.Errorf("string dictionary entry %d is duplicated", index)
		}
		seen[value] = struct{}{}
		dictionary[index] = value
		dictionaryBytes += int(length)
		offset = end
	}
	values := make([]string, int(valueCount))
	for index := range values {
		id, idErr := readSQLStringDictionaryUvarint(encoded, &offset, "value dictionary ID")
		if idErr != nil {
			return nil, idErr
		}
		if id >= dictionaryCount {
			return nil, fmt.Errorf("value %d dictionary ID %d is out of range", index, id)
		}
		values[index] = dictionary[id]
	}
	if offset != len(encoded) {
		return nil, fmt.Errorf("string dictionary has %d trailing bytes", len(encoded)-offset)
	}
	return values, nil
}

func readSQLStringDictionaryUvarint(encoded []byte, offset *int, label string) (uint64, error) {
	if offset == nil || *offset < 0 || *offset >= len(encoded) {
		return 0, fmt.Errorf("string dictionary %s is truncated", label)
	}
	value, size := binary.Uvarint(encoded[*offset:])
	if size <= 0 {
		return 0, fmt.Errorf("string dictionary %s is invalid", label)
	}
	*offset += size
	return value, nil
}
