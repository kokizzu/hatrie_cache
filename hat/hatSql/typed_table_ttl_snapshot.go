package hatSql

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	typedTableTTLStateMagic   = "HTTL1"
	typedTableTTLStateVersion = 1

	// MaxTypedTableTTLStateBytes prevents malformed or unexpectedly large
	// deadline snapshots from allocating without a caller-visible bound.
	MaxTypedTableTTLStateBytes = 16 << 20
)

var (
	ErrTypedTableTTLStateUnsupported = errors.New("typed table TTL state is unsupported")
	ErrTypedTableTTLStateInvalid     = errors.New("typed table TTL state is invalid")
)

type typedTableTTLStateRecord struct {
	key      string
	deadline int64
}

// MarshalTTLState returns a deterministic CRC-protected snapshot of
// processing-time deadlines. Row data is intentionally excluded; restore it
// first, then restore this state before starting a TTL scheduler.
func (table *TypedTable) MarshalTTLState() ([]byte, error) {
	if table == nil {
		return nil, fmt.Errorf("typed table is nil")
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	if table.ttl == nil || table.ttl.options.Mode != TypedTableTTLProcessingTime {
		return nil, ErrTypedTableTTLStateUnsupported
	}
	if len(table.keys) > int(^uint32(0)) || len(table.schema.Name) > int(^uint32(0)) {
		return nil, fmt.Errorf("%w: row or table name count overflows format", ErrTypedTableTTLStateInvalid)
	}
	size := len(typedTableTTLStateMagic) + 1 + 1 + 2 + 8 + 4 + len(table.schema.Name) + 4
	for index, key := range table.keys {
		if len(key) > int(^uint32(0)) || index >= len(table.ttl.deadlines) {
			return nil, fmt.Errorf("%w: invalid deadline row %d", ErrTypedTableTTLStateInvalid, index)
		}
		size += 4 + len(key) + 8
		if size > MaxTypedTableTTLStateBytes-4 {
			return nil, fmt.Errorf("%w: snapshot exceeds %d bytes", ErrTypedTableTTLStateInvalid, MaxTypedTableTTLStateBytes)
		}
	}
	encoded := make([]byte, 0, size+4)
	encoded = append(encoded, typedTableTTLStateMagic...)
	encoded = append(encoded, typedTableTTLStateVersion, byte(TypedTableTTLProcessingTime), 0, 0)
	encoded = appendUint64(encoded, uint64(table.ttl.options.Lifetime))
	encoded = appendUint32(encoded, uint32(len(table.schema.Name)))
	encoded = append(encoded, table.schema.Name...)
	encoded = appendUint32(encoded, uint32(len(table.keys)))
	for index, key := range table.keys {
		encoded = appendUint32(encoded, uint32(len(key)))
		encoded = append(encoded, key...)
		encoded = appendUint64(encoded, uint64(table.ttl.deadlines[index]))
	}
	return appendUint32(encoded, crc32.ChecksumIEEE(encoded)), nil
}

// RestoreTTLState validates and atomically installs a processing-time deadline
// snapshot for the table's current physical rows. The snapshot must contain
// exactly the current key set, but record order may differ.
func (table *TypedTable) RestoreTTLState(encoded []byte) error {
	if table == nil {
		return fmt.Errorf("typed table is nil")
	}
	if len(encoded) > MaxTypedTableTTLStateBytes || len(encoded) < len(typedTableTTLStateMagic)+1+1+2+8+4+4+4 {
		return fmt.Errorf("%w: snapshot length is invalid", ErrTypedTableTTLStateInvalid)
	}
	payloadLength := len(encoded) - 4
	if binary.LittleEndian.Uint32(encoded[payloadLength:]) != crc32.ChecksumIEEE(encoded[:payloadLength]) {
		return fmt.Errorf("%w: checksum mismatch", ErrTypedTableTTLStateInvalid)
	}
	payload := encoded[:payloadLength]
	position := 0
	if !bytes.Equal(payload[position:position+len(typedTableTTLStateMagic)], []byte(typedTableTTLStateMagic)) {
		return fmt.Errorf("%w: magic mismatch", ErrTypedTableTTLStateInvalid)
	}
	position += len(typedTableTTLStateMagic)
	if position+1+1+2+8 > len(payload) {
		return fmt.Errorf("%w: header is truncated", ErrTypedTableTTLStateInvalid)
	}
	if payload[position] != typedTableTTLStateVersion {
		return fmt.Errorf("%w: unsupported version %d", ErrTypedTableTTLStateInvalid, payload[position])
	}
	position++
	if TypedTableTTLMode(payload[position]) != TypedTableTTLProcessingTime {
		return ErrTypedTableTTLStateUnsupported
	}
	position++
	position += 2
	lifetime := int64(binary.LittleEndian.Uint64(payload[position : position+8]))
	position += 8
	tableName, ok := readTTLStateString(payload, &position)
	if !ok {
		return fmt.Errorf("%w: table name is truncated", ErrTypedTableTTLStateInvalid)
	}
	rowCount, ok := readTTLStateUint32(payload, &position)
	if !ok || uint64(rowCount) > uint64(len(payload)-position)/12 {
		return fmt.Errorf("%w: row count is invalid", ErrTypedTableTTLStateInvalid)
	}
	records := make([]typedTableTTLStateRecord, 0, int(rowCount))
	for index := uint32(0); index < rowCount; index++ {
		key, ok := readTTLStateString(payload, &position)
		if !ok || position+8 > len(payload) {
			return fmt.Errorf("%w: row %d is truncated", ErrTypedTableTTLStateInvalid, index)
		}
		deadline := int64(binary.LittleEndian.Uint64(payload[position : position+8]))
		position += 8
		records = append(records, typedTableTTLStateRecord{key: key, deadline: deadline})
	}
	if position != len(payload) {
		return fmt.Errorf("%w: trailing bytes", ErrTypedTableTTLStateInvalid)
	}

	table.mu.Lock()
	defer table.mu.Unlock()
	if table.ttl == nil || table.ttl.options.Mode != TypedTableTTLProcessingTime {
		return ErrTypedTableTTLStateUnsupported
	}
	if table.schema.Name != tableName || lifetime != int64(table.ttl.options.Lifetime) || len(records) != len(table.keys) {
		return fmt.Errorf("%w: schema, lifetime, or row count mismatch", ErrTypedTableTTLStateInvalid)
	}
	deadlines := make([]int64, len(table.keys))
	seen := make(map[string]struct{}, len(records))
	for _, record := range records {
		if _, duplicate := seen[record.key]; duplicate {
			return fmt.Errorf("%w: duplicate key %q", ErrTypedTableTTLStateInvalid, record.key)
		}
		index, exists := table.positions[record.key]
		if !exists {
			return fmt.Errorf("%w: key %q is not in table", ErrTypedTableTTLStateInvalid, record.key)
		}
		seen[record.key] = struct{}{}
		deadlines[index] = record.deadline
	}
	if len(seen) != len(table.keys) {
		return fmt.Errorf("%w: snapshot does not cover every row", ErrTypedTableTTLStateInvalid)
	}
	table.ttl.deadlines = deadlines
	table.rebuildTypedTableTTLExpiryIndexLocked(len(table.keys))
	return nil
}

func appendUint32(destination []byte, value uint32) []byte {
	var encoded [4]byte
	binary.LittleEndian.PutUint32(encoded[:], value)
	return append(destination, encoded[:]...)
}

func appendUint64(destination []byte, value uint64) []byte {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], value)
	return append(destination, encoded[:]...)
}

func readTTLStateUint32(encoded []byte, position *int) (uint32, bool) {
	if *position < 0 || len(encoded)-*position < 4 {
		return 0, false
	}
	value := binary.LittleEndian.Uint32(encoded[*position : *position+4])
	*position += 4
	return value, true
}

func readTTLStateString(encoded []byte, position *int) (string, bool) {
	length, ok := readTTLStateUint32(encoded, position)
	if !ok || uint64(length) > uint64(len(encoded)-*position) {
		return "", false
	}
	end := *position + int(length)
	value := string(encoded[*position:end])
	*position = end
	return value, true
}
