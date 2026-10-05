package hatSql

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sort"
)

const (
	// MaxSQLPublicationSnapshotBytes bounds one durable publication snapshot.
	// It applies before any decoded history is published.
	MaxSQLPublicationSnapshotBytes = 64 << 20

	sqlPublicationSnapshotHeaderSize       = 4 + 8
	sqlPublicationSnapshotTrailerSize      = 4
	sqlPublicationSnapshotMaxValueDepth    = 16
	sqlPublicationSnapshotMaxValueElements = 1 << 16

	sqlPublicationSnapshotValueScalar byte = 1
	sqlPublicationSnapshotValueMap    byte = 2
	sqlPublicationSnapshotValueSlice  byte = 3
)

var sqlPublicationSnapshotMagic = [4]byte{'H', 'P', 'S', '1'}

// MarshalBinary returns a bounded, CRC-protected snapshot of publication
// history. Subscriber channels and checkpoints are deliberately excluded;
// consumers persist their own checkpoint after the downstream side effect.
func (publication *SQLPublication) MarshalBinary() ([]byte, error) {
	if publication == nil {
		return nil, fmt.Errorf("%w: nil publication", ErrSQLPublicationInvalid)
	}
	publication.mu.Lock()
	defer publication.mu.Unlock()

	payload := make([]byte, 0, 1024)
	appendSnapshotUvarint := func(value uint64) {
		var encoded [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(encoded[:], value)
		payload = append(payload, encoded[:n]...)
	}
	appendSnapshotBytes := func(value []byte) error {
		if len(payload) > MaxSQLPublicationSnapshotBytes || len(value) > MaxSQLPublicationSnapshotBytes-len(payload) {
			return fmt.Errorf("%w: snapshot exceeds %d bytes", ErrSQLPublicationLimit, MaxSQLPublicationSnapshotBytes)
		}
		payload = append(payload, value...)
		return nil
	}
	appendSnapshotString := func(value string) error {
		if err := validateSQLPublicationText(value, maxSQLPublicationTextBytes, "snapshot text"); err != nil {
			return err
		}
		appendSnapshotUvarint(uint64(len(value)))
		return appendSnapshotBytes([]byte(value))
	}

	for _, option := range []int{
		publication.options.maxHistoryBatches,
		publication.options.maxBatchDeltas,
		publication.options.maxPendingBatches,
		publication.options.maxSubscribers,
		publication.options.maxColumns,
		publication.options.maxRowColumns,
	} {
		appendSnapshotUvarint(uint64(option))
	}
	if err := appendSnapshotString(publication.name); err != nil {
		return nil, err
	}
	appendSnapshotUvarint(uint64(len(publication.columns)))
	for _, column := range publication.columns {
		if err := appendSnapshotString(column); err != nil {
			return nil, err
		}
	}
	appendSnapshotUvarint(uint64(len(publication.history)))
	for _, batch := range publication.history {
		var fixed [17]byte
		binary.BigEndian.PutUint64(fixed[0:8], batch.Revision)
		binary.BigEndian.PutUint64(fixed[8:16], batch.Frontier)
		if batch.Progress {
			fixed[16] |= 1 << 0
		}
		if batch.Complete {
			fixed[16] |= 1 << 1
		}
		if batch.Reset {
			fixed[16] |= 1 << 2
		}
		if err := appendSnapshotBytes(fixed[:]); err != nil {
			return nil, err
		}
		appendSnapshotUvarint(uint64(len(batch.Columns)))
		for _, column := range batch.Columns {
			if err := appendSnapshotString(column); err != nil {
				return nil, err
			}
		}
		appendSnapshotUvarint(uint64(len(batch.Deltas)))
		for _, delta := range batch.Deltas {
			var encodedDiff [binary.MaxVarintLen64]byte
			n := binary.PutVarint(encodedDiff[:], delta.Diff)
			appendSnapshotUvarint(uint64(n))
			if err := appendSnapshotBytes(encodedDiff[:n]); err != nil {
				return nil, err
			}
			encodedRow, err := encodeSQLPublicationRow(delta.Row)
			if err != nil {
				return nil, err
			}
			appendSnapshotUvarint(uint64(len(encodedRow)))
			if err := appendSnapshotBytes(encodedRow); err != nil {
				return nil, err
			}
		}
	}
	var latest [17]byte
	binary.BigEndian.PutUint64(latest[0:8], publication.latest.Revision)
	binary.BigEndian.PutUint64(latest[8:16], publication.latest.Frontier)
	if publication.closed {
		latest[16] = 1
	}
	if err := appendSnapshotBytes(latest[:]); err != nil {
		return nil, err
	}

	if len(payload) > MaxSQLPublicationSnapshotBytes {
		return nil, fmt.Errorf("%w: snapshot exceeds %d bytes", ErrSQLPublicationLimit, MaxSQLPublicationSnapshotBytes)
	}
	frame := make([]byte, sqlPublicationSnapshotHeaderSize+len(payload)+sqlPublicationSnapshotTrailerSize)
	copy(frame[:4], sqlPublicationSnapshotMagic[:])
	binary.BigEndian.PutUint64(frame[4:12], uint64(len(payload)))
	copy(frame[12:], payload)
	binary.BigEndian.PutUint32(frame[12+len(payload):], crc32.ChecksumIEEE(frame[:12+len(payload)]))
	return frame, nil
}

// UnmarshalSQLPublication restores a publication from MarshalBinary output.
// The returned publication has no subscribers; consumers must restore and
// acknowledge their own checkpoints separately.
func UnmarshalSQLPublication(encoded []byte) (*SQLPublication, error) {
	if len(encoded) < sqlPublicationSnapshotHeaderSize+sqlPublicationSnapshotTrailerSize {
		return nil, fmt.Errorf("%w: snapshot is truncated", ErrSQLPublicationInvalid)
	}
	if len(encoded) > MaxSQLPublicationSnapshotBytes+sqlPublicationSnapshotHeaderSize+sqlPublicationSnapshotTrailerSize {
		return nil, fmt.Errorf("%w: snapshot exceeds %d bytes", ErrSQLPublicationLimit, MaxSQLPublicationSnapshotBytes)
	}
	if string(encoded[:4]) != string(sqlPublicationSnapshotMagic[:]) {
		return nil, fmt.Errorf("%w: snapshot header", ErrSQLPublicationInvalid)
	}
	payloadLength := binary.BigEndian.Uint64(encoded[4:12])
	if payloadLength > MaxSQLPublicationSnapshotBytes {
		return nil, fmt.Errorf("%w: snapshot payload exceeds %d bytes", ErrSQLPublicationLimit, MaxSQLPublicationSnapshotBytes)
	}
	expectedLength := uint64(sqlPublicationSnapshotHeaderSize+sqlPublicationSnapshotTrailerSize) + payloadLength
	if expectedLength != uint64(len(encoded)) {
		return nil, fmt.Errorf("%w: snapshot length", ErrSQLPublicationInvalid)
	}
	checksumOffset := len(encoded) - sqlPublicationSnapshotTrailerSize
	if want, got := binary.BigEndian.Uint32(encoded[checksumOffset:]), crc32.ChecksumIEEE(encoded[:checksumOffset]); want != got {
		return nil, fmt.Errorf("%w: snapshot checksum", ErrSQLPublicationInvalid)
	}

	reader := sqlPublicationSnapshotReader{data: encoded[sqlPublicationSnapshotHeaderSize:checksumOffset]}
	values := make([]int, 6)
	for index := range values {
		value, err := reader.readInt("option")
		if err != nil {
			return nil, err
		}
		values[index] = value
	}
	name, err := reader.readString(maxSQLPublicationTextBytes, "publication name")
	if err != nil {
		return nil, err
	}
	columnCount, err := reader.readCount(maxSQLPublicationColumns, "columns")
	if err != nil {
		return nil, err
	}
	columns := make([]string, columnCount)
	for index := range columns {
		columns[index], err = reader.readString(maxSQLPublicationTextBytes, "column")
		if err != nil {
			return nil, err
		}
	}
	options := SQLPublicationOptions{
		MaxHistoryBatches: values[0],
		MaxBatchDeltas:    values[1],
		MaxPendingBatches: values[2],
		MaxSubscribers:    values[3],
		MaxColumns:        values[4],
		MaxRowColumns:     values[5],
	}
	publication, err := NewSQLPublication(name, columns, options)
	if err != nil {
		return nil, err
	}
	historyCount, err := reader.readCount(publication.options.maxHistoryBatches, "history batches")
	if err != nil {
		return nil, err
	}
	history := make([]SQLPublicationBatch, historyCount)
	for index := range history {
		batch, err := reader.readBatch(publication.options)
		if err != nil {
			return nil, fmt.Errorf("%w: history batch %d: %v", ErrSQLPublicationInvalid, index, err)
		}
		history[index] = batch
	}
	latestRevision, err := reader.readUint64("latest revision")
	if err != nil {
		return nil, err
	}
	latestFrontier, err := reader.readUint64("latest frontier")
	if err != nil {
		return nil, err
	}
	closed, err := reader.readByte("closed")
	if err != nil {
		return nil, err
	}
	if closed > 1 || reader.remaining() != 0 {
		return nil, fmt.Errorf("%w: snapshot trailing state", ErrSQLPublicationInvalid)
	}
	if err := validateSQLPublicationHistory(publication, history, SQLPublicationCheckpoint{Revision: latestRevision, Frontier: latestFrontier}, closed == 1); err != nil {
		return nil, err
	}
	publication.history = history
	publication.latest = SQLPublicationCheckpoint{Revision: latestRevision, Frontier: latestFrontier}
	publication.closed = closed == 1
	return publication, nil
}

type sqlPublicationSnapshotReader struct {
	data []byte
	off  int
}

func (reader *sqlPublicationSnapshotReader) remaining() int {
	if reader == nil || reader.off >= len(reader.data) {
		return 0
	}
	return len(reader.data) - reader.off
}

func (reader *sqlPublicationSnapshotReader) readByte(field string) (byte, error) {
	if reader == nil || reader.off >= len(reader.data) {
		return 0, fmt.Errorf("%w: truncated %s", ErrSQLPublicationInvalid, field)
	}
	value := reader.data[reader.off]
	reader.off++
	return value, nil
}

func (reader *sqlPublicationSnapshotReader) readUint64(field string) (uint64, error) {
	if reader == nil || len(reader.data)-reader.off < 8 {
		return 0, fmt.Errorf("%w: truncated %s", ErrSQLPublicationInvalid, field)
	}
	value := binary.BigEndian.Uint64(reader.data[reader.off : reader.off+8])
	reader.off += 8
	return value, nil
}

func (reader *sqlPublicationSnapshotReader) readUvarint(field string) (uint64, error) {
	if reader == nil || reader.off >= len(reader.data) {
		return 0, fmt.Errorf("%w: truncated %s", ErrSQLPublicationInvalid, field)
	}
	value, size := binary.Uvarint(reader.data[reader.off:])
	if size <= 0 {
		return 0, fmt.Errorf("%w: invalid %s", ErrSQLPublicationInvalid, field)
	}
	reader.off += size
	return value, nil
}

func (reader *sqlPublicationSnapshotReader) readInt(field string) (int, error) {
	value, err := reader.readUvarint(field)
	if err != nil {
		return 0, err
	}
	maxInt := uint64(^uint(0) >> 1)
	if value > maxInt {
		return 0, fmt.Errorf("%w: %s is too large", ErrSQLPublicationLimit, field)
	}
	return int(value), nil
}

func (reader *sqlPublicationSnapshotReader) readCount(maximum int, field string) (int, error) {
	value, err := reader.readInt(field)
	if err != nil {
		return 0, err
	}
	if value > maximum {
		return 0, fmt.Errorf("%w: %s", ErrSQLPublicationLimit, field)
	}
	return value, nil
}

func (reader *sqlPublicationSnapshotReader) readBytes(maximum int, field string) ([]byte, error) {
	length, err := reader.readInt(field + " length")
	if err != nil {
		return nil, err
	}
	if length > maximum || length > reader.remaining() {
		return nil, fmt.Errorf("%w: %s length", ErrSQLPublicationLimit, field)
	}
	value := reader.data[reader.off : reader.off+length]
	reader.off += length
	return value, nil
}

func (reader *sqlPublicationSnapshotReader) readString(maximum int, field string) (string, error) {
	value, err := reader.readBytes(maximum, field)
	if err != nil {
		return "", err
	}
	result := string(value)
	if err := validateSQLPublicationText(result, maximum, field); err != nil {
		return "", err
	}
	return result, nil
}

func (reader *sqlPublicationSnapshotReader) readBatch(options normalizedSQLPublicationOptions) (SQLPublicationBatch, error) {
	revision, err := reader.readUint64("revision")
	if err != nil {
		return SQLPublicationBatch{}, err
	}
	frontier, err := reader.readUint64("frontier")
	if err != nil {
		return SQLPublicationBatch{}, err
	}
	flags, err := reader.readByte("batch flags")
	if err != nil {
		return SQLPublicationBatch{}, err
	}
	if flags&^byte(7) != 0 {
		return SQLPublicationBatch{}, fmt.Errorf("%w: batch flags", ErrSQLPublicationInvalid)
	}
	columnCount, err := reader.readCount(options.maxColumns, "batch columns")
	if err != nil {
		return SQLPublicationBatch{}, err
	}
	columns := make([]string, columnCount)
	for index := range columns {
		columns[index], err = reader.readString(maxSQLPublicationTextBytes, "batch column")
		if err != nil {
			return SQLPublicationBatch{}, err
		}
	}
	deltaCount, err := reader.readCount(options.maxBatchDeltas, "batch deltas")
	if err != nil {
		return SQLPublicationBatch{}, err
	}
	deltas := make([]SQLPublicationDelta, deltaCount)
	for index := range deltas {
		diffBytes, err := reader.readBytes(binary.MaxVarintLen64, "delta diff")
		if err != nil {
			return SQLPublicationBatch{}, err
		}
		diff, size := binary.Varint(diffBytes)
		if size <= 0 || size != len(diffBytes) || diff == 0 {
			return SQLPublicationBatch{}, fmt.Errorf("%w: delta %d", ErrSQLPublicationInvalid, index)
		}
		rowBytes, err := reader.readBytes(MaxSQLPublicationSnapshotBytes, "delta row")
		if err != nil {
			return SQLPublicationBatch{}, err
		}
		row, err := decodeSQLPublicationRow(rowBytes, options.maxRowColumns)
		if err != nil {
			return SQLPublicationBatch{}, fmt.Errorf("%w: delta %d row: %v", ErrSQLPublicationInvalid, index, err)
		}
		deltas[index] = SQLPublicationDelta{Row: row, Diff: diff}
	}
	return SQLPublicationBatch{
		Revision: revision,
		Frontier: frontier,
		Columns:  columns,
		Deltas:   deltas,
		Progress: flags&(1<<0) != 0,
		Complete: flags&(1<<1) != 0,
		Reset:    flags&(1<<2) != 0,
	}, nil
}

func validateSQLPublicationHistory(publication *SQLPublication, history []SQLPublicationBatch, latest SQLPublicationCheckpoint, closed bool) error {
	if publication == nil {
		return fmt.Errorf("%w: nil publication", ErrSQLPublicationInvalid)
	}
	if latest.Revision == 0 && latest.Frontier != 0 {
		return fmt.Errorf("%w: latest frontier without revision", ErrSQLPublicationInvalid)
	}
	if len(history) == 0 {
		if latest.Revision != 0 || latest.Frontier != 0 {
			return fmt.Errorf("%w: latest state without history", ErrSQLPublicationInvalid)
		}
		return nil
	}
	previous := uint64(0)
	var previousFrontier uint64
	for index, batch := range history {
		if len(batch.Columns) > 0 && !sameSQLPublicationColumns(batch.Columns, publication.columns) {
			return fmt.Errorf("%w: history schema at batch %d", ErrSQLPublicationInvalid, index)
		}
		if batch.Revision == 0 || (index > 0 && batch.Revision != previous+1) {
			return fmt.Errorf("%w: history revision at batch %d", ErrSQLPublicationSequence, index)
		}
		if index > 0 && batch.Frontier < previousFrontier {
			return fmt.Errorf("%w: history frontier at batch %d", ErrSQLPublicationSequence, index)
		}
		if batch.Complete && !closed {
			return fmt.Errorf("%w: complete history is not closed", ErrSQLPublicationInvalid)
		}
		for deltaIndex, delta := range batch.Deltas {
			if delta.Diff == 0 {
				return fmt.Errorf("%w: zero delta at batch %d/%d", ErrSQLPublicationInvalid, index, deltaIndex)
			}
			if _, err := publication.copyAndValidateRow(delta.Row); err != nil {
				return err
			}
		}
		previous = batch.Revision
		previousFrontier = batch.Frontier
	}
	if latest.Revision != previous || latest.Frontier != previousFrontier {
		return fmt.Errorf("%w: latest state does not match history", ErrSQLPublicationSequence)
	}
	return nil
}

func encodeSQLPublicationRow(row Row) ([]byte, error) {
	if row == nil {
		return []byte{0}, nil
	}
	keys := make([]string, 0, len(row))
	for key := range row {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	encoded := make([]byte, 0, 1+binary.MaxVarintLen64)
	encoded = append(encoded, 1)
	encoded = appendSQLPublicationUvarint(encoded, uint64(len(keys)))
	for _, key := range keys {
		if err := validateSQLPublicationText(key, maxSQLPublicationTextBytes, "row column"); err != nil {
			return nil, err
		}
		encoded = appendSQLPublicationBytes(encoded, []byte(key))
		value, err := encodeSQLPublicationValue(row[key], 0)
		if err != nil {
			return nil, err
		}
		encoded = appendSQLPublicationBytes(encoded, value)
		if len(encoded) > MaxSQLPublicationSnapshotBytes {
			return nil, fmt.Errorf("%w: row exceeds %d bytes", ErrSQLPublicationLimit, MaxSQLPublicationSnapshotBytes)
		}
	}
	return encoded, nil
}

func decodeSQLPublicationRow(encoded []byte, maximumColumns int) (Row, error) {
	reader := sqlPublicationSnapshotReader{data: encoded}
	present, err := reader.readByte("row presence")
	if err != nil {
		return nil, err
	}
	if present == 0 {
		if reader.remaining() != 0 {
			return nil, fmt.Errorf("%w: nil row has trailing data", ErrSQLPublicationInvalid)
		}
		return nil, nil
	}
	if present != 1 {
		return nil, fmt.Errorf("%w: row presence", ErrSQLPublicationInvalid)
	}
	count, err := reader.readCount(maximumColumns, "row columns")
	if err != nil {
		return nil, err
	}
	row := make(Row, count)
	for index := 0; index < count; index++ {
		key, err := reader.readString(maxSQLPublicationTextBytes, "row column")
		if err != nil {
			return nil, err
		}
		if _, exists := row[key]; exists {
			return nil, fmt.Errorf("%w: duplicate row column %q", ErrSQLPublicationInvalid, key)
		}
		valueBytes, err := reader.readBytes(MaxSQLPublicationSnapshotBytes, "row value")
		if err != nil {
			return nil, err
		}
		value, err := decodeSQLPublicationValue(valueBytes, 0)
		if err != nil {
			return nil, err
		}
		row[key] = value
	}
	if reader.remaining() != 0 {
		return nil, fmt.Errorf("%w: row trailing data", ErrSQLPublicationInvalid)
	}
	return row, nil
}

func encodeSQLPublicationValue(value interface{}, depth int) ([]byte, error) {
	if depth > sqlPublicationSnapshotMaxValueDepth {
		return nil, fmt.Errorf("%w: value nesting depth", ErrSQLPublicationLimit)
	}
	if scalar, err := sqlAppendArgExtremeScalar(nil, value); err == nil {
		encoded := []byte{sqlPublicationSnapshotValueScalar}
		encoded = appendSQLPublicationBytes(encoded, scalar)
		return encoded, nil
	}
	switch value := value.(type) {
	case map[string]interface{}:
		if len(value) > sqlPublicationSnapshotMaxValueElements {
			return nil, fmt.Errorf("%w: map value elements", ErrSQLPublicationLimit)
		}
		keys := make([]string, 0, len(value))
		for key := range value {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		encoded := []byte{sqlPublicationSnapshotValueMap}
		encoded = appendSQLPublicationUvarint(encoded, uint64(len(keys)))
		for _, key := range keys {
			if err := validateSQLPublicationText(key, maxSQLPublicationTextBytes, "map key"); err != nil {
				return nil, err
			}
			encoded = appendSQLPublicationBytes(encoded, []byte(key))
			child, err := encodeSQLPublicationValue(value[key], depth+1)
			if err != nil {
				return nil, err
			}
			encoded = appendSQLPublicationBytes(encoded, child)
			if len(encoded) > MaxSQLPublicationSnapshotBytes {
				return nil, fmt.Errorf("%w: map exceeds %d bytes", ErrSQLPublicationLimit, MaxSQLPublicationSnapshotBytes)
			}
		}
		return encoded, nil
	case []interface{}:
		if len(value) > sqlPublicationSnapshotMaxValueElements {
			return nil, fmt.Errorf("%w: slice value elements", ErrSQLPublicationLimit)
		}
		encoded := []byte{sqlPublicationSnapshotValueSlice}
		encoded = appendSQLPublicationUvarint(encoded, uint64(len(value)))
		for _, item := range value {
			child, err := encodeSQLPublicationValue(item, depth+1)
			if err != nil {
				return nil, err
			}
			encoded = appendSQLPublicationBytes(encoded, child)
			if len(encoded) > MaxSQLPublicationSnapshotBytes {
				return nil, fmt.Errorf("%w: slice exceeds %d bytes", ErrSQLPublicationLimit, MaxSQLPublicationSnapshotBytes)
			}
		}
		return encoded, nil
	default:
		return nil, fmt.Errorf("%w: unsupported publication value type %T", ErrSQLPublicationInvalid, value)
	}
}

func decodeSQLPublicationValue(encoded []byte, depth int) (interface{}, error) {
	if depth > sqlPublicationSnapshotMaxValueDepth {
		return nil, fmt.Errorf("%w: value nesting depth", ErrSQLPublicationLimit)
	}
	reader := sqlPublicationSnapshotReader{data: encoded}
	tag, err := reader.readByte("value tag")
	if err != nil {
		return nil, err
	}
	switch tag {
	case sqlPublicationSnapshotValueScalar:
		valueBytes, err := reader.readBytes(maxSQLPublicationTextBytes, "scalar value")
		if err != nil {
			return nil, err
		}
		value, next, err := sqlReadArgExtremeScalar(valueBytes, 0)
		if err != nil || next != len(valueBytes) {
			if err == nil {
				err = fmt.Errorf("scalar value has trailing data")
			}
			return nil, fmt.Errorf("%w: %v", ErrSQLPublicationInvalid, err)
		}
		if reader.remaining() != 0 {
			return nil, fmt.Errorf("%w: scalar value trailing data", ErrSQLPublicationInvalid)
		}
		return value, nil
	case sqlPublicationSnapshotValueMap:
		count, err := reader.readCount(sqlPublicationSnapshotMaxValueElements, "map value elements")
		if err != nil {
			return nil, err
		}
		value := make(map[string]interface{}, count)
		for index := 0; index < count; index++ {
			key, err := reader.readString(maxSQLPublicationTextBytes, "map key")
			if err != nil {
				return nil, err
			}
			if _, exists := value[key]; exists {
				return nil, fmt.Errorf("%w: duplicate map key %q", ErrSQLPublicationInvalid, key)
			}
			childBytes, err := reader.readBytes(MaxSQLPublicationSnapshotBytes, "map value")
			if err != nil {
				return nil, err
			}
			child, err := decodeSQLPublicationValue(childBytes, depth+1)
			if err != nil {
				return nil, err
			}
			value[key] = child
		}
		if reader.remaining() != 0 {
			return nil, fmt.Errorf("%w: map value trailing data", ErrSQLPublicationInvalid)
		}
		return value, nil
	case sqlPublicationSnapshotValueSlice:
		count, err := reader.readCount(sqlPublicationSnapshotMaxValueElements, "slice value elements")
		if err != nil {
			return nil, err
		}
		value := make([]interface{}, count)
		for index := range value {
			childBytes, err := reader.readBytes(MaxSQLPublicationSnapshotBytes, "slice value")
			if err != nil {
				return nil, err
			}
			value[index], err = decodeSQLPublicationValue(childBytes, depth+1)
			if err != nil {
				return nil, err
			}
		}
		if reader.remaining() != 0 {
			return nil, fmt.Errorf("%w: slice value trailing data", ErrSQLPublicationInvalid)
		}
		return value, nil
	default:
		return nil, fmt.Errorf("%w: unknown value tag %d", ErrSQLPublicationInvalid, tag)
	}
}

func appendSQLPublicationUvarint(dst []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(encoded[:], value)
	return append(dst, encoded[:n]...)
}

func appendSQLPublicationBytes(dst, value []byte) []byte {
	dst = appendSQLPublicationUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}
