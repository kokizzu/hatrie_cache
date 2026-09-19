package hatSql

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"sort"
	"time"
)

const (
	differentialCheckpointVersion                = 1
	differentialCheckpointMagic                  = "HDF1"
	DefaultDifferentialCheckpointMaxEncodedBytes = 64 << 20
	DefaultDifferentialCheckpointMaxRows         = 1 << 20
	DefaultDifferentialCheckpointMaxFieldsPerRow = 1024
	DefaultDifferentialCheckpointMaxValueDepth   = 16
	differentialCheckpointMinInt8                = -(1 << 7)
	differentialCheckpointMaxInt8                = 1<<7 - 1
	differentialCheckpointMinInt16               = -(1 << 15)
	differentialCheckpointMaxInt16               = 1<<15 - 1
	differentialCheckpointMinInt32               = -(1 << 31)
	differentialCheckpointMaxInt32               = 1<<31 - 1
	differentialCheckpointMaxUint8               = 1<<8 - 1
	differentialCheckpointMaxUint16              = 1<<16 - 1
	differentialCheckpointMaxUint32              = 1<<32 - 1
)

var (
	// ErrDifferentialCheckpointInvalid reports invalid checkpoint input or options.
	ErrDifferentialCheckpointInvalid = errors.New("differential checkpoint is invalid")
	// ErrDifferentialCheckpointCorrupt reports a malformed or checksum-invalid payload.
	ErrDifferentialCheckpointCorrupt = errors.New("differential checkpoint is corrupt")
	// ErrDifferentialCheckpointTooLarge reports a configured checkpoint bound.
	ErrDifferentialCheckpointTooLarge = errors.New("differential checkpoint is too large")
	// ErrDifferentialCheckpointUnsupported reports a value type without a stable wire tag.
	ErrDifferentialCheckpointUnsupported = errors.New("differential checkpoint value type is unsupported")
)

var differentialCheckpointCRCTable = crc32.MakeTable(crc32.Castagnoli)

// DifferentialCheckpoint is a portable batch of signed updates and the
// logical frontier at which the batch was captured.
type DifferentialCheckpoint struct {
	Frontier uint64
	Rows     []DifferentialRow
}

// DifferentialCheckpointCodecOptions bounds both encoding and decoding.
// Zero fields use the conservative defaults documented by the constants.
type DifferentialCheckpointCodecOptions struct {
	MaxEncodedBytes int
	MaxRows         int
	MaxFieldsPerRow int
	MaxValueDepth   int
}

const (
	differentialCheckpointValueNil byte = iota
	differentialCheckpointValueBool
	differentialCheckpointValueString
	differentialCheckpointValueBytes
	differentialCheckpointValueInt
	differentialCheckpointValueInt8
	differentialCheckpointValueInt16
	differentialCheckpointValueInt32
	differentialCheckpointValueInt64
	differentialCheckpointValueUint
	differentialCheckpointValueUint8
	differentialCheckpointValueUint16
	differentialCheckpointValueUint32
	differentialCheckpointValueUint64
	differentialCheckpointValueFloat32
	differentialCheckpointValueFloat64
	differentialCheckpointValueDuration
	differentialCheckpointValueTime
	differentialCheckpointValueJSONNumber
	differentialCheckpointValueRow
	differentialCheckpointValueMap
	differentialCheckpointValueInterfaceSlice
	differentialCheckpointValueStringSlice
	differentialCheckpointValueInt64Slice
	differentialCheckpointValueUint64Slice
	differentialCheckpointValueFloat64Slice
	differentialCheckpointValueBoolSlice
	differentialCheckpointValueJSONRaw
)

// EncodeDifferentialCheckpoint encodes a checkpoint using the default bounds.
func EncodeDifferentialCheckpoint(checkpoint DifferentialCheckpoint) ([]byte, error) {
	return EncodeDifferentialCheckpointWithOptions(checkpoint, DifferentialCheckpointCodecOptions{})
}

// EncodeDifferentialCheckpointWithOptions encodes a deterministic HDF1
// payload. Map fields are sorted, rows are canonically ordered, and the
// payload ends with a CRC32C checksum.
func EncodeDifferentialCheckpointWithOptions(checkpoint DifferentialCheckpoint, options DifferentialCheckpointCodecOptions) ([]byte, error) {
	normalized, err := normalizeDifferentialCheckpointCodecOptions(options)
	if err != nil {
		return nil, err
	}
	if len(checkpoint.Rows) > normalized.MaxRows {
		return nil, ErrDifferentialCheckpointTooLarge
	}
	rowCapacity := len(checkpoint.Rows) * 32
	if rowCapacity > normalized.MaxEncodedBytes {
		rowCapacity = normalized.MaxEncodedBytes
	}
	rowData := make([]byte, 0, rowCapacity)
	encodedRows := make([]differentialCheckpointRowSpan, len(checkpoint.Rows))
	for index, row := range checkpoint.Rows {
		start := len(rowData)
		rowData, err = appendDifferentialCheckpointEncodedRow(rowData, row, normalized)
		if err != nil {
			return nil, fmt.Errorf("row %d: %w", index, err)
		}
		encodedRows[index] = differentialCheckpointRowSpan{start: start, end: len(rowData)}
	}
	sort.Slice(encodedRows, func(left, right int) bool {
		leftRow := rowData[encodedRows[left].start:encodedRows[left].end]
		rightRow := rowData[encodedRows[right].start:encodedRows[right].end]
		return bytes.Compare(leftRow, rightRow) < 0
	})

	encoded := make([]byte, 0, len(rowData)+32)
	encoded = append(encoded, differentialCheckpointMagic...)
	encoded = append(encoded, differentialCheckpointVersion)
	encoded = appendDifferentialCheckpointUvarint(encoded, checkpoint.Frontier)
	encoded = appendDifferentialCheckpointUvarint(encoded, uint64(len(encodedRows)))
	for _, row := range encodedRows {
		encoded = append(encoded, rowData[row.start:row.end]...)
	}
	if len(encoded)+4 > normalized.MaxEncodedBytes {
		return nil, ErrDifferentialCheckpointTooLarge
	}
	checksum := crc32.Checksum(encoded, differentialCheckpointCRCTable)
	var checksumBytes [4]byte
	binary.LittleEndian.PutUint32(checksumBytes[:], checksum)
	encoded = append(encoded, checksumBytes[:]...)
	return encoded, nil
}

// DecodeDifferentialCheckpoint validates and decodes a complete HDF1 payload.
// It returns no partial checkpoint when any row, value, bound, or checksum is
// invalid.
func DecodeDifferentialCheckpoint(encoded []byte) (DifferentialCheckpoint, error) {
	return DecodeDifferentialCheckpointWithOptions(encoded, DifferentialCheckpointCodecOptions{})
}

// DecodeDifferentialCheckpointWithOptions decodes a bounded HDF1 payload.
func DecodeDifferentialCheckpointWithOptions(encoded []byte, options DifferentialCheckpointCodecOptions) (DifferentialCheckpoint, error) {
	normalized, err := normalizeDifferentialCheckpointCodecOptions(options)
	if err != nil {
		return DifferentialCheckpoint{}, err
	}
	if len(encoded) > normalized.MaxEncodedBytes {
		return DifferentialCheckpoint{}, ErrDifferentialCheckpointTooLarge
	}
	if len(encoded) < len(differentialCheckpointMagic)+1+4 {
		return DifferentialCheckpoint{}, ErrDifferentialCheckpointCorrupt
	}
	body := encoded[:len(encoded)-4]
	if string(body[:len(differentialCheckpointMagic)]) != differentialCheckpointMagic {
		return DifferentialCheckpoint{}, ErrDifferentialCheckpointCorrupt
	}
	if body[len(differentialCheckpointMagic)] != differentialCheckpointVersion {
		return DifferentialCheckpoint{}, ErrDifferentialCheckpointCorrupt
	}
	actualChecksum := crc32.Checksum(body, differentialCheckpointCRCTable)
	wireChecksum := binary.LittleEndian.Uint32(encoded[len(encoded)-4:])
	if actualChecksum != wireChecksum {
		return DifferentialCheckpoint{}, ErrDifferentialCheckpointCorrupt
	}

	reader := differentialCheckpointReader{data: body, offset: len(differentialCheckpointMagic) + 1, options: normalized}
	frontier, err := reader.readUvarint()
	if err != nil {
		return DifferentialCheckpoint{}, err
	}
	rowCount, err := reader.readUvarint()
	if err != nil {
		return DifferentialCheckpoint{}, err
	}
	if rowCount > uint64(normalized.MaxRows) {
		return DifferentialCheckpoint{}, ErrDifferentialCheckpointTooLarge
	}
	rows := make([]DifferentialRow, int(rowCount))
	for index := range rows {
		rows[index], err = reader.readRow()
		if err != nil {
			return DifferentialCheckpoint{}, fmt.Errorf("row %d: %w", index, err)
		}
	}
	if reader.offset != len(body) {
		return DifferentialCheckpoint{}, ErrDifferentialCheckpointCorrupt
	}
	return DifferentialCheckpoint{Frontier: frontier, Rows: rows}, nil
}

func normalizeDifferentialCheckpointCodecOptions(options DifferentialCheckpointCodecOptions) (DifferentialCheckpointCodecOptions, error) {
	if options.MaxEncodedBytes < 0 || options.MaxRows < 0 || options.MaxFieldsPerRow < 0 || options.MaxValueDepth < 0 {
		return DifferentialCheckpointCodecOptions{}, ErrDifferentialCheckpointInvalid
	}
	if options.MaxEncodedBytes == 0 {
		options.MaxEncodedBytes = DefaultDifferentialCheckpointMaxEncodedBytes
	}
	if options.MaxRows == 0 {
		options.MaxRows = DefaultDifferentialCheckpointMaxRows
	}
	if options.MaxFieldsPerRow == 0 {
		options.MaxFieldsPerRow = DefaultDifferentialCheckpointMaxFieldsPerRow
	}
	if options.MaxValueDepth == 0 {
		options.MaxValueDepth = DefaultDifferentialCheckpointMaxValueDepth
	}
	return options, nil
}

type differentialCheckpointRowSpan struct {
	start int
	end   int
}

func appendDifferentialCheckpointEncodedRow(destination []byte, row DifferentialRow, options DifferentialCheckpointCodecOptions) ([]byte, error) {
	if row.Key == "" {
		return nil, ErrDifferentialCheckpointInvalid
	}
	destination = appendDifferentialCheckpointString(destination, row.Key)
	destination = appendDifferentialCheckpointUvarint(destination, row.Time)
	destination = appendDifferentialCheckpointUvarint(destination, differentialCheckpointZigZag(row.Diff))
	var err error
	destination, err = appendDifferentialCheckpointRow(destination, row.Row, options)
	if err != nil {
		return nil, err
	}
	return destination, nil
}

func appendDifferentialCheckpointRow(destination []byte, row Row, options DifferentialCheckpointCodecOptions) ([]byte, error) {
	if row == nil {
		return append(destination, 0), nil
	}
	destination = append(destination, 1)
	return appendDifferentialCheckpointMap(destination, map[string]interface{}(row), options, 0)
}

func appendDifferentialCheckpointMap(destination []byte, values map[string]interface{}, options DifferentialCheckpointCodecOptions, depth int) ([]byte, error) {
	if depth > options.MaxValueDepth {
		return nil, ErrDifferentialCheckpointTooLarge
	}
	if len(values) > options.MaxFieldsPerRow {
		return nil, ErrDifferentialCheckpointTooLarge
	}
	var keyStorage [16]string
	keys := keyStorage[:0]
	if len(values) > len(keyStorage) {
		keys = make([]string, 0, len(values))
	}
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	destination = appendDifferentialCheckpointUvarint(destination, uint64(len(keys)))
	for _, key := range keys {
		destination = appendDifferentialCheckpointString(destination, key)
		var err error
		destination, err = appendDifferentialCheckpointValue(destination, values[key], options, depth+1)
		if err != nil {
			return nil, err
		}
	}
	return destination, nil
}

func appendDifferentialCheckpointValue(destination []byte, value interface{}, options DifferentialCheckpointCodecOptions, depth int) ([]byte, error) {
	if depth > options.MaxValueDepth {
		return nil, ErrDifferentialCheckpointTooLarge
	}
	switch value := value.(type) {
	case nil:
		return append(destination, differentialCheckpointValueNil), nil
	case bool:
		destination = append(destination, differentialCheckpointValueBool)
		if value {
			return append(destination, 1), nil
		}
		return append(destination, 0), nil
	case string:
		return appendDifferentialCheckpointStringValue(destination, differentialCheckpointValueString, value), nil
	case []byte:
		return appendDifferentialCheckpointOptionalBytes(destination, differentialCheckpointValueBytes, value), nil
	case int:
		return appendDifferentialCheckpointSigned(destination, differentialCheckpointValueInt, int64(value)), nil
	case int8:
		return appendDifferentialCheckpointSigned(destination, differentialCheckpointValueInt8, int64(value)), nil
	case int16:
		return appendDifferentialCheckpointSigned(destination, differentialCheckpointValueInt16, int64(value)), nil
	case int32:
		return appendDifferentialCheckpointSigned(destination, differentialCheckpointValueInt32, int64(value)), nil
	case int64:
		return appendDifferentialCheckpointSigned(destination, differentialCheckpointValueInt64, value), nil
	case uint:
		return appendDifferentialCheckpointUnsigned(destination, differentialCheckpointValueUint, uint64(value)), nil
	case uint8:
		return appendDifferentialCheckpointUnsigned(destination, differentialCheckpointValueUint8, uint64(value)), nil
	case uint16:
		return appendDifferentialCheckpointUnsigned(destination, differentialCheckpointValueUint16, uint64(value)), nil
	case uint32:
		return appendDifferentialCheckpointUnsigned(destination, differentialCheckpointValueUint32, uint64(value)), nil
	case uint64:
		return appendDifferentialCheckpointUnsigned(destination, differentialCheckpointValueUint64, value), nil
	case float32:
		destination = append(destination, differentialCheckpointValueFloat32)
		var bits [4]byte
		binary.LittleEndian.PutUint32(bits[:], math.Float32bits(value))
		return append(destination, bits[:]...), nil
	case float64:
		destination = append(destination, differentialCheckpointValueFloat64)
		var bits [8]byte
		binary.LittleEndian.PutUint64(bits[:], math.Float64bits(value))
		return append(destination, bits[:]...), nil
	case time.Duration:
		return appendDifferentialCheckpointSigned(destination, differentialCheckpointValueDuration, int64(value)), nil
	case time.Time:
		encoded, err := value.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("time: %w", err)
		}
		return appendDifferentialCheckpointOptionalBytes(destination, differentialCheckpointValueTime, encoded), nil
	case json.Number:
		return appendDifferentialCheckpointStringValue(destination, differentialCheckpointValueJSONNumber, string(value)), nil
	case Row:
		destination = append(destination, differentialCheckpointValueRow)
		if value == nil {
			return append(destination, 0), nil
		}
		encoded, err := appendDifferentialCheckpointMap(append(destination, 1), map[string]interface{}(value), options, depth)
		return encoded, err
	case map[string]interface{}:
		destination = append(destination, differentialCheckpointValueMap)
		if value == nil {
			return append(destination, 0), nil
		}
		encoded, err := appendDifferentialCheckpointMap(append(destination, 1), value, options, depth)
		return encoded, err
	case []interface{}:
		return appendDifferentialCheckpointInterfaceSlice(destination, value, options, depth)
	case []string:
		return appendDifferentialCheckpointStringSlice(destination, value)
	case []int64:
		return appendDifferentialCheckpointInt64Slice(destination, value)
	case []uint64:
		return appendDifferentialCheckpointUint64Slice(destination, value)
	case []float64:
		return appendDifferentialCheckpointFloat64Slice(destination, value)
	case []bool:
		return appendDifferentialCheckpointBoolSlice(destination, value)
	case json.RawMessage:
		if !json.Valid(value) {
			return nil, ErrDifferentialCheckpointInvalid
		}
		return appendDifferentialCheckpointOptionalBytes(destination, differentialCheckpointValueJSONRaw, value), nil
	default:
		return nil, fmt.Errorf("%w: %T", ErrDifferentialCheckpointUnsupported, value)
	}
}

func appendDifferentialCheckpointStringValue(destination []byte, tag byte, value string) []byte {
	destination = append(destination, tag)
	return appendDifferentialCheckpointString(destination, value)
}

func appendDifferentialCheckpointString(destination []byte, value string) []byte {
	destination = appendDifferentialCheckpointUvarint(destination, uint64(len(value)))
	return append(destination, value...)
}

func appendDifferentialCheckpointOptionalBytes(destination []byte, tag byte, value []byte) []byte {
	destination = append(destination, tag)
	if value == nil {
		return append(destination, 0)
	}
	destination = append(destination, 1)
	destination = appendDifferentialCheckpointUvarint(destination, uint64(len(value)))
	return append(destination, value...)
}

func appendDifferentialCheckpointSigned(destination []byte, tag byte, value int64) []byte {
	destination = append(destination, tag)
	return appendDifferentialCheckpointUvarint(destination, differentialCheckpointZigZag(value))
}

func appendDifferentialCheckpointUnsigned(destination []byte, tag byte, value uint64) []byte {
	destination = append(destination, tag)
	return appendDifferentialCheckpointUvarint(destination, value)
}

func appendDifferentialCheckpointInterfaceSlice(destination []byte, values []interface{}, options DifferentialCheckpointCodecOptions, depth int) ([]byte, error) {
	destination = append(destination, differentialCheckpointValueInterfaceSlice)
	if values == nil {
		return append(destination, 0), nil
	}
	destination = append(destination, 1)
	destination = appendDifferentialCheckpointUvarint(destination, uint64(len(values)))
	for _, value := range values {
		var err error
		destination, err = appendDifferentialCheckpointValue(destination, value, options, depth+1)
		if err != nil {
			return nil, err
		}
	}
	return destination, nil
}

func appendDifferentialCheckpointStringSlice(destination []byte, values []string) ([]byte, error) {
	destination = append(destination, differentialCheckpointValueStringSlice)
	if values == nil {
		return append(destination, 0), nil
	}
	destination = append(destination, 1)
	destination = appendDifferentialCheckpointUvarint(destination, uint64(len(values)))
	for _, value := range values {
		destination = appendDifferentialCheckpointString(destination, value)
	}
	return destination, nil
}

func appendDifferentialCheckpointInt64Slice(destination []byte, values []int64) ([]byte, error) {
	destination = append(destination, differentialCheckpointValueInt64Slice)
	if values == nil {
		return append(destination, 0), nil
	}
	destination = append(destination, 1)
	destination = appendDifferentialCheckpointUvarint(destination, uint64(len(values)))
	for _, value := range values {
		destination = appendDifferentialCheckpointUvarint(destination, differentialCheckpointZigZag(value))
	}
	return destination, nil
}

func appendDifferentialCheckpointUint64Slice(destination []byte, values []uint64) ([]byte, error) {
	destination = append(destination, differentialCheckpointValueUint64Slice)
	if values == nil {
		return append(destination, 0), nil
	}
	destination = append(destination, 1)
	destination = appendDifferentialCheckpointUvarint(destination, uint64(len(values)))
	for _, value := range values {
		destination = appendDifferentialCheckpointUvarint(destination, value)
	}
	return destination, nil
}

func appendDifferentialCheckpointFloat64Slice(destination []byte, values []float64) ([]byte, error) {
	destination = append(destination, differentialCheckpointValueFloat64Slice)
	if values == nil {
		return append(destination, 0), nil
	}
	destination = append(destination, 1)
	destination = appendDifferentialCheckpointUvarint(destination, uint64(len(values)))
	for _, value := range values {
		var bits [8]byte
		binary.LittleEndian.PutUint64(bits[:], math.Float64bits(value))
		destination = append(destination, bits[:]...)
	}
	return destination, nil
}

func appendDifferentialCheckpointBoolSlice(destination []byte, values []bool) ([]byte, error) {
	destination = append(destination, differentialCheckpointValueBoolSlice)
	if values == nil {
		return append(destination, 0), nil
	}
	destination = append(destination, 1)
	destination = appendDifferentialCheckpointUvarint(destination, uint64(len(values)))
	for _, value := range values {
		if value {
			destination = append(destination, 1)
		} else {
			destination = append(destination, 0)
		}
	}
	return destination, nil
}

func appendDifferentialCheckpointUvarint(destination []byte, value uint64) []byte {
	var buffer [binary.MaxVarintLen64]byte
	count := binary.PutUvarint(buffer[:], value)
	return append(destination, buffer[:count]...)
}

func differentialCheckpointZigZag(value int64) uint64 {
	return uint64(value<<1) ^ uint64(value>>63)
}

type differentialCheckpointReader struct {
	data    []byte
	offset  int
	options DifferentialCheckpointCodecOptions
}

func (reader *differentialCheckpointReader) readUvarint() (uint64, error) {
	if reader.offset >= len(reader.data) {
		return 0, ErrDifferentialCheckpointCorrupt
	}
	value, count := binary.Uvarint(reader.data[reader.offset:])
	if count <= 0 {
		return 0, ErrDifferentialCheckpointCorrupt
	}
	reader.offset += count
	return value, nil
}

func (reader *differentialCheckpointReader) readByte() (byte, error) {
	if reader.offset >= len(reader.data) {
		return 0, ErrDifferentialCheckpointCorrupt
	}
	value := reader.data[reader.offset]
	reader.offset++
	return value, nil
}

func (reader *differentialCheckpointReader) readBytes() ([]byte, error) {
	length, err := reader.readUvarint()
	if err != nil {
		return nil, err
	}
	if length > uint64(reader.options.MaxEncodedBytes) || length > uint64(len(reader.data)-reader.offset) {
		return nil, ErrDifferentialCheckpointCorrupt
	}
	end := reader.offset + int(length)
	value := make([]byte, int(length))
	copy(value, reader.data[reader.offset:end])
	reader.offset = end
	return value, nil
}

func (reader *differentialCheckpointReader) readOptionalBytes() ([]byte, error) {
	present, err := reader.readByte()
	if err != nil {
		return nil, err
	}
	if present == 0 {
		return nil, nil
	}
	if present != 1 {
		return nil, ErrDifferentialCheckpointCorrupt
	}
	return reader.readBytes()
}

func (reader *differentialCheckpointReader) readString() (string, error) {
	value, err := reader.readBytes()
	if err != nil {
		return "", err
	}
	return string(value), nil
}

func (reader *differentialCheckpointReader) readRow() (DifferentialRow, error) {
	key, err := reader.readString()
	if err != nil {
		return DifferentialRow{}, err
	}
	if key == "" {
		return DifferentialRow{}, ErrDifferentialCheckpointCorrupt
	}
	timeValue, err := reader.readUvarint()
	if err != nil {
		return DifferentialRow{}, err
	}
	diffValue, err := reader.readUvarint()
	if err != nil {
		return DifferentialRow{}, err
	}
	row, err := reader.readRowValue()
	if err != nil {
		return DifferentialRow{}, err
	}
	return DifferentialRow{Key: key, Time: timeValue, Diff: differentialCheckpointUnZigZag(diffValue), Row: row}, nil
}

func (reader *differentialCheckpointReader) readRowValue() (Row, error) {
	present, err := reader.readByte()
	if err != nil {
		return nil, err
	}
	if present == 0 {
		return nil, nil
	}
	if present != 1 {
		return nil, ErrDifferentialCheckpointCorrupt
	}
	values, err := reader.readMap(reader.options.MaxValueDepth)
	if err != nil {
		return nil, err
	}
	return Row(values), nil
}

func (reader *differentialCheckpointReader) readMap(depth int) (map[string]interface{}, error) {
	if depth < 0 {
		return nil, ErrDifferentialCheckpointTooLarge
	}
	count, err := reader.readUvarint()
	if err != nil {
		return nil, err
	}
	if count > uint64(reader.options.MaxFieldsPerRow) {
		return nil, ErrDifferentialCheckpointTooLarge
	}
	values := make(map[string]interface{}, int(count))
	for index := uint64(0); index < count; index++ {
		key, err := reader.readString()
		if err != nil {
			return nil, err
		}
		if _, found := values[key]; found {
			return nil, ErrDifferentialCheckpointCorrupt
		}
		value, err := reader.readValue(depth - 1)
		if err != nil {
			return nil, err
		}
		values[key] = value
	}
	return values, nil
}

func (reader *differentialCheckpointReader) readValue(depth int) (interface{}, error) {
	if depth < 0 {
		return nil, ErrDifferentialCheckpointTooLarge
	}
	tag, err := reader.readByte()
	if err != nil {
		return nil, err
	}
	signed := func() (int64, error) {
		value, err := reader.readUvarint()
		return differentialCheckpointUnZigZag(value), err
	}
	unsigned := func() (uint64, error) { return reader.readUvarint() }
	switch tag {
	case differentialCheckpointValueNil:
		return nil, nil
	case differentialCheckpointValueBool:
		value, err := reader.readByte()
		if err != nil || value > 1 {
			return nil, ErrDifferentialCheckpointCorrupt
		}
		return value == 1, nil
	case differentialCheckpointValueString:
		return reader.readString()
	case differentialCheckpointValueBytes:
		return reader.readOptionalBytes()
	case differentialCheckpointValueInt:
		value, err := signed()
		if err != nil || int64(int(value)) != value {
			return nil, ErrDifferentialCheckpointCorrupt
		}
		return int(value), err
	case differentialCheckpointValueInt8:
		value, err := signed()
		if err != nil || value < differentialCheckpointMinInt8 || value > differentialCheckpointMaxInt8 {
			return nil, ErrDifferentialCheckpointCorrupt
		}
		return int8(value), nil
	case differentialCheckpointValueInt16:
		value, err := signed()
		if err != nil || value < differentialCheckpointMinInt16 || value > differentialCheckpointMaxInt16 {
			return nil, ErrDifferentialCheckpointCorrupt
		}
		return int16(value), nil
	case differentialCheckpointValueInt32:
		value, err := signed()
		if err != nil || value < differentialCheckpointMinInt32 || value > differentialCheckpointMaxInt32 {
			return nil, ErrDifferentialCheckpointCorrupt
		}
		return int32(value), nil
	case differentialCheckpointValueInt64:
		return signed()
	case differentialCheckpointValueUint:
		value, err := unsigned()
		if err != nil || uint64(uint(value)) != value {
			return nil, ErrDifferentialCheckpointCorrupt
		}
		return uint(value), err
	case differentialCheckpointValueUint8:
		value, err := unsigned()
		if err != nil || value > differentialCheckpointMaxUint8 {
			return nil, ErrDifferentialCheckpointCorrupt
		}
		return uint8(value), nil
	case differentialCheckpointValueUint16:
		value, err := unsigned()
		if err != nil || value > differentialCheckpointMaxUint16 {
			return nil, ErrDifferentialCheckpointCorrupt
		}
		return uint16(value), nil
	case differentialCheckpointValueUint32:
		value, err := unsigned()
		if err != nil || value > differentialCheckpointMaxUint32 {
			return nil, ErrDifferentialCheckpointCorrupt
		}
		return uint32(value), nil
	case differentialCheckpointValueUint64:
		return unsigned()
	case differentialCheckpointValueFloat32:
		bits, err := reader.readFixed(4)
		if err != nil {
			return nil, err
		}
		return math.Float32frombits(binary.LittleEndian.Uint32(bits)), nil
	case differentialCheckpointValueFloat64:
		bits, err := reader.readFixed(8)
		if err != nil {
			return nil, err
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(bits)), nil
	case differentialCheckpointValueDuration:
		value, err := signed()
		return time.Duration(value), err
	case differentialCheckpointValueTime:
		value, err := reader.readOptionalBytes()
		if err != nil {
			return nil, err
		}
		var decoded time.Time
		if value == nil {
			return nil, ErrDifferentialCheckpointCorrupt
		}
		if err := decoded.UnmarshalBinary(value); err != nil {
			return nil, ErrDifferentialCheckpointCorrupt
		}
		return decoded, nil
	case differentialCheckpointValueJSONNumber:
		value, err := reader.readString()
		return json.Number(value), err
	case differentialCheckpointValueRow:
		present, err := reader.readByte()
		if err != nil {
			return nil, err
		}
		if present == 0 {
			return Row(nil), nil
		}
		if present != 1 {
			return nil, ErrDifferentialCheckpointCorrupt
		}
		values, err := reader.readMap(depth - 1)
		return Row(values), err
	case differentialCheckpointValueMap:
		present, err := reader.readByte()
		if err != nil {
			return nil, err
		}
		if present == 0 {
			return map[string]interface{}(nil), nil
		}
		if present != 1 {
			return nil, ErrDifferentialCheckpointCorrupt
		}
		return reader.readMap(depth - 1)
	case differentialCheckpointValueInterfaceSlice:
		return reader.readInterfaceSlice(depth - 1)
	case differentialCheckpointValueStringSlice:
		return reader.readStringSlice()
	case differentialCheckpointValueInt64Slice:
		return reader.readInt64Slice()
	case differentialCheckpointValueUint64Slice:
		return reader.readUint64Slice()
	case differentialCheckpointValueFloat64Slice:
		return reader.readFloat64Slice()
	case differentialCheckpointValueBoolSlice:
		return reader.readBoolSlice()
	case differentialCheckpointValueJSONRaw:
		value, err := reader.readOptionalBytes()
		if err != nil || value != nil && !json.Valid(value) {
			return nil, ErrDifferentialCheckpointCorrupt
		}
		return json.RawMessage(value), nil
	default:
		return nil, ErrDifferentialCheckpointCorrupt
	}
}

func (reader *differentialCheckpointReader) readFixed(length int) ([]byte, error) {
	if length < 0 || length > len(reader.data)-reader.offset {
		return nil, ErrDifferentialCheckpointCorrupt
	}
	value := reader.data[reader.offset : reader.offset+length]
	reader.offset += length
	return value, nil
}

func (reader *differentialCheckpointReader) readPresenceAndCount() (uint64, error) {
	present, err := reader.readByte()
	if err != nil {
		return 0, err
	}
	if present == 0 {
		return ^uint64(0), nil
	}
	if present != 1 {
		return 0, ErrDifferentialCheckpointCorrupt
	}
	count, err := reader.readUvarint()
	if err != nil {
		return 0, err
	}
	if count > uint64(reader.options.MaxFieldsPerRow) {
		return 0, ErrDifferentialCheckpointTooLarge
	}
	return count, nil
}

func (reader *differentialCheckpointReader) readInterfaceSlice(depth int) ([]interface{}, error) {
	count, err := reader.readPresenceAndCount()
	if err != nil {
		return nil, err
	}
	if count == ^uint64(0) {
		return []interface{}(nil), nil
	}
	values := make([]interface{}, int(count))
	for index := range values {
		values[index], err = reader.readValue(depth)
		if err != nil {
			return nil, err
		}
	}
	return values, nil
}

func (reader *differentialCheckpointReader) readStringSlice() ([]string, error) {
	count, err := reader.readPresenceAndCount()
	if err != nil {
		return nil, err
	}
	if count == ^uint64(0) {
		return []string(nil), nil
	}
	values := make([]string, int(count))
	for index := range values {
		values[index], err = reader.readString()
		if err != nil {
			return nil, err
		}
	}
	return values, nil
}

func (reader *differentialCheckpointReader) readInt64Slice() ([]int64, error) {
	count, err := reader.readPresenceAndCount()
	if err != nil {
		return nil, err
	}
	if count == ^uint64(0) {
		return []int64(nil), nil
	}
	values := make([]int64, int(count))
	for index := range values {
		value, readErr := reader.readUvarint()
		if readErr != nil {
			return nil, readErr
		}
		values[index] = differentialCheckpointUnZigZag(value)
	}
	return values, nil
}

func (reader *differentialCheckpointReader) readUint64Slice() ([]uint64, error) {
	count, err := reader.readPresenceAndCount()
	if err != nil {
		return nil, err
	}
	if count == ^uint64(0) {
		return []uint64(nil), nil
	}
	values := make([]uint64, int(count))
	for index := range values {
		values[index], err = reader.readUvarint()
		if err != nil {
			return nil, err
		}
	}
	return values, nil
}

func (reader *differentialCheckpointReader) readFloat64Slice() ([]float64, error) {
	count, err := reader.readPresenceAndCount()
	if err != nil {
		return nil, err
	}
	if count == ^uint64(0) {
		return []float64(nil), nil
	}
	values := make([]float64, int(count))
	for index := range values {
		bits, readErr := reader.readFixed(8)
		if readErr != nil {
			return nil, readErr
		}
		values[index] = math.Float64frombits(binary.LittleEndian.Uint64(bits))
	}
	return values, nil
}

func (reader *differentialCheckpointReader) readBoolSlice() ([]bool, error) {
	count, err := reader.readPresenceAndCount()
	if err != nil {
		return nil, err
	}
	if count == ^uint64(0) {
		return []bool(nil), nil
	}
	values := make([]bool, int(count))
	for index := range values {
		value, readErr := reader.readByte()
		if readErr != nil || value > 1 {
			return nil, ErrDifferentialCheckpointCorrupt
		}
		values[index] = value == 1
	}
	return values, nil
}

func differentialCheckpointUnZigZag(value uint64) int64 {
	return int64(value>>1) ^ -int64(value&1)
}
