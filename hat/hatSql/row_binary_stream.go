package hatSql

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

const (
	// SQLRowBinaryStreamContentType identifies the self-describing SQL stream.
	SQLRowBinaryStreamContentType = "application/x-hatrie-rowbinary"
	sqlRowBinaryStreamFormat      = "hatrie-rowbinary"
	sqlRowBinaryStreamVersion     = 1
	maxSQLRowBinaryStreamHeader   = 1 << 20
)

var sqlRowBinaryStreamMagic = [4]byte{'H', 'R', 'S', '1'}

// SQLRowBinaryStreamColumn is the schema metadata carried in a stream
// prelude. The row payload itself remains the compact schema-ordered format.
type SQLRowBinaryStreamColumn struct {
	Name             string           `json:"name"`
	Type             SQLRowBinaryType `json:"type"`
	Nullable         bool             `json:"nullable"`
	EnumValues       []string         `json:"enum_values,omitempty"`
	DecimalScale     uint8            `json:"decimal_scale,omitempty"`
	DecimalPrecision uint8            `json:"decimal_precision,omitempty"`
}

// SQLRowBinaryStreamHeader describes the versioned stream prelude.
type SQLRowBinaryStreamHeader struct {
	Format  string                     `json:"format"`
	Version int                        `json:"version"`
	Columns []SQLRowBinaryStreamColumn `json:"columns"`
}

// SQLRowBinaryStreamWriter writes a self-describing RowBinary stream without
// retaining the result set. Every inferred column is nullable so a later row
// can legally contain NULL even when the first row did not.
type SQLRowBinaryStreamWriter struct {
	writer          io.Writer
	columnNames     []string
	columns         []SQLRowBinaryColumn
	columnsProvided bool
	headerWritten   bool
	finished        bool
	rows            int
	buffer          []byte
}

// SQLRowBinaryStreamReader incrementally decodes one self-describing stream.
// It keeps only the current row and one bounded variable-length value.
type SQLRowBinaryStreamReader struct {
	reader  *bufio.Reader
	closer  io.Closer
	columns []SQLRowBinaryColumn
	row     Row
	err     error
	done    bool
	rows    int
}

// NewSQLRowBinaryStreamReader reads the stream prelude and returns a reader
// that decodes rows as Next is called. An io.Closer source is closed by Close.
func NewSQLRowBinaryStreamReader(source io.Reader) (*SQLRowBinaryStreamReader, error) {
	if source == nil {
		return nil, fmt.Errorf("SQL RowBinary stream source is nil")
	}
	buffered, ok := source.(*bufio.Reader)
	if !ok {
		buffered = bufio.NewReader(source)
	}
	columns, err := readSQLRowBinaryStreamHeader(buffered)
	if err != nil {
		return nil, err
	}
	var closer io.Closer
	if sourceCloser, ok := source.(io.Closer); ok {
		closer = sourceCloser
	}
	return &SQLRowBinaryStreamReader{reader: buffered, closer: closer, columns: columns}, nil
}

// Next advances to the next row. It returns false at clean EOF or on a
// malformed/truncated row; Err distinguishes those cases.
func (reader *SQLRowBinaryStreamReader) Next() bool {
	if reader == nil || reader.done || reader.err != nil {
		return false
	}
	if reader.rows >= maxSQLRowBinaryRows {
		reader.err = fmt.Errorf("SQL RowBinary stream row count exceeds limit %d", maxSQLRowBinaryRows)
		reader.done = true
		return false
	}
	row, err := readSQLRowBinaryStreamRow(reader.reader, reader.columns, reader.rows)
	if err == io.EOF {
		reader.done = true
		return false
	}
	if err != nil {
		reader.err = err
		reader.done = true
		return false
	}
	reader.row = row
	reader.rows++
	return true
}

// Row returns the row from the most recent successful Next call.
func (reader *SQLRowBinaryStreamReader) Row() Row {
	if reader == nil {
		return nil
	}
	return reader.row
}

// Columns returns a copy of the stream schema.
func (reader *SQLRowBinaryStreamReader) Columns() []SQLRowBinaryColumn {
	if reader == nil {
		return nil
	}
	columns := make([]SQLRowBinaryColumn, len(reader.columns))
	copy(columns, reader.columns)
	for index := range columns {
		columns[index].EnumValues = append([]string(nil), reader.columns[index].EnumValues...)
	}
	return columns
}

// Err reports a malformed, truncated, or otherwise failed stream read.
func (reader *SQLRowBinaryStreamReader) Err() error {
	if reader == nil {
		return nil
	}
	return reader.err
}

// Close releases the underlying source when it implements io.Closer.
func (reader *SQLRowBinaryStreamReader) Close() error {
	if reader == nil || reader.closer == nil {
		return nil
	}
	closer := reader.closer
	reader.closer = nil
	reader.done = true
	return closer.Close()
}

// NewSQLRowBinaryStreamWriter creates a streaming encoder for output columns
// in query order. The writer is not touched until WriteRow or Finish.
func NewSQLRowBinaryStreamWriter(writer io.Writer, columnNames []string) *SQLRowBinaryStreamWriter {
	return &SQLRowBinaryStreamWriter{
		writer:      writer,
		columnNames: append([]string(nil), columnNames...),
	}
}

// NewSQLRowBinaryStreamWriterWithColumns creates a streaming encoder with an
// explicit physical schema. Typed Decimal128/Decimal256 values use the
// supplied scale and precision because those values carry only their integer
// coefficient.
func NewSQLRowBinaryStreamWriterWithColumns(writer io.Writer, columns []SQLRowBinaryColumn) (*SQLRowBinaryStreamWriter, error) {
	if writer == nil {
		return nil, fmt.Errorf("SQL RowBinary stream writer destination is nil")
	}
	if err := validateSQLRowBinaryColumns(columns); err != nil {
		return nil, err
	}
	copied := make([]SQLRowBinaryColumn, len(columns))
	copy(copied, columns)
	for index := range copied {
		copied[index].EnumValues = append([]string(nil), columns[index].EnumValues...)
	}
	return &SQLRowBinaryStreamWriter{writer: writer, columns: copied, columnsProvided: true}, nil
}

// WriteRow infers the physical schema from the first row and writes one row.
// Columns whose first value is NULL use JSON as a lossless conservative type.
func (writer *SQLRowBinaryStreamWriter) WriteRow(row Row) error {
	if writer == nil {
		return fmt.Errorf("SQL RowBinary stream writer is nil")
	}
	if writer.finished {
		return fmt.Errorf("SQL RowBinary stream writer is already finished")
	}
	if writer.writer == nil {
		return fmt.Errorf("SQL RowBinary stream writer destination is nil")
	}
	if writer.rows >= maxSQLRowBinaryRows {
		return fmt.Errorf("SQL RowBinary stream row count exceeds limit %d", maxSQLRowBinaryRows)
	}
	if !writer.headerWritten && !writer.columnsProvided {
		columns, err := inferSQLRowBinaryStreamColumns(writer.columnNames, row)
		if err != nil {
			return err
		}
		writer.columns = columns
	}
	writer.buffer = writer.buffer[:0]
	var err error
	writer.buffer, err = appendSQLRowBinaryStreamRow(writer.buffer, writer.columns, row, writer.rows)
	if err != nil {
		return err
	}
	if !writer.headerWritten {
		if err := writer.writeHeader(); err != nil {
			return err
		}
		writer.headerWritten = true
	}
	if err := writeSQLRowBinaryStreamAll(writer.writer, writer.buffer); err != nil {
		return err
	}
	writer.rows++
	return nil
}

// Finish writes metadata for an empty stream or closes a non-empty stream.
// The format is terminated by EOF; no result set is retained by the writer.
func (writer *SQLRowBinaryStreamWriter) Finish() error {
	if writer == nil {
		return fmt.Errorf("SQL RowBinary stream writer is nil")
	}
	if writer.finished {
		return nil
	}
	if writer.writer == nil {
		return fmt.Errorf("SQL RowBinary stream writer destination is nil")
	}
	if !writer.headerWritten && !writer.columnsProvided {
		columns, err := inferSQLRowBinaryStreamColumns(writer.columnNames, nil)
		if err != nil {
			return err
		}
		writer.columns = columns
		if err := writer.writeHeader(); err != nil {
			return err
		}
		writer.headerWritten = true
	}
	writer.finished = true
	return nil
}

func (writer *SQLRowBinaryStreamWriter) writeHeader() error {
	headerColumns := make([]SQLRowBinaryStreamColumn, len(writer.columns))
	for index, column := range writer.columns {
		headerColumns[index] = SQLRowBinaryStreamColumn{
			Name:             column.Name,
			Type:             column.Type,
			Nullable:         column.Nullable,
			EnumValues:       append([]string(nil), column.EnumValues...),
			DecimalScale:     column.DecimalScale,
			DecimalPrecision: column.DecimalPrecision,
		}
	}
	header, err := json.Marshal(SQLRowBinaryStreamHeader{
		Format:  sqlRowBinaryStreamFormat,
		Version: sqlRowBinaryStreamVersion,
		Columns: headerColumns,
	})
	if err != nil {
		return fmt.Errorf("encode SQL RowBinary stream header: %w", err)
	}
	if len(header) > maxSQLRowBinaryStreamHeader {
		return fmt.Errorf("SQL RowBinary stream header exceeds %d bytes", maxSQLRowBinaryStreamHeader)
	}
	prefix := make([]byte, 0, len(sqlRowBinaryStreamMagic)+binary.MaxVarintLen64+len(header))
	prefix = append(prefix, sqlRowBinaryStreamMagic[:]...)
	var length [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(length[:], uint64(len(header)))
	prefix = append(prefix, length[:n]...)
	prefix = append(prefix, header...)
	return writeSQLRowBinaryStreamAll(writer.writer, prefix)
}

// DecodeSQLRowBinaryStream decodes the stream prelude and all rows. It is
// intended for clients that already have the complete response body; servers
// can consume the payload with DecodeSQLRowBinary after reading the header.
func DecodeSQLRowBinaryStream(encoded []byte) ([]SQLRowBinaryColumn, []SQLRow, error) {
	columns, payload, err := splitSQLRowBinaryStream(encoded)
	if err != nil {
		return nil, nil, err
	}
	rows, err := decodeSQLRowBinaryValidated(columns, payload)
	if err != nil {
		return nil, nil, err
	}
	return columns, rows, nil
}

func splitSQLRowBinaryStream(encoded []byte) ([]SQLRowBinaryColumn, []byte, error) {
	if len(encoded) < len(sqlRowBinaryStreamMagic) || !bytes.Equal(encoded[:len(sqlRowBinaryStreamMagic)], sqlRowBinaryStreamMagic[:]) {
		return nil, nil, fmt.Errorf("invalid SQL RowBinary stream magic")
	}
	offset := len(sqlRowBinaryStreamMagic)
	headerLength, size := binary.Uvarint(encoded[offset:])
	if size <= 0 {
		return nil, nil, fmt.Errorf("invalid SQL RowBinary stream header length")
	}
	offset += size
	if headerLength > maxSQLRowBinaryStreamHeader || headerLength > uint64(len(encoded)-offset) {
		return nil, nil, fmt.Errorf("SQL RowBinary stream header length %d exceeds input or limit", headerLength)
	}
	headerEnd := offset + int(headerLength)
	var header SQLRowBinaryStreamHeader
	if err := json.Unmarshal(encoded[offset:headerEnd], &header); err != nil {
		return nil, nil, fmt.Errorf("decode SQL RowBinary stream header: %w", err)
	}
	if header.Format != sqlRowBinaryStreamFormat || header.Version != sqlRowBinaryStreamVersion {
		return nil, nil, fmt.Errorf("unsupported SQL RowBinary stream format %q version %d", header.Format, header.Version)
	}
	columns := make([]SQLRowBinaryColumn, len(header.Columns))
	for index, column := range header.Columns {
		columns[index] = SQLRowBinaryColumn{Name: column.Name, Type: column.Type, Nullable: column.Nullable, EnumValues: append([]string(nil), column.EnumValues...), DecimalScale: column.DecimalScale, DecimalPrecision: column.DecimalPrecision}
	}
	if err := validateSQLRowBinaryStreamColumns(columns); err != nil {
		return nil, nil, err
	}
	return columns, encoded[headerEnd:], nil
}

func readSQLRowBinaryStreamHeader(reader *bufio.Reader) ([]SQLRowBinaryColumn, error) {
	var magic [len(sqlRowBinaryStreamMagic)]byte
	if _, err := io.ReadFull(reader, magic[:]); err != nil {
		return nil, fmt.Errorf("read SQL RowBinary stream magic: %w", err)
	}
	if magic != sqlRowBinaryStreamMagic {
		return nil, fmt.Errorf("invalid SQL RowBinary stream magic")
	}
	headerLength, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("read SQL RowBinary stream header length: %w", err)
	}
	if headerLength > maxSQLRowBinaryStreamHeader {
		return nil, fmt.Errorf("SQL RowBinary stream header length %d exceeds limit", headerLength)
	}
	headerData := make([]byte, int(headerLength))
	if _, err := io.ReadFull(reader, headerData); err != nil {
		return nil, fmt.Errorf("read SQL RowBinary stream header: %w", err)
	}
	var header SQLRowBinaryStreamHeader
	if err := json.Unmarshal(headerData, &header); err != nil {
		return nil, fmt.Errorf("decode SQL RowBinary stream header: %w", err)
	}
	if header.Format != sqlRowBinaryStreamFormat || header.Version != sqlRowBinaryStreamVersion {
		return nil, fmt.Errorf("unsupported SQL RowBinary stream format %q version %d", header.Format, header.Version)
	}
	columns := make([]SQLRowBinaryColumn, len(header.Columns))
	for index, column := range header.Columns {
		columns[index] = SQLRowBinaryColumn{Name: column.Name, Type: column.Type, Nullable: column.Nullable, EnumValues: append([]string(nil), column.EnumValues...), DecimalScale: column.DecimalScale, DecimalPrecision: column.DecimalPrecision}
	}
	if err := validateSQLRowBinaryStreamColumns(columns); err != nil {
		return nil, err
	}
	return columns, nil
}

const maxSQLRowBinaryStreamValueBytes = 64 << 20

func readSQLRowBinaryStreamRow(reader *bufio.Reader, columns []SQLRowBinaryColumn, rowIndex int) (Row, error) {
	if _, err := reader.Peek(1); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, err
	}
	row := make(Row, len(columns))
	started := false
	for _, column := range columns {
		if column.Nullable {
			marker, err := reader.ReadByte()
			if err != nil {
				if err == io.EOF && !started {
					return nil, io.EOF
				}
				return nil, fmt.Errorf("SQL RowBinary stream row %d column %q NULL marker: %w", rowIndex, column.Name, unexpectedSQLRowBinaryStreamEOF(err))
			}
			started = true
			switch marker {
			case 1:
				row[column.Name] = nil
				continue
			case 0:
			default:
				return nil, fmt.Errorf("SQL RowBinary stream row %d column %q has invalid NULL marker %d", rowIndex, column.Name, marker)
			}
		}
		value, err := readSQLRowBinaryStreamValue(reader, column.Type, rowIndex, column.Name)
		if err != nil {
			return nil, fmt.Errorf("SQL RowBinary stream row %d column %q: %w", rowIndex, column.Name, unexpectedSQLRowBinaryStreamEOF(err))
		}
		if err := validateSQLRowBinaryDecodedValue(column, value, rowIndex); err != nil {
			return nil, err
		}
		started = true
		row[column.Name] = value
	}
	return row, nil
}

func readSQLRowBinaryStreamValue(reader *bufio.Reader, kind SQLRowBinaryType, row int, column string) (interface{}, error) {
	switch kind {
	case SQLRowBinaryString, SQLRowBinaryBytes, SQLRowBinaryJSON:
		length, err := binary.ReadUvarint(reader)
		if err != nil {
			return nil, err
		}
		if length > maxSQLRowBinaryStreamValueBytes {
			return nil, fmt.Errorf("value length %d exceeds limit %d", length, maxSQLRowBinaryStreamValueBytes)
		}
		value := make([]byte, int(length))
		if _, err := io.ReadFull(reader, value); err != nil {
			return nil, err
		}
		switch kind {
		case SQLRowBinaryString:
			return string(value), nil
		case SQLRowBinaryJSON:
			if !json.Valid(value) {
				return nil, fmt.Errorf("invalid JSON payload")
			}
			return json.RawMessage(value), nil
		default:
			return value, nil
		}
	default:
		size := sqlRowBinaryStreamFixedWidth(kind)
		if size == 0 {
			return nil, fmt.Errorf("unsupported physical type %d", kind)
		}
		var fixed [32]byte
		if _, err := io.ReadFull(reader, fixed[:size]); err != nil {
			return nil, err
		}
		value, _, err := decodeSQLRowBinaryValue(kind, fixed[:size], 0, row, column)
		return value, err
	}
}

func sqlRowBinaryStreamFixedWidth(kind SQLRowBinaryType) int {
	switch kind {
	case SQLRowBinaryInt64, SQLRowBinaryUint64, SQLRowBinaryFloat64, SQLRowBinaryDateTime, SQLRowBinaryDuration:
		return 8
	case SQLRowBinaryBool:
		return 1
	case SQLRowBinaryDate:
		return 4
	case SQLRowBinaryUUID:
		return 16
	case SQLRowBinaryIPv4:
		return 4
	case SQLRowBinaryIPv6:
		return 16
	case SQLRowBinaryEnum8:
		return 1
	case SQLRowBinaryEnum16:
		return 2
	case SQLRowBinaryDecimal128:
		return 16
	case SQLRowBinaryDecimal256:
		return 32
	default:
		return 0
	}
}

func unexpectedSQLRowBinaryStreamEOF(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}

func inferSQLRowBinaryStreamColumns(names []string, row Row) ([]SQLRowBinaryColumn, error) {
	if len(names) == 0 {
		return nil, fmt.Errorf("SQL RowBinary stream requires at least one column")
	}
	columns := make([]SQLRowBinaryColumn, len(names))
	seen := make(map[string]struct{}, len(names))
	for index, name := range names {
		if strings.TrimSpace(name) == "" {
			return nil, fmt.Errorf("SQL RowBinary stream column %d has an empty name", index)
		}
		if _, found := seen[name]; found {
			return nil, fmt.Errorf("SQL RowBinary stream column %q is duplicated", name)
		}
		seen[name] = struct{}{}
		kind := SQLRowBinaryJSON
		if row != nil {
			value := row[name]
			kind = inferSQLRowBinaryStreamType(value)
			switch value.(type) {
			case SQLDecimal128, SQLDecimal256:
				return nil, fmt.Errorf("SQL RowBinary stream column %q requires explicit decimal scale and precision", name)
			}
		}
		columns[index] = SQLRowBinaryColumn{Name: name, Type: kind, Nullable: true}
	}
	return columns, nil
}

func inferSQLRowBinaryStreamType(value interface{}) SQLRowBinaryType {
	switch value.(type) {
	case int, int8, int16, int32, int64:
		return SQLRowBinaryInt64
	case uint, uint8, uint16, uint32, uint64:
		return SQLRowBinaryUint64
	case float32, float64:
		return SQLRowBinaryFloat64
	case bool:
		return SQLRowBinaryBool
	case string:
		return SQLRowBinaryString
	case sqlDate:
		return SQLRowBinaryDate
	case sqlUUID:
		return SQLRowBinaryUUID
	case sqlIPv4:
		return SQLRowBinaryIPv4
	case sqlIPv6:
		return SQLRowBinaryIPv6
	case SQLEnum8:
		return SQLRowBinaryEnum8
	case SQLEnum16:
		return SQLRowBinaryEnum16
	case SQLDecimal128:
		return SQLRowBinaryDecimal128
	case SQLDecimal256:
		return SQLRowBinaryDecimal256
	case sqlDecimal, sqlDuration:
		return SQLRowBinaryString
	case []byte:
		return SQLRowBinaryBytes
	case time.Time:
		return SQLRowBinaryDateTime
	case time.Duration:
		return SQLRowBinaryDuration
	case json.RawMessage:
		return SQLRowBinaryJSON
	default:
		return SQLRowBinaryJSON
	}
}

func appendSQLRowBinaryStreamRow(destination []byte, columns []SQLRowBinaryColumn, row Row, rowIndex int) ([]byte, error) {
	for _, column := range columns {
		value := interface{}(nil)
		if row != nil {
			value = row[column.Name]
		}
		if value == nil {
			if !column.Nullable {
				return nil, fmt.Errorf("SQL RowBinary stream row %d column %q is NULL but not nullable", rowIndex, column.Name)
			}
			destination = append(destination, 1)
			continue
		}
		if column.Nullable {
			destination = append(destination, 0)
		}
		normalized, err := normalizeSQLRowBinaryStreamValue(column, value)
		if err != nil {
			return nil, fmt.Errorf("SQL RowBinary stream row %d column %q: %w", rowIndex, column.Name, err)
		}
		destination, err = appendSQLRowBinaryColumnValue(destination, column, normalized, rowIndex)
		if err != nil {
			return nil, err
		}
	}
	return destination, nil
}

func normalizeSQLRowBinaryStreamValue(column SQLRowBinaryColumn, value interface{}) (interface{}, error) {
	switch column.Type {
	case SQLRowBinaryInt64:
		switch converted := value.(type) {
		case int:
			return int64(converted), nil
		case int8:
			return int64(converted), nil
		case int16:
			return int64(converted), nil
		case int32:
			return int64(converted), nil
		case int64:
			return converted, nil
		default:
			return nil, fmt.Errorf("expects an integer, got %T", value)
		}
	case SQLRowBinaryUint64:
		switch converted := value.(type) {
		case uint:
			return uint64(converted), nil
		case uint8:
			return uint64(converted), nil
		case uint16:
			return uint64(converted), nil
		case uint32:
			return uint64(converted), nil
		case uint64:
			return converted, nil
		default:
			return nil, fmt.Errorf("expects an unsigned integer, got %T", value)
		}
	case SQLRowBinaryFloat64:
		switch converted := value.(type) {
		case float32:
			return float64(converted), nil
		case float64:
			return converted, nil
		default:
			return nil, fmt.Errorf("expects a floating-point number, got %T", value)
		}
	case SQLRowBinaryBool:
		converted, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("expects bool, got %T", value)
		}
		return converted, nil
	case SQLRowBinaryString:
		switch converted := value.(type) {
		case string:
			return converted, nil
		case sqlDecimal:
			return string(converted), nil
		case sqlDuration:
			return string(converted), nil
		default:
			return nil, fmt.Errorf("expects string, got %T", value)
		}
	case SQLRowBinaryBytes:
		converted, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("expects []byte, got %T", value)
		}
		return converted, nil
	case SQLRowBinaryDate:
		switch converted := value.(type) {
		case time.Time:
			return converted, nil
		case sqlDate:
			parsed, err := time.Parse("2006-01-02", string(converted))
			if err != nil {
				return nil, fmt.Errorf("invalid DATE %q: %w", converted, err)
			}
			return parsed.UTC(), nil
		default:
			return nil, fmt.Errorf("expects time.Time or DATE, got %T", value)
		}
	case SQLRowBinaryDateTime:
		converted, ok := value.(time.Time)
		if !ok {
			return nil, fmt.Errorf("expects time.Time, got %T", value)
		}
		return converted, nil
	case SQLRowBinaryDuration:
		converted, ok := value.(time.Duration)
		if !ok {
			return nil, fmt.Errorf("expects time.Duration, got %T", value)
		}
		return converted, nil
	case SQLRowBinaryUUID:
		return normalizeSQLRowBinaryStreamUUID(value)
	case SQLRowBinaryIPv4:
		switch converted := value.(type) {
		case sqlIPv4:
			return converted, nil
		case string:
			parsed, err := ParseSQLIPv4(converted)
			if err != nil {
				return nil, err
			}
			return parsed, nil
		default:
			return nil, fmt.Errorf("expects IPv4 string or SQLIPv4, got %T", value)
		}
	case SQLRowBinaryIPv6:
		switch converted := value.(type) {
		case sqlIPv6:
			return converted, nil
		case string:
			parsed, err := ParseSQLIPv6(converted)
			if err != nil {
				return nil, err
			}
			return parsed, nil
		default:
			return nil, fmt.Errorf("expects IPv6 string or SQLIPv6, got %T", value)
		}
	case SQLRowBinaryEnum8:
		switch converted := value.(type) {
		case SQLEnum8:
			return converted, nil
		case uint8:
			return SQLEnum8(converted), nil
		default:
			return nil, fmt.Errorf("expects SQLEnum8, got %T", value)
		}
	case SQLRowBinaryEnum16:
		switch converted := value.(type) {
		case SQLEnum16:
			return converted, nil
		case uint16:
			return SQLEnum16(converted), nil
		default:
			return nil, fmt.Errorf("expects SQLEnum16, got %T", value)
		}
	case SQLRowBinaryDecimal128:
		switch converted := value.(type) {
		case SQLDecimal128:
			return converted, nil
		case SQLDecimal:
			parsed, err := ParseSQLDecimal128(string(converted), column.DecimalScale)
			if err != nil {
				return nil, err
			}
			return parsed, nil
		case string:
			parsed, err := ParseSQLDecimal128(converted, column.DecimalScale)
			if err != nil {
				return nil, err
			}
			return parsed, nil
		default:
			return nil, fmt.Errorf("expects SQLDecimal128 or decimal string, got %T", value)
		}
	case SQLRowBinaryDecimal256:
		switch converted := value.(type) {
		case SQLDecimal256:
			return converted, nil
		case SQLDecimal:
			parsed, err := ParseSQLDecimal256(string(converted), column.DecimalScale)
			if err != nil {
				return nil, err
			}
			return parsed, nil
		case string:
			parsed, err := ParseSQLDecimal256(converted, column.DecimalScale)
			if err != nil {
				return nil, err
			}
			return parsed, nil
		default:
			return nil, fmt.Errorf("expects SQLDecimal256 or decimal string, got %T", value)
		}
	case SQLRowBinaryJSON:
		if raw, ok := value.(json.RawMessage); ok {
			if !json.Valid(raw) {
				return nil, fmt.Errorf("invalid JSON payload")
			}
			return raw, nil
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("encode JSON value: %w", err)
		}
		return json.RawMessage(encoded), nil
	default:
		return nil, fmt.Errorf("unsupported physical type %d", column.Type)
	}
}

func normalizeSQLRowBinaryStreamUUID(value interface{}) ([16]byte, error) {
	if converted, ok := value.([16]byte); ok {
		return converted, nil
	}
	var text string
	switch converted := value.(type) {
	case string:
		text = converted
	case sqlUUID:
		text = string(converted)
	default:
		return [16]byte{}, fmt.Errorf("expects UUID string or [16]byte, got %T", value)
	}
	text = strings.ReplaceAll(text, "-", "")
	if len(text) != 32 {
		return [16]byte{}, fmt.Errorf("UUID %q has invalid length", text)
	}
	var decoded [16]byte
	if _, err := hex.Decode(decoded[:], []byte(text)); err != nil {
		return [16]byte{}, fmt.Errorf("invalid UUID %q: %w", text, err)
	}
	return decoded, nil
}

func writeSQLRowBinaryStreamAll(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written <= 0 || written > len(data) {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}
