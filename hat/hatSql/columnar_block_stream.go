package hatSql

import (
	"bufio"
	"bytes"
	"compress/flate"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	// SQLColumnarBlockStreamContentType identifies the opt-in typed block
	// stream used for column-oriented query transfer.
	SQLColumnarBlockStreamContentType        = "application/x-hatrie-columnar"
	sqlColumnarBlockStreamFormat             = "hatrie-columnar-block"
	sqlColumnarBlockStreamLegacyVersion      = 1
	sqlColumnarBlockStreamVersion            = 2
	sqlColumnarBlockStreamDefaultRows        = 1024
	sqlColumnarBlockStreamMaxRows            = 1 << 16
	sqlColumnarBlockStreamMaxBytes           = 64 << 20
	sqlColumnarBlockFrameEnd                 = 0
	sqlColumnarBlockFrameData                = 1
	sqlColumnarBlockEncodingRaw         byte = 0
	sqlColumnarBlockEncodingFlate       byte = 1
	sqlColumnarBlockEncodingDictionary  byte = 2
)

var sqlColumnarBlockStreamMagic = [4]byte{'H', 'C', 'B', '1'}

// SQLColumnarBlockStreamHeader describes the versioned stream prelude.
// BlockRows is a sender hint and an upper bound enforced by the reader.
type SQLColumnarBlockStreamHeader struct {
	Format    string                     `json:"format"`
	Version   int                        `json:"version"`
	BlockRows int                        `json:"block_rows"`
	Columns   []SQLRowBinaryStreamColumn `json:"columns"`
}

// SQLColumnarBlockStreamProgress is the amount of data that has been
// completely framed on the wire. A reader only advances it after decoding a
// complete block, making it safe for progress reporting and resume markers.
type SQLColumnarBlockStreamProgress struct {
	Blocks uint64
	Rows   uint64
}

// SQLColumnarBlockStreamCompression controls the per-column payload encoding.
// None is the default and emits the version 1 wire format for compatibility
// with existing producers. Auto keeps Flate output only when it is smaller
// than the raw payload. Flate always emits compressed payloads.
type SQLColumnarBlockStreamCompression uint8

const (
	SQLColumnarBlockStreamCompressionNone SQLColumnarBlockStreamCompression = iota
	SQLColumnarBlockStreamCompressionAuto
	SQLColumnarBlockStreamCompressionFlate
)

// SQLColumnarBlockStreamDictionary controls dictionary encoding for repeated
// string columns. Auto keeps dictionary output only when it is smaller than
// the raw column payload; None preserves the existing encoding choice.
type SQLColumnarBlockStreamDictionary uint8

const (
	SQLColumnarBlockStreamDictionaryNone SQLColumnarBlockStreamDictionary = iota
	SQLColumnarBlockStreamDictionaryAuto
)

// SQLColumnarBlockStreamOptions configures a columnar block writer. A zero
// value preserves the legacy raw v1 wire format. Adaptive compression is
// explicit because its CPU and allocation cost is workload-dependent.
type SQLColumnarBlockStreamOptions struct {
	Compression      SQLColumnarBlockStreamCompression
	CompressionLevel int
	Dictionary       SQLColumnarBlockStreamDictionary
}

// SQLColumnarBlockStreamWriter writes a bounded sequence of typed column
// blocks. Each block contains one independently framed payload per column,
// followed by cumulative block and row progress.
type SQLColumnarBlockStreamWriter struct {
	writer          io.Writer
	columnNames     []string
	columns         []SQLRowBinaryColumn
	columnsProvided bool
	options         SQLColumnarBlockStreamOptions
	optionsErr      error
	version         int
	blockRows       int
	buffers         [][]byte
	previousLengths []int
	rowsInBlock     int
	acceptedRows    uint64
	progress        SQLColumnarBlockStreamProgress
	headerWritten   bool
	finished        bool
}

// SQLColumnarBlockStreamReader incrementally decodes one typed block stream.
// Block returns the rows from the most recent successful NextBlock call.
type SQLColumnarBlockStreamReader struct {
	reader    *bufio.Reader
	closer    io.Closer
	columns   []SQLRowBinaryColumn
	version   int
	blockRows int
	block     []Row
	progress  SQLColumnarBlockStreamProgress
	err       error
	done      bool
}

// NewSQLColumnarBlockStreamWriter creates a stream writer whose schema is
// inferred from the first row. The inferred schema keeps the requested names
// and marks every column nullable, matching the existing streaming RowBinary
// behavior.
func NewSQLColumnarBlockStreamWriter(writer io.Writer, columnNames []string, blockRows int) *SQLColumnarBlockStreamWriter {
	return NewSQLColumnarBlockStreamWriterWithOptions(writer, columnNames, blockRows, SQLColumnarBlockStreamOptions{})
}

// NewSQLColumnarBlockStreamWriterWithOptions creates a stream writer whose
// schema is inferred from the first row and whose payload encoding follows
// options.
func NewSQLColumnarBlockStreamWriterWithOptions(writer io.Writer, columnNames []string, blockRows int, options SQLColumnarBlockStreamOptions) *SQLColumnarBlockStreamWriter {
	options, optionsErr := normalizeSQLColumnarBlockStreamOptions(options)
	version := sqlColumnarBlockStreamVersion
	if options.Compression == SQLColumnarBlockStreamCompressionNone && options.Dictionary == SQLColumnarBlockStreamDictionaryNone {
		version = sqlColumnarBlockStreamLegacyVersion
	}
	return &SQLColumnarBlockStreamWriter{
		writer:      writer,
		columnNames: append([]string(nil), columnNames...),
		options:     options,
		optionsErr:  optionsErr,
		version:     version,
		blockRows:   normalizeSQLColumnarBlockRows(blockRows),
	}
}

// NewSQLColumnarBlockStreamWriterWithColumns creates a stream writer with an
// explicit physical schema. The schema is copied before any rows are written.
func NewSQLColumnarBlockStreamWriterWithColumns(writer io.Writer, columns []SQLRowBinaryColumn, blockRows int) *SQLColumnarBlockStreamWriter {
	return NewSQLColumnarBlockStreamWriterWithColumnsAndOptions(writer, columns, blockRows, SQLColumnarBlockStreamOptions{})
}

// NewSQLColumnarBlockStreamWriterWithColumnsAndOptions creates a stream
// writer with an explicit physical schema and payload encoding options.
func NewSQLColumnarBlockStreamWriterWithColumnsAndOptions(writer io.Writer, columns []SQLRowBinaryColumn, blockRows int, options SQLColumnarBlockStreamOptions) *SQLColumnarBlockStreamWriter {
	options, optionsErr := normalizeSQLColumnarBlockStreamOptions(options)
	version := sqlColumnarBlockStreamVersion
	if options.Compression == SQLColumnarBlockStreamCompressionNone && options.Dictionary == SQLColumnarBlockStreamDictionaryNone {
		version = sqlColumnarBlockStreamLegacyVersion
	}
	return &SQLColumnarBlockStreamWriter{
		writer:          writer,
		columns:         cloneSQLRowBinaryColumns(columns),
		columnsProvided: true,
		options:         options,
		optionsErr:      optionsErr,
		version:         version,
		blockRows:       normalizeSQLColumnarBlockRows(blockRows),
	}
}

// Progress reports blocks that have been fully emitted. Rows buffered in a
// partial block are deliberately excluded until Flush or Finish succeeds.
func (writer *SQLColumnarBlockStreamWriter) Progress() SQLColumnarBlockStreamProgress {
	if writer == nil {
		return SQLColumnarBlockStreamProgress{}
	}
	return writer.progress
}

// WriteRow adds one row to the current block and automatically flushes a full
// block. The row map is read synchronously and is not retained by the writer.
func (writer *SQLColumnarBlockStreamWriter) WriteRow(row Row) error {
	if writer == nil {
		return fmt.Errorf("SQL columnar block stream writer is nil")
	}
	if writer.finished {
		return fmt.Errorf("SQL columnar block stream writer is already finished")
	}
	if writer.writer == nil {
		return fmt.Errorf("SQL columnar block stream writer destination is nil")
	}
	if writer.acceptedRows >= maxSQLRowBinaryRows {
		return fmt.Errorf("SQL columnar block stream row count exceeds limit %d", maxSQLRowBinaryRows)
	}
	if err := writer.initialize(row); err != nil {
		return err
	}
	for index := range writer.previousLengths {
		writer.previousLengths[index] = len(writer.buffers[index])
	}
	for index, column := range writer.columns {
		value := interface{}(nil)
		if row != nil {
			value = row[column.Name]
		}
		if value == nil {
			if !column.Nullable {
				writer.rollbackRow()
				return fmt.Errorf("RowBinary row %d column %q is NULL but not nullable", writer.acceptedRows, column.Name)
			}
			writer.buffers[index] = append(writer.buffers[index], 1)
			continue
		}
		if column.Nullable {
			writer.buffers[index] = append(writer.buffers[index], 0)
		}
		encoded, err := appendSQLRowBinaryColumnValue(writer.buffers[index], column, value, int(writer.acceptedRows))
		if err != nil {
			writer.rollbackRow()
			return err
		}
		writer.buffers[index] = encoded
	}
	writer.rowsInBlock++
	writer.acceptedRows++
	if writer.rowsInBlock == writer.blockRows {
		return writer.Flush()
	}
	return nil
}

// Flush emits the current partial block. It is useful when a producer wants
// progress before the query reaches the configured block size.
func (writer *SQLColumnarBlockStreamWriter) Flush() error {
	if writer == nil {
		return fmt.Errorf("SQL columnar block stream writer is nil")
	}
	if writer.finished {
		return fmt.Errorf("SQL columnar block stream writer is already finished")
	}
	if writer.rowsInBlock == 0 {
		return nil
	}
	if !writer.headerWritten {
		return fmt.Errorf("SQL columnar block stream header is not written")
	}
	var blockBytes uint64
	for _, column := range writer.buffers {
		blockBytes += uint64(len(column))
		if blockBytes > sqlColumnarBlockStreamMaxBytes {
			return fmt.Errorf("SQL columnar block payload exceeds %d bytes", sqlColumnarBlockStreamMaxBytes)
		}
	}
	if err := writeSQLColumnarBlockFrame(writer.writer, writer.progress, writer.rowsInBlock, writer.columns, writer.buffers, writer.version, writer.options); err != nil {
		return err
	}
	writer.progress.Blocks++
	writer.progress.Rows += uint64(writer.rowsInBlock)
	writer.rowsInBlock = 0
	for index := range writer.buffers {
		writer.buffers[index] = writer.buffers[index][:0]
	}
	return nil
}

// Finish flushes the final partial block and writes the explicit end frame.
// Repeated calls after a successful finish are idempotent.
func (writer *SQLColumnarBlockStreamWriter) Finish() error {
	if writer == nil {
		return fmt.Errorf("SQL columnar block stream writer is nil")
	}
	if writer.finished {
		return nil
	}
	if writer.writer == nil {
		return fmt.Errorf("SQL columnar block stream writer destination is nil")
	}
	if err := writer.initialize(nil); err != nil {
		return err
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	if err := writeSQLColumnarBlockByte(writer.writer, sqlColumnarBlockFrameEnd); err != nil {
		return err
	}
	writer.finished = true
	return nil
}

func (writer *SQLColumnarBlockStreamWriter) initialize(firstRow Row) error {
	if writer.optionsErr != nil {
		return writer.optionsErr
	}
	if writer.blockRows <= 0 || writer.blockRows > sqlColumnarBlockStreamMaxRows {
		return fmt.Errorf("SQL columnar block row limit %d is outside 1..%d", writer.blockRows, sqlColumnarBlockStreamMaxRows)
	}
	if !writer.columnsProvided && writer.columns == nil {
		columns, err := inferSQLRowBinaryStreamColumns(writer.columnNames, firstRow)
		if err != nil {
			return err
		}
		writer.columns = columns
	}
	if !writer.headerWritten {
		if err := validateSQLRowBinaryStreamColumns(writer.columns); err != nil {
			return err
		}
		writer.buffers = make([][]byte, len(writer.columns))
		writer.previousLengths = make([]int, len(writer.columns))
		if err := writer.writeHeader(); err != nil {
			return err
		}
		writer.headerWritten = true
	}
	return nil
}

func (writer *SQLColumnarBlockStreamWriter) rollbackRow() {
	for index, length := range writer.previousLengths {
		writer.buffers[index] = writer.buffers[index][:length]
	}
}

func (writer *SQLColumnarBlockStreamWriter) writeHeader() error {
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
	header, err := json.Marshal(SQLColumnarBlockStreamHeader{
		Format:    sqlColumnarBlockStreamFormat,
		Version:   writer.version,
		BlockRows: writer.blockRows,
		Columns:   headerColumns,
	})
	if err != nil {
		return fmt.Errorf("encode SQL columnar block stream header: %w", err)
	}
	if len(header) > maxSQLRowBinaryStreamHeader {
		return fmt.Errorf("SQL columnar block stream header exceeds %d bytes", maxSQLRowBinaryStreamHeader)
	}
	if err := writeSQLColumnarBlockBytes(writer.writer, sqlColumnarBlockStreamMagic[:]); err != nil {
		return err
	}
	if err := writeSQLColumnarBlockUvarint(writer.writer, uint64(len(header))); err != nil {
		return err
	}
	return writeSQLColumnarBlockBytes(writer.writer, header)
}

// NewSQLColumnarBlockStreamReader reads and validates the stream prelude.
func NewSQLColumnarBlockStreamReader(source io.Reader) (*SQLColumnarBlockStreamReader, error) {
	if source == nil {
		return nil, fmt.Errorf("SQL columnar block stream source is nil")
	}
	buffered, ok := source.(*bufio.Reader)
	if !ok {
		buffered = bufio.NewReader(source)
	}
	columns, blockRows, version, err := readSQLColumnarBlockStreamHeader(buffered)
	if err != nil {
		return nil, err
	}
	var closer io.Closer
	if sourceCloser, ok := source.(io.Closer); ok {
		closer = sourceCloser
	}
	return &SQLColumnarBlockStreamReader{reader: buffered, closer: closer, columns: columns, version: version, blockRows: blockRows}, nil
}

// NextBlock advances to the next complete block. Clean EOF is not accepted:
// a valid stream must carry the explicit end frame.
func (reader *SQLColumnarBlockStreamReader) NextBlock() bool {
	return reader.nextBlock(nil)
}

// NextBlockFields advances to the next block while materializing only the
// requested columns. The framed bytes for skipped columns are consumed and
// validated for length, but their values are not decoded or allocated.
func (reader *SQLColumnarBlockStreamReader) NextBlockFields(fields []string) bool {
	if reader == nil || reader.done || reader.err != nil {
		return false
	}
	selected, err := reader.selectColumns(fields)
	if err != nil {
		reader.err = err
		reader.done = true
		return false
	}
	return reader.nextBlock(selected)
}

func (reader *SQLColumnarBlockStreamReader) nextBlock(selected []bool) bool {
	if reader == nil || reader.done || reader.err != nil {
		return false
	}
	frame, err := reader.reader.ReadByte()
	if err != nil {
		reader.err = fmt.Errorf("read SQL columnar block stream frame: %w", err)
		reader.done = true
		return false
	}
	switch frame {
	case sqlColumnarBlockFrameEnd:
		reader.done = true
		return false
	case sqlColumnarBlockFrameData:
		return reader.readBlock(selected)
	default:
		reader.err = fmt.Errorf("SQL columnar block stream has unsupported frame %d", frame)
		reader.done = true
		return false
	}
}

// Block returns the rows decoded by the most recent successful NextBlock.
func (reader *SQLColumnarBlockStreamReader) Block() []Row {
	if reader == nil {
		return nil
	}
	return reader.block
}

// Columns returns a defensive copy of the stream schema.
func (reader *SQLColumnarBlockStreamReader) Columns() []SQLRowBinaryColumn {
	if reader == nil {
		return nil
	}
	return cloneSQLRowBinaryColumns(reader.columns)
}

// Progress reports complete blocks consumed from the wire.
func (reader *SQLColumnarBlockStreamReader) Progress() SQLColumnarBlockStreamProgress {
	if reader == nil {
		return SQLColumnarBlockStreamProgress{}
	}
	return reader.progress
}

// Err reports a malformed, truncated, or otherwise failed stream read.
func (reader *SQLColumnarBlockStreamReader) Err() error {
	if reader == nil {
		return nil
	}
	return reader.err
}

// Close releases the underlying source when it implements io.Closer.
func (reader *SQLColumnarBlockStreamReader) Close() error {
	if reader == nil || reader.closer == nil {
		return nil
	}
	closer := reader.closer
	reader.closer = nil
	reader.done = true
	return closer.Close()
}

func (reader *SQLColumnarBlockStreamReader) readBlock(selected []bool) bool {
	blockIndex, err := readSQLColumnarBlockUvarint(reader.reader, "block index")
	if err != nil {
		return reader.fail(err)
	}
	rowCount, err := readSQLColumnarBlockUvarint(reader.reader, "row count")
	if err != nil {
		return reader.fail(err)
	}
	cumulativeBlocks, err := readSQLColumnarBlockUvarint(reader.reader, "cumulative block count")
	if err != nil {
		return reader.fail(err)
	}
	cumulativeRows, err := readSQLColumnarBlockUvarint(reader.reader, "cumulative row count")
	if err != nil {
		return reader.fail(err)
	}
	columnCount, err := readSQLColumnarBlockUvarint(reader.reader, "column count")
	if err != nil {
		return reader.fail(err)
	}
	if blockIndex != reader.progress.Blocks {
		return reader.fail(fmt.Errorf("SQL columnar block index %d, want %d", blockIndex, reader.progress.Blocks))
	}
	if rowCount == 0 || rowCount > uint64(reader.blockRows) || rowCount > sqlColumnarBlockStreamMaxRows {
		return reader.fail(fmt.Errorf("SQL columnar block row count %d exceeds limit %d", rowCount, reader.blockRows))
	}
	if cumulativeBlocks != reader.progress.Blocks+1 {
		return reader.fail(fmt.Errorf("SQL columnar block cumulative count %d, want %d", cumulativeBlocks, reader.progress.Blocks+1))
	}
	if cumulativeRows != reader.progress.Rows+rowCount || cumulativeRows > maxSQLRowBinaryRows {
		return reader.fail(fmt.Errorf("SQL columnar block cumulative rows %d are inconsistent", cumulativeRows))
	}
	if columnCount != uint64(len(reader.columns)) {
		return reader.fail(fmt.Errorf("SQL columnar block column count %d, want %d", columnCount, len(reader.columns)))
	}

	selectedCount := len(reader.columns)
	if selected != nil {
		selectedCount = 0
		for _, include := range selected {
			if include {
				selectedCount++
			}
		}
	}
	block := make([]Row, int(rowCount))
	for index := range block {
		block[index] = make(Row, selectedCount)
	}
	var blockBytes uint64
	var decodedBlockBytes uint64
	for columnIndex, column := range reader.columns {
		encoding := sqlColumnarBlockEncodingRaw
		if reader.version >= sqlColumnarBlockStreamVersion {
			encoding, err = reader.reader.ReadByte()
			if err != nil {
				return reader.fail(fmt.Errorf("read SQL columnar block column %q encoding: %w", column.Name, err))
			}
			if encoding != sqlColumnarBlockEncodingRaw && encoding != sqlColumnarBlockEncodingFlate && encoding != sqlColumnarBlockEncodingDictionary {
				return reader.fail(fmt.Errorf("SQL columnar block column %q has unsupported encoding %d", column.Name, encoding))
			}
		}
		payloadLength, err := readSQLColumnarBlockUvarint(reader.reader, fmt.Sprintf("column %q payload length", column.Name))
		if err != nil {
			return reader.fail(err)
		}
		if payloadLength > sqlColumnarBlockStreamMaxBytes || payloadLength > uint64(sqlColumnarBlockStreamMaxBytes)-blockBytes {
			return reader.fail(fmt.Errorf("SQL columnar block payload exceeds %d bytes", sqlColumnarBlockStreamMaxBytes))
		}
		if selected != nil && !selected[columnIndex] {
			discarded, err := reader.reader.Discard(int(payloadLength))
			if err != nil || discarded != int(payloadLength) {
				if err == nil {
					err = io.ErrUnexpectedEOF
				}
				return reader.fail(fmt.Errorf("read skipped SQL columnar block column %q: %w", column.Name, err))
			}
			blockBytes += payloadLength
			continue
		}
		payload := make([]byte, int(payloadLength))
		if _, err := io.ReadFull(reader.reader, payload); err != nil {
			return reader.fail(fmt.Errorf("read SQL columnar block column %q: %w", column.Name, err))
		}
		blockBytes += payloadLength
		if encoding == sqlColumnarBlockEncodingDictionary {
			logicalBytes, err := decodeSQLColumnarBlockDictionaryColumn(block, column, payload, sqlColumnarBlockStreamMaxBytes-decodedBlockBytes)
			if err != nil {
				return reader.fail(fmt.Errorf("decode SQL columnar block column %q: %w", column.Name, err))
			}
			decodedBlockBytes += logicalBytes
			continue
		}
		decoded, err := decodeSQLColumnarBlockPayload(payload, encoding, sqlColumnarBlockStreamMaxBytes-decodedBlockBytes)
		if err != nil {
			return reader.fail(fmt.Errorf("decode SQL columnar block column %q: %w", column.Name, err))
		}
		decodedBlockBytes += uint64(len(decoded))
		if err := decodeSQLColumnarBlockColumn(block, column, decoded); err != nil {
			return reader.fail(err)
		}
	}
	reader.block = block
	reader.progress = SQLColumnarBlockStreamProgress{Blocks: cumulativeBlocks, Rows: cumulativeRows}
	return true
}

func (reader *SQLColumnarBlockStreamReader) selectColumns(fields []string) ([]bool, error) {
	if fields == nil {
		return nil, nil
	}
	selected := make([]bool, len(reader.columns))
	indexes := make(map[string]int, len(reader.columns))
	for index, column := range reader.columns {
		indexes[column.Name] = index
	}
	for _, field := range fields {
		index, ok := indexes[field]
		if !ok {
			return nil, fmt.Errorf("SQL columnar block stream column %q is not present", field)
		}
		selected[index] = true
	}
	return selected, nil
}

func (reader *SQLColumnarBlockStreamReader) fail(err error) bool {
	reader.err = err
	reader.done = true
	return false
}

func decodeSQLColumnarBlockColumn(rows []Row, column SQLRowBinaryColumn, payload []byte) error {
	offset := 0
	for rowIndex := range rows {
		if column.Nullable {
			if offset >= len(payload) {
				return fmt.Errorf("SQL columnar block column %q row %d is missing its NULL marker", column.Name, rowIndex)
			}
			marker := payload[offset]
			offset++
			switch marker {
			case 1:
				rows[rowIndex][column.Name] = nil
				continue
			case 0:
			default:
				return fmt.Errorf("SQL columnar block column %q row %d has invalid NULL marker %d", column.Name, rowIndex, marker)
			}
		}
		value, next, err := decodeSQLRowBinaryValue(column.Type, payload, offset, rowIndex, column.Name)
		if err != nil {
			return err
		}
		if err := validateSQLRowBinaryDecodedValue(column, value, rowIndex); err != nil {
			return err
		}
		if column.Type == SQLRowBinaryJSON {
			value = append(json.RawMessage(nil), value.(json.RawMessage)...)
		}
		rows[rowIndex][column.Name] = value
		offset = next
	}
	if offset != len(payload) {
		return fmt.Errorf("SQL columnar block column %q has %d trailing bytes", column.Name, len(payload)-offset)
	}
	return nil
}

func encodeSQLColumnarBlockDictionaryPayload(column SQLRowBinaryColumn, rows int, raw []byte) ([]byte, bool, error) {
	if rows <= 0 {
		return nil, false, fmt.Errorf("SQL columnar dictionary row count %d is invalid", rows)
	}
	dictionaryCapacity := rows
	if dictionaryCapacity > 64 {
		dictionaryCapacity = 64
	}
	dictionary := make(map[uint32][]uint64, dictionaryCapacity)
	valueCapacity := rows
	if valueCapacity > 64 {
		valueCapacity = 64
	}
	values := make([]string, 0, valueCapacity)
	maxDictionaryEntries := rows / 2
	if maxDictionaryEntries < 16 {
		maxDictionaryEntries = 16
	}
	if maxDictionaryEntries > 64 {
		maxDictionaryEntries = 64
	}
	codes := make([]byte, 0, len(raw))
	offset := 0
	for rowIndex := 0; rowIndex < rows; rowIndex++ {
		if column.Nullable {
			if offset >= len(raw) {
				return nil, false, fmt.Errorf("SQL columnar dictionary column %q row %d is missing its NULL marker", column.Name, rowIndex)
			}
			marker := raw[offset]
			offset++
			switch marker {
			case 1:
				codes = append(codes, marker)
				continue
			case 0:
				codes = append(codes, marker)
			default:
				return nil, false, fmt.Errorf("SQL columnar dictionary column %q row %d has invalid NULL marker %d", column.Name, rowIndex, marker)
			}
		}
		value, next, err := decodeSQLRowBinaryBytes(raw, offset, rowIndex, column.Name)
		if err != nil {
			return nil, false, err
		}
		hash := crc32.ChecksumIEEE(value)
		var id uint64
		found := false
		for _, candidateID := range dictionary[hash] {
			if sqlRowBinaryDictionaryBytesEqual(values[candidateID], value) {
				id = candidateID
				found = true
				break
			}
		}
		if !found {
			id = uint64(len(values))
			values = append(values, string(value))
			dictionary[hash] = append(dictionary[hash], id)
			if len(values) > maxDictionaryEntries {
				return nil, false, nil
			}
		}
		codes = appendSQLRowBinaryDictionaryUvarint(codes, id)
		offset = next
	}
	if offset != len(raw) {
		return nil, false, fmt.Errorf("SQL columnar dictionary column %q has %d trailing bytes", column.Name, len(raw)-offset)
	}
	payload := make([]byte, 0, len(raw))
	payload = appendSQLRowBinaryDictionaryUvarint(payload, uint64(len(values)))
	for _, value := range values {
		payload = appendSQLRowBinaryDictionaryString(payload, value)
	}
	payload = append(payload, codes...)
	if len(payload) >= len(raw) {
		return nil, false, nil
	}
	return payload, true, nil
}

func decodeSQLColumnarBlockDictionaryColumn(rows []Row, column SQLRowBinaryColumn, payload []byte, maxBytes uint64) (uint64, error) {
	if column.Type != SQLRowBinaryString {
		return 0, fmt.Errorf("SQL columnar dictionary column %q has unsupported type %d", column.Name, column.Type)
	}
	offset := 0
	dictionaryCount, err := readSQLRowBinaryDictionaryUvarint(payload, &offset, "entry count")
	if err != nil {
		return 0, err
	}
	if dictionaryCount > uint64(len(rows)) {
		return 0, fmt.Errorf("SQL columnar dictionary column %q has %d entries for %d rows", column.Name, dictionaryCount, len(rows))
	}
	dictionary := make([]string, int(dictionaryCount))
	for index := range dictionary {
		value, next, err := decodeSQLRowBinaryDictionaryString(payload, offset)
		if err != nil {
			return 0, fmt.Errorf("read dictionary entry %d: %w", index, err)
		}
		dictionary[index] = value
		offset = next
	}
	var logicalBytes uint64
	addLogicalBytes := func(value uint64) error {
		if value > maxBytes || logicalBytes > maxBytes-value {
			return fmt.Errorf("logical payload exceeds %d bytes", maxBytes)
		}
		logicalBytes += value
		return nil
	}
	for rowIndex := range rows {
		if column.Nullable {
			if offset >= len(payload) {
				return 0, fmt.Errorf("SQL columnar dictionary column %q row %d is missing its NULL marker", column.Name, rowIndex)
			}
			marker := payload[offset]
			offset++
			if err := addLogicalBytes(1); err != nil {
				return 0, err
			}
			switch marker {
			case 1:
				rows[rowIndex][column.Name] = nil
				continue
			case 0:
			default:
				return 0, fmt.Errorf("SQL columnar dictionary column %q row %d has invalid NULL marker %d", column.Name, rowIndex, marker)
			}
		}
		id, err := readSQLRowBinaryDictionaryUvarint(payload, &offset, fmt.Sprintf("row %d value id", rowIndex))
		if err != nil {
			return 0, err
		}
		if id >= dictionaryCount {
			return 0, fmt.Errorf("SQL columnar dictionary column %q row %d references value id %d of %d", column.Name, rowIndex, id, dictionaryCount)
		}
		value := dictionary[id]
		if err := addLogicalBytes(uint64(len(value)) + sqlColumnarBlockUvarintSize(uint64(len(value)))); err != nil {
			return 0, err
		}
		rows[rowIndex][column.Name] = value
	}
	if offset != len(payload) {
		return 0, fmt.Errorf("SQL columnar dictionary column %q has %d trailing bytes", column.Name, len(payload)-offset)
	}
	return logicalBytes, nil
}

func sqlColumnarBlockUvarintSize(value uint64) uint64 {
	var encoded [binary.MaxVarintLen64]byte
	return uint64(binary.PutUvarint(encoded[:], value))
}

func writeSQLColumnarBlockFrame(writer io.Writer, progress SQLColumnarBlockStreamProgress, rows int, schemas []SQLRowBinaryColumn, columns [][]byte, version int, options SQLColumnarBlockStreamOptions) error {
	if err := writeSQLColumnarBlockByte(writer, sqlColumnarBlockFrameData); err != nil {
		return err
	}
	if err := writeSQLColumnarBlockUvarint(writer, progress.Blocks); err != nil {
		return err
	}
	if err := writeSQLColumnarBlockUvarint(writer, uint64(rows)); err != nil {
		return err
	}
	if err := writeSQLColumnarBlockUvarint(writer, progress.Blocks+1); err != nil {
		return err
	}
	if err := writeSQLColumnarBlockUvarint(writer, progress.Rows+uint64(rows)); err != nil {
		return err
	}
	if len(schemas) != len(columns) {
		return fmt.Errorf("SQL columnar block schema count %d does not match payload count %d", len(schemas), len(columns))
	}
	if err := writeSQLColumnarBlockUvarint(writer, uint64(len(columns))); err != nil {
		return err
	}
	var totalBytes uint64
	for index, column := range columns {
		encoding := sqlColumnarBlockEncodingRaw
		payload := column
		if version >= sqlColumnarBlockStreamVersion {
			var err error
			encoding, payload, err = encodeSQLColumnarBlockPayload(schemas[index], rows, column, options)
			if err != nil {
				return fmt.Errorf("encode SQL columnar block column %d: %w", index, err)
			}
		}
		totalBytes += uint64(len(payload))
		if totalBytes > sqlColumnarBlockStreamMaxBytes {
			return fmt.Errorf("SQL columnar block %d payload exceeds %d bytes", index, sqlColumnarBlockStreamMaxBytes)
		}
		if version >= sqlColumnarBlockStreamVersion {
			if err := writeSQLColumnarBlockByte(writer, encoding); err != nil {
				return err
			}
		}
		if err := writeSQLColumnarBlockUvarint(writer, uint64(len(payload))); err != nil {
			return err
		}
		if err := writeSQLColumnarBlockBytes(writer, payload); err != nil {
			return err
		}
	}
	return nil
}

func normalizeSQLColumnarBlockStreamOptions(options SQLColumnarBlockStreamOptions) (SQLColumnarBlockStreamOptions, error) {
	switch options.Compression {
	case SQLColumnarBlockStreamCompressionAuto, SQLColumnarBlockStreamCompressionFlate:
		if options.CompressionLevel == 0 {
			options.CompressionLevel = flate.BestSpeed
		}
		if options.CompressionLevel < flate.HuffmanOnly || options.CompressionLevel > flate.BestCompression {
			return options, fmt.Errorf("SQL columnar block stream compression level %d is outside %d..%d", options.CompressionLevel, flate.HuffmanOnly, flate.BestCompression)
		}
	case SQLColumnarBlockStreamCompressionNone:
	default:
		return options, fmt.Errorf("SQL columnar block stream compression mode %d is unsupported", options.Compression)
	}
	switch options.Dictionary {
	case SQLColumnarBlockStreamDictionaryNone, SQLColumnarBlockStreamDictionaryAuto:
	default:
		return options, fmt.Errorf("SQL columnar block stream dictionary mode %d is unsupported", options.Dictionary)
	}
	return options, nil
}

func encodeSQLColumnarBlockPayload(column SQLRowBinaryColumn, rows int, raw []byte, options SQLColumnarBlockStreamOptions) (byte, []byte, error) {
	if options.Dictionary == SQLColumnarBlockStreamDictionaryAuto && column.Type == SQLRowBinaryString {
		dictionary, ok, err := encodeSQLColumnarBlockDictionaryPayload(column, rows, raw)
		if err != nil {
			return 0, nil, err
		}
		if ok {
			return sqlColumnarBlockEncodingDictionary, dictionary, nil
		}
	}
	if options.Compression == SQLColumnarBlockStreamCompressionNone {
		return sqlColumnarBlockEncodingRaw, raw, nil
	}
	var compressed bytes.Buffer
	compressor, err := flate.NewWriter(&compressed, options.CompressionLevel)
	if err != nil {
		return 0, nil, err
	}
	if _, err := compressor.Write(raw); err != nil {
		_ = compressor.Close()
		return 0, nil, err
	}
	if err := compressor.Close(); err != nil {
		return 0, nil, err
	}
	if options.Compression == SQLColumnarBlockStreamCompressionAuto && compressed.Len() >= len(raw) {
		return sqlColumnarBlockEncodingRaw, raw, nil
	}
	return sqlColumnarBlockEncodingFlate, compressed.Bytes(), nil
}

func decodeSQLColumnarBlockPayload(payload []byte, encoding byte, maxBytes uint64) ([]byte, error) {
	switch encoding {
	case sqlColumnarBlockEncodingRaw:
		if uint64(len(payload)) > maxBytes {
			return nil, fmt.Errorf("raw payload exceeds %d bytes", maxBytes)
		}
		return payload, nil
	case sqlColumnarBlockEncodingFlate:
		decompressor := flate.NewReader(bytes.NewReader(payload))
		decoded, readErr := io.ReadAll(io.LimitReader(decompressor, int64(maxBytes)+1))
		closeErr := decompressor.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read compressed payload: %w", readErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("close compressed payload: %w", closeErr)
		}
		if uint64(len(decoded)) > maxBytes {
			return nil, fmt.Errorf("decompressed payload exceeds %d bytes", maxBytes)
		}
		return decoded, nil
	default:
		return nil, fmt.Errorf("unsupported payload encoding %d", encoding)
	}
}

func readSQLColumnarBlockStreamHeader(reader *bufio.Reader) ([]SQLRowBinaryColumn, int, int, error) {
	var magic [len(sqlColumnarBlockStreamMagic)]byte
	if _, err := io.ReadFull(reader, magic[:]); err != nil {
		return nil, 0, 0, fmt.Errorf("read SQL columnar block stream magic: %w", err)
	}
	if magic != sqlColumnarBlockStreamMagic {
		return nil, 0, 0, fmt.Errorf("invalid SQL columnar block stream magic")
	}
	headerLength, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("read SQL columnar block stream header length: %w", err)
	}
	if headerLength > maxSQLRowBinaryStreamHeader {
		return nil, 0, 0, fmt.Errorf("SQL columnar block stream header length %d exceeds limit", headerLength)
	}
	headerData := make([]byte, int(headerLength))
	if _, err := io.ReadFull(reader, headerData); err != nil {
		return nil, 0, 0, fmt.Errorf("read SQL columnar block stream header: %w", err)
	}
	var header SQLColumnarBlockStreamHeader
	if err := json.Unmarshal(headerData, &header); err != nil {
		return nil, 0, 0, fmt.Errorf("decode SQL columnar block stream header: %w", err)
	}
	if header.Format != sqlColumnarBlockStreamFormat || (header.Version != sqlColumnarBlockStreamLegacyVersion && header.Version != sqlColumnarBlockStreamVersion) {
		return nil, 0, 0, fmt.Errorf("unsupported SQL columnar block stream format %q version %d", header.Format, header.Version)
	}
	if header.BlockRows <= 0 || header.BlockRows > sqlColumnarBlockStreamMaxRows {
		return nil, 0, 0, fmt.Errorf("SQL columnar block stream row limit %d is outside 1..%d", header.BlockRows, sqlColumnarBlockStreamMaxRows)
	}
	columns := make([]SQLRowBinaryColumn, len(header.Columns))
	for index, column := range header.Columns {
		columns[index] = SQLRowBinaryColumn{
			Name:             column.Name,
			Type:             column.Type,
			Nullable:         column.Nullable,
			EnumValues:       append([]string(nil), column.EnumValues...),
			DecimalScale:     column.DecimalScale,
			DecimalPrecision: column.DecimalPrecision,
		}
	}
	if err := validateSQLRowBinaryStreamColumns(columns); err != nil {
		return nil, 0, 0, err
	}
	return columns, header.BlockRows, header.Version, nil
}

func readSQLColumnarBlockUvarint(reader *bufio.Reader, label string) (uint64, error) {
	value, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, fmt.Errorf("read SQL columnar block %s: %w", label, err)
	}
	return value, nil
}

func writeSQLColumnarBlockByte(writer io.Writer, value byte) error {
	var encoded [1]byte
	encoded[0] = value
	return writeSQLColumnarBlockBytes(writer, encoded[:])
}

func writeSQLColumnarBlockUvarint(writer io.Writer, value uint64) error {
	var encoded [binary.MaxVarintLen64]byte
	length := binary.PutUvarint(encoded[:], value)
	return writeSQLColumnarBlockBytes(writer, encoded[:length])
}

func writeSQLColumnarBlockBytes(writer io.Writer, value []byte) error {
	if _, err := writeSQLColumnarBlockAll(writer, value); err != nil {
		return err
	}
	return nil
}

func writeSQLColumnarBlockAll(writer io.Writer, value []byte) (int, error) {
	written := 0
	for len(value) != 0 {
		count, err := writer.Write(value)
		if count > 0 {
			written += count
			value = value[count:]
		}
		if err != nil {
			return written, err
		}
		if count == 0 {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

func normalizeSQLColumnarBlockRows(rows int) int {
	if rows <= 0 {
		return sqlColumnarBlockStreamDefaultRows
	}
	return rows
}

func cloneSQLRowBinaryColumns(columns []SQLRowBinaryColumn) []SQLRowBinaryColumn {
	if columns == nil {
		return nil
	}
	cloned := make([]SQLRowBinaryColumn, len(columns))
	copy(cloned, columns)
	for index := range cloned {
		cloned[index].EnumValues = append([]string(nil), columns[index].EnumValues...)
	}
	return cloned
}
