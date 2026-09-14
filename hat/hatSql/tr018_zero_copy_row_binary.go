package hatSql

import (
	"encoding/json"
	"fmt"
)

// SQLRowBinaryBorrowedField describes one field in a borrowed RowBinary row.
// Data points into the encoded input supplied to the reader and is valid only
// until the next call to Next or Reset. A NULL field has Null set and no Data.
type SQLRowBinaryBorrowedField struct {
	Type SQLRowBinaryType
	Null bool
	Data []byte
}

// SQLRowBinaryBorrowedRow contains schema-ordered fields without materializing
// a SQLRow map. Its field slice is reused for every row returned by a reader.
type SQLRowBinaryBorrowedRow struct {
	Fields []SQLRowBinaryBorrowedField
}

// SQLRowBinaryBorrowedReader iterates over a complete RowBinary payload while
// borrowing variable-length and fixed-width field bytes from the input. The
// input must remain alive and unchanged while its fields are being consumed.
// Use the regular DecodeSQLRowBinary API when owned Go values are required.
type SQLRowBinaryBorrowedReader struct {
	columns []SQLRowBinaryColumn
	encoded []byte
	offset  int
	rows    int
	row     SQLRowBinaryBorrowedRow
	err     error
	done    bool
}

// NewSQLRowBinaryBorrowedReader validates the schema and creates a reusable
// reader. Payload validation is performed incrementally by Next.
func NewSQLRowBinaryBorrowedReader(columns []SQLRowBinaryColumn, encoded []byte) (*SQLRowBinaryBorrowedReader, error) {
	if err := validateSQLRowBinaryColumns(columns); err != nil {
		return nil, err
	}
	reader := &SQLRowBinaryBorrowedReader{
		columns: columns,
		row: SQLRowBinaryBorrowedRow{
			Fields: make([]SQLRowBinaryBorrowedField, len(columns)),
		},
	}
	if err := reader.Reset(encoded); err != nil {
		return nil, err
	}
	return reader, nil
}

// Reset reuses the reader for another complete RowBinary payload. It does not
// copy encoded; callers must keep the payload alive and unchanged until the
// borrowed fields are no longer used.
func (reader *SQLRowBinaryBorrowedReader) Reset(encoded []byte) error {
	if reader == nil {
		return fmt.Errorf("RowBinary borrowed reader is nil")
	}
	reader.encoded = encoded
	reader.offset = 0
	reader.rows = 0
	reader.err = nil
	reader.done = false
	if len(reader.row.Fields) != len(reader.columns) {
		reader.row.Fields = make([]SQLRowBinaryBorrowedField, len(reader.columns))
	}
	for index := range reader.row.Fields {
		reader.row.Fields[index] = SQLRowBinaryBorrowedField{Type: reader.columns[index].Type}
	}
	return nil
}

// Next advances to the next row. Fields returned by Row are valid only when
// Next returns true and until the next call to Next or Reset.
func (reader *SQLRowBinaryBorrowedReader) Next() bool {
	if reader == nil || reader.err != nil || reader.done {
		return false
	}
	if reader.offset >= len(reader.encoded) {
		reader.done = true
		return false
	}
	if reader.rows >= maxSQLRowBinaryRows {
		return reader.fail(fmt.Errorf("RowBinary borrowed row count exceeds limit %d", maxSQLRowBinaryRows))
	}

	for index, column := range reader.columns {
		field := &reader.row.Fields[index]
		field.Type = column.Type
		field.Null = false
		field.Data = nil
		if column.Nullable {
			if reader.offset >= len(reader.encoded) {
				return reader.fail(fmt.Errorf("RowBinary borrowed row %d column %q is missing its NULL marker", reader.rows, column.Name))
			}
			marker := reader.encoded[reader.offset]
			reader.offset++
			switch marker {
			case 0:
			case 1:
				field.Null = true
				continue
			default:
				return reader.fail(fmt.Errorf("RowBinary borrowed row %d column %q has invalid NULL marker %d", reader.rows, column.Name, marker))
			}
		}

		valueStart := reader.offset
		var next int
		var err error
		switch column.Type {
		case SQLRowBinaryString, SQLRowBinaryBytes, SQLRowBinaryJSON:
			field.Data, next, err = decodeSQLRowBinaryBytes(reader.encoded, reader.offset, reader.rows, column.Name)
			if err != nil {
				return reader.fail(err)
			}
			if column.Type == SQLRowBinaryJSON && !json.Valid(field.Data) {
				return reader.fail(fmt.Errorf("RowBinary borrowed row %d column %q has invalid JSON", reader.rows, column.Name))
			}
		default:
			next, err = skipSQLRowBinaryValue(column.Type, reader.encoded, reader.offset, reader.rows, column.Name)
			if err != nil {
				return reader.fail(err)
			}
			if err := validateSQLRowBinaryEncodedFixedValue(column, reader.encoded, reader.offset, reader.rows); err != nil {
				return reader.fail(err)
			}
			field.Data = reader.encoded[valueStart:next]
		}
		reader.offset = next
	}
	reader.rows++
	return true
}

// Row returns the current borrowed row without allocating. The returned field
// bytes must be consumed before the next call to Next or Reset.
func (reader *SQLRowBinaryBorrowedReader) Row() SQLRowBinaryBorrowedRow {
	if reader == nil {
		return SQLRowBinaryBorrowedRow{}
	}
	return reader.row
}

// Columns returns the schema used by the reader. The slice is the original
// schema supplied to the constructor and should be treated as read-only.
func (reader *SQLRowBinaryBorrowedReader) Columns() []SQLRowBinaryColumn {
	if reader == nil {
		return nil
	}
	return reader.columns
}

// Err returns the first malformed-payload error, or nil after clean EOF.
func (reader *SQLRowBinaryBorrowedReader) Err() error {
	if reader == nil {
		return fmt.Errorf("RowBinary borrowed reader is nil")
	}
	return reader.err
}

func (reader *SQLRowBinaryBorrowedReader) fail(err error) bool {
	reader.err = err
	reader.done = true
	return false
}
