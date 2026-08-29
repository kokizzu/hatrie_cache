package hatSql

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/parquet-go/parquet-go"
)

// ExternalTable is an immutable external-table snapshot.
type ExternalTable struct {
	Columns []string
	Rows    []Row
}

// ExternalTables stores imported tabular snapshots for EXTERNAL('name') SQL
// scans. It accepts bytes supplied by the caller; it does not open paths or
// make network requests.
type ExternalTables struct {
	mu     sync.RWMutex
	tables map[string]ExternalTable
}

// NewExternalTables creates an empty external-table registry.
func NewExternalTables() *ExternalTables {
	return &ExternalTables{tables: make(map[string]ExternalTable)}
}

// ImportCSV parses a header-based RFC 4180 CSV document and replaces name.
// Every CSV cell is preserved as text; callers can cast values in SQL.
func (tables *ExternalTables) ImportCSV(name string, data []byte) error {
	reader := csv.NewReader(bytes.NewReader(data))
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("parse CSV: %w", err)
	}
	if len(records) == 0 {
		return fmt.Errorf("CSV requires a header row")
	}
	columns, err := externalTableColumns(records[0])
	if err != nil {
		return err
	}
	rows := make([]Row, 0, len(records)-1)
	for rowIndex, record := range records[1:] {
		if len(record) != len(columns) {
			return fmt.Errorf("CSV row %d has %d fields, want %d", rowIndex+2, len(record), len(columns))
		}
		row := make(Row, len(columns))
		for column, value := range record {
			row[columns[column]] = value
		}
		rows = append(rows, row)
	}
	return tables.Register(name, ExternalTable{Columns: columns, Rows: rows})
}

// ImportJSON parses either one JSON object or an array of JSON objects and
// replaces name. JSON value types are preserved for SQL comparison and casts.
func (tables *ExternalTables) ImportJSON(name string, data []byte) error {
	rows, err := ParseJSONRows(data)
	if err != nil {
		return err
	}
	return tables.Register(name, ExternalTable{Columns: externalTableRowColumns(rows), Rows: rows})
}

// ImportNDJSON parses one JSON object per non-empty line and replaces name.
// Values retain their JSON types for SQL comparison and casts. The existing
// table remains unchanged when any record is invalid.
func (tables *ExternalTables) ImportNDJSON(name string, data []byte) error {
	lines := bytes.Split(data, []byte{'\n'})
	rows := make([]Row, 0, len(lines))
	for lineNumber, line := range lines {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		var row Row
		if err := json.Unmarshal(line, &row); err != nil {
			return fmt.Errorf("parse NDJSON record %d: %w", lineNumber+1, err)
		}
		if row == nil {
			return fmt.Errorf("NDJSON record %d must be an object", lineNumber+1)
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		return fmt.Errorf("NDJSON requires at least one object record")
	}
	return tables.Register(name, ExternalTable{Columns: externalTableRowColumns(rows), Rows: rows})
}

// ImportArrow parses a flat Apache Arrow IPC stream and replaces name. Boolean,
// float64, and UTF-8 columns are supported; nested and mixed-type columns are
// rejected rather than coerced.
func (tables *ExternalTables) ImportArrow(name string, data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("Arrow data is empty")
	}
	reader, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("parse Arrow IPC: %w", err)
	}
	defer reader.Release()
	columns := make([]string, 0)
	rows := make([]Row, 0)
	for reader.Next() {
		record := reader.Record()
		if len(columns) == 0 {
			columns = make([]string, record.NumCols())
			for index := range columns {
				columns[index] = record.Schema().Field(index).Name
			}
			if _, err := externalTableColumns(columns); err != nil {
				return err
			}
		}
		if record.NumCols() != int64(len(columns)) {
			return fmt.Errorf("Arrow record has %d columns, want %d", record.NumCols(), len(columns))
		}
		for rowIndex := 0; rowIndex < int(record.NumRows()); rowIndex++ {
			row := make(Row, len(columns))
			for columnIndex, column := range columns {
				value, exists, err := externalArrowValue(record.Column(columnIndex), rowIndex)
				if err != nil {
					return fmt.Errorf("Arrow column %q: %w", column, err)
				}
				if exists {
					row[column] = value
				}
			}
			rows = append(rows, row)
		}
	}
	if err := reader.Err(); err != nil {
		return fmt.Errorf("read Arrow IPC: %w", err)
	}
	if len(columns) == 0 {
		return fmt.Errorf("Arrow IPC requires one record with columns")
	}
	return tables.Register(name, ExternalTable{Columns: columns, Rows: rows})
}

// ImportParquet parses a flat Parquet table and replaces name. Nested and
// repeated schemas are rejected because SQL external tables expose one scalar
// value per column. Parquet UTF-8/binary values become strings; primitive
// numeric and boolean values retain their Go representations.
func (tables *ExternalTables) ImportParquet(name string, data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("Parquet data is empty")
	}
	file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return fmt.Errorf("parse Parquet: %w", err)
	}
	columns, err := externalParquetColumns(file.Schema().Columns())
	if err != nil {
		return err
	}
	reader := parquet.NewRowGroupRowReader(parquet.MultiRowGroup(file.RowGroups()...))
	defer reader.Close()
	rows := make([]Row, 0)
	buffer := make([]parquet.Row, 128)
	for {
		count, readErr := reader.ReadRows(buffer)
		for _, parquetRow := range buffer[:count] {
			row := make(Row, len(columns))
			parquetRow.Range(func(columnIndex int, values []parquet.Value) bool {
				if columnIndex >= len(columns) || len(values) == 0 {
					return true
				}
				value := values[len(values)-1]
				if !value.IsNull() {
					row[columns[columnIndex]] = externalParquetValue(value)
				}
				return true
			})
			rows = append(rows, row)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return fmt.Errorf("read Parquet rows: %w", readErr)
		}
	}
	return tables.Register(name, ExternalTable{Columns: columns, Rows: rows})
}

// Register replaces a table snapshot after validating its name and columns.
func (tables *ExternalTables) Register(name string, table ExternalTable) error {
	if tables == nil {
		return fmt.Errorf("external tables are nil")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("external table name is required")
	}
	columns, err := externalTableColumns(table.Columns)
	if err != nil {
		return err
	}
	for rowIndex, row := range table.Rows {
		for column := range row {
			if !externalTableHasColumn(columns, column) {
				return fmt.Errorf("external table %q row %d has undeclared column %q", name, rowIndex+1, column)
			}
		}
	}
	table.Columns = columns
	table.Rows = CloneRows(table.Rows)
	tables.mu.Lock()
	tables.tables[name] = table
	tables.mu.Unlock()
	return nil
}

// Get returns an independent snapshot of one imported table.
func (tables *ExternalTables) Get(name string) (ExternalTable, bool) {
	if tables == nil {
		return ExternalTable{}, false
	}
	tables.mu.RLock()
	table, ok := tables.tables[strings.TrimSpace(name)]
	tables.mu.RUnlock()
	if !ok {
		return ExternalTable{}, false
	}
	return cloneExternalTable(table), true
}

// ExportCSV encodes the registered table in its declared column order.
func (tables *ExternalTables) ExportCSV(name string) ([]byte, error) {
	table, ok := tables.Get(name)
	if !ok {
		return nil, fmt.Errorf("external table %q does not exist", strings.TrimSpace(name))
	}
	buffer := bytes.Buffer{}
	writer := csv.NewWriter(&buffer)
	if err := writer.Write(table.Columns); err != nil {
		return nil, err
	}
	for _, row := range table.Rows {
		record := make([]string, len(table.Columns))
		for column, name := range table.Columns {
			if value := row[name]; value != nil {
				record[column] = fmt.Sprint(value)
			}
		}
		if err := writer.Write(record); err != nil {
			return nil, err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// ExportJSON encodes the registered table as a JSON array of objects.
func (tables *ExternalTables) ExportJSON(name string) ([]byte, error) {
	table, ok := tables.Get(name)
	if !ok {
		return nil, fmt.Errorf("external table %q does not exist", strings.TrimSpace(name))
	}
	return json.Marshal(table.Rows)
}

// ExportNDJSON encodes the registered table as one JSON object per line.
func (tables *ExternalTables) ExportNDJSON(name string) ([]byte, error) {
	table, ok := tables.Get(name)
	if !ok {
		return nil, fmt.Errorf("external table %q does not exist", strings.TrimSpace(name))
	}
	buffer := bytes.Buffer{}
	encoder := json.NewEncoder(&buffer)
	for _, row := range table.Rows {
		if err := encoder.Encode(row); err != nil {
			return nil, err
		}
	}
	return buffer.Bytes(), nil
}

// ExportArrow encodes one table as a flat Apache Arrow IPC stream. Each column
// must contain only booleans, numbers, strings, or NULL values.
func (tables *ExternalTables) ExportArrow(name string) ([]byte, error) {
	table, ok := tables.Get(name)
	if !ok {
		return nil, fmt.Errorf("external table %q does not exist", strings.TrimSpace(name))
	}
	types := make([]externalArrowType, len(table.Columns))
	fields := make([]arrow.Field, len(table.Columns))
	for index, column := range table.Columns {
		kind, err := externalArrowColumnType(table.Rows, column)
		if err != nil {
			return nil, err
		}
		types[index] = kind
		fields[index] = arrow.Field{Name: column, Type: kind.dataType(), Nullable: true}
	}
	schema := arrow.NewSchema(fields, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	for _, row := range table.Rows {
		for index, column := range table.Columns {
			value := row[column]
			if err := types[index].append(builder.Field(index), value); err != nil {
				return nil, fmt.Errorf("Arrow column %q: %w", column, err)
			}
		}
	}
	record := builder.NewRecord()
	defer record.Release()
	buffer := bytes.Buffer{}
	writer := ipc.NewWriter(&buffer, ipc.WithSchema(schema))
	if err := writer.Write(record); err != nil {
		return nil, fmt.Errorf("write Arrow IPC: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close Arrow IPC: %w", err)
	}
	return buffer.Bytes(), nil
}

// ExportParquet encodes one table as a flat Parquet document. Values are
// serialized as optional UTF-8 text to retain arbitrary external-table values
// without type inference; SQL callers can cast fields on read.
func (tables *ExternalTables) ExportParquet(name string) ([]byte, error) {
	table, ok := tables.Get(name)
	if !ok {
		return nil, fmt.Errorf("external table %q does not exist", strings.TrimSpace(name))
	}
	group := make(parquet.Group, len(table.Columns))
	for _, column := range table.Columns {
		group[column] = parquet.Optional(parquet.String())
	}
	schema := parquet.NewSchema("external", group)
	paths := schema.Columns()
	columns, err := externalParquetColumns(paths)
	if err != nil {
		return nil, err
	}
	columnIndexes := make(map[string]int, len(columns))
	for index, column := range columns {
		columnIndexes[column] = index
	}
	rows := make([]parquet.Row, len(table.Rows))
	for rowIndex, row := range table.Rows {
		parquetRow := make(parquet.Row, len(columns))
		for column, name := range columns {
			value := parquet.NullValue().Level(0, 0, column)
			if source, exists := row[name]; exists && source != nil {
				value = parquet.ValueOf(fmt.Sprint(source)).Level(0, 1, columnIndexes[name])
			}
			parquetRow[column] = value
		}
		rows[rowIndex] = parquetRow
	}
	buffer := bytes.Buffer{}
	writer := parquet.NewWriter(&buffer, schema)
	if _, err := writer.WriteRows(rows); err != nil {
		return nil, fmt.Errorf("write Parquet rows: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close Parquet writer: %w", err)
	}
	return buffer.Bytes(), nil
}

// ResolveSQLSource lets a registry be passed directly to query execution for
// external-only queries. CACHE and KEYS require an application resolver.
func (tables *ExternalTables) ResolveSQLSource(name string, key string) ([]Row, error) {
	return nil, fmt.Errorf("source %s(%q) is not available from external tables", name, key)
}

// ResolveSQLExternalSource supplies a cloned external-table snapshot to SQL.
func (tables *ExternalTables) ResolveSQLExternalSource(name string) ([]Row, error) {
	table, ok := tables.Get(name)
	if !ok {
		return nil, fmt.Errorf("external table %q does not exist", strings.TrimSpace(name))
	}
	return table.Rows, nil
}

// ParseJSONRows parses one JSON object or an array of JSON objects.
func ParseJSONRows(data []byte) ([]Row, error) {
	var rows []Row
	if err := json.Unmarshal(data, &rows); err == nil {
		return rows, nil
	}
	var row Row
	if err := json.Unmarshal(data, &row); err == nil {
		return []Row{row}, nil
	}
	return nil, fmt.Errorf("JSON external table must contain an object or an array of objects")
}

func externalTableColumns(columns []string) ([]string, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("external table requires at least one column")
	}
	result := make([]string, len(columns))
	seen := make(map[string]struct{}, len(columns))
	for index, column := range columns {
		column = strings.TrimSpace(column)
		if column == "" {
			return nil, fmt.Errorf("external table column %d is empty", index+1)
		}
		if _, exists := seen[column]; exists {
			return nil, fmt.Errorf("external table column %q is duplicated", column)
		}
		seen[column] = struct{}{}
		result[index] = column
	}
	return result, nil
}

func externalTableRowColumns(rows []Row) []string {
	columns := map[string]struct{}{}
	for _, row := range rows {
		for column := range row {
			columns[column] = struct{}{}
		}
	}
	result := make([]string, 0, len(columns))
	for column := range columns {
		result = append(result, column)
	}
	sort.Strings(result)
	return result
}

func externalTableHasColumn(columns []string, name string) bool {
	for _, column := range columns {
		if column == name {
			return true
		}
	}
	return false
}

func externalParquetColumns(paths [][]string) ([]string, error) {
	if len(paths) == 0 {
		return nil, fmt.Errorf("Parquet table has no columns")
	}
	columns := make([]string, len(paths))
	seen := make(map[string]struct{}, len(paths))
	for index, path := range paths {
		if len(path) != 1 || strings.TrimSpace(path[0]) == "" {
			return nil, fmt.Errorf("Parquet external tables require flat scalar columns")
		}
		if _, exists := seen[path[0]]; exists {
			return nil, fmt.Errorf("Parquet column %q is duplicated", path[0])
		}
		seen[path[0]] = struct{}{}
		columns[index] = path[0]
	}
	return columns, nil
}

func externalParquetValue(value parquet.Value) interface{} {
	switch value.Kind() {
	case parquet.Boolean:
		return value.Boolean()
	case parquet.Int32:
		return int64(value.Int32())
	case parquet.Int64:
		return value.Int64()
	case parquet.Float:
		return float64(value.Float())
	case parquet.Double:
		return value.Double()
	case parquet.ByteArray, parquet.FixedLenByteArray:
		return string(value.ByteArray())
	default:
		return value.String()
	}
}

type externalArrowType uint8

const (
	externalArrowBoolean externalArrowType = iota + 1
	externalArrowFloat64
	externalArrowString
)

func externalArrowColumnType(rows []Row, column string) (externalArrowType, error) {
	kind := externalArrowType(0)
	for _, row := range rows {
		value, exists := row[column]
		if !exists || value == nil {
			continue
		}
		candidate, ok := externalArrowTypeOf(value)
		if !ok {
			return 0, fmt.Errorf("has unsupported value type %T", value)
		}
		if kind != 0 && kind != candidate {
			return 0, fmt.Errorf("mixes incompatible value types")
		}
		kind = candidate
	}
	if kind == 0 {
		return externalArrowString, nil
	}
	return kind, nil
}

func externalArrowTypeOf(value interface{}) (externalArrowType, bool) {
	switch value.(type) {
	case bool:
		return externalArrowBoolean, true
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return externalArrowFloat64, true
	case string, []byte:
		return externalArrowString, true
	default:
		return 0, false
	}
}

func (kind externalArrowType) dataType() arrow.DataType {
	switch kind {
	case externalArrowBoolean:
		return arrow.FixedWidthTypes.Boolean
	case externalArrowFloat64:
		return arrow.PrimitiveTypes.Float64
	default:
		return arrow.BinaryTypes.String
	}
}

func (kind externalArrowType) append(builder array.Builder, value interface{}) error {
	if value == nil {
		switch typed := builder.(type) {
		case *array.BooleanBuilder:
			typed.AppendNull()
		case *array.Float64Builder:
			typed.AppendNull()
		case *array.StringBuilder:
			typed.AppendNull()
		default:
			return errors.New("has unsupported Arrow builder")
		}
		return nil
	}
	switch kind {
	case externalArrowBoolean:
		value, ok := value.(bool)
		if !ok {
			return fmt.Errorf("has incompatible value type %T", value)
		}
		builder.(*array.BooleanBuilder).Append(value)
	case externalArrowFloat64:
		value, ok := externalArrowNumber(value)
		if !ok {
			return fmt.Errorf("has incompatible value type %T", value)
		}
		builder.(*array.Float64Builder).Append(value)
	case externalArrowString:
		switch value := value.(type) {
		case string:
			builder.(*array.StringBuilder).Append(value)
		case []byte:
			builder.(*array.StringBuilder).Append(string(value))
		default:
			return fmt.Errorf("has incompatible value type %T", value)
		}
	}
	return nil
}

func externalArrowNumber(value interface{}) (float64, bool) {
	switch value := value.(type) {
	case int:
		return float64(value), true
	case int8:
		return float64(value), true
	case int16:
		return float64(value), true
	case int32:
		return float64(value), true
	case int64:
		return float64(value), true
	case uint:
		return float64(value), true
	case uint8:
		return float64(value), true
	case uint16:
		return float64(value), true
	case uint32:
		return float64(value), true
	case uint64:
		return float64(value), true
	case float32:
		return float64(value), true
	case float64:
		return value, true
	default:
		return 0, false
	}
}

func externalArrowValue(column arrow.Array, index int) (interface{}, bool, error) {
	if column.IsNull(index) {
		return nil, false, nil
	}
	switch column := column.(type) {
	case *array.Boolean:
		return column.Value(index), true, nil
	case *array.Float64:
		return column.Value(index), true, nil
	case *array.String:
		return column.Value(index), true, nil
	default:
		return nil, false, fmt.Errorf("has unsupported Arrow type %s", column.DataType())
	}
}

func cloneExternalTable(table ExternalTable) ExternalTable {
	table.Columns = append([]string(nil), table.Columns...)
	table.Rows = CloneRows(table.Rows)
	return table
}
