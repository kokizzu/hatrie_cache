package hatSql

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

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

func cloneExternalTable(table ExternalTable) ExternalTable {
	table.Columns = append([]string(nil), table.Columns...)
	table.Rows = CloneRows(table.Rows)
	return table
}
