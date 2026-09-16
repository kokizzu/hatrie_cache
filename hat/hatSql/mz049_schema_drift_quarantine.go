package hatSql

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
)

var (
	// ErrExternalSchemaQuarantineLimit reports an input or quarantine bound
	// that would require dropping a row or an issue.
	ErrExternalSchemaQuarantineLimit = errors.New("hatSql: external schema quarantine limit exceeded")
)

const (
	defaultExternalSchemaQuarantineMaxRows            = 4096
	defaultExternalSchemaQuarantineMaxQuarantinedRows = 1024
	defaultExternalSchemaQuarantineMaxIssuesPerRow    = 64
	maxExternalSchemaQuarantineRows                   = 1_000_000
	maxExternalSchemaQuarantineIssues                 = 1024
)

// ExternalSchemaQuarantineOptions bounds callback-based validation. Zero
// values select bounded defaults. AllowNumericPromotion permits a numeric
// value of any supported numeric kind for a Float64 column.
type ExternalSchemaQuarantineOptions struct {
	MaxRows               int
	MaxQuarantinedRows    int
	MaxIssuesPerRow       int
	AllowNumericPromotion bool
}

func (options ExternalSchemaQuarantineOptions) normalize() (ExternalSchemaQuarantineOptions, error) {
	if options.MaxRows < 0 {
		return ExternalSchemaQuarantineOptions{}, errors.New("external schema quarantine MaxRows cannot be negative")
	}
	if options.MaxRows > maxExternalSchemaQuarantineRows {
		return ExternalSchemaQuarantineOptions{}, fmt.Errorf("external schema quarantine MaxRows exceeds maximum %d", maxExternalSchemaQuarantineRows)
	}
	if options.MaxQuarantinedRows < 0 {
		return ExternalSchemaQuarantineOptions{}, errors.New("external schema quarantine MaxQuarantinedRows cannot be negative")
	}
	if options.MaxQuarantinedRows > maxExternalSchemaQuarantineRows {
		return ExternalSchemaQuarantineOptions{}, fmt.Errorf("external schema quarantine MaxQuarantinedRows exceeds maximum %d", maxExternalSchemaQuarantineRows)
	}
	if options.MaxIssuesPerRow < 0 {
		return ExternalSchemaQuarantineOptions{}, errors.New("external schema quarantine MaxIssuesPerRow cannot be negative")
	}
	if options.MaxIssuesPerRow > maxExternalSchemaQuarantineIssues {
		return ExternalSchemaQuarantineOptions{}, fmt.Errorf("external schema quarantine MaxIssuesPerRow exceeds maximum %d", maxExternalSchemaQuarantineIssues)
	}
	if options.MaxRows == 0 {
		options.MaxRows = defaultExternalSchemaQuarantineMaxRows
	}
	if options.MaxQuarantinedRows == 0 {
		options.MaxQuarantinedRows = defaultExternalSchemaQuarantineMaxQuarantinedRows
	}
	if options.MaxIssuesPerRow == 0 {
		options.MaxIssuesPerRow = defaultExternalSchemaQuarantineMaxIssuesPerRow
	}
	return options, nil
}

// ExternalSchemaDriftIssue identifies one row/column incompatibility.
type ExternalSchemaDriftIssue struct {
	Column   string
	Expected SQLRowBinaryType
	Actual   SQLRowBinaryType
	Reason   string
}

// ExternalSchemaQuarantineRecord is delivered for one invalid row. RowNumber
// is one-based and Issues are deterministic in schema/lexicographic order.
type ExternalSchemaQuarantineRecord struct {
	RowNumber int
	Row       Row
	Issues    []ExternalSchemaDriftIssue
}

// ExternalSchemaQuarantineStats reports rows whose callbacks completed.
type ExternalSchemaQuarantineStats struct {
	AcceptedRows    int
	QuarantinedRows int
}

// QuarantineExternalRows validates rows against an existing typed schema and
// routes each row to exactly one callback. Valid rows are sent to accept;
// schema-drift rows are sent to quarantine with bounded diagnostics. The
// function invokes callbacks in input order and retains no rows itself.
//
// Both callbacks are required so a caller cannot accidentally discard drift.
// A callback error or a configured limit stops processing and returns the
// completed counts. Existing import paths remain unchanged unless a caller
// explicitly wraps them with this function.
func QuarantineExternalRows(rows []Row, columns []SQLRowBinaryColumn, options ExternalSchemaQuarantineOptions, accept func(Row) error, quarantine func(ExternalSchemaQuarantineRecord) error) (ExternalSchemaQuarantineStats, error) {
	var stats ExternalSchemaQuarantineStats
	if accept == nil {
		return stats, errors.New("external schema quarantine accept callback is required")
	}
	if quarantine == nil {
		return stats, errors.New("external schema quarantine callback is required")
	}
	normalized, err := options.normalize()
	if err != nil {
		return stats, err
	}
	if len(rows) > normalized.MaxRows {
		return stats, fmt.Errorf("%w: row count %d exceeds maximum %d", ErrExternalSchemaQuarantineLimit, len(rows), normalized.MaxRows)
	}
	processor, err := newExternalSchemaQuarantineProcessor(columns, normalized)
	if err != nil {
		return stats, err
	}
	for rowIndex, row := range rows {
		if err := processor.process(row, rowIndex+1, accept, quarantine); err != nil {
			return processor.stats, err
		}
	}
	return processor.stats, nil
}

// QuarantineExternalJSONEachRow reads JSON objects with UseNumber and routes
// them directly to the same callbacks as QuarantineExternalRows. It retains
// no input rows and caps the parser's row limit at the quarantine limit.
func QuarantineExternalJSONEachRow(reader io.Reader, importOptions ExternalImportOptions, columns []SQLRowBinaryColumn, options ExternalSchemaQuarantineOptions, accept func(Row) error, quarantine func(ExternalSchemaQuarantineRecord) error) (ExternalSchemaQuarantineStats, error) {
	var stats ExternalSchemaQuarantineStats
	if accept == nil {
		return stats, errors.New("external schema quarantine accept callback is required")
	}
	if quarantine == nil {
		return stats, errors.New("external schema quarantine callback is required")
	}
	normalized, err := options.normalize()
	if err != nil {
		return stats, err
	}
	processor, err := newExternalSchemaQuarantineProcessor(columns, normalized)
	if err != nil {
		return stats, err
	}
	normalizedImport, err := importOptions.normalize()
	if err != nil {
		return stats, err
	}
	if normalizedImport.MaxRows > normalized.MaxRows {
		normalizedImport.MaxRows = normalized.MaxRows
	}
	counting, limited, err := newExternalImportReader(reader, normalizedImport)
	if err != nil {
		return stats, err
	}
	scanner := bufio.NewScanner(limited)
	bufferSize := normalizedImport.MaxRecordBytes
	if bufferSize > 64*1024 {
		bufferSize = 64 * 1024
	}
	if bufferSize < 1 {
		bufferSize = 1
	}
	scanner.Buffer(make([]byte, bufferSize), normalizedImport.MaxRecordBytes)
	rowNumber := 0
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		rowNumber++
		decoder := json.NewDecoder(bytes.NewReader(line))
		decoder.UseNumber()
		var row Row
		if err := decoder.Decode(&row); err != nil {
			return processor.stats, fmt.Errorf("parse external schema quarantine record %d: %w", lineNumber, err)
		}
		if row == nil {
			return processor.stats, fmt.Errorf("external schema quarantine record %d must be an object", lineNumber)
		}
		if err := externalSchemaInferenceJSONEOF(decoder); err != nil {
			return processor.stats, fmt.Errorf("external schema quarantine record %d: %w", lineNumber, err)
		}
		if err := processor.process(row, rowNumber, accept, quarantine); err != nil {
			return processor.stats, err
		}
	}
	if err := scanner.Err(); err != nil {
		return processor.stats, fmt.Errorf("scan external schema quarantine input: %w", err)
	}
	if !externalImportBytesWithinLimit(counting, normalizedImport) {
		return processor.stats, fmt.Errorf("external schema quarantine byte limit exceeded: maximum %d bytes", normalizedImport.MaxBytes)
	}
	return processor.stats, nil
}

type externalSchemaQuarantineProcessor struct {
	columns     []SQLRowBinaryColumn
	columnNames map[string]struct{}
	options     ExternalSchemaQuarantineOptions
	stats       ExternalSchemaQuarantineStats
}

func newExternalSchemaQuarantineProcessor(columns []SQLRowBinaryColumn, options ExternalSchemaQuarantineOptions) (*externalSchemaQuarantineProcessor, error) {
	if len(columns) == 0 {
		return nil, errors.New("external schema quarantine requires at least one schema column")
	}
	if err := validateSQLRowBinaryStreamColumns(columns); err != nil {
		return nil, fmt.Errorf("external schema quarantine schema: %w", err)
	}
	columnNames := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		columnNames[column.Name] = struct{}{}
	}
	return &externalSchemaQuarantineProcessor{columns: columns, columnNames: columnNames, options: options}, nil
}

func (processor *externalSchemaQuarantineProcessor) process(row Row, rowNumber int, accept func(Row) error, quarantine func(ExternalSchemaQuarantineRecord) error) error {
	if rowNumber > processor.options.MaxRows {
		return fmt.Errorf("%w: row count exceeds maximum %d", ErrExternalSchemaQuarantineLimit, processor.options.MaxRows)
	}
	issues := externalSchemaDriftIssues(row, processor.columns, processor.columnNames, processor.options)
	if len(issues) == 0 {
		if err := accept(row); err != nil {
			return fmt.Errorf("accept external schema row %d: %w", rowNumber, err)
		}
		processor.stats.AcceptedRows++
		return nil
	}
	if processor.stats.QuarantinedRows >= processor.options.MaxQuarantinedRows {
		return fmt.Errorf("%w: quarantined row count exceeds maximum %d", ErrExternalSchemaQuarantineLimit, processor.options.MaxQuarantinedRows)
	}
	if err := quarantine(ExternalSchemaQuarantineRecord{RowNumber: rowNumber, Row: row, Issues: issues}); err != nil {
		return fmt.Errorf("quarantine external schema row %d: %w", rowNumber, err)
	}
	processor.stats.QuarantinedRows++
	return nil
}

func externalSchemaDriftIssues(row Row, columns []SQLRowBinaryColumn, columnNames map[string]struct{}, options ExternalSchemaQuarantineOptions) []ExternalSchemaDriftIssue {
	var issues []ExternalSchemaDriftIssue
	for _, column := range columns {
		value, exists := row[column.Name]
		if !exists {
			if !column.Nullable {
				issues = appendExternalSchemaDriftIssue(issues, ExternalSchemaDriftIssue{Column: column.Name, Expected: column.Type, Reason: "missing required column"}, options.MaxIssuesPerRow)
			}
			continue
		}
		if value == nil {
			if !column.Nullable {
				issues = appendExternalSchemaDriftIssue(issues, ExternalSchemaDriftIssue{Column: column.Name, Expected: column.Type, Reason: "null in non-nullable column"}, options.MaxIssuesPerRow)
			}
			continue
		}
		if externalSchemaQuarantineValueCompatible(column, value, options.AllowNumericPromotion) {
			continue
		}
		issues = appendExternalSchemaDriftIssue(issues, ExternalSchemaDriftIssue{
			Column:   column.Name,
			Expected: column.Type,
			Actual:   externalSchemaValueKind(value),
			Reason:   "value type does not match schema",
		}, options.MaxIssuesPerRow)
	}

	var unknownNames []string
	unknownCount := 0
	for name := range row {
		if _, known := columnNames[name]; known {
			continue
		}
		unknownCount++
		if len(unknownNames) < options.MaxIssuesPerRow {
			unknownNames = append(unknownNames, name)
		}
	}
	sort.Strings(unknownNames)
	for _, name := range unknownNames {
		issues = appendExternalSchemaDriftIssue(issues, ExternalSchemaDriftIssue{Column: name, Reason: "unknown column"}, options.MaxIssuesPerRow)
	}
	if unknownCount > len(unknownNames) && len(issues) < options.MaxIssuesPerRow {
		issues = append(issues, ExternalSchemaDriftIssue{Reason: "additional drift issues omitted"})
	}
	return issues
}

func appendExternalSchemaDriftIssue(issues []ExternalSchemaDriftIssue, issue ExternalSchemaDriftIssue, maxIssues int) []ExternalSchemaDriftIssue {
	if len(issues) >= maxIssues {
		return issues
	}
	return append(issues, issue)
}

func externalSchemaQuarantineValueCompatible(column SQLRowBinaryColumn, value interface{}, allowNumericPromotion bool) bool {
	actual := externalSchemaValueKind(value)
	switch column.Type {
	case SQLRowBinaryJSON:
		return externalSchemaQuarantineJSONValue(value)
	case SQLRowBinaryDate:
		return actual == SQLRowBinaryDateTime
	case SQLRowBinaryDateTime:
		return actual == SQLRowBinaryDateTime
	case SQLRowBinaryInt64, SQLRowBinaryUint64, SQLRowBinaryFloat64, SQLRowBinaryBool, SQLRowBinaryString, SQLRowBinaryBytes, SQLRowBinaryDuration:
		if actual == column.Type {
			return true
		}
		return allowNumericPromotion && column.Type == SQLRowBinaryFloat64 && externalSchemaNumericKind(actual)
	default:
		_, err := appendSQLRowBinaryColumnValue(nil, column, value, 0)
		return err == nil
	}
}

func externalSchemaQuarantineJSONValue(value interface{}) bool {
	if raw, ok := value.(json.RawMessage); ok {
		return json.Valid(raw)
	}
	switch value := value.(type) {
	case nil, bool, string:
		return true
	case int, int8, int16, int32, int64:
		return true
	case uint, uint8, uint16, uint32, uint64, uintptr:
		return true
	case float32:
		return !math.IsNaN(float64(value)) && !math.IsInf(float64(value), 0)
	case float64:
		return !math.IsNaN(value) && !math.IsInf(value, 0)
	case json.Number:
		text := value.String()
		return text != "" && json.Valid([]byte(text))
	case map[string]interface{}:
		for _, nested := range value {
			if !externalSchemaQuarantineJSONValue(nested) {
				return false
			}
		}
		return true
	case []interface{}:
		for _, nested := range value {
			if !externalSchemaQuarantineJSONValue(nested) {
				return false
			}
		}
		return true
	}
	_, err := json.Marshal(value)
	return err == nil
}
