package hatSql

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

type SourceSchemaMode string

const (
	SourceSchemaStrict   SourceSchemaMode = "strict"
	SourceSchemaInferred SourceSchemaMode = "inferred"
)

type SourceFieldType string

const (
	SourceTypeString  SourceFieldType = "string"
	SourceTypeInteger SourceFieldType = "integer"
	SourceTypeNumber  SourceFieldType = "number"
	SourceTypeBoolean SourceFieldType = "boolean"
)

type SourceFieldSchema struct {
	Type     SourceFieldType
	Required bool
}
type SourceSchema struct {
	Mode          SourceSchemaMode
	Fields        map[string]SourceFieldSchema
	AllowAdditive bool
}
type SourceImportOptions struct {
	Schema                                 SourceSchema
	Source, KeyColumn, Version, Quarantine string
	IngestedAt                             time.Time
}
type RowValidationError struct {
	Row            int
	Field, Message string
}
type SourceImportReport struct {
	Accepted, Rejected int
	Validation         []RowValidationError
	ErrorCSV           []byte
}

// CopyCSV performs deterministic row-by-row CSV ingestion. Bad rows are
// collected, written to the named quarantine source, and represented in ErrorCSV.
func (tables *ExternalTables) CopyCSV(name string, data []byte, options SourceImportOptions) (SourceImportReport, error) {
	records, err := csv.NewReader(bytes.NewReader(data)).ReadAll()
	if err != nil {
		return SourceImportReport{}, fmt.Errorf("parse CSV: %w", err)
	}
	if len(records) == 0 {
		return SourceImportReport{}, fmt.Errorf("CSV requires a header row")
	}
	columns, err := externalTableColumns(records[0])
	if err != nil {
		return SourceImportReport{}, err
	}
	schema, err := normalizeSourceSchema(options.Schema, columns)
	if err != nil {
		return SourceImportReport{}, err
	}
	if options.IngestedAt.IsZero() {
		options.IngestedAt = time.Now().UTC()
	}
	accepted, rejected := make([]Row, 0, len(records)-1), make([]Row, 0)
	report := SourceImportReport{}
	for index, record := range records[1:] {
		rowNumber := index + 2
		if len(record) != len(columns) {
			report.Rejected++
			report.Validation = append(report.Validation, RowValidationError{Row: rowNumber, Message: fmt.Sprintf("expected %d fields, got %d", len(columns), len(record))})
			continue
		}
		row := make(Row, len(columns)+4)
		for column, value := range record {
			row[columns[column]] = value
		}
		errors := validateSourceRow(row, rowNumber, schema)
		if len(errors) > 0 {
			report.Rejected++
			report.Validation = append(report.Validation, errors...)
			rejected = append(rejected, row)
			continue
		}
		row["_source"], row["_version"], row["_ingested_at"] = options.Source, options.Version, options.IngestedAt.Format(time.RFC3339Nano)
		if options.KeyColumn != "" {
			row["_key"] = fmt.Sprint(row[options.KeyColumn])
		}
		accepted = append(accepted, row)
		report.Accepted++
	}
	if err := tables.Register(name, ExternalTable{Columns: append(columns, "_source", "_key", "_ingested_at", "_version"), Rows: accepted}); err != nil {
		return SourceImportReport{}, err
	}
	if options.Quarantine != "" {
		if err := tables.Register(options.Quarantine, ExternalTable{Columns: columns, Rows: rejected}); err != nil {
			return SourceImportReport{}, err
		}
	}
	report.ErrorCSV = sourceValidationCSV(report.Validation)
	return report, nil
}

func normalizeSourceSchema(schema SourceSchema, columns []string) (SourceSchema, error) {
	if schema.Mode == "" {
		schema.Mode = SourceSchemaInferred
	}
	if schema.Mode != SourceSchemaStrict && schema.Mode != SourceSchemaInferred {
		return schema, fmt.Errorf("invalid source schema mode %q", schema.Mode)
	}
	if schema.Fields == nil {
		schema.Fields = make(map[string]SourceFieldSchema)
	}
	if schema.Mode == SourceSchemaStrict {
		for _, column := range columns {
			if _, ok := schema.Fields[column]; !ok && !schema.AllowAdditive {
				return schema, fmt.Errorf("strict schema missing column %q", column)
			}
		}
	}
	for _, column := range columns {
		if _, ok := schema.Fields[column]; !ok {
			schema.Fields[column] = SourceFieldSchema{Type: SourceTypeString}
		}
	}
	return schema, nil
}
func validateSourceRow(row Row, number int, schema SourceSchema) []RowValidationError {
	var result []RowValidationError
	for field, rule := range schema.Fields {
		value := strings.TrimSpace(fmt.Sprint(row[field]))
		if rule.Required && value == "" {
			result = append(result, RowValidationError{number, field, "required value is missing"})
			continue
		}
		if value == "" {
			continue
		}
		if !sourceValueMatches(value, rule.Type) {
			result = append(result, RowValidationError{number, field, "expected " + string(rule.Type)})
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Field < result[j].Field })
	return result
}
func sourceValueMatches(value string, kind SourceFieldType) bool {
	switch kind {
	case SourceTypeString, "":
		return true
	case SourceTypeInteger:
		_, err := strconv.ParseInt(value, 10, 64)
		return err == nil
	case SourceTypeNumber:
		_, err := strconv.ParseFloat(value, 64)
		return err == nil
	case SourceTypeBoolean:
		_, err := strconv.ParseBool(value)
		return err == nil
	default:
		return false
	}
}
func sourceValidationCSV(errors []RowValidationError) []byte {
	buffer := bytes.Buffer{}
	writer := csv.NewWriter(&buffer)
	_ = writer.Write([]string{"row", "field", "error"})
	for _, item := range errors {
		_ = writer.Write([]string{strconv.Itoa(item.Row), item.Field, item.Message})
	}
	writer.Flush()
	return buffer.Bytes()
}
