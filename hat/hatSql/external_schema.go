package hatSql

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	defaultExternalSchemaInferenceMaxRows    = 4096
	defaultExternalSchemaInferenceMaxColumns = 1024
	defaultExternalSchemaInferenceMaxBytes   = 64 << 20
	maxExternalSchemaInferenceRows           = 1_000_000
	maxExternalSchemaInferenceColumns        = 65_536
)

// ExternalSchemaInferenceOptions bounds schema inference over caller-owned
// rows or external JSON. Zero values select bounded defaults. Input rows over
// MaxRows are rejected rather than silently omitted from the inferred schema.
// AllowNumericPromotion permits mixed signed, unsigned, and floating numeric
// values to become Float64; otherwise ambiguous mixtures become JSON.
type ExternalSchemaInferenceOptions struct {
	MaxRows               int
	MaxColumns            int
	MaxBytes              int64
	AllowNumericPromotion bool
}

func (options ExternalSchemaInferenceOptions) normalize() (ExternalSchemaInferenceOptions, error) {
	if options.MaxRows < 0 {
		return ExternalSchemaInferenceOptions{}, errors.New("external schema inference MaxRows cannot be negative")
	}
	if options.MaxRows > maxExternalSchemaInferenceRows {
		return ExternalSchemaInferenceOptions{}, fmt.Errorf("external schema inference MaxRows exceeds maximum %d", maxExternalSchemaInferenceRows)
	}
	if options.MaxColumns < 0 {
		return ExternalSchemaInferenceOptions{}, errors.New("external schema inference MaxColumns cannot be negative")
	}
	if options.MaxColumns > maxExternalSchemaInferenceColumns {
		return ExternalSchemaInferenceOptions{}, fmt.Errorf("external schema inference MaxColumns exceeds maximum %d", maxExternalSchemaInferenceColumns)
	}
	if options.MaxBytes < 0 {
		return ExternalSchemaInferenceOptions{}, errors.New("external schema inference MaxBytes cannot be negative")
	}
	if options.MaxBytes > int64(defaultExternalSchemaInferenceMaxBytes) {
		return ExternalSchemaInferenceOptions{}, fmt.Errorf("external schema inference MaxBytes exceeds maximum %d", defaultExternalSchemaInferenceMaxBytes)
	}
	if options.MaxRows == 0 {
		options.MaxRows = defaultExternalSchemaInferenceMaxRows
	}
	if options.MaxColumns == 0 {
		options.MaxColumns = defaultExternalSchemaInferenceMaxColumns
	}
	if options.MaxBytes == 0 {
		options.MaxBytes = defaultExternalSchemaInferenceMaxBytes
	}
	return options, nil
}

type externalSchemaColumnState struct {
	presentRows int
	kind        SQLRowBinaryType
	seenKind    bool
	nullable    bool
}

// InferExternalSchema infers a stable, sorted RowBinary-compatible schema
// from dynamic external rows. Missing and nil values make a column nullable;
// mixed or unsupported values conservatively become SQLRowBinaryJSON.
func InferExternalSchema(rows []Row, options ExternalSchemaInferenceOptions) ([]SQLRowBinaryColumn, error) {
	normalized, err := options.normalize()
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, errors.New("external schema inference requires at least one row")
	}
	if len(rows) > normalized.MaxRows {
		return nil, fmt.Errorf("external schema inference row count %d exceeds maximum %d", len(rows), normalized.MaxRows)
	}

	states := make(map[string]externalSchemaColumnState)
	for rowIndex, row := range rows {
		for name, value := range row {
			if strings.TrimSpace(name) == "" {
				return nil, fmt.Errorf("external schema inference row %d has an empty column name", rowIndex+1)
			}
			state, exists := states[name]
			if !exists {
				if len(states) >= normalized.MaxColumns {
					return nil, fmt.Errorf("external schema inference column count exceeds maximum %d", normalized.MaxColumns)
				}
				state = externalSchemaColumnState{}
			}
			state.presentRows++
			if value == nil {
				state.nullable = true
				states[name] = state
				continue
			}
			kind := externalSchemaValueKind(value)
			if !state.seenKind {
				state.kind = kind
				state.seenKind = true
			} else {
				state.kind = mergeExternalSchemaKinds(state.kind, kind, normalized.AllowNumericPromotion)
			}
			states[name] = state
		}
	}
	if len(states) == 0 {
		return nil, errors.New("external schema inference found no columns")
	}

	names := make([]string, 0, len(states))
	for name := range states {
		names = append(names, name)
	}
	sort.Strings(names)
	columns := make([]SQLRowBinaryColumn, 0, len(names))
	for _, name := range names {
		state := states[name]
		kind := state.kind
		if !state.seenKind {
			kind = SQLRowBinaryJSON
		}
		columns = append(columns, SQLRowBinaryColumn{
			Name:     name,
			Type:     kind,
			Nullable: state.nullable || state.presentRows < len(rows),
		})
	}
	return columns, nil
}

// InferExternalJSONSchema infers a schema from one JSON object or an array of
// JSON objects. JSON numbers are decoded as json.Number so integers larger
// than float64's exact range retain their integer classification.
func InferExternalJSONSchema(data []byte, options ExternalSchemaInferenceOptions) ([]SQLRowBinaryColumn, error) {
	normalized, err := options.normalize()
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, errors.New("external schema inference JSON is empty")
	}
	if int64(len(data)) > normalized.MaxBytes {
		return nil, fmt.Errorf("external schema inference JSON exceeds maximum %d bytes", normalized.MaxBytes)
	}
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil, errors.New("external schema inference JSON is empty")
	}
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.UseNumber()
	switch trimmed[0] {
	case '{':
		var row Row
		if err := decoder.Decode(&row); err != nil {
			return nil, fmt.Errorf("parse external schema inference JSON object: %w", err)
		}
		if row == nil {
			return nil, errors.New("external schema inference JSON must contain an object")
		}
		if err := externalSchemaInferenceJSONEOF(decoder); err != nil {
			return nil, err
		}
		return InferExternalSchema([]Row{row}, normalized)
	case '[':
		rows, err := decodeExternalSchemaJSONArray(decoder, normalized.MaxRows)
		if err != nil {
			return nil, err
		}
		return InferExternalSchema(rows, normalized)
	default:
		return nil, errors.New("external schema inference JSON must contain an object or an array of objects")
	}
}

// InferExternalNDJSONSchema infers a schema from one JSON object per
// non-empty line. It shares the same conservative promotion and bounds as
// InferExternalJSONSchema.
func InferExternalNDJSONSchema(data []byte, options ExternalSchemaInferenceOptions) ([]SQLRowBinaryColumn, error) {
	normalized, err := options.normalize()
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, errors.New("external schema inference NDJSON is empty")
	}
	if int64(len(data)) > normalized.MaxBytes {
		return nil, fmt.Errorf("external schema inference NDJSON exceeds maximum %d bytes", normalized.MaxBytes)
	}
	rows := make([]Row, 0)
	for lineNumber, line := range bytes.Split(data, []byte{'\n'}) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		if len(rows) >= normalized.MaxRows {
			return nil, fmt.Errorf("external schema inference NDJSON row count exceeds maximum %d", normalized.MaxRows)
		}
		decoder := json.NewDecoder(bytes.NewReader(line))
		decoder.UseNumber()
		var row Row
		if err := decoder.Decode(&row); err != nil {
			return nil, fmt.Errorf("parse external schema inference NDJSON record %d: %w", lineNumber+1, err)
		}
		if row == nil {
			return nil, fmt.Errorf("external schema inference NDJSON record %d must be an object", lineNumber+1)
		}
		if err := externalSchemaInferenceJSONEOF(decoder); err != nil {
			return nil, fmt.Errorf("external schema inference NDJSON record %d: %w", lineNumber+1, err)
		}
		rows = append(rows, row)
	}
	return InferExternalSchema(rows, normalized)
}

// InferSchema infers the schema of a registered immutable external table.
func (tables *ExternalTables) InferSchema(name string, options ExternalSchemaInferenceOptions) ([]SQLRowBinaryColumn, error) {
	if tables == nil {
		return nil, errors.New("external tables are nil")
	}
	table, ok := tables.exportTable(name)
	if !ok {
		return nil, fmt.Errorf("external table %q does not exist", strings.TrimSpace(name))
	}
	return InferExternalSchema(table.Rows, options)
}

func decodeExternalSchemaJSONArray(decoder *json.Decoder, maxRows int) ([]Row, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("parse external schema inference JSON array: %w", err)
	}
	if delimiter, ok := token.(json.Delim); !ok || delimiter != '[' {
		return nil, errors.New("external schema inference JSON array header is invalid")
	}
	rows := make([]Row, 0)
	for decoder.More() {
		if len(rows) >= maxRows {
			return nil, fmt.Errorf("external schema inference JSON row count exceeds maximum %d", maxRows)
		}
		var row Row
		if err := decoder.Decode(&row); err != nil {
			return nil, fmt.Errorf("parse external schema inference JSON row %d: %w", len(rows)+1, err)
		}
		if row == nil {
			return nil, fmt.Errorf("external schema inference JSON row %d must be an object", len(rows)+1)
		}
		rows = append(rows, row)
	}
	end, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("parse external schema inference JSON array end: %w", err)
	}
	if delimiter, ok := end.(json.Delim); !ok || delimiter != ']' {
		return nil, errors.New("external schema inference JSON array end is invalid")
	}
	if err := externalSchemaInferenceJSONEOF(decoder); err != nil {
		return nil, err
	}
	return rows, nil
}

func externalSchemaInferenceJSONEOF(decoder *json.Decoder) error {
	var trailing interface{}
	if err := decoder.Decode(&trailing); err == io.EOF {
		return nil
	} else if err == nil {
		return errors.New("external schema inference JSON has trailing values")
	} else {
		return fmt.Errorf("external schema inference JSON has invalid trailing data: %w", err)
	}
}

func externalSchemaValueKind(value interface{}) SQLRowBinaryType {
	switch value := value.(type) {
	case bool:
		return SQLRowBinaryBool
	case int, int8, int16, int32, int64:
		return SQLRowBinaryInt64
	case uint, uint8, uint16, uint32, uint64, uintptr:
		return SQLRowBinaryUint64
	case float32, float64:
		return SQLRowBinaryFloat64
	case json.Number:
		return externalSchemaJSONNumberKind(value)
	case string:
		return SQLRowBinaryString
	case []byte:
		return SQLRowBinaryBytes
	case time.Time:
		return SQLRowBinaryDateTime
	case time.Duration:
		return SQLRowBinaryDuration
	default:
		return SQLRowBinaryJSON
	}
}

func externalSchemaJSONNumberKind(number json.Number) SQLRowBinaryType {
	text := number.String()
	if !strings.ContainsAny(text, ".eE") {
		if _, err := number.Int64(); err == nil {
			return SQLRowBinaryInt64
		}
		if _, err := strconv.ParseUint(text, 10, 64); err == nil {
			return SQLRowBinaryUint64
		}
		return SQLRowBinaryJSON
	}
	value, err := number.Float64()
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return SQLRowBinaryJSON
	}
	return SQLRowBinaryFloat64
}

func mergeExternalSchemaKinds(left, right SQLRowBinaryType, allowNumericPromotion bool) SQLRowBinaryType {
	if left == right {
		return left
	}
	if left == SQLRowBinaryJSON || right == SQLRowBinaryJSON {
		return SQLRowBinaryJSON
	}
	if allowNumericPromotion && externalSchemaNumericKind(left) && externalSchemaNumericKind(right) {
		return SQLRowBinaryFloat64
	}
	return SQLRowBinaryJSON
}

func externalSchemaNumericKind(kind SQLRowBinaryType) bool {
	switch kind {
	case SQLRowBinaryInt64, SQLRowBinaryUint64, SQLRowBinaryFloat64:
		return true
	default:
		return false
	}
}
