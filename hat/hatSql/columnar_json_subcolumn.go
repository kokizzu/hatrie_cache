package hatSql

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
)

// ErrColumnarJSONSubcolumnInvalid identifies malformed typed JSON path data.
var ErrColumnarJSONSubcolumnInvalid = errors.New("hatriecache: invalid columnar JSON subcolumn")

// ColumnarJSONSubcolumnKind identifies the compact scalar representation.
type ColumnarJSONSubcolumnKind uint8

const (
	ColumnarJSONSubcolumnInt64 ColumnarJSONSubcolumnKind = iota + 1
	ColumnarJSONSubcolumnFloat64
	ColumnarJSONSubcolumnString
	ColumnarJSONSubcolumnBool
	ColumnarJSONSubcolumnJSON
)

// ColumnarJSONSubcolumnKey identifies one canonical JSON path rooted at a
// source field.
type ColumnarJSONSubcolumnKey struct {
	Field string
	Path  string
}

// ColumnarJSONSubcolumnRequest identifies one path requested by a SQL scan.
type ColumnarJSONSubcolumnRequest struct {
	Field string
	Path  string
}

// ColumnarJSONSubcolumnValue preserves the difference between a missing path
// and an existing JSON null value.
type ColumnarJSONSubcolumnValue struct {
	Present bool
	Value   interface{}
}

// ColumnarJSONSubcolumn stores one scalar JSON path without retaining a JSON
// object or per-row interface value. Present and Validity are bitmaps; a nil
// Present bitmap means every path exists, while a nil Validity bitmap means
// every existing path is non-NULL.
type ColumnarJSONSubcolumn struct {
	Kind     ColumnarJSONSubcolumnKind
	Rows     int
	Int64    []int64
	Float64  []float64
	Strings  []string
	BoolBits []byte
	// JSONOffsets and JSONData store valid object/array values in one
	// contiguous buffer. JSONOffsets has Rows+1 entries for the JSON kind.
	JSONOffsets []uint32
	JSONData    []byte
	Present     []byte
	Validity    []byte
}

// NewColumnarJSONSubcolumn infers a compact scalar kind from the non-NULL
// values. Integer and floating-point values may be mixed and are promoted to
// float64; all other mixed kinds are rejected.
func NewColumnarJSONSubcolumn(values []ColumnarJSONSubcolumnValue) (ColumnarJSONSubcolumn, error) {
	kind, err := inferColumnarJSONSubcolumnKind(values)
	if err != nil {
		return ColumnarJSONSubcolumn{}, err
	}
	return NewColumnarJSONSubcolumnOfKind(kind, values)
}

// NewColumnarJSONSubcolumnOfKind builds a compact scalar path with an
// explicit kind. It supports all-missing and all-NULL paths when the source
// schema already knows the intended type.
func NewColumnarJSONSubcolumnOfKind(kind ColumnarJSONSubcolumnKind, values []ColumnarJSONSubcolumnValue) (ColumnarJSONSubcolumn, error) {
	if !validColumnarJSONSubcolumnKind(kind) {
		return ColumnarJSONSubcolumn{}, fmt.Errorf("%w: unknown kind %d", ErrColumnarJSONSubcolumnInvalid, kind)
	}
	if len(values) > maxColumnarNestedInt() {
		return ColumnarJSONSubcolumn{}, fmt.Errorf("%w: row count is too large", ErrColumnarJSONSubcolumnInvalid)
	}
	column := ColumnarJSONSubcolumn{Kind: kind, Rows: len(values)}
	bitmapBytes := columnarPackedBitmapBytes(len(values))
	if kind == ColumnarJSONSubcolumnJSON {
		column.JSONOffsets = make([]uint32, len(values)+1)
	}
	if len(values) > 0 {
		column.Present = make([]byte, bitmapBytes)
		column.Validity = make([]byte, bitmapBytes)
		switch kind {
		case ColumnarJSONSubcolumnInt64:
			column.Int64 = make([]int64, len(values))
		case ColumnarJSONSubcolumnFloat64:
			column.Float64 = make([]float64, len(values))
		case ColumnarJSONSubcolumnString:
			column.Strings = make([]string, len(values))
		case ColumnarJSONSubcolumnBool:
			column.BoolBits = make([]byte, bitmapBytes)
		}
	}
	allPresent, allValid := true, true
	for row, entry := range values {
		if kind == ColumnarJSONSubcolumnJSON {
			column.JSONOffsets[row+1] = column.JSONOffsets[row]
		}
		if !entry.Present {
			allPresent, allValid = false, false
			continue
		}
		column.Present[row>>3] |= byte(1 << uint(row&7))
		if entry.Value == nil {
			allValid = false
			continue
		}
		column.Validity[row>>3] |= byte(1 << uint(row&7))
		if err := column.store(row, entry.Value); err != nil {
			return ColumnarJSONSubcolumn{}, err
		}
	}
	if allPresent {
		column.Present = nil
	}
	if allValid {
		column.Validity = nil
	}
	if err := column.Validate(len(values)); err != nil {
		return ColumnarJSONSubcolumn{}, err
	}
	return column, nil
}

// MaterializeJSONSubcolumn extracts one path from JSON documents and stores
// scalar values in typed arrays or objects/arrays as compact raw JSON. Raw
// objects and arrays are decoded only when JSON_QUERY reads a row.
func MaterializeJSONSubcolumn(path string, documents []interface{}) (ColumnarJSONSubcolumn, error) {
	canonical, err := NormalizeJSONPath(strings.TrimSpace(path))
	if err != nil {
		return ColumnarJSONSubcolumn{}, err
	}
	values := make([]ColumnarJSONSubcolumnValue, len(documents))
	for row, document := range documents {
		value, present, err := JSONPathValue(document, canonical)
		if err != nil {
			return ColumnarJSONSubcolumn{}, err
		}
		values[row] = ColumnarJSONSubcolumnValue{Present: present, Value: value}
	}
	return NewColumnarJSONSubcolumn(values)
}

// Validate checks row counts, bitmap lengths, and that exactly one payload
// shape matches Kind.
func (column ColumnarJSONSubcolumn) Validate(rowCount int) error {
	if rowCount < 0 || column.Rows != rowCount {
		return fmt.Errorf("%w: rows=%d want=%d", ErrColumnarJSONSubcolumnInvalid, column.Rows, rowCount)
	}
	bitmapBytes := columnarPackedBitmapBytes(rowCount)
	if len(column.Present) != 0 && len(column.Present) != bitmapBytes {
		return fmt.Errorf("%w: present bytes=%d rows=%d", ErrColumnarJSONSubcolumnInvalid, len(column.Present), rowCount)
	}
	if len(column.Validity) != 0 && len(column.Validity) != bitmapBytes {
		return fmt.Errorf("%w: validity bytes=%d rows=%d", ErrColumnarJSONSubcolumnInvalid, len(column.Validity), rowCount)
	}
	switch column.Kind {
	case ColumnarJSONSubcolumnInt64:
		if len(column.Int64) != rowCount || len(column.Float64) != 0 || len(column.Strings) != 0 || len(column.BoolBits) != 0 {
			return fmt.Errorf("%w: invalid int64 payload", ErrColumnarJSONSubcolumnInvalid)
		}
	case ColumnarJSONSubcolumnFloat64:
		if len(column.Float64) != rowCount || len(column.Int64) != 0 || len(column.Strings) != 0 || len(column.BoolBits) != 0 {
			return fmt.Errorf("%w: invalid float64 payload", ErrColumnarJSONSubcolumnInvalid)
		}
	case ColumnarJSONSubcolumnString:
		if len(column.Strings) != rowCount || len(column.Int64) != 0 || len(column.Float64) != 0 || len(column.BoolBits) != 0 {
			return fmt.Errorf("%w: invalid string payload", ErrColumnarJSONSubcolumnInvalid)
		}
	case ColumnarJSONSubcolumnBool:
		if len(column.BoolBits) != bitmapBytes || len(column.Int64) != 0 || len(column.Float64) != 0 || len(column.Strings) != 0 {
			return fmt.Errorf("%w: invalid bool payload", ErrColumnarJSONSubcolumnInvalid)
		}
	case ColumnarJSONSubcolumnJSON:
		if len(column.JSONOffsets) != rowCount+1 || len(column.Int64) != 0 || len(column.Float64) != 0 || len(column.Strings) != 0 || len(column.BoolBits) != 0 {
			return fmt.Errorf("%w: invalid JSON payload", ErrColumnarJSONSubcolumnInvalid)
		}
		if len(column.JSONOffsets) != 0 && uint64(column.JSONOffsets[len(column.JSONOffsets)-1]) != uint64(len(column.JSONData)) {
			return fmt.Errorf("%w: JSON payload offsets do not cover data", ErrColumnarJSONSubcolumnInvalid)
		}
		for row := 0; row < rowCount; row++ {
			if !columnarJSONSubcolumnBitmapSet(column.Present, row) || !columnarJSONSubcolumnBitmapSet(column.Validity, row) {
				continue
			}
			start := column.JSONOffsets[row]
			end := column.JSONOffsets[row+1]
			if end < start || uint64(end) > uint64(len(column.JSONData)) || !columnarJSONSubcolumnContainer(column.JSONData[start:end]) {
				return fmt.Errorf("%w: invalid JSON value at row %d", ErrColumnarJSONSubcolumnInvalid, row)
			}
		}
	default:
		return fmt.Errorf("%w: unknown kind %d", ErrColumnarJSONSubcolumnInvalid, column.Kind)
	}
	return nil
}

// Value returns one scalar value and whether its JSON path exists. An
// existing JSON null returns (nil, true); a missing path returns (nil, false).
func (column ColumnarJSONSubcolumn) Value(row int) (interface{}, bool) {
	value, present, valid := column.lookup(row)
	if !present {
		return nil, false
	}
	if !valid {
		return nil, true
	}
	return value, true
}

// Exists reports whether a JSON path exists at row. JSON null is present;
// out-of-range rows and missing paths are false. It avoids touching the
// payload, which keeps JSON_EXISTS allocation-free for columnar scans.
func (column ColumnarJSONSubcolumn) Exists(row int) bool {
	if row < 0 || row >= column.Rows {
		return false
	}
	return columnarJSONSubcolumnBitmapSet(column.Present, row)
}

func (column ColumnarJSONSubcolumn) lookup(row int) (interface{}, bool, bool) {
	if row < 0 || row >= column.Rows {
		return nil, false, false
	}
	bitmapIndex := row >> 3
	bitmapMask := byte(1 << uint(row&7))
	if len(column.Present) != 0 && (bitmapIndex >= len(column.Present) || column.Present[bitmapIndex]&bitmapMask == 0) {
		return nil, false, false
	}
	valid := len(column.Validity) == 0 || bitmapIndex < len(column.Validity) && column.Validity[bitmapIndex]&bitmapMask != 0
	if !valid {
		return nil, true, false
	}
	switch column.Kind {
	case ColumnarJSONSubcolumnInt64:
		if row >= len(column.Int64) {
			return nil, false, false
		}
		return column.Int64[row], true, true
	case ColumnarJSONSubcolumnFloat64:
		if row >= len(column.Float64) {
			return nil, false, false
		}
		return column.Float64[row], true, true
	case ColumnarJSONSubcolumnString:
		if row >= len(column.Strings) {
			return nil, false, false
		}
		return column.Strings[row], true, true
	case ColumnarJSONSubcolumnBool:
		if bitmapIndex >= len(column.BoolBits) {
			return nil, false, false
		}
		return column.BoolBits[bitmapIndex]&bitmapMask != 0, true, true
	case ColumnarJSONSubcolumnJSON:
		if row+1 >= len(column.JSONOffsets) {
			return nil, false, false
		}
		start := column.JSONOffsets[row]
		end := column.JSONOffsets[row+1]
		if end < start || uint64(end) > uint64(len(column.JSONData)) {
			return nil, false, false
		}
		return json.RawMessage(column.JSONData[start:end]), true, true
	default:
		return nil, false, false
	}
}

func (column *ColumnarJSONSubcolumn) store(row int, value interface{}) error {
	switch column.Kind {
	case ColumnarJSONSubcolumnInt64:
		integer, ok := columnarJSONSubcolumnInt64(value)
		if !ok {
			return fmt.Errorf("%w: value at row %d is not int64-compatible", ErrColumnarJSONSubcolumnInvalid, row)
		}
		column.Int64[row] = integer
	case ColumnarJSONSubcolumnFloat64:
		number, ok := columnarJSONSubcolumnFloat64(value)
		if !ok {
			return fmt.Errorf("%w: value at row %d is not float64-compatible", ErrColumnarJSONSubcolumnInvalid, row)
		}
		column.Float64[row] = number
	case ColumnarJSONSubcolumnString:
		text, ok := value.(string)
		if !ok {
			return fmt.Errorf("%w: value at row %d is not string", ErrColumnarJSONSubcolumnInvalid, row)
		}
		column.Strings[row] = text
	case ColumnarJSONSubcolumnBool:
		boolean, ok := value.(bool)
		if !ok {
			return fmt.Errorf("%w: value at row %d is not bool", ErrColumnarJSONSubcolumnInvalid, row)
		}
		if boolean {
			column.BoolBits[row>>3] |= byte(1 << uint(row&7))
		}
	case ColumnarJSONSubcolumnJSON:
		raw, ok := columnarJSONSubcolumnJSONBytes(value)
		if !ok {
			return fmt.Errorf("%w: value at row %d is not a JSON object or array", ErrColumnarJSONSubcolumnInvalid, row)
		}
		if uint64(len(column.JSONData)) > uint64(math.MaxUint32)-uint64(len(raw)) {
			return fmt.Errorf("%w: JSON payload is too large", ErrColumnarJSONSubcolumnInvalid)
		}
		column.JSONData = append(column.JSONData, raw...)
		column.JSONOffsets[row+1] = uint32(len(column.JSONData))
	default:
		return fmt.Errorf("%w: unknown kind %d", ErrColumnarJSONSubcolumnInvalid, column.Kind)
	}
	return nil
}

func inferColumnarJSONSubcolumnKind(values []ColumnarJSONSubcolumnValue) (ColumnarJSONSubcolumnKind, error) {
	hasInteger, hasFloat, hasString, hasBool, hasJSON := false, false, false, false, false
	for _, entry := range values {
		if !entry.Present || entry.Value == nil {
			continue
		}
		switch {
		case columnarJSONSubcolumnIsInteger(entry.Value):
			hasInteger = true
		case columnarJSONSubcolumnIsFloat(entry.Value):
			hasFloat = true
		case func() bool { _, ok := entry.Value.(string); return ok }():
			hasString = true
		case func() bool { _, ok := entry.Value.(bool); return ok }():
			hasBool = true
		case columnarJSONSubcolumnIsJSON(entry.Value):
			hasJSON = true
		default:
			return 0, fmt.Errorf("%w: unsupported scalar type %T", ErrColumnarJSONSubcolumnInvalid, entry.Value)
		}
	}
	switch {
	case hasString && !hasInteger && !hasFloat && !hasBool:
		return ColumnarJSONSubcolumnString, nil
	case hasBool && !hasInteger && !hasFloat && !hasString:
		return ColumnarJSONSubcolumnBool, nil
	case hasJSON && !hasInteger && !hasFloat && !hasString && !hasBool:
		return ColumnarJSONSubcolumnJSON, nil
	case (hasInteger || hasFloat) && !hasString && !hasBool:
		if hasFloat {
			return ColumnarJSONSubcolumnFloat64, nil
		}
		return ColumnarJSONSubcolumnInt64, nil
	case !hasInteger && !hasFloat && !hasString && !hasBool:
		return 0, fmt.Errorf("%w: kind cannot be inferred from empty or NULL values", ErrColumnarJSONSubcolumnInvalid)
	default:
		return 0, fmt.Errorf("%w: mixed scalar kinds", ErrColumnarJSONSubcolumnInvalid)
	}
}

func validColumnarJSONSubcolumnKind(kind ColumnarJSONSubcolumnKind) bool {
	return kind >= ColumnarJSONSubcolumnInt64 && kind <= ColumnarJSONSubcolumnJSON
}

func columnarJSONSubcolumnBitmapSet(bitmap []byte, row int) bool {
	return len(bitmap) == 0 || row>>3 < len(bitmap) && bitmap[row>>3]&(byte(1)<<uint(row&7)) != 0
}

func columnarJSONSubcolumnIsJSON(value interface{}) bool {
	switch value := value.(type) {
	case json.RawMessage:
		return columnarJSONSubcolumnContainer(value)
	case []byte:
		return columnarJSONSubcolumnContainer(value)
	case map[string]interface{}, SQLRow, []interface{}:
		return true
	default:
		return false
	}
}

func columnarJSONSubcolumnJSONBytes(value interface{}) ([]byte, bool) {
	switch value := value.(type) {
	case json.RawMessage:
		if !columnarJSONSubcolumnContainer(value) {
			return nil, false
		}
		return value, true
	case []byte:
		if !columnarJSONSubcolumnContainer(value) {
			return nil, false
		}
		return value, true
	case map[string]interface{}, SQLRow, []interface{}:
		encoded, err := json.Marshal(value)
		if err != nil || !columnarJSONSubcolumnContainer(encoded) {
			return nil, false
		}
		return encoded, true
	default:
		return nil, false
	}
}

func columnarJSONSubcolumnContainer(value []byte) bool {
	if !json.Valid(value) {
		return false
	}
	for _, character := range value {
		switch character {
		case ' ', '\t', '\n', '\r':
			continue
		case '{', '[':
			return true
		default:
			return false
		}
	}
	return false
}

func columnarJSONSubcolumnIsInteger(value interface{}) bool {
	_, ok := columnarJSONSubcolumnInt64(value)
	return ok
}

func columnarJSONSubcolumnInt64(value interface{}) (int64, bool) {
	switch value := value.(type) {
	case int:
		return int64(value), true
	case int8:
		return int64(value), true
	case int16:
		return int64(value), true
	case int32:
		return int64(value), true
	case int64:
		return value, true
	case uint:
		if uint64(value) <= math.MaxInt64 {
			return int64(value), true
		}
	case uint8:
		return int64(value), true
	case uint16:
		return int64(value), true
	case uint32:
		return int64(value), true
	case uint64:
		if value <= math.MaxInt64 {
			return int64(value), true
		}
	}
	return 0, false
}

func columnarJSONSubcolumnIsFloat(value interface{}) bool {
	switch value.(type) {
	case float32, float64:
		return true
	default:
		return false
	}
}

func columnarJSONSubcolumnFloat64(value interface{}) (float64, bool) {
	switch value := value.(type) {
	case float32:
		return float64(value), true
	case float64:
		return value, true
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
	}
	return 0, false
}
