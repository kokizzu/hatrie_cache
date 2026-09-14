package hatSql

import (
	"errors"
	"fmt"
	"sort"
)

// ErrColumnarMapLayoutInvalid identifies malformed map offsets, keys, values,
// or row validity metadata.
var ErrColumnarMapLayoutInvalid = errors.New("hatriecache: invalid columnar map layout")

// ColumnarMapColumn stores object/map values as one offset vector and flat,
// sorted key/value vectors. A nil validity bitmap means every row contains an
// object; set bits identify non-NULL object rows when the bitmap is present.
// Missing keys and explicit NULL values remain distinct through key presence.
type ColumnarMapColumn struct {
	Offsets  []uint32
	Keys     []string
	Values   []interface{}
	Validity []byte
}

// NewColumnarMapColumn builds a compact map column. Keys are sorted within
// each row so lookups use binary search without constructing a row map.
func NewColumnarMapColumn(rows []map[string]interface{}) (ColumnarMapColumn, error) {
	if len(rows) > maxColumnarNestedInt() {
		return ColumnarMapColumn{}, fmt.Errorf("%w: row count is too large", ErrColumnarMapLayoutInvalid)
	}
	offsets := make([]uint32, len(rows)+1)
	total := uint64(0)
	allValid := true
	validity := make([]byte, columnarPackedBitmapBytes(len(rows)))
	keys := make([]string, 0)
	values := make([]interface{}, 0)
	for rowIndex, row := range rows {
		if row == nil {
			allValid = false
		} else {
			validity[rowIndex>>3] |= byte(1 << (rowIndex & 7))
		}
		rowKeys := make([]string, 0, len(row))
		for key := range row {
			rowKeys = append(rowKeys, key)
		}
		sort.Strings(rowKeys)
		total += uint64(len(rowKeys))
		if total > uint64(^uint32(0)) || total > uint64(maxColumnarNestedInt()) {
			return ColumnarMapColumn{}, fmt.Errorf("%w: key/value count is too large", ErrColumnarMapLayoutInvalid)
		}
		for _, key := range rowKeys {
			keys = append(keys, key)
			values = append(values, row[key])
		}
		offsets[rowIndex+1] = uint32(total)
	}
	if allValid {
		validity = nil
	}
	column := ColumnarMapColumn{Offsets: offsets, Keys: keys, Values: values, Validity: validity}
	if err := column.Validate(len(rows)); err != nil {
		return ColumnarMapColumn{}, err
	}
	return column, nil
}

// Validate checks the row offsets, sorted unique keys, value count, and
// optional validity bitmap.
func (column ColumnarMapColumn) Validate(rowCount int) error {
	if rowCount < 0 || len(column.Offsets) != rowCount+1 || len(column.Offsets) == 0 || column.Offsets[0] != 0 {
		return fmt.Errorf("%w: offsets length=%d rows=%d", ErrColumnarMapLayoutInvalid, len(column.Offsets), rowCount)
	}
	if len(column.Keys) != len(column.Values) {
		return fmt.Errorf("%w: keys=%d values=%d", ErrColumnarMapLayoutInvalid, len(column.Keys), len(column.Values))
	}
	if len(column.Validity) != 0 && len(column.Validity) != columnarPackedBitmapBytes(rowCount) {
		return fmt.Errorf("%w: validity bytes=%d rows=%d", ErrColumnarMapLayoutInvalid, len(column.Validity), rowCount)
	}
	previous := uint32(0)
	for row := 0; row < rowCount; row++ {
		end := column.Offsets[row+1]
		if end < previous || uint64(end) > uint64(len(column.Keys)) {
			return fmt.Errorf("%w: offset %d is outside %d entries", ErrColumnarMapLayoutInvalid, end, len(column.Keys))
		}
		for index := int(previous); index < int(end); index++ {
			if index > int(previous) && column.Keys[index-1] >= column.Keys[index] {
				return fmt.Errorf("%w: row %d keys are not strictly sorted", ErrColumnarMapLayoutInvalid, row)
			}
		}
		previous = end
	}
	if int(previous) != len(column.Keys) {
		return fmt.Errorf("%w: final offset=%d entries=%d", ErrColumnarMapLayoutInvalid, previous, len(column.Keys))
	}
	return nil
}

// Lookup returns one map key without materializing the row object. The second
// result reports key presence, so a present nil value is distinct from a
// missing key.
func (column ColumnarMapColumn) Lookup(row int, key string) (interface{}, bool) {
	start, end, ok := column.valueRange(row)
	if !ok || !column.validRow(row) {
		return nil, false
	}
	position := sort.Search(end-start, func(index int) bool {
		return column.Keys[start+index] >= key
	})
	if position >= end-start || column.Keys[start+position] != key {
		return nil, false
	}
	return column.Values[start+position], true
}

// Value returns an independent map for compatibility with direct field
// projection. JSON subcolumn execution uses Lookup and does not allocate this
// map.
func (column ColumnarMapColumn) Value(row int) (map[string]interface{}, bool) {
	start, end, ok := column.valueRange(row)
	if !ok {
		return nil, false
	}
	if !column.validRow(row) {
		return nil, true
	}
	values := make(map[string]interface{}, end-start)
	for index := start; index < end; index++ {
		values[column.Keys[index]] = column.Values[index]
	}
	return values, true
}

// Clone returns an independent copy of the physical map layout.
func (column ColumnarMapColumn) Clone() ColumnarMapColumn {
	return ColumnarMapColumn{
		Offsets:  append([]uint32(nil), column.Offsets...),
		Keys:     append([]string(nil), column.Keys...),
		Values:   append([]interface{}(nil), column.Values...),
		Validity: append([]byte(nil), column.Validity...),
	}
}

func (column ColumnarMapColumn) valueRange(row int) (int, int, bool) {
	if row < 0 || row+1 >= len(column.Offsets) {
		return 0, 0, false
	}
	start, end := int(column.Offsets[row]), int(column.Offsets[row+1])
	if start < 0 || end < start || end > len(column.Keys) || end > len(column.Values) {
		return 0, 0, false
	}
	return start, end, true
}

func (column ColumnarMapColumn) validRow(row int) bool {
	return len(column.Validity) == 0 || row >= 0 && row>>3 < len(column.Validity) && column.Validity[row>>3]&(1<<uint(row&7)) != 0
}

type sqlColumnarMapRow struct {
	column ColumnarMapColumn
	row    int
}

func (value sqlColumnarMapRow) sqlJSONMember(key string) (interface{}, bool) {
	return value.column.Lookup(value.row, key)
}

func (value sqlColumnarMapRow) sqlJSONMaterialize() interface{} {
	result, _ := value.column.Value(value.row)
	return result
}

func sqlColumnarJSONFieldInput(expr sqlExpr, row sqlExecRow) (interface{}, bool) {
	if expr.kind != "field" {
		return nil, false
	}
	for current := &row; current != nil; current = current.outer {
		if current.columnar == nil || expr.qualifier != "" && expr.qualifier != current.singleAlias {
			continue
		}
		column, ok := current.columnar.MapColumns[expr.name]
		if !ok {
			continue
		}
		return sqlColumnarMapRow{column: column, row: current.columnarRow}, true
	}
	return nil, false
}
