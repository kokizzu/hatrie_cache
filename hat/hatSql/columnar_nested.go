package hatSql

import (
	"errors"
	"fmt"
	"math"
	"sort"
)

// ErrColumnarNestedLayoutInvalid identifies malformed offsets, child lengths,
// or row counts in an array or nested column.
var ErrColumnarNestedLayoutInvalid = errors.New("hatriecache: invalid columnar nested layout")

// ColumnarListColumn stores an array column as one shared offset vector and a
// flat value vector. Offsets has one more entry than the number of rows.
type ColumnarListColumn struct {
	Offsets []uint32
	Values  []interface{}
}

// NewColumnarListColumn builds an offset-based array column from row arrays.
// Values are copied into one flat backing slice; individual row slices are not
// retained by the returned layout.
func NewColumnarListColumn(rows [][]interface{}) (ColumnarListColumn, error) {
	if len(rows) == math.MaxInt {
		return ColumnarListColumn{}, fmt.Errorf("%w: row count is too large", ErrColumnarNestedLayoutInvalid)
	}
	offsets := make([]uint32, len(rows)+1)
	total := uint64(0)
	for index, row := range rows {
		total += uint64(len(row))
		if total > math.MaxUint32 || total > uint64(maxColumnarNestedInt()) {
			return ColumnarListColumn{}, fmt.Errorf("%w: flattened value count is too large", ErrColumnarNestedLayoutInvalid)
		}
		offsets[index+1] = uint32(total)
	}
	values := make([]interface{}, 0, int(total))
	for _, row := range rows {
		values = append(values, row...)
	}
	column := ColumnarListColumn{Offsets: offsets, Values: values}
	if err := column.Validate(len(rows)); err != nil {
		return ColumnarListColumn{}, err
	}
	return column, nil
}

// Validate checks that offsets describe exactly rowCount rows and all flat
// values are addressed exactly once.
func (column ColumnarListColumn) Validate(rowCount int) error {
	if err := validateColumnarNestedOffsets(column.Offsets, len(column.Values), rowCount); err != nil {
		return err
	}
	return nil
}

// Value returns an independent copy of one logical array row.
func (column ColumnarListColumn) Value(row int) ([]interface{}, bool) {
	start, end, ok := column.valueRange(row)
	if !ok {
		return nil, false
	}
	values := make([]interface{}, end-start)
	copy(values, column.Values[start:end])
	return values, true
}

// Clone returns an independent copy of the physical array layout.
func (column ColumnarListColumn) Clone() ColumnarListColumn {
	return ColumnarListColumn{
		Offsets: append([]uint32(nil), column.Offsets...),
		Values:  append([]interface{}(nil), column.Values...),
	}
}

func (column ColumnarListColumn) valueRange(row int) (int, int, bool) {
	if row < 0 || row+1 >= len(column.Offsets) {
		return 0, 0, false
	}
	start, end := int(column.Offsets[row]), int(column.Offsets[row+1])
	if start < 0 || end < start || end > len(column.Values) {
		return 0, 0, false
	}
	return start, end, true
}

// ColumnarNestedColumn stores an array of records. All child fields share one
// offset vector and store their values flattened to the same child-row count.
type ColumnarNestedColumn struct {
	Offsets []uint32
	Fields  map[string][]interface{}
}

// NewColumnarNestedColumn builds a shared-offset nested column. Each input map
// describes one parent row and every present child field must have the same
// number of child values in that row. Missing fields are represented by nil
// values in the flattened child column.
func NewColumnarNestedColumn(rows []map[string][]interface{}) (ColumnarNestedColumn, error) {
	fieldSet := make(map[string]struct{})
	rowLengths := make([]int, len(rows))
	total := uint64(0)
	for rowIndex, row := range rows {
		rowLength := -1
		for field, values := range row {
			if field == "" {
				return ColumnarNestedColumn{}, fmt.Errorf("%w: nested field name is empty", ErrColumnarNestedLayoutInvalid)
			}
			fieldSet[field] = struct{}{}
			if rowLength < 0 {
				rowLength = len(values)
			} else if rowLength != len(values) {
				return ColumnarNestedColumn{}, fmt.Errorf("%w: nested row %d has mismatched child lengths", ErrColumnarNestedLayoutInvalid, rowIndex)
			}
		}
		if rowLength < 0 {
			rowLength = 0
		}
		rowLengths[rowIndex] = rowLength
		total += uint64(rowLength)
		if total > math.MaxUint32 || total > uint64(maxColumnarNestedInt()) {
			return ColumnarNestedColumn{}, fmt.Errorf("%w: flattened child count is too large", ErrColumnarNestedLayoutInvalid)
		}
	}
	fields := make([]string, 0, len(fieldSet))
	for field := range fieldSet {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	flatFields := make(map[string][]interface{}, len(fields))
	for _, field := range fields {
		flatFields[field] = make([]interface{}, 0, int(total))
	}
	for rowIndex, row := range rows {
		for _, field := range fields {
			values, found := row[field]
			if found {
				flatFields[field] = append(flatFields[field], values...)
				continue
			}
			for index := 0; index < rowLengths[rowIndex]; index++ {
				flatFields[field] = append(flatFields[field], nil)
			}
		}
	}
	offsets := make([]uint32, len(rows)+1)
	position := uint64(0)
	for index, rowLength := range rowLengths {
		position += uint64(rowLength)
		offsets[index+1] = uint32(position)
	}
	column := ColumnarNestedColumn{Offsets: offsets, Fields: flatFields}
	if err := column.Validate(len(rows)); err != nil {
		return ColumnarNestedColumn{}, err
	}
	return column, nil
}

// Validate checks shared offsets and confirms every child field has the full
// flattened child-row count.
func (column ColumnarNestedColumn) Validate(rowCount int) error {
	childCount := 0
	if len(column.Offsets) > 0 {
		lastOffset := uint64(column.Offsets[len(column.Offsets)-1])
		if lastOffset > uint64(maxColumnarNestedInt()) {
			return fmt.Errorf("%w: final offset is too large", ErrColumnarNestedLayoutInvalid)
		}
		childCount = int(lastOffset)
	}
	if err := validateColumnarNestedOffsets(column.Offsets, childCount, rowCount); err != nil {
		return err
	}
	if len(column.Fields) == 0 && childCount != 0 {
		return fmt.Errorf("%w: nested layout has child rows but no fields", ErrColumnarNestedLayoutInvalid)
	}
	for field, values := range column.Fields {
		if field == "" || len(values) != childCount {
			return fmt.Errorf("%w: nested field %q has %d values, want %d", ErrColumnarNestedLayoutInvalid, field, len(values), childCount)
		}
	}
	return nil
}

// Value returns independent record maps for one logical parent row.
func (column ColumnarNestedColumn) Value(row int) ([]map[string]interface{}, bool) {
	start, end, ok := column.nestedRange(row)
	if !ok {
		return nil, false
	}
	fields := make([]string, 0, len(column.Fields))
	for field := range column.Fields {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	values := make([]map[string]interface{}, end-start)
	for index := range values {
		record := make(map[string]interface{}, len(fields))
		for _, field := range fields {
			record[field] = column.Fields[field][start+index]
		}
		values[index] = record
	}
	return values, true
}

// Clone returns an independent copy of the physical nested layout.
func (column ColumnarNestedColumn) Clone() ColumnarNestedColumn {
	clone := ColumnarNestedColumn{Offsets: append([]uint32(nil), column.Offsets...)}
	if len(column.Fields) > 0 {
		clone.Fields = make(map[string][]interface{}, len(column.Fields))
		for field, values := range column.Fields {
			clone.Fields[field] = append([]interface{}(nil), values...)
		}
	}
	return clone
}

func (column ColumnarNestedColumn) nestedRange(row int) (int, int, bool) {
	if row < 0 || row+1 >= len(column.Offsets) {
		return 0, 0, false
	}
	start, end := int(column.Offsets[row]), int(column.Offsets[row+1])
	if start < 0 || end < start {
		return 0, 0, false
	}
	for _, values := range column.Fields {
		if end > len(values) {
			return 0, 0, false
		}
	}
	return start, end, true
}

func validateColumnarNestedOffsets(offsets []uint32, values, rowCount int) error {
	if rowCount < 0 || len(offsets) != rowCount+1 || len(offsets) == 0 || offsets[0] != 0 {
		return fmt.Errorf("%w: offsets length=%d rows=%d", ErrColumnarNestedLayoutInvalid, len(offsets), rowCount)
	}
	previous := uint32(0)
	for _, offset := range offsets {
		if offset < previous || uint64(offset) > uint64(values) {
			return fmt.Errorf("%w: offset %d is outside %d values", ErrColumnarNestedLayoutInvalid, offset, values)
		}
		previous = offset
	}
	if int(previous) != values {
		return fmt.Errorf("%w: final offset=%d values=%d", ErrColumnarNestedLayoutInvalid, previous, values)
	}
	return nil
}

func maxColumnarNestedInt() int {
	return int(^uint(0) >> 1)
}
