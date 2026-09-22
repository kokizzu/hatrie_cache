package hatDataStructure

import (
	"errors"
	"strings"
	"sync"
)

// ColumnarColumnKind identifies the fixed-width or offset-backed encoding of
// one analytical column.
type ColumnarColumnKind uint8

const (
	ColumnarInt64 ColumnarColumnKind = iota
	ColumnarFloat64
	ColumnarString
	ColumnarBool
	ColumnarBytes
)

var (
	ErrColumnarSpaceNil              = errors.New("columnar space is nil")
	ErrColumnarSpaceSchemaInvalid    = errors.New("columnar space schema is invalid")
	ErrColumnarSpaceValueInvalid     = errors.New("columnar space value is invalid")
	ErrColumnarSpaceCapacityExceeded = errors.New("columnar space capacity exceeded")
	ErrColumnarSpaceColumnNotFound   = errors.New("columnar space column not found")
	ErrColumnarSpaceRowInvalid       = errors.New("columnar space row is invalid")
	ErrColumnarSpaceClosed           = errors.New("columnar space is closed")
)

// ColumnarColumnSpec describes one column in append order.
type ColumnarColumnSpec struct {
	Name string
	Kind ColumnarColumnKind
}

// ColumnarSpaceOptions configures an independent append-only analytical
// space. Capacity zero means unlimited rows; a positive capacity also
// preallocates the fixed-width buffers.
type ColumnarSpaceOptions struct {
	Name     string
	Columns  []ColumnarColumnSpec
	Capacity int
}

// ColumnarValue is the append boundary for one typed cell. Valid=false stores
// a null while retaining the column kind for schema validation.
type ColumnarValue struct {
	Kind    ColumnarColumnKind
	Valid   bool
	Int64   int64
	Float64 float64
	String  string
	Bool    bool
	Bytes   []byte
}

// ColumnarColumnSnapshot is an immutable copy of one column's logical data.
// String and byte values use one offsets array plus one contiguous payload
// buffer. A nil Validity bitmap means every row is valid.
type ColumnarColumnSnapshot struct {
	Name          string
	Kind          ColumnarColumnKind
	Rows          int
	Validity      []byte
	Int64Values   []int64
	Float64Values []float64
	BoolBits      []byte
	StringOffsets []uint32
	StringData    []byte
	BytesOffsets  []uint32
	BytesData     []byte
}

// ValidAt reports whether row is non-null.
func (column ColumnarColumnSnapshot) ValidAt(row int) bool {
	if row < 0 || row >= column.Rows {
		return false
	}
	if len(column.Validity) == 0 {
		return true
	}
	return column.Validity[row>>3]&(1<<uint(row&7)) != 0
}

// BoolAt returns one packed boolean and its validity.
func (column ColumnarColumnSnapshot) BoolAt(row int) (bool, bool) {
	if !column.ValidAt(row) || row>>3 >= len(column.BoolBits) {
		return false, false
	}
	return column.BoolBits[row>>3]&(1<<uint(row&7)) != 0, true
}

// StringAt returns one offset-backed string and its validity.
func (column ColumnarColumnSnapshot) StringAt(row int) (string, bool) {
	if !column.ValidAt(row) || row < 0 || row+1 >= len(column.StringOffsets) {
		return "", false
	}
	start, end := column.StringOffsets[row], column.StringOffsets[row+1]
	if end < start || uint64(end) > uint64(len(column.StringData)) {
		return "", false
	}
	return string(column.StringData[start:end]), true
}

// BytesAt returns an independent copy of one offset-backed byte value.
func (column ColumnarColumnSnapshot) BytesAt(row int) ([]byte, bool) {
	if !column.ValidAt(row) || row < 0 || row+1 >= len(column.BytesOffsets) {
		return nil, false
	}
	start, end := column.BytesOffsets[row], column.BytesOffsets[row+1]
	if end < start || uint64(end) > uint64(len(column.BytesData)) {
		return nil, false
	}
	return append([]byte(nil), column.BytesData[start:end]...), true
}

type columnarSpaceColumn struct {
	spec ColumnarColumnSpec

	validity      []byte
	int64Values   []int64
	float64Values []float64
	boolBits      []byte
	stringOffsets []uint32
	stringData    []byte
	bytesOffsets  []uint32
	bytesData     []byte
}

// ColumnarSpace stores a fixed schema in independent typed buffers. It is
// safe for concurrent appends and reads; appends are validated atomically.
type ColumnarSpace struct {
	mu sync.RWMutex

	name     string
	capacity int
	rows     int
	closed   bool
	columns  []*columnarSpaceColumn
	byName   map[string]int
}

// NewColumnarSpace creates an independent typed columnar store.
func NewColumnarSpace(options ColumnarSpaceOptions) (*ColumnarSpace, error) {
	name := strings.TrimSpace(options.Name)
	if name == "" || len(options.Columns) == 0 || options.Capacity < 0 {
		return nil, ErrColumnarSpaceSchemaInvalid
	}
	space := &ColumnarSpace{
		name:     name,
		capacity: options.Capacity,
		columns:  make([]*columnarSpaceColumn, len(options.Columns)),
		byName:   make(map[string]int, len(options.Columns)),
	}
	for index, spec := range options.Columns {
		spec.Name = strings.TrimSpace(spec.Name)
		if spec.Name == "" || spec.Kind > ColumnarBytes {
			return nil, ErrColumnarSpaceSchemaInvalid
		}
		if _, exists := space.byName[spec.Name]; exists {
			return nil, ErrColumnarSpaceSchemaInvalid
		}
		column := &columnarSpaceColumn{spec: spec}
		if options.Capacity > 0 {
			column.preallocate(options.Capacity)
		}
		space.byName[spec.Name] = index
		space.columns[index] = column
	}
	return space, nil
}

// Name returns the logical space name.
func (space *ColumnarSpace) Name() string {
	if space == nil {
		return ""
	}
	space.mu.RLock()
	defer space.mu.RUnlock()
	return space.name
}

// Rows returns the number of appended rows.
func (space *ColumnarSpace) Rows() int {
	if space == nil {
		return 0
	}
	space.mu.RLock()
	defer space.mu.RUnlock()
	return space.rows
}

// ColumnCount returns the number of schema columns.
func (space *ColumnarSpace) ColumnCount() int {
	if space == nil {
		return 0
	}
	space.mu.RLock()
	defer space.mu.RUnlock()
	return len(space.columns)
}

// Schema returns a detached copy of the fixed column schema.
func (space *ColumnarSpace) Schema() []ColumnarColumnSpec {
	if space == nil {
		return nil
	}
	space.mu.RLock()
	defer space.mu.RUnlock()
	schema := make([]ColumnarColumnSpec, len(space.columns))
	for index, column := range space.columns {
		schema[index] = column.spec
	}
	return schema
}

// Append validates and appends one complete row. A failure leaves every
// column and the row count unchanged.
func (space *ColumnarSpace) Append(values []ColumnarValue) error {
	if space == nil {
		return ErrColumnarSpaceNil
	}
	space.mu.Lock()
	defer space.mu.Unlock()
	if space.closed {
		return ErrColumnarSpaceClosed
	}
	if len(values) != len(space.columns) {
		return ErrColumnarSpaceValueInvalid
	}
	if space.capacity > 0 && space.rows >= space.capacity {
		return ErrColumnarSpaceCapacityExceeded
	}
	for index, value := range values {
		column := space.columns[index]
		if err := validateColumnarValue(column.spec.Kind, value); err != nil {
			return err
		}
		if value.Valid {
			switch column.spec.Kind {
			case ColumnarString:
				if uint64(len(column.stringData))+uint64(len(value.String)) > uint64(^uint32(0)) {
					return ErrColumnarSpaceValueInvalid
				}
			case ColumnarBytes:
				if uint64(len(column.bytesData))+uint64(len(value.Bytes)) > uint64(^uint32(0)) {
					return ErrColumnarSpaceValueInvalid
				}
			}
		}
	}
	row := space.rows
	for index, value := range values {
		space.columns[index].append(row, value)
	}
	space.rows++
	return nil
}

// Column returns a detached snapshot of one typed column.
func (space *ColumnarSpace) Column(name string) (ColumnarColumnSnapshot, bool, error) {
	if space == nil {
		return ColumnarColumnSnapshot{}, false, ErrColumnarSpaceNil
	}
	space.mu.RLock()
	defer space.mu.RUnlock()
	if space.closed {
		return ColumnarColumnSnapshot{}, false, ErrColumnarSpaceClosed
	}
	index, found := space.byName[strings.TrimSpace(name)]
	if !found {
		return ColumnarColumnSnapshot{}, false, nil
	}
	return space.columns[index].snapshot(space.rows), true, nil
}

// ValueAt returns one independent cell and its non-null validity.
func (space *ColumnarSpace) ValueAt(row int, name string) (ColumnarValue, bool, error) {
	if space == nil {
		return ColumnarValue{}, false, ErrColumnarSpaceNil
	}
	space.mu.RLock()
	defer space.mu.RUnlock()
	if space.closed {
		return ColumnarValue{}, false, ErrColumnarSpaceClosed
	}
	if row < 0 || row >= space.rows {
		return ColumnarValue{}, false, ErrColumnarSpaceRowInvalid
	}
	index, found := space.byName[strings.TrimSpace(name)]
	if !found {
		return ColumnarValue{}, false, ErrColumnarSpaceColumnNotFound
	}
	return space.columns[index].valueAt(row), space.columns[index].validAt(row), nil
}

// MemoryBytes reports retained column-buffer bytes. It intentionally excludes
// map, mutex, schema, and allocator metadata so it can compare layouts.
func (space *ColumnarSpace) MemoryBytes() int64 {
	if space == nil {
		return 0
	}
	space.mu.RLock()
	defer space.mu.RUnlock()
	var total int64
	for _, column := range space.columns {
		total += column.memoryBytes()
	}
	return total
}

// Close makes future mutations and reads fail. It is idempotent.
func (space *ColumnarSpace) Close() error {
	if space == nil {
		return ErrColumnarSpaceNil
	}
	space.mu.Lock()
	defer space.mu.Unlock()
	space.closed = true
	return nil
}

func validateColumnarValue(kind ColumnarColumnKind, value ColumnarValue) error {
	if value.Kind != kind {
		return ErrColumnarSpaceValueInvalid
	}
	if !value.Valid {
		return nil
	}
	if kind == ColumnarString && uint64(len(value.String)) > uint64(^uint32(0)) {
		return ErrColumnarSpaceValueInvalid
	}
	if kind == ColumnarBytes && uint64(len(value.Bytes)) > uint64(^uint32(0)) {
		return ErrColumnarSpaceValueInvalid
	}
	return nil
}

func (column *columnarSpaceColumn) preallocate(capacity int) {
	switch column.spec.Kind {
	case ColumnarInt64:
		column.int64Values = make([]int64, 0, capacity)
	case ColumnarFloat64:
		column.float64Values = make([]float64, 0, capacity)
	case ColumnarString:
		column.stringOffsets = make([]uint32, 1, capacity+1)
	case ColumnarBool:
		column.boolBits = make([]byte, 0, (capacity+7)/8)
	case ColumnarBytes:
		column.bytesOffsets = make([]uint32, 1, capacity+1)
	}
}

func (column *columnarSpaceColumn) append(row int, value ColumnarValue) {
	if value.Valid {
		column.setValid(row)
	} else {
		column.ensureValidity(row)
	}
	switch column.spec.Kind {
	case ColumnarInt64:
		column.int64Values = append(column.int64Values, value.Int64)
	case ColumnarFloat64:
		column.float64Values = append(column.float64Values, value.Float64)
	case ColumnarString:
		if value.Valid {
			column.stringData = append(column.stringData, value.String...)
		}
		column.stringOffsets = append(column.stringOffsets, uint32(len(column.stringData)))
	case ColumnarBool:
		column.ensureBoolBit(row)
		if value.Valid && value.Bool {
			column.boolBits[row>>3] |= 1 << uint(row&7)
		}
	case ColumnarBytes:
		if value.Valid {
			column.bytesData = append(column.bytesData, value.Bytes...)
		}
		column.bytesOffsets = append(column.bytesOffsets, uint32(len(column.bytesData)))
	}
}

func (column *columnarSpaceColumn) ensureBoolBit(row int) {
	byteCount := (row >> 3) + 1
	if len(column.boolBits) >= byteCount {
		return
	}
	column.boolBits = append(column.boolBits, make([]byte, byteCount-len(column.boolBits))...)
}

func (column *columnarSpaceColumn) setValid(row int) {
	if len(column.validity) == 0 {
		return
	}
	column.validity[row>>3] |= 1 << uint(row&7)
}

func (column *columnarSpaceColumn) validAt(row int) bool {
	if len(column.validity) == 0 {
		return true
	}
	return column.validity[row>>3]&(1<<uint(row&7)) != 0
}

func (column *columnarSpaceColumn) ensureValidity(row int) {
	byteCount := (row >> 3) + 1
	if len(column.validity) == 0 {
		column.validity = make([]byte, byteCount)
		for prior := 0; prior < row; prior++ {
			column.validity[prior>>3] |= 1 << uint(prior&7)
		}
		return
	}
	if len(column.validity) < byteCount {
		column.validity = append(column.validity, make([]byte, byteCount-len(column.validity))...)
	}
}

func (column *columnarSpaceColumn) snapshot(rows int) ColumnarColumnSnapshot {
	snapshot := ColumnarColumnSnapshot{
		Name:          column.spec.Name,
		Kind:          column.spec.Kind,
		Rows:          rows,
		Validity:      append([]byte(nil), column.validity...),
		Int64Values:   append([]int64(nil), column.int64Values...),
		Float64Values: append([]float64(nil), column.float64Values...),
		BoolBits:      append([]byte(nil), column.boolBits...),
		StringOffsets: append([]uint32(nil), column.stringOffsets...),
		StringData:    append([]byte(nil), column.stringData...),
		BytesOffsets:  append([]uint32(nil), column.bytesOffsets...),
		BytesData:     append([]byte(nil), column.bytesData...),
	}
	return snapshot
}

func (column *columnarSpaceColumn) valueAt(row int) ColumnarValue {
	value := ColumnarValue{Kind: column.spec.Kind, Valid: column.validAt(row)}
	if !value.Valid {
		return value
	}
	switch column.spec.Kind {
	case ColumnarInt64:
		value.Int64 = column.int64Values[row]
	case ColumnarFloat64:
		value.Float64 = column.float64Values[row]
	case ColumnarString:
		start, end := column.stringOffsets[row], column.stringOffsets[row+1]
		value.String = string(column.stringData[start:end])
	case ColumnarBool:
		value.Bool = column.boolBits[row>>3]&(1<<uint(row&7)) != 0
	case ColumnarBytes:
		start, end := column.bytesOffsets[row], column.bytesOffsets[row+1]
		value.Bytes = append([]byte(nil), column.bytesData[start:end]...)
	}
	return value
}

func (column *columnarSpaceColumn) memoryBytes() int64 {
	var total int64
	total += int64(cap(column.validity))
	total += int64(cap(column.int64Values)) * 8
	total += int64(cap(column.float64Values)) * 8
	total += int64(cap(column.boolBits))
	total += int64(cap(column.stringOffsets)) * 4
	total += int64(cap(column.stringData))
	total += int64(cap(column.bytesOffsets)) * 4
	total += int64(cap(column.bytesData))
	return total
}
