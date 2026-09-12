package hatDataStructure

import (
	"errors"
	"fmt"
)

const (
	// MaxTupleFieldOffsetFields bounds the number of fields in one tuple.
	MaxTupleFieldOffsetFields = 1 << 20
	maxTupleFieldOffsetUint32 = uint64(^uint32(0))
)

var (
	// ErrTupleFieldOffsetCount indicates too many fields for one tuple.
	ErrTupleFieldOffsetCount = errors.New("hatDataStructure: tuple field count exceeds limit")
	// ErrTupleFieldOffsetLengths indicates that field lengths do not describe
	// the supplied data exactly.
	ErrTupleFieldOffsetLengths = errors.New("hatDataStructure: tuple field lengths do not match data")
	// ErrTupleFieldOffsetIndex indicates an invalid field index.
	ErrTupleFieldOffsetIndex = errors.New("hatDataStructure: tuple field index out of range")
)

// TupleFieldOffsetCache stores field boundaries for a variable-length tuple.
// The data slice is borrowed and the offsets are owned by the cache. Field
// reads are O(1) after construction and return borrowed subslices, so callers
// must keep data immutable while using the cache. A cache built with
// NewPackedTuple owns its copied data.
//
// The value is immutable after construction and is safe for concurrent reads
// as long as callers do not mutate the slice returned by Bytes or Field.
type TupleFieldOffsetCache struct {
	data    []byte
	offsets []uint32
	valid   []bool
}

// NewTupleFieldOffsetCache builds offsets over data using one length per
// field. It does not copy data, but it copies the derived offsets. The lengths
// must sum exactly to len(data).
func NewTupleFieldOffsetCache(data []byte, lengths []uint32) (TupleFieldOffsetCache, error) {
	if len(lengths) > MaxTupleFieldOffsetFields {
		return TupleFieldOffsetCache{}, fmt.Errorf("%w: got %d, maximum %d", ErrTupleFieldOffsetCount, len(lengths), MaxTupleFieldOffsetFields)
	}
	if uint64(len(data)) > maxTupleFieldOffsetUint32 {
		return TupleFieldOffsetCache{}, fmt.Errorf("%w: data length %d exceeds uint32 offsets", ErrTupleFieldOffsetLengths, len(data))
	}
	offsets := make([]uint32, len(lengths)+1)
	var offset uint64
	for index, length := range lengths {
		next := offset + uint64(length)
		if next < offset || next > uint64(len(data)) || next > maxTupleFieldOffsetUint32 {
			return TupleFieldOffsetCache{}, fmt.Errorf("%w: field %d ends at %d, data length %d", ErrTupleFieldOffsetLengths, index, next, len(data))
		}
		offset = next
		offsets[index+1] = uint32(offset)
	}
	if offset != uint64(len(data)) {
		return TupleFieldOffsetCache{}, fmt.Errorf("%w: lengths total %d, data length %d", ErrTupleFieldOffsetLengths, offset, len(data))
	}
	return TupleFieldOffsetCache{data: data, offsets: offsets}, nil
}

// NewPackedTuple copies fields into one contiguous data buffer and builds its
// offset table. The returned cache owns the copied bytes, while Field and
// Bytes still return borrowed views into that owned buffer.
func NewPackedTuple(fields [][]byte) (TupleFieldOffsetCache, error) {
	if len(fields) > MaxTupleFieldOffsetFields {
		return TupleFieldOffsetCache{}, fmt.Errorf("%w: got %d, maximum %d", ErrTupleFieldOffsetCount, len(fields), MaxTupleFieldOffsetFields)
	}
	var total uint64
	for index, field := range fields {
		total += uint64(len(field))
		if total < uint64(len(field)) || total > maxTupleFieldOffsetUint32 {
			return TupleFieldOffsetCache{}, fmt.Errorf("%w: field %d makes tuple length %d", ErrTupleFieldOffsetLengths, index, total)
		}
	}
	if total > uint64(^uint(0)>>1) {
		return TupleFieldOffsetCache{}, fmt.Errorf("%w: tuple length %d does not fit in int", ErrTupleFieldOffsetLengths, total)
	}
	data := make([]byte, int(total))
	offsets := make([]uint32, len(fields)+1)
	var offset uint32
	for index, field := range fields {
		copy(data[offset:], field)
		offset += uint32(len(field))
		offsets[index+1] = offset
	}
	return TupleFieldOffsetCache{data: data, offsets: offsets}, nil
}

// Field returns field index as a borrowed slice into the tuple data.
func (cache TupleFieldOffsetCache) Field(index int) ([]byte, error) {
	start, end, err := cache.Offset(index)
	if err != nil {
		return nil, err
	}
	return cache.data[start:end], nil
}

// FieldValid reports whether field index is non-NULL. Caches built by the
// existing constructors mark every field valid; typed tuple formats use the
// validity bitmap to preserve NULL separately from empty bytes.
func (cache TupleFieldOffsetCache) FieldValid(index int) (bool, error) {
	if _, _, err := cache.Offset(index); err != nil {
		return false, err
	}
	if len(cache.valid) == 0 {
		return true, nil
	}
	return cache.valid[index], nil
}

// FieldInto copies field index into dst, reusing dst's backing array when it
// has enough capacity. It is useful when the caller needs ownership instead of
// a borrowed field slice.
func (cache TupleFieldOffsetCache) FieldInto(index int, dst []byte) ([]byte, error) {
	field, err := cache.Field(index)
	if err != nil {
		return dst[:0], err
	}
	dst = dst[:0]
	if cap(dst) < len(field) {
		dst = make([]byte, 0, len(field))
	}
	return append(dst, field...), nil
}

// Offset returns the half-open byte interval occupied by field index.
func (cache TupleFieldOffsetCache) Offset(index int) (start, end uint32, err error) {
	if index < 0 || index >= len(cache.offsets)-1 {
		return 0, 0, fmt.Errorf("%w: got %d, field count %d", ErrTupleFieldOffsetIndex, index, cache.FieldCount())
	}
	return cache.offsets[index], cache.offsets[index+1], nil
}

// Bytes returns the complete tuple data as a borrowed slice. Callers must not
// mutate it while the cache is in use.
func (cache TupleFieldOffsetCache) Bytes() []byte { return cache.data }

// FieldCount returns the number of fields in the tuple.
func (cache TupleFieldOffsetCache) FieldCount() int {
	if len(cache.offsets) == 0 {
		return 0
	}
	return len(cache.offsets) - 1
}

// Clone returns an independently owned copy of the tuple data and offsets.
func (cache TupleFieldOffsetCache) Clone() TupleFieldOffsetCache {
	clone := TupleFieldOffsetCache{
		data:    append([]byte(nil), cache.data...),
		offsets: append([]uint32(nil), cache.offsets...),
		valid:   append([]bool(nil), cache.valid...),
	}
	return clone
}
