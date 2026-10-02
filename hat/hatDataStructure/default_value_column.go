package hatDataStructure

import (
	"errors"
	"math/bits"
	"unsafe"
)

const (
	// DefaultValueColumnDenseThreshold is the default non-default density at
	// which a column changes to its dense representation.
	DefaultValueColumnDenseThreshold = 0.75
	// DefaultValueColumnMinDenseRows avoids switching tiny columns merely
	// because their first value happens to be non-default.
	DefaultValueColumnMinDenseRows = 64
)

var (
	ErrDefaultValueColumnNil       = errors.New("hatDataStructure: default value column is nil")
	ErrDefaultValueColumnIndex     = errors.New("hatDataStructure: default value column index is out of range")
	ErrDefaultValueColumnCapacity  = errors.New("hatDataStructure: default value column capacity is negative")
	ErrDefaultValueColumnThreshold = errors.New("hatDataStructure: default value column density threshold is invalid")
)

// DefaultValueColumnOptions configures a DefaultValueColumn. Sparse storage
// keeps a bit for every row and a packed value only for rows different from
// Default. Once the density threshold is reached, the column switches to a
// dense representation so dense data does not retain sparse indexing cost.
type DefaultValueColumnOptions[T comparable] struct {
	Default        T
	Capacity       int
	DenseThreshold float64
}

// DefaultValueColumn stores an append-oriented typed column with compact
// default-value suppression. It is not safe for concurrent mutation.
type DefaultValueColumn[T comparable] struct {
	defaultValue   T
	denseThreshold float64
	capacityHint   int
	length         int
	nonDefault     int

	// Sparse representation. wordStarts[word] is the first packed value for
	// that mask word; the final entry is the packed-value length.
	masks      []uint64
	wordStarts []int
	values     []T

	// Dense is retained after the adaptive conversion; we intentionally do not
	// convert back on deletes to avoid repeated representation churn.
	dense []T
}

// NewDefaultValueColumn creates a column using the default adaptive density
// threshold.
func NewDefaultValueColumn[T comparable](defaultValue T, capacity int) (*DefaultValueColumn[T], error) {
	return NewDefaultValueColumnWithOptions(DefaultValueColumnOptions[T]{
		Default:        defaultValue,
		Capacity:       capacity,
		DenseThreshold: DefaultValueColumnDenseThreshold,
	})
}

// NewDefaultValueColumnWithOptions creates a compact default-suppressing
// column. Capacity is an allocation hint and may be zero.
func NewDefaultValueColumnWithOptions[T comparable](options DefaultValueColumnOptions[T]) (*DefaultValueColumn[T], error) {
	if options.Capacity < 0 {
		return nil, ErrDefaultValueColumnCapacity
	}
	if options.DenseThreshold <= 0 || options.DenseThreshold > 1 {
		return nil, ErrDefaultValueColumnThreshold
	}
	wordCapacity := (options.Capacity + 63) / 64
	column := &DefaultValueColumn[T]{
		defaultValue:   options.Default,
		denseThreshold: options.DenseThreshold,
		capacityHint:   options.Capacity,
	}
	if wordCapacity > 0 {
		column.masks = make([]uint64, 0, wordCapacity)
		column.wordStarts = make([]int, 1, wordCapacity+1)
		valueCapacity := options.Capacity / 8
		if valueCapacity < 1 {
			valueCapacity = 1
		}
		column.values = make([]T, 0, valueCapacity)
	}
	return column, nil
}

// Len returns the number of appended rows.
func (column *DefaultValueColumn[T]) Len() int {
	if column == nil {
		return 0
	}
	return column.length
}

// NonDefaultCount returns the number of rows whose value differs from the
// configured default. It remains exact after a dense conversion.
func (column *DefaultValueColumn[T]) NonDefaultCount() int {
	if column == nil {
		return 0
	}
	return column.nonDefault
}

// Dense reports whether the adaptive dense representation is active.
func (column *DefaultValueColumn[T]) Dense() bool {
	return column != nil && column.dense != nil
}

// StorageBytes estimates retained backing-array bytes, excluding slice and
// object headers. It is intended for diagnostics and benchmark comparisons.
func (column *DefaultValueColumn[T]) StorageBytes() int {
	if column == nil {
		return 0
	}
	valueBytes := int(unsafe.Sizeof(*new(T)))
	if column.dense != nil {
		return cap(column.dense) * valueBytes
	}
	return column.sparseStorageBytes(valueBytes)
}

// Append adds one row and returns an error only for a nil column.
func (column *DefaultValueColumn[T]) Append(value T) error {
	if column == nil {
		return ErrDefaultValueColumnNil
	}
	if column.dense != nil {
		column.dense = append(column.dense, value)
		column.length++
		if value != column.defaultValue {
			column.nonDefault++
		}
		return nil
	}

	row := column.length
	word := row >> 6
	if word == len(column.masks) {
		column.masks = append(column.masks, 0)
		column.wordStarts = append(column.wordStarts, len(column.values))
	}
	if value == column.defaultValue {
		column.length++
		if column.length == DefaultValueColumnMinDenseRows {
			column.maybeUseDense()
		}
		return nil
	}
	column.masks[word] |= uint64(1) << uint(row&63)
	column.values = append(column.values, value)
	column.nonDefault++
	column.wordStarts[len(column.wordStarts)-1] = len(column.values)
	column.length++
	column.maybeUseDense()
	return nil
}

// Set changes an existing row. Sparse updates that insert or remove a value
// shift only the packed non-default suffix and preserve row order.
func (column *DefaultValueColumn[T]) Set(index int, value T) error {
	if column == nil {
		return ErrDefaultValueColumnNil
	}
	if index < 0 || index >= column.length {
		return ErrDefaultValueColumnIndex
	}
	if column.dense != nil {
		previous := column.dense[index]
		column.dense[index] = value
		if previous == column.defaultValue && value != column.defaultValue {
			column.nonDefault++
		} else if previous != column.defaultValue && value == column.defaultValue {
			column.nonDefault--
		}
		return nil
	}

	word := index >> 6
	bit := uint(index & 63)
	mask := uint64(1) << bit
	present := column.masks[word]&mask != 0
	rank := column.wordStarts[word] + bits.OnesCount64(column.masks[word]&(mask-1))
	if present {
		switch {
		case value != column.defaultValue:
			column.values[rank] = value
			return nil
		default:
			copy(column.values[rank:], column.values[rank+1:])
			var zero T
			column.values[len(column.values)-1] = zero
			column.values = column.values[:len(column.values)-1]
			column.masks[word] &^= mask
			column.nonDefault--
			for position := word + 1; position < len(column.wordStarts); position++ {
				column.wordStarts[position]--
			}
			return nil
		}
	}
	if value == column.defaultValue {
		return nil
	}
	column.values = append(column.values, value)
	copy(column.values[rank+1:], column.values[rank:len(column.values)-1])
	column.values[rank] = value
	column.masks[word] |= mask
	column.nonDefault++
	for position := word + 1; position < len(column.wordStarts); position++ {
		column.wordStarts[position]++
	}
	column.maybeUseDense()
	return nil
}

// Get returns the value at index and false when index is outside the column.
func (column *DefaultValueColumn[T]) Get(index int) (T, bool) {
	var zero T
	if column == nil || index < 0 || index >= column.length {
		return zero, false
	}
	if column.dense != nil {
		return column.dense[index], true
	}
	word := index >> 6
	mask := uint64(1) << uint(index&63)
	if column.masks[word]&mask == 0 {
		return column.defaultValue, true
	}
	rank := column.wordStarts[word] + bits.OnesCount64(column.masks[word]&(mask-1))
	return column.values[rank], true
}

// Values copies all rows into dst, reusing its backing array when possible.
func (column *DefaultValueColumn[T]) Values(dst []T) []T {
	dst = dst[:0]
	if column == nil || column.length == 0 {
		return dst
	}
	if cap(dst) < column.length {
		dst = make([]T, column.length)
	} else {
		dst = dst[:column.length]
	}
	if column.dense != nil {
		copy(dst, column.dense)
		return dst
	}
	for index := range dst {
		dst[index] = column.defaultValue
	}
	for word, mask := range column.masks {
		valueIndex := column.wordStarts[word]
		for mask != 0 {
			bit := bits.TrailingZeros64(mask)
			index := word*64 + bit
			if index < len(dst) {
				dst[index] = column.values[valueIndex]
			}
			valueIndex++
			mask &= mask - 1
		}
	}
	return dst
}

func (column *DefaultValueColumn[T]) maybeUseDense() {
	if column.dense != nil || column.length < DefaultValueColumnMinDenseRows {
		return
	}
	denseRows := column.length
	if column.capacityHint > denseRows {
		denseRows = column.capacityHint
	}
	denseBytes := denseRows * int(unsafe.Sizeof(*new(T)))
	if float64(column.nonDefault)/float64(column.length) < column.denseThreshold && column.sparseStorageBytes(int(unsafe.Sizeof(*new(T)))) < denseBytes {
		return
	}
	denseCapacity := column.length
	if column.capacityHint > denseCapacity {
		denseCapacity = column.capacityHint
	}
	dense := make([]T, column.length, denseCapacity)
	for index := range dense {
		dense[index] = column.defaultValue
	}
	for word, mask := range column.masks {
		valueIndex := column.wordStarts[word]
		for mask != 0 {
			bit := bits.TrailingZeros64(mask)
			index := word*64 + bit
			if index < len(dense) {
				dense[index] = column.values[valueIndex]
			}
			valueIndex++
			mask &= mask - 1
		}
	}
	column.dense = dense
	column.masks = nil
	column.wordStarts = nil
	column.values = nil
}

func (column *DefaultValueColumn[T]) sparseStorageBytes(valueBytes int) int {
	return cap(column.masks)*8 + cap(column.wordStarts)*int(unsafe.Sizeof(int(0))) + cap(column.values)*valueBytes
}
