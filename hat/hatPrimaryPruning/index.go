// Package hatPrimaryPruning provides immutable composite primary-mark pruning.
package hatPrimaryPruning

import "errors"

var (
	ErrArityMismatch = errors.New("primary mark tuple arity mismatch")
	ErrInvalidConfig = errors.New("invalid primary pruning configuration")
	ErrInvalidMark   = errors.New("invalid primary mark bounds")
	ErrInvalidRange  = errors.New("invalid primary pruning range")
)

// KeyPart is one ordered tuple component. Null ordering is selected when the
// index is built; Value is ignored when Null is true.
type KeyPart struct {
	Value uint64
	Null  bool
}

// Mark stores inclusive minimum and maximum tuple bounds for one data mark.
type Mark struct {
	Min []KeyPart
	Max []KeyPart
}

// Range is an inclusive query range. Missing bounds represent an unbounded
// side; present bounds must have the index arity.
type Range struct {
	HasLower bool
	Lower    []KeyPart
	HasUpper bool
	Upper    []KeyPart
}

// Config controls SQL-style NULL ordering.
type Config struct {
	NullsLast bool
}

// DefaultConfig uses NULLS LAST ordering, matching the default chosen by the
// surrounding ordered-storage paths.
func DefaultConfig() Config {
	return Config{NullsLast: true}
}

// Index is an immutable packed collection of composite mark bounds. It is safe
// for concurrent read-only use after construction.
type Index struct {
	arity     int
	nullsLast bool
	ordered   bool
	// Each mark occupies [min tuple][max tuple] in bounds.
	bounds []KeyPart
}

// Build creates an index using DefaultConfig. The input marks and tuple slices
// are copied before return.
func Build(marks []Mark) (*Index, error) {
	return BuildWithConfig(marks, DefaultConfig())
}

// BuildWithConfig creates an immutable packed mark index.
func BuildWithConfig(marks []Mark, config Config) (*Index, error) {
	if len(marks) == 0 {
		return &Index{nullsLast: config.NullsLast}, nil
	}
	arity := len(marks[0].Min)
	if arity == 0 || len(marks[0].Max) != arity {
		return nil, ErrInvalidMark
	}
	index := &Index{
		arity:     arity,
		nullsLast: config.NullsLast,
		ordered:   true,
		bounds:    make([]KeyPart, len(marks)*arity*2),
	}
	for markIndex, mark := range marks {
		if len(mark.Min) != arity || len(mark.Max) != arity {
			return nil, ErrArityMismatch
		}
		if compareTuple(mark.Min, mark.Max, config.NullsLast) > 0 {
			return nil, ErrInvalidMark
		}
		if markIndex > 0 && compareTuple(marks[markIndex-1].Max, mark.Min, config.NullsLast) > 0 {
			index.ordered = false
		}
		offset := markIndex * arity * 2
		copy(index.bounds[offset:offset+arity], mark.Min)
		copy(index.bounds[offset+arity:offset+arity*2], mark.Max)
	}
	return index, nil
}

// Candidates appends indexes of marks that may overlap query into dst. Reusing
// dst lets a caller avoid an allocation on repeated pruning calls.
func (i *Index) Candidates(query Range, dst []int) ([]int, error) {
	if i == nil {
		return dst[:0], nil
	}
	if err := i.validateRange(query); err != nil {
		return nil, err
	}
	dst = dst[:0]
	start, end := 0, i.Len()
	if i.ordered {
		if query.HasLower {
			start = i.firstMaxAtLeast(query.Lower)
		}
		if query.HasUpper {
			end = i.firstMinGreater(query.Upper)
		}
	}
	for mark := start; mark < end; mark++ {
		if i.mayOverlap(mark, query) {
			dst = append(dst, mark)
		}
	}
	return dst, nil
}

// MayOverlap reports whether a mark may contain a tuple in query. Invalid
// ranges and out-of-range mark indexes return false; use Candidates when the
// caller needs validation errors.
func (i *Index) MayOverlap(mark int, query Range) bool {
	if i == nil || mark < 0 || mark >= i.Len() || i.validateRange(query) != nil {
		return false
	}
	return i.mayOverlap(mark, query)
}

// Len returns the number of marks.
func (i *Index) Len() int {
	if i == nil || i.arity == 0 {
		return 0
	}
	return len(i.bounds) / (i.arity * 2)
}

// Arity returns the tuple width.
func (i *Index) Arity() int {
	if i == nil {
		return 0
	}
	return i.arity
}

// MemoryBytes reports packed tuple-bound storage, excluding the small Index
// header and caller-owned query buffers.
func (i *Index) MemoryBytes() uint64 {
	if i == nil {
		return 0
	}
	return uint64(len(i.bounds)) * 16
}

func (i *Index) validateRange(query Range) error {
	if query.HasLower && len(query.Lower) != i.arity || query.HasUpper && len(query.Upper) != i.arity {
		return ErrArityMismatch
	}
	if query.HasLower && query.HasUpper && compareTuple(query.Lower, query.Upper, i.nullsLast) > 0 {
		return ErrInvalidRange
	}
	return nil
}

func (i *Index) mayOverlap(mark int, query Range) bool {
	offset := mark * i.arity * 2
	min := i.bounds[offset : offset+i.arity]
	max := i.bounds[offset+i.arity : offset+i.arity*2]
	if query.HasLower && compareTuple(max, query.Lower, i.nullsLast) < 0 {
		return false
	}
	if query.HasUpper && compareTuple(min, query.Upper, i.nullsLast) > 0 {
		return false
	}
	return true
}

func (i *Index) firstMaxAtLeast(query []KeyPart) int {
	low, high := 0, i.Len()
	for low < high {
		middle := low + (high-low)/2
		if compareTuple(i.maxTuple(middle), query, i.nullsLast) < 0 {
			low = middle + 1
		} else {
			high = middle
		}
	}
	return low
}

func (i *Index) firstMinGreater(query []KeyPart) int {
	low, high := 0, i.Len()
	for low < high {
		middle := low + (high-low)/2
		if compareTuple(i.minTuple(middle), query, i.nullsLast) <= 0 {
			low = middle + 1
		} else {
			high = middle
		}
	}
	return low
}

func (i *Index) minTuple(mark int) []KeyPart {
	offset := mark * i.arity * 2
	return i.bounds[offset : offset+i.arity]
}

func (i *Index) maxTuple(mark int) []KeyPart {
	offset := mark*i.arity*2 + i.arity
	return i.bounds[offset : offset+i.arity]
}

func compareTuple(left, right []KeyPart, nullsLast bool) int {
	for index := range left {
		if result := comparePart(left[index], right[index], nullsLast); result != 0 {
			return result
		}
	}
	return 0
}

func comparePart(left, right KeyPart, nullsLast bool) int {
	if left.Null || right.Null {
		if left.Null && right.Null {
			return 0
		}
		if left.Null == nullsLast {
			return 1
		}
		return -1
	}
	if left.Value < right.Value {
		return -1
	}
	if left.Value > right.Value {
		return 1
	}
	return 0
}
