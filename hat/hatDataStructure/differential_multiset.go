package hatDataStructure

import "errors"

var (
	ErrDifferentialMultisetInvalid = errors.New("hatriecache: differential multiset is invalid")
	ErrDifferentialOverflow        = errors.New("hatriecache: differential multiset diff overflows int64")
)

const (
	maxDifferentialInt64 = int64(^uint64(0) >> 1)
	minDifferentialInt64 = -maxDifferentialInt64 - 1
)

// DifferentialRecord is one consolidated (data, time, diff) entry.
type DifferentialRecord[T comparable] struct {
	Data T      `json:"data"`
	Time uint64 `json:"time"`
	Diff int64  `json:"diff"`
}

type differentialKey[T comparable] struct {
	data T
	time uint64
}

// DifferentialMultiset consolidates diffs for equal data and time. Entries
// whose diff returns to zero are removed immediately.
type DifferentialMultiset[T comparable] struct {
	entries map[differentialKey[T]]int64
}

// NewDifferentialMultiset creates an empty differential multiset.
func NewDifferentialMultiset[T comparable]() *DifferentialMultiset[T] {
	return &DifferentialMultiset[T]{entries: make(map[differentialKey[T]]int64)}
}

// Add applies diff to one data/time entry.
func (multiset *DifferentialMultiset[T]) Add(data T, timestamp uint64, diff int64) error {
	if multiset == nil {
		return ErrDifferentialMultisetInvalid
	}
	if multiset.entries == nil {
		multiset.entries = make(map[differentialKey[T]]int64)
	}
	key := differentialKey[T]{data: data, time: timestamp}
	current := multiset.entries[key]
	if diff > 0 && current > maxDifferentialInt64-diff {
		return ErrDifferentialOverflow
	}
	if diff < 0 && current < minDifferentialInt64-diff {
		return ErrDifferentialOverflow
	}
	next := current + diff
	if next == 0 {
		delete(multiset.entries, key)
		return nil
	}
	multiset.entries[key] = next
	return nil
}

// Get returns the consolidated diff for one data/time entry, or zero when it
// is absent.
func (multiset *DifferentialMultiset[T]) Get(data T, timestamp uint64) int64 {
	if multiset == nil {
		return 0
	}
	return multiset.entries[differentialKey[T]{data: data, time: timestamp}]
}

// Len returns the number of nonzero consolidated entries.
func (multiset *DifferentialMultiset[T]) Len() int {
	if multiset == nil {
		return 0
	}
	return len(multiset.entries)
}

// ForEach visits each nonzero entry. Map iteration order is unspecified.
func (multiset *DifferentialMultiset[T]) ForEach(visit func(DifferentialRecord[T])) {
	if multiset == nil || visit == nil {
		return
	}
	for key, diff := range multiset.entries {
		visit(DifferentialRecord[T]{Data: key.data, Time: key.time, Diff: diff})
	}
}
