// Package hatPredicate provides allocation-free batch predicate kernels.
package hatPredicate

import (
	"errors"
	"strings"
)

var (
	ErrMaskTooSmall     = errors.New("hatPredicate: destination mask is too small")
	ErrInvalidPredicate = errors.New("hatPredicate: invalid predicate")
)

// Int64Predicate selects a comparison for MatchInt64.
type Int64Predicate uint8

const (
	Int64Equal Int64Predicate = iota
	Int64NotEqual
	Int64Less
	Int64LessEqual
	Int64Greater
	Int64GreaterEqual
)

// StringPredicate selects a comparison for MatchString.
type StringPredicate uint8

const (
	StringEqual StringPredicate = iota
	StringNotEqual
	StringPrefix
	StringSuffix
	StringContains
)

// MaskWords returns the number of uint64 words needed to represent length
// input values.
func MaskWords(length int) int {
	if length <= 0 {
		return 0
	}
	return (length + 63) / 64
}

// MatchInt64 writes one bit per input value into mask and returns the number of
// matching values. The caller owns mask and can reuse it across batches; all
// supplied words are cleared before matching. No allocation is performed.
func MatchInt64(mask []uint64, values []int64, predicate Int64Predicate, target int64) (int, error) {
	return MatchInt64SIMD(mask, values, predicate, target)
}

// MatchString writes one bit per input string into mask and returns the number
// of matching values. It supports equality, inequality, prefix, suffix, and
// substring predicates without allocating result rows or strings.
func MatchString(mask []uint64, values []string, predicate StringPredicate, target string) (int, error) {
	if MaskWords(len(values)) > len(mask) {
		return 0, ErrMaskTooSmall
	}
	if predicate > StringContains {
		return 0, ErrInvalidPredicate
	}
	clearMask(mask)
	matches := 0
	for index, value := range values {
		matched := false
		switch predicate {
		case StringEqual:
			matched = value == target
		case StringNotEqual:
			matched = value != target
		case StringPrefix:
			matched = strings.HasPrefix(value, target)
		case StringSuffix:
			matched = strings.HasSuffix(value, target)
		case StringContains:
			matched = strings.Contains(value, target)
		}
		if matched {
			mask[index>>6] |= uint64(1) << uint(index&63)
			matches++
		}
	}
	return matches, nil
}

func clearMask(mask []uint64) {
	for index := range mask {
		mask[index] = 0
	}
}
