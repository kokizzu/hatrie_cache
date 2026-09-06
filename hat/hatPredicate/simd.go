package hatPredicate

// MatchInt64SIMD writes a reusable selection mask using the fastest safe
// kernel available on the current CPU. It preserves MatchInt64's validation,
// mask-clearing, and count semantics while allowing an architecture-specific
// implementation to handle supported predicates without allocations.
func MatchInt64SIMD(mask []uint64, values []int64, predicate Int64Predicate, target int64) (int, error) {
	if MaskWords(len(values)) > len(mask) {
		return 0, ErrMaskTooSmall
	}
	if predicate > Int64GreaterEqual {
		return 0, ErrInvalidPredicate
	}
	clearMask(mask)

	matches := 0
	for wordIndex, start := 0, 0; start < len(values); wordIndex, start = wordIndex+1, start+64 {
		end := start + 64
		if end > len(values) {
			end = len(values)
		}
		wordValues := values[start:end]
		if word, count, ok := matchInt64SIMDWord(wordValues, predicate, target); ok {
			mask[wordIndex] = word
			matches += count
			continue
		}
		word, count := matchInt64ScalarWord(wordValues, predicate, target)
		mask[wordIndex] = word
		matches += count
	}
	return matches, nil
}

func matchInt64ScalarWord(values []int64, predicate Int64Predicate, target int64) (uint64, int) {
	var word uint64
	matches := 0
	for index, value := range values {
		matched := false
		switch predicate {
		case Int64Equal:
			matched = value == target
		case Int64NotEqual:
			matched = value != target
		case Int64Less:
			matched = value < target
		case Int64LessEqual:
			matched = value <= target
		case Int64Greater:
			matched = value > target
		case Int64GreaterEqual:
			matched = value >= target
		}
		if matched {
			word |= uint64(1) << uint(index)
			matches++
		}
	}
	return word, matches
}
