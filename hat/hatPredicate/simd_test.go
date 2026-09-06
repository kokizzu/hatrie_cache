package hatPredicate_test

import (
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatPredicate"
)

func TestMatchInt64SIMDMatchesReferenceAcrossWordBoundaries(t *testing.T) {
	values := make([]int64, 131)
	for index := range values {
		values[index] = int64(index%11) - 5
	}
	values[64] = 1<<62 + 7
	values[130] = -1 << 62

	for _, test := range []struct {
		name      string
		predicate hatPredicate.Int64Predicate
		target    int64
	}{
		{name: "equal", predicate: hatPredicate.Int64Equal, target: 1},
		{name: "not equal", predicate: hatPredicate.Int64NotEqual, target: 1},
		{name: "less", predicate: hatPredicate.Int64Less, target: 1},
		{name: "less equal", predicate: hatPredicate.Int64LessEqual, target: 1},
		{name: "greater", predicate: hatPredicate.Int64Greater, target: 1},
		{name: "greater equal", predicate: hatPredicate.Int64GreaterEqual, target: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			wantMask, wantCount := referenceInt64Mask(values, test.predicate, test.target)
			mask := make([]uint64, hatPredicate.MaskWords(len(values))+1)
			for index := range mask {
				mask[index] = ^uint64(0)
			}
			gotCount, err := hatPredicate.MatchInt64SIMD(mask, values, test.predicate, test.target)
			if err != nil {
				t.Fatalf("MatchInt64SIMD() error = %v", err)
			}
			if gotCount != wantCount || !reflect.DeepEqual(mask[:len(wantMask)], wantMask) {
				t.Fatalf("MatchInt64SIMD() count=%d mask=%#v, want count=%d mask=%#v", gotCount, mask, wantCount, wantMask)
			}
			if mask[len(wantMask)] != 0 {
				t.Fatalf("MatchInt64SIMD() retained extra mask word %#x", mask[len(wantMask)])
			}
		})
	}
}

func TestMatchSIMDRejectsTheSameInvalidInputs(t *testing.T) {
	values := []int64{1, 2}
	if _, err := hatPredicate.MatchInt64SIMD(nil, values, hatPredicate.Int64Equal, 1); !errors.Is(err, hatPredicate.ErrMaskTooSmall) {
		t.Fatalf("short numeric mask error = %v, want ErrMaskTooSmall", err)
	}
	mask := make([]uint64, hatPredicate.MaskWords(len(values)))
	if _, err := hatPredicate.MatchInt64SIMD(mask, values, hatPredicate.Int64Predicate(99), 1); !errors.Is(err, hatPredicate.ErrInvalidPredicate) {
		t.Fatalf("invalid numeric predicate error = %v, want ErrInvalidPredicate", err)
	}
}

func referenceInt64Mask(values []int64, predicate hatPredicate.Int64Predicate, target int64) ([]uint64, int) {
	mask := make([]uint64, hatPredicate.MaskWords(len(values)))
	count := 0
	for index, value := range values {
		matched := false
		switch predicate {
		case hatPredicate.Int64Equal:
			matched = value == target
		case hatPredicate.Int64NotEqual:
			matched = value != target
		case hatPredicate.Int64Less:
			matched = value < target
		case hatPredicate.Int64LessEqual:
			matched = value <= target
		case hatPredicate.Int64Greater:
			matched = value > target
		case hatPredicate.Int64GreaterEqual:
			matched = value >= target
		}
		if matched {
			mask[index>>6] |= uint64(1) << uint(index&63)
			count++
		}
	}
	return mask, count
}
