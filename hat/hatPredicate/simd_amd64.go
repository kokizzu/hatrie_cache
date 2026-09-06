//go:build amd64

package hatPredicate

import (
	"math/bits"

	"golang.org/x/sys/cpu"
)

func matchInt64SIMDWord(values []int64, predicate Int64Predicate, target int64) (uint64, int, bool) {
	if !cpu.X86.HasAVX2 || len(values) == 0 || len(values)%4 != 0 || predicate != Int64Equal && predicate != Int64NotEqual {
		return 0, 0, false
	}
	word := matchInt64AVX2Equal(values, target)
	if predicate == Int64NotEqual {
		word = ^word
	}
	if len(values) < 64 {
		word &= (uint64(1) << uint(len(values))) - 1
	}
	return word, bits.OnesCount64(word), true
}

func matchInt64AVX2Equal(values []int64, target int64) uint64
