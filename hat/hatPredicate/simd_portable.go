//go:build !amd64

package hatPredicate

func matchInt64SIMDWord([]int64, Int64Predicate, int64) (uint64, int, bool) {
	return 0, 0, false
}
