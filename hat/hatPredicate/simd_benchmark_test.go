package hatPredicate_test

import (
	"testing"

	"hatrie_cache/hat/hatPredicate"
)

func BenchmarkMatchInt64SIMD(b *testing.B) {
	values := make([]int64, 100000)
	for i := range values {
		values[i] = int64(i % 17)
	}
	mask := make([]uint64, hatPredicate.MaskWords(len(values)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := hatPredicate.MatchInt64SIMD(mask, values, hatPredicate.Int64Equal, 7); err != nil {
			b.Fatal(err)
		}
	}
}
