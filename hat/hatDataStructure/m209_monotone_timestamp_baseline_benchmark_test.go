package hatDataStructure_test

import (
	"sync/atomic"
	"testing"
)

var m209RawAtomicAdvanceSink uint64

func BenchmarkM209RawAtomicAdvance(b *testing.B) {
	var value uint64
	for index := 0; index < b.N; index++ {
		next := uint64(index + 1)
		for {
			current := atomic.LoadUint64(&value)
			if next <= current || atomic.CompareAndSwapUint64(&value, current, next) {
				break
			}
		}
	}
	m209RawAtomicAdvanceSink = value
}
