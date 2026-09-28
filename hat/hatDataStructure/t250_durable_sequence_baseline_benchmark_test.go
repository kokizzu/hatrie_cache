package hatDataStructure

import (
	"sync/atomic"
	"testing"
)

func BenchmarkT250BaselineAtomicAdd(b *testing.B) {
	var current atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		current.Add(1)
	}
}
