package hatDataStructure

import (
	"strconv"
	"testing"
)

var frontierReadHoldBenchmarkSink uint64

func BenchmarkFrontierReadHoldSafeSince(b *testing.B) {
	for _, active := range []int{0, 1, 8, 64} {
		b.Run("active="+strconv.Itoa(active), func(b *testing.B) {
			holds := NewFrontierReadHoldSet()
			for index := 0; index < active; index++ {
				if _, err := holds.Acquire(uint64(index), uint64(index)+100); err != nil {
					b.Fatal(err)
				}
			}
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				frontierReadHoldBenchmarkSink = holds.SafeSince(^uint64(0))
			}
		})
	}
}

func BenchmarkFrontierReadHoldLifecycle(b *testing.B) {
	holds := NewFrontierReadHoldSet()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		hold, err := holds.Acquire(uint64(index), uint64(index)+1)
		if err != nil {
			b.Fatal(err)
		}
		if err := hold.Advance(uint64(index)+1, uint64(index)+2); err != nil {
			b.Fatal(err)
		}
		if err := hold.Release(); err != nil {
			b.Fatal(err)
		}
	}
}
