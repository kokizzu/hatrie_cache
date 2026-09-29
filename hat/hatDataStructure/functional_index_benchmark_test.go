package hatDataStructure

import (
	"fmt"
	"testing"
)

type functionalIndexBenchmarkValue struct {
	Key   int
	Value int
}

func BenchmarkFunctionalIndexLookupInto(b *testing.B) {
	for _, hits := range []int{1, 8, 128} {
		b.Run(fmt.Sprintf("hits=%d", hits), func(b *testing.B) {
			index, err := NewFunctionalIndex[functionalIndexBenchmarkValue, int](
				func(value functionalIndexBenchmarkValue) int { return value.Key },
				hits,
			)
			if err != nil {
				b.Fatal(err)
			}
			for id := 0; id < hits; id++ {
				if err := index.Upsert(uint64(id+1), functionalIndexBenchmarkValue{Key: 1, Value: id}); err != nil {
					b.Fatal(err)
				}
			}
			dst := make([]functionalIndexBenchmarkValue, 0, hits)
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				dst = index.LookupInto(1, dst)
			}
			b.StopTimer()
			if len(dst) != hits {
				b.Fatalf("LookupInto() length = %d, want %d", len(dst), hits)
			}
		})
	}
}
