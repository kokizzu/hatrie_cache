//go:build tt013baseline

package hatDataStructure

import "testing"

type tt013BaselineRangeValue struct {
	Key   int
	Value int
}

var tt013BaselineRangeSink []int

func BenchmarkTT013RangeScanBaseline(b *testing.B) {
	index, err := NewOrderedIndex(func(value tt013BaselineRangeValue) int { return value.Key }, func(left, right int) int {
		switch {
		case left < right:
			return -1
		case left > right:
			return 1
		default:
			return 0
		}
	}, 4096)
	if err != nil {
		b.Fatal(err)
	}
	for key := 0; key < 4096; key++ {
		if err := index.Upsert(uint64(key), tt013BaselineRangeValue{Key: key, Value: key * 2}); err != nil {
			b.Fatal(err)
		}
	}
	result := make([]int, 0, 16)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result = result[:0]
		iterator, ok := index.Range(1000, 1015)
		if !ok {
			b.Fatal("Range() returned no iterator")
		}
		for {
			entry, next, err := iterator.Next()
			if err != nil {
				b.Fatal(err)
			}
			if !next {
				break
			}
			result = append(result, entry.Value.Value)
		}
		iterator.Close()
	}
	tt013BaselineRangeSink = result
}
