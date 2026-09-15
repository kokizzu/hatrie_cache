package hatCache_test

import (
	"strconv"
	"testing"

	"hatrie_cache/hat/hatCache"
)

var benchmarkTopKInfo hatCache.TopKInfo

func benchmarkTopKPartitions(t *testing.B, size int) (hatCache.TopK, hatCache.TopK, []string) {
	t.Helper()

	left, err := hatCache.NewTopK(100)
	if err != nil {
		t.Fatal(err)
	}
	right, err := hatCache.NewTopK(100)
	if err != nil {
		t.Fatal(err)
	}

	values := make([]string, 0, size*2)
	for i := 0; i < size; i++ {
		value := "key-" + strconv.Itoa(i%2048)
		left.Add(value, 1)
		values = append(values, value)
	}
	for i := 0; i < size; i++ {
		value := "key-" + strconv.Itoa((i*17+3)%2048)
		right.Add(value, 1)
		values = append(values, value)
	}
	return left, right, values
}

func BenchmarkTopKMergeAgainstReplay(b *testing.B) {
	for _, size := range []int{128, 4096, 65536} {
		left, right, values := benchmarkTopKPartitions(b, size)

		b.Run("Merge/"+strconv.Itoa(size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				merged := left
				if err := merged.Merge(right); err != nil {
					b.Fatal(err)
				}
				benchmarkTopKInfo = merged.Info()
			}
		})

		b.Run("Replay/"+strconv.Itoa(size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				replayed, err := hatCache.NewTopK(100)
				if err != nil {
					b.Fatal(err)
				}
				for _, value := range values {
					replayed.Add(value, 1)
				}
				benchmarkTopKInfo = replayed.Info()
			}
		})
	}
}
