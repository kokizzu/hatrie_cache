//go:build !tt013baseline

package hatDataStructure

import "testing"

var tt013RangeTupleCacheSink []int

func BenchmarkTT013RangeTupleCacheHit(b *testing.B) {
	cache, err := NewRangeTupleCache[int, int](4)
	if err != nil {
		b.Fatal(err)
	}
	values := make([]int, 16)
	for index := range values {
		values[index] = (index + 1000) * 2
	}
	if err := cache.Put(1000, 1015, 1, values); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, ok := cache.Get(1000, 1015, 1)
		if !ok {
			b.Fatal("Get() missed cached range")
		}
		tt013RangeTupleCacheSink = result
	}
}
