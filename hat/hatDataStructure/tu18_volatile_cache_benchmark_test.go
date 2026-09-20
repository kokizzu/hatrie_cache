package hatDataStructure

import (
	"strconv"
	"testing"
	"time"
)

func BenchmarkTU18AfterVolatileCacheGet(b *testing.B) {
	cache, err := NewVolatileCache[string, int](VolatileCacheOptions[string, int]{Capacity: 10000})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 10000; i++ {
		if err := cache.Set("key-"+strconv.Itoa(i), i, 0); err != nil {
			b.Fatal(err)
		}
	}
	if err := cache.Set("hot", 1, 0); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tu18BenchmarkSinkInt, _ = cache.Get("hot")
	}
}

func BenchmarkTU18AfterVolatileCachePeek(b *testing.B) {
	cache, err := NewVolatileCache[string, int](VolatileCacheOptions[string, int]{Capacity: 1})
	if err != nil {
		b.Fatal(err)
	}
	if err := cache.Set("hot", 1, 0); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tu18BenchmarkSinkInt, _ = cache.Peek("hot")
	}
}

func BenchmarkTU18AfterVolatileCacheSet(b *testing.B) {
	cache, err := NewVolatileCache[string, int](VolatileCacheOptions[string, int]{Capacity: 1})
	if err != nil {
		b.Fatal(err)
	}
	if err := cache.Set("hot", 0, 0); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := cache.Set("hot", i, 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU18AfterVolatileCacheGetTTL(b *testing.B) {
	now := time.Unix(100, 0)
	cache, err := NewVolatileCache[string, int](VolatileCacheOptions[string, int]{
		Capacity: 1,
		Now:      func() time.Time { return now },
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := cache.Set("hot", 1, time.Hour); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tu18BenchmarkSinkInt, _ = cache.Get("hot")
	}
}
