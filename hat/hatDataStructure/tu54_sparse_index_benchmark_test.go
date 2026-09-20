package hatDataStructure

import (
	"runtime"
	"sort"
	"testing"
)

const benchmarkTU54Rows = 1 << 20

var benchmarkTU54Sink uint64

func benchmarkTU54Keys() []uint64 {
	keys := make([]uint64, benchmarkTU54Rows)
	for index := range keys {
		keys[index] = uint64(index)
	}
	return keys
}

func benchmarkTU54Queries() []uint64 {
	queries := make([]uint64, 4096)
	for index := range queries {
		queries[index] = uint64((index * 9973) % benchmarkTU54Rows)
	}
	return queries
}

func BenchmarkTU54FullIndexBuild1M(b *testing.B) {
	keys := benchmarkTU54Keys()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		copyOfKeys := append([]uint64(nil), keys...)
		benchmarkTU54Sink = copyOfKeys[len(copyOfKeys)-1]
		runtime.KeepAlive(copyOfKeys)
	}
}

func BenchmarkTU54SparseIndexBuild1M(b *testing.B) {
	keys := benchmarkTU54Keys()
	index, err := NewSparsePrimaryIndex(64, func(left, right uint64) bool {
		return left < right
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if err := index.Build(keys); err != nil {
			b.Fatal(err)
		}
		benchmarkTU54Sink = uint64(index.AnchorCount())
	}
}

func BenchmarkTU54FullIndexLookup1M(b *testing.B) {
	keys := benchmarkTU54Keys()
	queries := benchmarkTU54Queries()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		query := queries[iteration%len(queries)]
		position := sort.Search(len(keys), func(index int) bool {
			return keys[index] >= query
		})
		benchmarkTU54Sink = uint64(position)
	}
}

func BenchmarkTU54SparseIndexLookup1M(b *testing.B) {
	keys := benchmarkTU54Keys()
	queries := benchmarkTU54Queries()
	index, err := NewSparsePrimaryIndex(64, func(left, right uint64) bool {
		return left < right
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := index.Build(keys); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		query := queries[iteration%len(queries)]
		window, ok := index.Window(query)
		if !ok {
			b.Fatal("sparse index returned no window")
		}
		benchmarkTU54Sink = window.Start
	}
}
