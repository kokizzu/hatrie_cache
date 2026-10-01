package hatDataStructure

import "testing"

func BenchmarkTU23TupleMultikeyIndexBuild(b *testing.B) {
	const rowCount = 10000
	dimensions := make([][][]string, rowCount)
	for row := range dimensions {
		dimensions[row] = tu23BenchmarkDimensions(row)
	}
	b.ReportAllocs()
	for range b.N {
		index := NewTupleMultikeyIndex(TupleMultikeyIndexOptions{})
		for row, rowDimensions := range dimensions {
			if err := index.Set(uint64(row), rowDimensions); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkTU23TupleMultikeyIndexLookup(b *testing.B) {
	const rowCount = 10000
	index := NewTupleMultikeyIndex(TupleMultikeyIndexOptions{})
	for row := 0; row < rowCount; row++ {
		if err := index.Set(uint64(row), tu23BenchmarkDimensions(row)); err != nil {
			b.Fatal(err)
		}
	}
	values := []string{"region-1", "kind-3"}
	want := len(index.Lookup(values, nil))
	destination := make([]uint64, 0, want)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		got := index.Lookup(values, destination)
		if len(got) != want {
			b.Fatalf("lookup count = %d, want %d", len(got), want)
		}
		destination = got
	}
}
