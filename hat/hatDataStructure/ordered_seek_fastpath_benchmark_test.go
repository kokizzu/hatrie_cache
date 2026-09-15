package hatDataStructure

import (
	"strconv"
	"testing"
)

func BenchmarkC220OrderedIndexSeek(b *testing.B) {
	for _, size := range []int{0, 1, 2, 8, 64} {
		for _, workload := range []struct {
			name string
			key  int
		}{
			{name: "first", key: 0},
			{name: "middle", key: size / 2},
			{name: "after", key: size},
		} {
			b.Run(strconv.Itoa(size)+"/"+workload.name, func(b *testing.B) {
				index, err := NewOrderedIndex(
					func(value int) int { return value },
					orderedIndexC220Compare,
					size,
				)
				if err != nil {
					b.Fatal(err)
				}
				for value := 0; value < size; value++ {
					if err := index.Upsert(uint64(value+1), value); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					iterator, ok := index.Seek(workload.key)
					if !ok {
						orderedIndexBenchmarkSink = -1
						continue
					}
					entry, next, err := iterator.Next()
					if err != nil || !next {
						b.Fatalf("Seek(%d).Next() = %#v, %v, %v", workload.key, entry, next, err)
					}
					orderedIndexBenchmarkSink = entry.Value
					iterator.Close()
				}
			})
		}
	}
}

func BenchmarkC220OrderedIndexSnapshotSeek(b *testing.B) {
	for _, size := range []int{1, 2, 8, 64} {
		for _, workload := range []struct {
			name string
			key  int
		}{
			{name: "first", key: 0},
			{name: "middle", key: size / 2},
			{name: "after", key: size},
		} {
			b.Run(strconv.Itoa(size)+"/"+workload.name, func(b *testing.B) {
				index, err := NewOrderedIndex(
					func(value int) int { return value },
					orderedIndexC220Compare,
					size,
				)
				if err != nil {
					b.Fatal(err)
				}
				for value := 0; value < size; value++ {
					if err := index.Upsert(uint64(value+1), value); err != nil {
						b.Fatal(err)
					}
				}
				cursor, ok := index.SnapshotCursor()
				if !ok {
					b.Fatal("SnapshotCursor() = false")
				}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if err := cursor.Seek(workload.key); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				cursor.Close()
			})
		}
	}
}

func BenchmarkC220OrderedIndexReverseSeek(b *testing.B) {
	for _, size := range []int{0, 1, 2, 8, 64} {
		for _, workload := range []struct {
			name string
			key  int
		}{
			{name: "before-first", key: 0},
			{name: "at-middle", key: size / 2},
			{name: "after-last", key: size},
		} {
			for _, mode := range []struct {
				name      string
				inclusive bool
			}{
				{name: "strict", inclusive: false},
				{name: "inclusive", inclusive: true},
			} {
				b.Run(strconv.Itoa(size)+"/"+mode.name+"/"+workload.name, func(b *testing.B) {
					index, err := NewOrderedIndex(
						func(value int) int { return value },
						orderedIndexC220Compare,
						size,
					)
					if err != nil {
						b.Fatal(err)
					}
					for value := 0; value < size; value++ {
						if err := index.Upsert(uint64(value+1), value); err != nil {
							b.Fatal(err)
						}
					}
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						var iterator OrderedIndexIterator[int, int]
						var ok bool
						if mode.inclusive {
							iterator, ok = index.SeekBeforeOrEqual(workload.key)
						} else {
							iterator, ok = index.SeekBefore(workload.key)
						}
						if !ok {
							orderedIndexBenchmarkSink = -1
							continue
						}
						entry, next, err := iterator.Next()
						if err != nil || !next {
							b.Fatalf("reverse seek at %d = %#v, %v, %v", workload.key, entry, next, err)
						}
						orderedIndexBenchmarkSink = entry.Value
						iterator.Close()
					}
				})
			}
		}
	}
}

func BenchmarkC220OrderedIndexReverseSnapshotSeek(b *testing.B) {
	for _, size := range []int{1, 2, 8, 64} {
		for _, mode := range []struct {
			name      string
			inclusive bool
		}{
			{name: "strict", inclusive: false},
			{name: "inclusive", inclusive: true},
		} {
			b.Run(strconv.Itoa(size)+"/"+mode.name, func(b *testing.B) {
				index, err := NewOrderedIndex(
					func(value int) int { return value },
					orderedIndexC220Compare,
					size,
				)
				if err != nil {
					b.Fatal(err)
				}
				for value := 0; value < size; value++ {
					if err := index.Upsert(uint64(value+1), value); err != nil {
						b.Fatal(err)
					}
				}
				cursor, ok := index.LastSnapshotCursor()
				if !ok {
					b.Fatal("LastSnapshotCursor() = false")
				}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if mode.inclusive {
						if err := cursor.SeekBeforeOrEqual(size / 2); err != nil {
							b.Fatal(err)
						}
					} else if err := cursor.SeekBefore(size / 2); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				cursor.Close()
			})
		}
	}
}

func orderedIndexC220Compare(left, right int) int {
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}
