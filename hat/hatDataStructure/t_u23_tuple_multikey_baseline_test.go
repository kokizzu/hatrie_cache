package hatDataStructure

import (
	"strconv"
	"strings"
	"testing"
)

type tu23BaselineIndex struct {
	byKey map[string][]uint64
}

func newTU23BaselineIndex() *tu23BaselineIndex {
	return &tu23BaselineIndex{byKey: make(map[string][]uint64)}
}

func (index *tu23BaselineIndex) set(id uint64, dimensions [][]string) {
	for _, key := range tu23ExpandKeys(dimensions) {
		index.byKey[key] = append(index.byKey[key], id)
	}
}

func (index *tu23BaselineIndex) lookupTuple(values []string, dst []uint64) []uint64 {
	dst = dst[:0]
	return append(dst, index.byKey[strings.Join(values, "\x00")]...)
}

func tu23BenchmarkDimensions(row int) [][]string {
	return [][]string{
		{"region-" + strconv.Itoa(row%4), "region-" + strconv.Itoa((row+1)%4)},
		{"kind-" + strconv.Itoa(row%8), "kind-" + strconv.Itoa((row+3)%8)},
	}
}

func tu23ExpandKeys(dimensions [][]string) []string {
	if len(dimensions) == 0 {
		return nil
	}
	keys := []string{""}
	for _, dimension := range dimensions {
		if len(dimension) == 0 {
			return nil
		}
		next := make([]string, 0, len(keys)*len(dimension))
		for _, prefix := range keys {
			for _, value := range dimension {
				if prefix == "" {
					next = append(next, value)
					continue
				}
				next = append(next, prefix+"\x00"+value)
			}
		}
		keys = next
	}
	return keys
}

func BenchmarkTU23BaselineMapOfSlicesBuild(b *testing.B) {
	const rowCount = 10000
	dimensions := make([][][]string, rowCount)
	for row := range dimensions {
		dimensions[row] = tu23BenchmarkDimensions(row)
	}
	b.ReportAllocs()
	for range b.N {
		index := newTU23BaselineIndex()
		for row, rowDimensions := range dimensions {
			index.set(uint64(row), rowDimensions)
		}
	}
}

func BenchmarkTU23BaselineMapOfSlicesLookup(b *testing.B) {
	const rowCount = 10000
	index := newTU23BaselineIndex()
	for row := 0; row < rowCount; row++ {
		index.set(uint64(row), tu23BenchmarkDimensions(row))
	}
	values := []string{"region-1", "kind-3"}
	want := len(index.byKey[strings.Join(values, "\x00")])
	destination := make([]uint64, 0, want)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		got := index.lookupTuple(values, destination)
		if len(got) != want {
			b.Fatalf("lookup count = %d, want %d", len(got), want)
		}
		destination = got
	}
}
