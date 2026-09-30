package hatDataStructure

import (
	"strconv"
	"testing"
)

var topKEntriesIntoBenchmarkSink []TopKEntry[string]

func BenchmarkTopKEntriesAllocBaseline(b *testing.B) {
	top := topKEntriesIntoFixture()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		topKEntriesIntoBenchmarkSink = top.Entries()
	}
}

func BenchmarkTopKEntriesIntoReuse(b *testing.B) {
	top := topKEntriesIntoFixture()
	destination := make([]TopKEntry[string], 0, len(top.counters))
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		destination = top.EntriesInto(destination[:0])
		topKEntriesIntoBenchmarkSink = destination
	}
}

func topKEntriesIntoFixture() TopK[string] {
	top, err := NewTopK[string](128)
	if err != nil {
		panic(err)
	}
	for index := 0; index < 1024; index++ {
		top.Add(strconv.Itoa(index % 256))
	}
	return top
}
