package hatDataStructure

import (
	"strconv"
	"testing"
)

var tu20BenchmarkSink int

func BenchmarkTU20BeforeMapGet(b *testing.B) {
	items := make(map[string]int, 10000)
	for i := 0; i < 10000; i++ {
		items["key-"+strconv.Itoa(i)] = i
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tu20BenchmarkSink = items["hot"]
	}
}

func BenchmarkTU20BeforeMapSet(b *testing.B) {
	items := make(map[string]int, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		items["hot"] = i
	}
	tu20BenchmarkSink = items["hot"]
}
