package hatDataStructure

import (
	"strconv"
	"testing"
)

var tu18BenchmarkSinkInt int

func BenchmarkTU18BeforeMapGet(b *testing.B) {
	items := make(map[string]int, 10000)
	for i := 0; i < 10000; i++ {
		items[benchmarkTU18Key(i)] = i
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tu18BenchmarkSinkInt = items["hot"]
	}
}

func BenchmarkTU18BeforeMapSet(b *testing.B) {
	items := make(map[string]int, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		items["hot"] = i
	}
	tu18BenchmarkSinkInt = items["hot"]
}

func benchmarkTU18Key(i int) string {
	return "key-" + strconv.Itoa(i)
}
