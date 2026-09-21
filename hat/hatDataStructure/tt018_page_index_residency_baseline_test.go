package hatDataStructure

import "testing"

var tt018PageIndexBaselineSink int

func BenchmarkTT018MapPageIndexGet(b *testing.B) {
	pages := make(map[int]int, 1024)
	for index := 0; index < 1024; index++ {
		pages[index] = index
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		tt018PageIndexBaselineSink += pages[index&1023]
	}
}

func BenchmarkTT018MapPageIndexFill(b *testing.B) {
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		pages := make(map[int]int, 1024)
		for page := 0; page < 1024; page++ {
			pages[page] = page
		}
		tt018PageIndexBaselineSink += len(pages)
	}
}
