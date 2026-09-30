package hatDataStructure

import (
	"strconv"
	"testing"
)

var vectorSearchIntoBenchmarkSink []VectorMatch

func BenchmarkVectorSearchBaseline(b *testing.B) {
	index, query := vectorSearchIntoFixture()
	for _, limit := range []int{10, 10000} {
		b.Run("limit"+strconv.Itoa(limit), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				matches, err := index.Search(query, limit, nil)
				if err != nil {
					b.Fatal(err)
				}
				vectorSearchIntoBenchmarkSink = matches
			}
		})
	}
}

func BenchmarkVectorSearchIntoReuse(b *testing.B) {
	index, query := vectorSearchIntoFixture()
	for _, limit := range []int{10, 10000} {
		b.Run("limit"+strconv.Itoa(limit), func(b *testing.B) {
			destination := make([]VectorMatch, 0, limit)
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				matches, err := index.SearchInto(destination[:0], query, limit, nil)
				if err != nil {
					b.Fatal(err)
				}
				destination = matches
				vectorSearchIntoBenchmarkSink = matches
			}
		})
	}
}

func vectorSearchIntoFixture() (*VectorIndex, []float32) {
	index, err := NewVectorIndex(8)
	if err != nil {
		panic(err)
	}
	for item := 0; item < 10000; item++ {
		values := []float32{
			float32(item%997 + 1),
			float32((item*13)%991 + 1),
			float32((item*17)%983 + 1),
			float32((item*19)%977 + 1),
			float32((item*23)%971 + 1),
			float32((item*29)%967 + 1),
			float32((item*31)%953 + 1),
			float32((item*37)%947 + 1),
		}
		if err := index.Upsert(strconv.Itoa(item), values); err != nil {
			panic(err)
		}
	}
	return index, []float32{1, 2, 3, 5, 7, 11, 13, 17}
}
