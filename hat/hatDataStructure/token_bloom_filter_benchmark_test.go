package hatDataStructure_test

import (
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func BenchmarkTokenBloomFilterContainsAllTokens(b *testing.B) {
	filter, err := hatDataStructure.NewTokenBloomFilterWithShape(1<<20, 3)
	if err != nil {
		b.Fatal(err)
	}
	filter.AddText("fast low memory storage for café users")
	query := "FAST storage users"
	b.ReportAllocs()
	b.SetBytes(int64(len(query)))
	for index := 0; index < b.N; index++ {
		if !filter.ContainsAllTokens(query) {
			b.Fatal("filter rejected inserted tokens")
		}
	}
}

func BenchmarkTokenBloomFilterContainsAnyTokens(b *testing.B) {
	filter, err := hatDataStructure.NewTokenBloomFilterWithShape(1<<20, 3)
	if err != nil {
		b.Fatal(err)
	}
	filter.AddText("fast low memory storage for café users")
	query := "absent users"
	b.ReportAllocs()
	b.SetBytes(int64(len(query)))
	for index := 0; index < b.N; index++ {
		if !filter.ContainsAnyTokens(query) {
			b.Fatal("filter rejected an inserted token")
		}
	}
}

func BenchmarkTokenBloomFilterAddText(b *testing.B) {
	filter, err := hatDataStructure.NewTokenBloomFilterWithShape(1<<20, 3)
	if err != nil {
		b.Fatal(err)
	}
	text := "Fast, low-memory storage for café users"
	b.ReportAllocs()
	b.SetBytes(int64(len(text)))
	for index := 0; index < b.N; index++ {
		filter.AddText(text)
	}
}
