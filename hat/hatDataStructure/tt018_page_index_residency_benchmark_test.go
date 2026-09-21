package hatDataStructure

import "testing"

var tt018PageIndexResidencySink int

func BenchmarkTT018PageIndexResidencyGet(b *testing.B) {
	cache, err := NewPageIndexResidency[int, int](PageIndexResidencyOptions{
		MaxBytes:   1024,
		MaxEntries: 1024,
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 1024; index++ {
		if !cache.Put(index, index, 1) {
			b.Fatal("Put rejected resident page")
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		value, ok := cache.Get(index & 1023)
		if !ok {
			b.Fatal("Get missed resident page")
		}
		tt018PageIndexResidencySink += value
	}
}

func BenchmarkTT018PageIndexResidencyFill(b *testing.B) {
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		cache, err := NewPageIndexResidency[int, int](PageIndexResidencyOptions{
			MaxBytes:   1024,
			MaxEntries: 1024,
		})
		if err != nil {
			b.Fatal(err)
		}
		for page := 0; page < 1024; page++ {
			cache.Put(page, page, 1)
		}
		tt018PageIndexResidencySink += cache.Len()
	}
}
