package hatDataStructure

import "testing"

func BenchmarkTU19BaselineApplyUpdates(b *testing.B) {
	fields := make([][]byte, 128)
	for index := range fields {
		fields[index] = []byte("fixed-value")
	}
	cache, err := NewPackedTuple(fields)
	if err != nil {
		b.Fatal(err)
	}
	update := []TupleFieldUpdate{{Index: len(fields) / 2, Kind: TupleFieldSet, Value: []byte("fixed-edit")}}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		updated, err := cache.ApplyUpdates(update)
		if err != nil {
			b.Fatal(err)
		}
		cache = updated
	}
}

func BenchmarkTU19BaselineRepack(b *testing.B) {
	fields := make([][]byte, 128)
	for index := range fields {
		fields[index] = []byte("fixed-value")
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		fields[len(fields)/2] = []byte("fixed-edit")
		if _, err := NewPackedTuple(fields); err != nil {
			b.Fatal(err)
		}
	}
}
