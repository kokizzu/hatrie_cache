package hatDataStructure

import "testing"

func BenchmarkTU19DirectApply(b *testing.B) {
	cache, err := NewPackedTuple([][]byte{[]byte("key"), make([]byte, 8)})
	if err != nil {
		b.Fatal(err)
	}
	update := []TupleFieldUpdate{{Index: 1, Kind: TupleFieldAddInt64, Delta: 1}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache, err = cache.ApplyUpdates(update)
		if err != nil {
			b.Fatal(err)
		}
	}
}
