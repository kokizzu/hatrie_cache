package hatDataStructure

import "testing"

func BenchmarkT229SpacePutBaseline(b *testing.B) {
	space, err := NewSpace(SpaceOptions{Memtx: MemtxSpaceOptions{MaxRecords: 1}})
	if err != nil {
		b.Fatal(err)
	}
	value := []byte("value")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := space.Put("key", value); err != nil {
			b.Fatal(err)
		}
	}
}
