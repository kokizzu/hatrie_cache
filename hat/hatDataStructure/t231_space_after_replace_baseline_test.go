package hatDataStructure

import "testing"

func BenchmarkT231SpacePutBaseline(b *testing.B) {
	space, err := NewSpace(SpaceOptions{Memtx: MemtxSpaceOptions{MaxRecords: 1}})
	if err != nil {
		b.Fatal(err)
	}
	value := []byte("value")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := space.Put("key", value); err != nil {
			b.Fatal(err)
		}
	}
}
