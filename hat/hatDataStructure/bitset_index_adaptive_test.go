package hatDataStructure

import (
	"reflect"
	"testing"
)

func TestBitsetIndexAdaptivePostingLifecycle(t *testing.T) {
	index, err := NewBitsetIndex[string](128, 4)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(90, "blue"); err != nil {
		t.Fatal(err)
	}
	if got := index.LookupIDs("blue"); !reflect.DeepEqual(got, []uint32{90}) {
		t.Fatalf("singleton LookupIDs() = %v", got)
	}
	if err := index.Upsert(3, "blue"); err != nil {
		t.Fatal(err)
	}
	if got := index.LookupIDs("blue"); !reflect.DeepEqual(got, []uint32{3, 90}) {
		t.Fatalf("promoted LookupIDs() = %v", got)
	}
	if !index.Delete(3) {
		t.Fatal("Delete(3) = false")
	}
	if got := index.LookupIDs("blue"); !reflect.DeepEqual(got, []uint32{90}) {
		t.Fatalf("demoted LookupIDs() = %v", got)
	}
	if err := index.Upsert(4, "green"); err != nil {
		t.Fatal(err)
	}
	if got := index.DistinctKeys(); got != 2 {
		t.Fatalf("DistinctKeys() = %d, want 2", got)
	}
	if !index.Delete(90) || !index.Delete(4) {
		t.Fatal("deleting final postings failed")
	}
	if got := index.DistinctKeys(); got != 0 {
		t.Fatalf("DistinctKeys() after delete = %d, want 0", got)
	}
}

var bitsetAdaptiveBenchmarkSink uint32

func BenchmarkBitsetIndexAdaptiveSingletonLookup(b *testing.B) {
	index, err := NewBitsetIndex[int](100_000, 1)
	if err != nil {
		b.Fatal(err)
	}
	if err := index.Upsert(99_999, 1); err != nil {
		b.Fatal(err)
	}
	scratch := make([]uint32, 0, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		scratch = index.LookupIDsInto(1, scratch)
		bitsetAdaptiveBenchmarkSink += scratch[0]
	}
}

func BenchmarkBitsetIndexAdaptiveDenseLookup(b *testing.B) {
	index, err := NewBitsetIndex[int](100_000, 1)
	if err != nil {
		b.Fatal(err)
	}
	for slot := uint32(0); slot < 10_000; slot++ {
		if err := index.Upsert(slot, 1); err != nil {
			b.Fatal(err)
		}
	}
	scratch := make([]uint32, 0, 10_000)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		scratch = index.LookupIDsInto(1, scratch)
		bitsetAdaptiveBenchmarkSink += scratch[iteration%len(scratch)]
	}
}

func BenchmarkBitsetIndexAdaptiveBuildSingletons(b *testing.B) {
	const (
		capacity = 16_384
		entries  = 128
	)
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		index, err := NewBitsetIndex[int](capacity, entries)
		if err != nil {
			b.Fatal(err)
		}
		for slot := 0; slot < entries; slot++ {
			if err := index.Upsert(uint32(slot), slot); err != nil {
				b.Fatal(err)
			}
		}
		bitsetAdaptiveBenchmarkSink += uint32(index.Len())
	}
}
