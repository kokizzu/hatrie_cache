package hatDataStructure

import (
	"reflect"
	"testing"
)

func TestHashIndexAdaptivePostingLifecycle(t *testing.T) {
	zeroIndex, err := NewHashIndex(func(value int) int { return value }, HashIndexOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := zeroIndex.Upsert(0, 0); err != nil {
		t.Fatal(err)
	}
	if got := zeroIndex.LookupIDs(0); !reflect.DeepEqual(got, []uint64{0}) {
		t.Fatalf("zero-ID LookupIDs() = %v", got)
	}

	index, err := NewHashIndex(
		func(value int) string {
			if value == 90 || value == 3 {
				return "blue"
			}
			return "green"
		},
		HashIndexOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(90, 90); err != nil {
		t.Fatal(err)
	}
	if got := index.LookupIDs("blue"); !reflect.DeepEqual(got, []uint64{90}) {
		t.Fatalf("singleton LookupIDs() = %v", got)
	}
	if err := index.Upsert(3, 3); err != nil {
		t.Fatal(err)
	}
	if got := index.LookupIDs("blue"); !reflect.DeepEqual(got, []uint64{3, 90}) {
		t.Fatalf("promoted LookupIDs() = %v", got)
	}
	if !index.Delete(3) {
		t.Fatal("Delete(3) = false")
	}
	if got := index.LookupIDs("blue"); !reflect.DeepEqual(got, []uint64{90}) {
		t.Fatalf("demoted LookupIDs() = %v", got)
	}
	if !index.Delete(90) || index.Contains("blue") {
		t.Fatal("deleting the final posting failed")
	}
}

var hashIndexAdaptiveBenchmarkSink uint64

func BenchmarkHashIndexAdaptiveSingletonLookup(b *testing.B) {
	index, err := NewHashIndex(func(value int) int { return value }, HashIndexOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if err := index.Upsert(99_999, 99_999); err != nil {
		b.Fatal(err)
	}
	scratch := make([]uint64, 0, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		scratch = index.LookupIDsInto(99_999, scratch)
		hashIndexAdaptiveBenchmarkSink += scratch[0]
	}
}

func BenchmarkHashIndexAdaptiveDenseLookup(b *testing.B) {
	index, err := NewHashIndex(func(value int) int { return 1 }, HashIndexOptions{})
	if err != nil {
		b.Fatal(err)
	}
	for id := 0; id < 10_000; id++ {
		if err := index.Upsert(uint64(id), id); err != nil {
			b.Fatal(err)
		}
	}
	scratch := make([]uint64, 0, 10_000)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		scratch = index.LookupIDsInto(1, scratch)
		hashIndexAdaptiveBenchmarkSink += scratch[iteration%len(scratch)]
	}
}

func BenchmarkHashIndexAdaptiveBuildSingletons(b *testing.B) {
	const entries = 128
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		index, err := NewHashIndex(func(value int) int { return value }, HashIndexOptions{Capacity: entries})
		if err != nil {
			b.Fatal(err)
		}
		for id := 0; id < entries; id++ {
			if err := index.Upsert(uint64(id), id); err != nil {
				b.Fatal(err)
			}
		}
		hashIndexAdaptiveBenchmarkSink += uint64(index.Len())
	}
}
