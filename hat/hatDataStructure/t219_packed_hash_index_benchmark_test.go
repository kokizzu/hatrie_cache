//go:build t219

package hatDataStructure

import "testing"

const t219PackedHashIndexRows = 65536

var t219PackedHashIndexSink int

func t219PackedHashIndexHash(value int) uint64 {
	return uint64(value) * 2654435761
}

func t219PackedHashIndexEntries() []HashIndexEntry[int, int] {
	entries := make([]HashIndexEntry[int, int], t219PackedHashIndexRows)
	for value := range entries {
		entries[value] = HashIndexEntry[int, int]{ID: uint64(value + 1), Key: value, Value: value * 3}
	}
	return entries
}

func BenchmarkT219PackedHashIndexLookup(b *testing.B) {
	entries := t219PackedHashIndexEntries()
	packed, err := NewPackedHashIndex[int, int](t219PackedHashIndexHash, entries)
	if err != nil {
		b.Fatal(err)
	}
	mutable, err := NewHashIndex(func(value int) int { return value }, HashIndexOptions{Unique: true, Capacity: len(entries)})
	if err != nil {
		b.Fatal(err)
	}
	for _, entry := range entries {
		if err := mutable.Upsert(entry.ID, entry.Key); err != nil {
			b.Fatal(err)
		}
	}
	b.Run("packed-open-addressing", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			entry, ok := packed.Lookup(iteration % len(entries))
			if !ok {
				b.Fatal("packed Lookup() = false")
			}
			t219PackedHashIndexSink = entry.Value
		}
	})
	b.Run("mutable-map", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			entry, ok := mutable.LookupOne(iteration % len(entries))
			if !ok {
				b.Fatal("mutable LookupOne() = false")
			}
			t219PackedHashIndexSink = entry.Value
		}
	})
}

func BenchmarkT219PackedHashIndexBuild(b *testing.B) {
	entries := t219PackedHashIndexEntries()
	b.Run("packed-open-addressing", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			index, err := NewPackedHashIndex[int, int](t219PackedHashIndexHash, entries)
			if err != nil {
				b.Fatal(err)
			}
			t219PackedHashIndexSink = index.Len()
		}
	})
	b.Run("mutable-map", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			index, err := NewHashIndex(func(value int) int { return value }, HashIndexOptions{Unique: true, Capacity: len(entries)})
			if err != nil {
				b.Fatal(err)
			}
			for _, entry := range entries {
				if err := index.Upsert(entry.ID, entry.Value); err != nil {
					b.Fatal(err)
				}
			}
			t219PackedHashIndexSink = index.Len()
		}
	})
}
