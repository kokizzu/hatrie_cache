//go:build t218

package hatDataStructure

import "testing"

type t218BenchmarkKey struct {
	Group uint32
	Seq   uint32
}

type t218BenchmarkRow struct {
	ID    uint64
	Key   t218BenchmarkKey
	Value int64
}

const (
	t218BenchmarkPartCount = 16
	t218BenchmarkRowsPart  = 1024
)

var t218BenchmarkSink uint64

func compareT218BenchmarkKey(left, right t218BenchmarkKey) int {
	if left.Group < right.Group {
		return -1
	}
	if left.Group > right.Group {
		return 1
	}
	if left.Seq < right.Seq {
		return -1
	}
	if left.Seq > right.Seq {
		return 1
	}
	return 0
}

func t218BenchmarkParts() [][]TreeIndexEntry[int64, t218BenchmarkKey] {
	parts := make([][]TreeIndexEntry[int64, t218BenchmarkKey], t218BenchmarkPartCount)
	for part := range parts {
		entries := make([]TreeIndexEntry[int64, t218BenchmarkKey], t218BenchmarkRowsPart)
		for row := range entries {
			entries[row] = TreeIndexEntry[int64, t218BenchmarkKey]{
				ID: uint64(part*t218BenchmarkRowsPart + row + 1),
				Key: t218BenchmarkKey{
					Group: uint32((row*37 + part*17) % 256),
					Seq:   uint32(row*t218BenchmarkPartCount + part),
				},
				Value: int64(part*t218BenchmarkRowsPart + row),
			}
		}
		parts[part] = entries
	}
	return parts
}

func newT218BenchmarkMultiPartIndex(parts [][]TreeIndexEntry[int64, t218BenchmarkKey]) *MultiPartTreeIndex[int64, t218BenchmarkKey] {
	index, err := NewMultiPartTreeIndex[int64, t218BenchmarkKey](compareT218BenchmarkKey)
	if err != nil {
		panic(err)
	}
	for _, part := range parts {
		if _, err := index.AddPart(part); err != nil {
			panic(err)
		}
	}
	return index
}

func newT218BenchmarkOrderedIndex(parts [][]TreeIndexEntry[int64, t218BenchmarkKey]) *OrderedIndex[t218BenchmarkRow, t218BenchmarkKey] {
	index, err := NewOrderedIndex(
		func(row t218BenchmarkRow) t218BenchmarkKey { return row.Key },
		compareT218BenchmarkKey,
		t218BenchmarkPartCount*t218BenchmarkRowsPart,
	)
	if err != nil {
		panic(err)
	}
	for _, part := range parts {
		for _, entry := range part {
			if err := index.Upsert(entry.ID, t218BenchmarkRow{ID: entry.ID, Key: entry.Key, Value: entry.Value}); err != nil {
				panic(err)
			}
		}
	}
	return index
}

func BenchmarkT218MultiPartPublish(b *testing.B) {
	parts := t218BenchmarkParts()
	b.ReportAllocs()
	for range b.N {
		index := newT218BenchmarkMultiPartIndex(parts)
		t218BenchmarkSink += uint64(index.Len() + index.PartCount())
	}
}

func BenchmarkT218SingleOrderedIndexBuild(b *testing.B) {
	parts := t218BenchmarkParts()
	b.ReportAllocs()
	for range b.N {
		index := newT218BenchmarkOrderedIndex(parts)
		t218BenchmarkSink += uint64(index.Len())
	}
}

func BenchmarkT218MultiPartPrefixScan(b *testing.B) {
	index := newT218BenchmarkMultiPartIndex(t218BenchmarkParts())
	start := t218BenchmarkKey{Group: 64}
	end := t218BenchmarkKey{Group: 64, Seq: ^uint32(0)}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		iterator, ok := index.Prefix(start, end)
		if !ok {
			b.Fatal("expected prefix range")
		}
		for {
			entry, ok, err := iterator.Next()
			if err != nil {
				b.Fatal(err)
			}
			if !ok {
				break
			}
			t218BenchmarkSink += entry.ID
		}
	}
}

func BenchmarkT218SingleOrderedPrefixScan(b *testing.B) {
	index := newT218BenchmarkOrderedIndex(t218BenchmarkParts())
	start := t218BenchmarkKey{Group: 64}
	end := t218BenchmarkKey{Group: 64, Seq: ^uint32(0)}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		iterator, ok := index.Range(start, end)
		if !ok {
			b.Fatal("expected prefix range")
		}
		for {
			entry, ok, err := iterator.Next()
			if err != nil {
				b.Fatal(err)
			}
			if !ok {
				break
			}
			t218BenchmarkSink += entry.ID
		}
	}
}
