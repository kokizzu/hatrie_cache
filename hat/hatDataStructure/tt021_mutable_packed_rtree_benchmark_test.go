package hatDataStructure

import "testing"

var tt021MutablePackedSink int

func BenchmarkTT021MutablePackedRTreeUpdateQuery(b *testing.B) {
	entries := make([]MutableSpatialEntry[int], 0, 10000)
	for _, entry := range tt021BaselineEntries() {
		entries = append(entries, MutableSpatialEntry[int]{
			ID:     uint64(entry.value),
			Bounds: SpatialBox{MinX: entry.minX, MinY: entry.minY, MaxX: entry.maxX, MaxY: entry.maxY},
			Value:  entry.value,
		})
	}
	tree, err := NewMutablePackedRTree(entries, PackedRTreeOptions{})
	if err != nil {
		b.Fatal(err)
	}
	query := SpatialBox{MinX: 50, MinY: 50, MaxX: 59, MaxY: 59}
	moved := SpatialBox{MinX: 50, MinY: 50, MaxX: 50.75, MaxY: 50.75}
	destination := make([]int, 0, 100)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := tree.Upsert(5050, moved, 5050); err != nil {
			b.Fatal(err)
		}
		values, err := tree.QueryInto(query, destination)
		if err != nil {
			b.Fatal(err)
		}
		for _, value := range values {
			tt021MutablePackedSink += value
		}
		destination = values
	}
}

func BenchmarkTT021MutablePackedRTreeReadOnlyQuery(b *testing.B) {
	entries := make([]MutableSpatialEntry[int], 0, 10000)
	for _, entry := range tt021BaselineEntries() {
		entries = append(entries, MutableSpatialEntry[int]{
			ID:     uint64(entry.value),
			Bounds: SpatialBox{MinX: entry.minX, MinY: entry.minY, MaxX: entry.maxX, MaxY: entry.maxY},
			Value:  entry.value,
		})
	}
	tree, err := NewMutablePackedRTree(entries, PackedRTreeOptions{})
	if err != nil {
		b.Fatal(err)
	}
	query := SpatialBox{MinX: 50, MinY: 50, MaxX: 59, MaxY: 59}
	destination := make([]int, 0, 100)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		values, err := tree.QueryInto(query, destination)
		if err != nil {
			b.Fatal(err)
		}
		for _, value := range values {
			tt021MutablePackedSink += value
		}
		destination = values
	}
}

func BenchmarkTT021MutablePackedRTreeUpdateCompact(b *testing.B) {
	entries := make([]MutableSpatialEntry[int], 0, 10000)
	for _, entry := range tt021BaselineEntries() {
		entries = append(entries, MutableSpatialEntry[int]{
			ID:     uint64(entry.value),
			Bounds: SpatialBox{MinX: entry.minX, MinY: entry.minY, MaxX: entry.maxX, MaxY: entry.maxY},
			Value:  entry.value,
		})
	}
	tree, err := NewMutablePackedRTree(entries, PackedRTreeOptions{})
	if err != nil {
		b.Fatal(err)
	}
	moved := SpatialBox{MinX: 50, MinY: 50, MaxX: 50.75, MaxY: 50.75}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := tree.Upsert(5050, moved, 5050); err != nil {
			b.Fatal(err)
		}
		if err := tree.Compact(); err != nil {
			b.Fatal(err)
		}
		tt021MutablePackedSink += tree.Len()
	}
}
