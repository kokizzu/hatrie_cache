package hatDataStructure

import "testing"

var tt021MutableBaselineSink int

func BenchmarkTT021ExistingRTreeUpdateQuery(b *testing.B) {
	tree := NewDefaultRTree()
	for _, entry := range tt021BaselineEntries() {
		if err := tree.Upsert(uint64(entry.value), RTreeBounds{
			MinX: entry.minX,
			MinY: entry.minY,
			MaxX: entry.maxX,
			MaxY: entry.maxY,
		}); err != nil {
			b.Fatal(err)
		}
	}
	query := RTreeBounds{MinX: 50, MinY: 50, MaxX: 59, MaxY: 59}
	moved := RTreeBounds{MinX: 50, MinY: 50, MaxX: 50.75, MaxY: 50.75}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := tree.Upsert(5050, moved); err != nil {
			b.Fatal(err)
		}
		values, err := tree.Search(query)
		if err != nil {
			b.Fatal(err)
		}
		for _, value := range values {
			tt021MutableBaselineSink += int(value)
		}
	}
}

func BenchmarkTT021PackedRebuildUpdateQuery(b *testing.B) {
	entries := tt021PackedRTreeEntries()
	query := SpatialBox{MinX: 50, MinY: 50, MaxX: 59, MaxY: 59}
	moved := SpatialBox{MinX: 50, MinY: 50, MaxX: 50.75, MaxY: 50.75}
	destination := make([]int, 0, 100)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		entries[5050].Bounds = moved
		tree, err := NewPackedRTree(entries, PackedRTreeOptions{})
		if err != nil {
			b.Fatal(err)
		}
		values, err := tree.QueryInto(query, destination)
		if err != nil {
			b.Fatal(err)
		}
		for _, value := range values {
			tt021MutableBaselineSink += value
		}
		destination = values
	}
}

func BenchmarkTT021LinearUpdateQuery(b *testing.B) {
	entries := tt021BaselineEntries()
	query := tt021BaselineBox{minX: 50, minY: 50, maxX: 59, maxY: 59}
	moved := tt021BaselineBox{minX: 50, minY: 50, maxX: 50.75, maxY: 50.75}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		entries[5050].tt021BaselineBox = moved
		for _, entry := range entries {
			if entry.maxX >= query.minX && entry.minX <= query.maxX && entry.maxY >= query.minY && entry.minY <= query.maxY {
				tt021MutableBaselineSink += entry.value
			}
		}
	}
}
