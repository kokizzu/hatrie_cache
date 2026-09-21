package hatDataStructure

import "testing"

var tt021PackedRTreeSink int

func BenchmarkTT021PackedRTreeBuild(b *testing.B) {
	entries := tt021PackedRTreeEntries()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		tree, err := NewPackedRTree(entries, PackedRTreeOptions{})
		if err != nil {
			b.Fatal(err)
		}
		tt021PackedRTreeSink += tree.Len()
	}
}

func BenchmarkTT021PackedRTreeQueryInto(b *testing.B) {
	entries := tt021PackedRTreeEntries()
	tree, err := NewPackedRTree(entries, PackedRTreeOptions{})
	if err != nil {
		b.Fatal(err)
	}
	query := SpatialBox{MinX: 50, MinY: 50, MaxX: 59, MaxY: 59}
	destination := make([]int, 0, 100)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		values, queryErr := tree.QueryInto(query, destination)
		if queryErr != nil {
			b.Fatal(queryErr)
		}
		for _, value := range values {
			tt021PackedRTreeSink += value
		}
		destination = values
	}
}

func BenchmarkTT021PackedRTreeVisit(b *testing.B) {
	tree, err := NewPackedRTree(tt021PackedRTreeEntries(), PackedRTreeOptions{})
	if err != nil {
		b.Fatal(err)
	}
	query := SpatialBox{MinX: 50, MinY: 50, MaxX: 59, MaxY: 59}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		count, visitErr := tree.Visit(query, func(value int) bool {
			tt021PackedRTreeSink += value
			return true
		})
		if visitErr != nil || count != 100 {
			b.Fatalf("Visit() = %d/%v, want 100/nil", count, visitErr)
		}
	}
}

func tt021PackedRTreeEntries() []SpatialEntry[int] {
	baseline := tt021BaselineEntries()
	entries := make([]SpatialEntry[int], len(baseline))
	for index, entry := range baseline {
		entries[index] = SpatialEntry[int]{
			Bounds: SpatialBox{MinX: entry.minX, MinY: entry.minY, MaxX: entry.maxX, MaxY: entry.maxY},
			Value:  entry.value,
		}
	}
	return entries
}
