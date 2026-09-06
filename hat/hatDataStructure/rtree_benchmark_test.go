package hatDataStructure

import (
	"runtime"
	"testing"
)

func BenchmarkRTreeSearch10K(b *testing.B) {
	tree := NewDefaultRTree()
	for id := uint64(0); id < 10000; id++ {
		x := float64(id % 100)
		y := float64(id / 100)
		if err := tree.Upsert(id, RTreeBounds{MinX: x, MinY: y, MaxX: x + 0.5, MaxY: y + 0.5}); err != nil {
			b.Fatal(err)
		}
	}
	query := RTreeBounds{MinX: 10, MinY: 10, MaxX: 20, MaxY: 20}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		ids, err := tree.Search(query)
		if err != nil {
			b.Fatal(err)
		}
		runtime.KeepAlive(ids)
	}
}

func BenchmarkRTreeSearch10KReuse(b *testing.B) {
	tree := NewDefaultRTree()
	for id := uint64(0); id < 10000; id++ {
		x := float64(id % 100)
		y := float64(id / 100)
		if err := tree.Upsert(id, RTreeBounds{MinX: x, MinY: y, MaxX: x + 0.5, MaxY: y + 0.5}); err != nil {
			b.Fatal(err)
		}
	}
	query := RTreeBounds{MinX: 10, MinY: 10, MaxX: 20, MaxY: 20}
	ids := make([]uint64, 0, 128)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		var err error
		ids, err = tree.SearchInto(ids[:0], query)
		if err != nil {
			b.Fatal(err)
		}
		runtime.KeepAlive(ids)
	}
}

func BenchmarkLinearRectangleSearch10K(b *testing.B) {
	items := make([]rtreeItem, 10000)
	for id := range items {
		x := float64(id % 100)
		y := float64(id / 100)
		items[id] = rtreeItem{id: uint64(id), bounds: RTreeBounds{MinX: x, MinY: y, MaxX: x + 0.5, MaxY: y + 0.5}}
	}
	query := RTreeBounds{MinX: 10, MinY: 10, MaxX: 20, MaxY: 20}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		ids := make([]uint64, 0)
		for _, item := range items {
			if item.bounds.Intersects(query) {
				ids = append(ids, item.id)
			}
		}
		runtime.KeepAlive(ids)
	}
}
