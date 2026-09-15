package hatDataStructure

import (
	"runtime"
	"testing"
)

func BenchmarkRTreeSearchSmallResultC219(b *testing.B) {
	tree := NewDefaultRTree()
	for id := uint64(0); id < 10000; id++ {
		x := float64(id % 100)
		y := float64(id / 100)
		if err := tree.Upsert(id, RTreeBounds{MinX: x, MinY: y, MaxX: x + 0.5, MaxY: y + 0.5}); err != nil {
			b.Fatal(err)
		}
	}
	queries := []struct {
		name   string
		bounds RTreeBounds
	}{
		{name: "empty", bounds: RTreeBounds{MinX: 1000, MinY: 1000, MaxX: 1001, MaxY: 1001}},
		{name: "one", bounds: RTreeBounds{MinX: 10, MinY: 10, MaxX: 10.5, MaxY: 10.5}},
		{name: "two", bounds: RTreeBounds{MinX: 10, MinY: 10, MaxX: 11.5, MaxY: 10.5}},
		{name: "many", bounds: RTreeBounds{MinX: 10, MinY: 10, MaxX: 20, MaxY: 20}},
	}
	for _, query := range queries {
		b.Run(query.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				ids, err := tree.Search(query.bounds)
				if err != nil {
					b.Fatal(err)
				}
				runtime.KeepAlive(ids)
			}
		})
	}
}
