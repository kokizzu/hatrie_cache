package hatDataStructure

import "testing"

func BenchmarkTU25DirectRTreeUpsert(b *testing.B) {
	tree := NewDefaultRTree()
	bounds := tu25PointBounds(12, 34)
	for id := uint64(0); id < 1024; id++ {
		if err := tree.Upsert(id, bounds); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := tree.Upsert(uint64(i&1023), bounds); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU25DirectRTreeSearch(b *testing.B) {
	tree := NewDefaultRTree()
	bounds := tu25PointBounds(12, 34)
	for id := uint64(0); id < 1024; id++ {
		if err := tree.Upsert(id, bounds); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := tree.Search(bounds); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU25CatalogUpsert(b *testing.B) {
	catalog := NewRTreeSpaceCatalog[tu25Record]()
	if err := catalog.Create(tu25RTreeDefinition()); err != nil {
		b.Fatal(err)
	}
	record := tu25Record{Indexed: true, X: 12, Y: 34}
	for id := uint64(0); id < 1024; id++ {
		if err := catalog.Upsert("geo", id, record); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := catalog.Upsert("geo", uint64(i&1023), record); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU25CatalogSearch(b *testing.B) {
	catalog := NewRTreeSpaceCatalog[tu25Record]()
	if err := catalog.Create(tu25RTreeDefinition()); err != nil {
		b.Fatal(err)
	}
	record := tu25Record{Indexed: true, X: 12, Y: 34}
	for id := uint64(0); id < 1024; id++ {
		if err := catalog.Upsert("geo", id, record); err != nil {
			b.Fatal(err)
		}
	}
	bounds := tu25PointBounds(12, 34)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := catalog.Search("geo", bounds); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU25CatalogRebuild(b *testing.B) {
	catalog := NewRTreeSpaceCatalog[tu25Record]()
	if err := catalog.Create(tu25RTreeDefinition()); err != nil {
		b.Fatal(err)
	}
	rows := make([]RTreeSpaceRow[tu25Record], 1024)
	for id := range rows {
		rows[id] = RTreeSpaceRow[tu25Record]{
			ID:    uint64(id),
			Value: tu25Record{Indexed: true, X: 12, Y: 34},
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := catalog.Rebuild("geo", rows); err != nil {
			b.Fatal(err)
		}
	}
}
