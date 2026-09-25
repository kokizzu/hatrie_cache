package hatSchema

import (
	"strconv"
	"testing"
)

var tt030SpaceCatalogBenchmarkSink []SpaceDefinition

func BenchmarkTT030SpaceCatalogMutation(b *testing.B) {
	definitions := make([]SpaceDefinition, 8)
	changes := make([]SpaceCatalogChange, len(definitions))
	for index := range definitions {
		definitions[index] = tt030SpaceDefinition("space-"+strconv.Itoa(index), uint64(index+1))
		changes[index] = SpaceCatalogChange{
			Kind:       SpaceCatalogChangeUpsert,
			Definition: definitions[index],
		}
	}

	b.Run("sequential_upsert", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			catalog, err := NewSpaceCatalog(nil)
			if err != nil {
				b.Fatal(err)
			}
			for index := range definitions {
				if err := catalog.Upsert(definitions[index]); err != nil {
					b.Fatal(err)
				}
			}
			tt030SpaceCatalogBenchmarkSink = catalog.List()
		}
	})

	b.Run("atomic_batch", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			catalog, err := NewSpaceCatalog(nil)
			if err != nil {
				b.Fatal(err)
			}
			if err := catalog.ApplyAtomic(changes); err != nil {
				b.Fatal(err)
			}
			tt030SpaceCatalogBenchmarkSink = catalog.List()
		}
	})
}
