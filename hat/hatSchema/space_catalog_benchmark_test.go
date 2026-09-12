package hatSchema

import (
	"strconv"
	"testing"
)

func BenchmarkSpaceCatalogOperations(b *testing.B) {
	definitions := make([]SpaceDefinition, 64)
	for index := range definitions {
		name := "space-" + strconv.Itoa(index)
		definitions[index] = SpaceDefinition{
			Name:    name,
			Version: 1,
			Source:  Source{Name: name, Columns: []Column{{Name: "id", Type: TypeText}, {Name: "value", Type: TypeText}}},
			Indexes: []IndexDefinition{{Name: "primary", Kind: IndexKindTree, Columns: []string{"id"}, Unique: true}},
		}
	}
	catalog, err := NewSpaceCatalog(definitions)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("lookup", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			if _, ok := catalog.Lookup("space-31"); !ok {
				b.Fatal("space-31 missing")
			}
		}
	})
	b.Run("list", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			if got := catalog.List(); len(got) != len(definitions) {
				b.Fatalf("List() length = %d, want %d", len(got), len(definitions))
			}
		}
	})
}
