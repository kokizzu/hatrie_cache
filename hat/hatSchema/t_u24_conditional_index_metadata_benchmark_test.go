package hatSchema

import "testing"

func BenchmarkTU24ConditionalIndexMetadata(b *testing.B) {
	catalog, err := NewSpaceCatalog([]SpaceDefinition{tu24BenchmarkSpaceDefinition()})
	if err != nil {
		b.Fatal(err)
	}

	b.Run("manual-lookup-filter", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			definition, ok := catalog.Lookup("events")
			if !ok {
				b.Fatal("Lookup() did not find events")
			}
			conditional := make([]IndexDefinition, 0, 4)
			for _, index := range definition.Indexes {
				if index.Predicate != "" {
					conditional = append(conditional, index)
				}
			}
			if len(conditional) != 4 {
				b.Fatalf("manual conditional indexes = %d, want 4", len(conditional))
			}
		}
	})

	b.Run("catalog-conditional-view", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			conditional, ok := catalog.ConditionalIndexes("events")
			if !ok || len(conditional) != 4 {
				b.Fatalf("ConditionalIndexes() = %d/%v, want 4/true", len(conditional), ok)
			}
		}
	})
}

func tu24BenchmarkSpaceDefinition() SpaceDefinition {
	columns := []Column{{Name: "id", Type: TypeText}, {Name: "tenant", Type: TypeText}, {Name: "state", Type: TypeText}}
	for index := 0; index < 32; index++ {
		columns = append(columns, Column{Name: "field" + string(rune('a'+index)), Type: TypeText})
	}
	indexes := make([]IndexDefinition, 0, 32)
	for index := 0; index < 32; index++ {
		field := "field" + string(rune('a'+index))
		definition := IndexDefinition{Name: "index_" + field, Kind: IndexKindTree, Columns: []string{field}}
		if index%8 == 0 {
			definition.Predicate = "state = 'open'"
			definition.PredicateColumns = []string{"state"}
		}
		indexes = append(indexes, definition)
	}
	return SpaceDefinition{
		Name:    "events",
		Version: 1,
		Source:  Source{Name: "events", Columns: columns},
		Indexes: indexes,
	}
}
