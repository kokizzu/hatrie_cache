package hatSchema

import (
	"errors"
	"testing"
)

func TestSpaceCatalogStoresNamedVersionedIndexes(t *testing.T) {
	catalog, err := NewSpaceCatalog([]SpaceDefinition{
		{
			Name:    "orders",
			Version: 7,
			Source: Source{
				Name: "orders",
				Columns: []Column{
					{Name: "id", Type: TypeText},
					{Name: "region", Type: TypeText},
				},
			},
			Indexes: []IndexDefinition{{Name: "primary", Kind: IndexKindTree, Columns: []string{"id"}, Unique: true}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := catalog.Upsert(SpaceDefinition{
		Name:    " customers ",
		Version: 2,
		Source:  Source{Name: "customers", Columns: []Column{{Name: "id", Type: TypeText}}},
	}); err != nil {
		t.Fatal(err)
	}
	space, ok := catalog.Lookup(" orders ")
	if !ok || space.Name != "orders" || space.Version != 7 || len(space.Indexes) != 1 || !space.Indexes[0].Unique {
		t.Fatalf("Lookup() = %#v/%v", space, ok)
	}
	space.Source.Columns[0].Name = "mutated"
	space.Indexes[0].Columns[0] = "mutated"
	unchanged, ok := catalog.Lookup("orders")
	if !ok || unchanged.Source.Columns[0].Name != "id" || unchanged.Indexes[0].Columns[0] != "id" {
		t.Fatalf("Lookup() did not isolate returned definition: %#v/%v", unchanged, ok)
	}
	list := catalog.List()
	if len(list) != 2 || list[0].Name != "customers" || list[1].Name != "orders" {
		t.Fatalf("List() = %#v, want deterministic name order", list)
	}
	if !catalog.Delete("orders") || catalog.Delete("orders") {
		t.Fatal("Delete() did not report one removal")
	}
}

func TestSpaceCatalogRejectsInvalidDefinitions(t *testing.T) {
	catalog, err := NewSpaceCatalog(nil)
	if err != nil {
		t.Fatal(err)
	}
	invalid := []SpaceDefinition{
		{Source: Source{Columns: []Column{{Name: "id", Type: TypeText}}}},
		{Name: "orders", Source: Source{Name: "other", Columns: []Column{{Name: "id", Type: TypeText}}}},
		{Name: "orders", Source: Source{Name: "orders", Columns: []Column{{Name: "id", Type: TypeText}}}, Indexes: []IndexDefinition{{Name: "bad", Kind: IndexKindTree, Columns: []string{"missing"}}}},
		{Name: "orders", Source: Source{Name: "orders", Columns: []Column{{Name: "id", Type: TypeText}}}, Indexes: []IndexDefinition{{Name: "duplicate", Kind: IndexKindTree, Columns: []string{"id"}}, {Name: "duplicate", Kind: IndexKindHash, Columns: []string{"id"}}}},
		{Name: "orders", Source: Source{Name: "orders", Columns: []Column{{Name: "id", Type: TypeText}}}, Indexes: []IndexDefinition{{Name: "bad", Kind: IndexKind("unknown"), Columns: []string{"id"}}}},
	}
	for _, definition := range invalid {
		if err := catalog.Upsert(definition); !errors.Is(err, ErrSpaceCatalogInvalid) && !errors.Is(err, ErrSpaceCatalogNameRequired) {
			t.Fatalf("Upsert(%#v) error = %v, want catalog validation error", definition, err)
		}
	}
}
