package hatSchema

import (
	"errors"
	"reflect"
	"testing"
)

func tu24ConditionalSpaceDefinition() SpaceDefinition {
	return SpaceDefinition{
		Name:    "events",
		Version: 4,
		Source: Source{
			Name: "events",
			Columns: []Column{
				{Name: "id", Type: TypeText},
				{Name: "tenant", Type: TypeText},
				{Name: "state", Type: TypeText},
			},
		},
		Indexes: []IndexDefinition{
			{
				Name:             "tenant_open",
				Kind:             IndexKindTree,
				Columns:          []string{"tenant"},
				Predicate:        "state = 'open'",
				PredicateColumns: []string{"state"},
			},
			{Name: "id", Kind: IndexKindHash, Columns: []string{"id"}, Unique: true},
		},
	}
}

func TestTU24ConditionalIndexesValidateAndExposePlannerMetadata(t *testing.T) {
	catalog, err := NewSpaceCatalog([]SpaceDefinition{tu24ConditionalSpaceDefinition()})
	if err != nil {
		t.Fatalf("NewSpaceCatalog() error = %v", err)
	}
	indexes, ok := catalog.ConditionalIndexes(" events ")
	if !ok || len(indexes) != 1 {
		t.Fatalf("ConditionalIndexes() = %#v/%v, want one conditional index", indexes, ok)
	}
	want := IndexDefinition{
		Name:             "tenant_open",
		Kind:             IndexKindTree,
		Columns:          []string{"tenant"},
		Predicate:        "state = 'open'",
		PredicateColumns: []string{"state"},
	}
	if !reflect.DeepEqual(indexes[0], want) {
		t.Fatalf("conditional metadata = %#v, want %#v", indexes[0], want)
	}
	indexes[0].PredicateColumns[0] = "mutated"
	unchanged, ok := catalog.ConditionalIndexes("events")
	if !ok || unchanged[0].PredicateColumns[0] != "state" {
		t.Fatalf("ConditionalIndexes() did not clone metadata: %#v/%v", unchanged, ok)
	}

	updated := tu24ConditionalSpaceDefinition()
	updated.Indexes[0].Predicate = "state = 'closed'"
	if err := catalog.Upsert(updated); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	updatedIndexes, ok := catalog.ConditionalIndexes("events")
	if !ok || len(updatedIndexes) != 1 || updatedIndexes[0].Predicate != "state = 'closed'" {
		t.Fatalf("updated conditional metadata = %#v/%v", updatedIndexes, ok)
	}
	if !catalog.Delete("events") {
		t.Fatal("Delete() returned false")
	}
	if _, ok := catalog.ConditionalIndexes("events"); ok {
		t.Fatal("ConditionalIndexes() returned metadata after delete")
	}
}

func TestTU24ConditionalIndexesRejectInvalidPredicateMetadata(t *testing.T) {
	cases := []SpaceDefinition{
		{Name: "events", Source: Source{Name: "events", Columns: []Column{{Name: "tenant", Type: TypeText}}}, Indexes: []IndexDefinition{{Name: "bad", Kind: IndexKindTree, Columns: []string{"tenant"}, Predicate: "tenant <> ''"}}},
		{Name: "events", Source: Source{Name: "events", Columns: []Column{{Name: "tenant", Type: TypeText}}}, Indexes: []IndexDefinition{{Name: "bad", Kind: IndexKindTree, Columns: []string{"tenant"}, PredicateColumns: []string{"tenant"}}}},
		{Name: "events", Source: Source{Name: "events", Columns: []Column{{Name: "tenant", Type: TypeText}}}, Indexes: []IndexDefinition{{Name: "bad", Kind: IndexKindTree, Columns: []string{"tenant"}, Predicate: "tenant <> ''", PredicateColumns: []string{"missing"}}}},
		{Name: "events", Source: Source{Name: "events", Columns: []Column{{Name: "tenant", Type: TypeText}}}, Indexes: []IndexDefinition{{Name: "bad", Kind: IndexKindTree, Columns: []string{"tenant"}, Predicate: "tenant <> ''", PredicateColumns: []string{"tenant", "tenant"}}}},
	}
	for index, definition := range cases {
		_, err := NewSpaceCatalog([]SpaceDefinition{definition})
		if !errors.Is(err, ErrSpaceCatalogInvalid) {
			t.Fatalf("case %d error = %v, want ErrSpaceCatalogInvalid", index, err)
		}
	}
}
