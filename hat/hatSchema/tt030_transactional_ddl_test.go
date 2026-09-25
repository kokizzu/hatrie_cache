package hatSchema

import (
	"errors"
	"reflect"
	"testing"
)

func tt030SpaceDefinition(name string, version uint64) SpaceDefinition {
	return SpaceDefinition{
		Name:    name,
		Version: version,
		Source:  Source{Name: name, Columns: []Column{{Name: "id", Type: TypeInteger}}},
	}
}

func TestTT030SpaceCatalogApplyAtomicPublishesAllChanges(t *testing.T) {
	catalog, err := NewSpaceCatalog([]SpaceDefinition{tt030SpaceDefinition("users", 1)})
	if err != nil {
		t.Fatalf("NewSpaceCatalog() error = %v", err)
	}
	if err := catalog.ApplyAtomic([]SpaceCatalogChange{
		{Kind: SpaceCatalogChangeUpsert, Definition: tt030SpaceDefinition("orders", 2)},
		{Kind: SpaceCatalogChangeDelete, Name: "users"},
	}); err != nil {
		t.Fatalf("ApplyAtomic() error = %v", err)
	}
	wantCatalog, err := NewSpaceCatalog([]SpaceDefinition{tt030SpaceDefinition("orders", 2)})
	if err != nil {
		t.Fatalf("NewSpaceCatalog(want) error = %v", err)
	}
	want := wantCatalog.List()
	if got := catalog.List(); !reflect.DeepEqual(got, want) {
		t.Fatalf("List() = %#v, want %#v", got, want)
	}
}

func TestTT030SpaceCatalogApplyAtomicRollsBackInvalidBatch(t *testing.T) {
	initial := tt030SpaceDefinition("users", 1)
	catalog, err := NewSpaceCatalog([]SpaceDefinition{initial})
	if err != nil {
		t.Fatalf("NewSpaceCatalog() error = %v", err)
	}
	want := catalog.List()
	bad := tt030SpaceDefinition("broken", 2)
	bad.Source.Columns = []Column{{Name: "id", Type: TypeInteger}, {Name: "id", Type: TypeText}}
	err = catalog.ApplyAtomic([]SpaceCatalogChange{
		{Kind: SpaceCatalogChangeUpsert, Definition: tt030SpaceDefinition("orders", 2)},
		{Kind: SpaceCatalogChangeUpsert, Definition: bad},
		{Kind: SpaceCatalogChangeDelete, Name: "users"},
	})
	if !errors.Is(err, ErrSpaceCatalogInvalid) {
		t.Fatalf("ApplyAtomic() error = %v, want ErrSpaceCatalogInvalid", err)
	}
	if got := catalog.List(); !reflect.DeepEqual(got, want) {
		t.Fatalf("List() after rejected batch = %#v, want %#v", got, want)
	}
}

func TestTT030SpaceCatalogApplyAtomicRejectsInvalidChangeKind(t *testing.T) {
	catalog, err := NewSpaceCatalog(nil)
	if err != nil {
		t.Fatalf("NewSpaceCatalog() error = %v", err)
	}
	err = catalog.ApplyAtomic([]SpaceCatalogChange{{Kind: SpaceCatalogChangeKind("rename"), Name: "users"}})
	if !errors.Is(err, ErrSpaceCatalogChangeInvalid) {
		t.Fatalf("ApplyAtomic() error = %v, want ErrSpaceCatalogChangeInvalid", err)
	}
	if got := catalog.List(); len(got) != 0 {
		t.Fatalf("List() after invalid kind = %#v, want empty", got)
	}
}

func TestTT030SpaceCatalogApplyAtomicMissingDeleteIsIdempotent(t *testing.T) {
	catalog, err := NewSpaceCatalog(nil)
	if err != nil {
		t.Fatalf("NewSpaceCatalog() error = %v", err)
	}
	if err := catalog.ApplyAtomic([]SpaceCatalogChange{{Kind: SpaceCatalogChangeDelete, Name: "missing"}}); err != nil {
		t.Fatalf("ApplyAtomic() missing delete error = %v", err)
	}
}
