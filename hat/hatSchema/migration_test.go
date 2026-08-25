package hatSchema

import (
	"reflect"
	"testing"
)

func TestMigrationApplyAndRevertAreVersionedAndAtomic(t *testing.T) {
	schema := Schema{}
	createUsers := Migration{
		Version: 1,
		Name:    "create users",
		Up: []Change{{Kind: ChangeCreateSource, Source: Source{
			Name:    "users",
			Columns: []Column{{Name: "id", Type: TypeInteger}},
		}}},
		Down: []Change{{Kind: ChangeDropSource, SourceName: "users"}},
	}
	addEmail := Migration{
		Version: 2,
		Name:    "add email",
		Up:      []Change{{Kind: ChangeAddColumn, SourceName: "users", Column: Column{Name: "email", Type: TypeText}}},
		Down:    []Change{{Kind: ChangeDropColumn, SourceName: "users", Column: Column{Name: "email", Type: TypeText}}},
	}
	if err := Apply(&schema, createUsers); err != nil {
		t.Fatalf("Apply(create users) error = %v", err)
	}
	if err := Apply(&schema, addEmail); err != nil {
		t.Fatalf("Apply(add email) error = %v", err)
	}
	if schema.Version != 2 || !reflect.DeepEqual(schema.Sources["users"].Columns, []Column{{Name: "id", Type: TypeInteger}, {Name: "email", Type: TypeText}}) {
		t.Fatalf("applied schema = %#v", schema)
	}
	if err := Revert(&schema, addEmail); err != nil {
		t.Fatalf("Revert(add email) error = %v", err)
	}
	if schema.Version != 1 || !reflect.DeepEqual(schema.Sources["users"].Columns, []Column{{Name: "id", Type: TypeInteger}}) {
		t.Fatalf("reverted schema = %#v", schema)
	}
	before := schema.Clone()
	if err := Apply(&schema, Migration{Version: 2, Name: "invalid", Up: []Change{{Kind: ChangeAddColumn, SourceName: "missing", Column: Column{Name: "x", Type: TypeText}}}, Down: []Change{{Kind: ChangeDropColumn, SourceName: "missing", Column: Column{Name: "x", Type: TypeText}}}}); err == nil {
		t.Fatal("Apply(invalid) error = nil")
	}
	if !reflect.DeepEqual(schema, before) {
		t.Fatalf("failed migration mutated schema: got %#v, want %#v", schema, before)
	}
}
