package hatSchema

import "testing"

func TestPreviewMigrationDoesNotPublishOrShareSchemaState(t *testing.T) {
	schema := Schema{
		Version: 1,
		Sources: map[string]Source{
			"users": {
				Name:    "users",
				Columns: []Column{{Name: "id", Type: TypeInteger}},
			},
		},
	}
	migration := Migration{
		Version: 2,
		Name:    "add email",
		Up: []Change{{
			Kind:       ChangeAddColumn,
			SourceName: "users",
			Column:     Column{Name: "email", Type: TypeText},
		}},
		Down: []Change{{
			Kind:       ChangeDropColumn,
			SourceName: "users",
			Column:     Column{Name: "email"},
		}},
	}

	preview, err := Preview(&schema, migration)
	if err != nil {
		t.Fatalf("Preview() error = %v", err)
	}
	if schema.Version != 1 || len(schema.Sources["users"].Columns) != 1 {
		t.Fatalf("schema changed during Preview() = %#v", schema)
	}
	if preview.Version != 2 || len(preview.Sources["users"].Columns) != 2 {
		t.Fatalf("preview = %#v, want version 2 with two columns", preview)
	}
	preview.Sources["users"] = Source{Name: "changed", Columns: []Column{{Name: "other", Type: TypeText}}}
	if schema.Sources["users"].Name != "users" || len(schema.Sources["users"].Columns) != 1 {
		t.Fatal("Preview() returned schema state sharing mutable backing")
	}
}

func TestPreviewRejectsInvalidMigrationWithoutMutatingSchema(t *testing.T) {
	schema := Schema{
		Version: 4,
		Sources: map[string]Source{
			"users": {Name: "users", Columns: []Column{{Name: "id", Type: TypeInteger}}},
		},
	}
	preview, err := Preview(&schema, Migration{Version: 6, Name: "skipped version", Up: []Change{{Kind: ChangeAddColumn, SourceName: "users", Column: Column{Name: "email", Type: TypeText}}}, Down: []Change{{Kind: ChangeDropColumn, SourceName: "users", Column: Column{Name: "email"}}}})
	if err == nil {
		t.Fatal("Preview() accepted a non-sequential migration")
	}
	if preview.Version != 0 || schema.Version != 4 || len(schema.Sources["users"].Columns) != 1 {
		t.Fatalf("Preview() changed state on error: preview=%#v schema=%#v", preview, schema)
	}
}
