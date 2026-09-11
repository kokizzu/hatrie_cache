package hatSchema

import "testing"

func BenchmarkPreviewMigration(b *testing.B) {
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
	b.ReportAllocs()
	for b.Loop() {
		preview, err := Preview(&schema, migration)
		if err != nil {
			b.Fatal(err)
		}
		if preview.Version != migration.Version {
			b.Fatalf("preview version = %d, want %d", preview.Version, migration.Version)
		}
	}
}
