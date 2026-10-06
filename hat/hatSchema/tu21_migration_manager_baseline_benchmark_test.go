package hatSchema

import "testing"

func BenchmarkTU21BaselinePreviewAndFingerprint(b *testing.B) {
	base := tu21BaselineSchema()
	migration := tu21BaselineMigration()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		updated, err := Preview(&base, migration)
		if err != nil {
			b.Fatal(err)
		}
		_ = updated.Fingerprint()
	}
}

func tu21BaselineSchema() Schema {
	return Schema{
		Version: 0,
		Sources: map[string]Source{
			"users": {
				Name: "users",
				Columns: []Column{
					{Name: "id", Type: TypeInteger, NotNull: true},
				},
			},
		},
	}
}

func tu21BaselineMigration() Migration {
	return Migration{
		Version: 1,
		Name: "add-email",
		Up: []Change{{
			Kind:       ChangeAddColumn,
			SourceName: "users",
			Column:     Column{Name: "email", Type: TypeText},
		}},
		Down: []Change{{
			Kind:       ChangeDropColumn,
			SourceName: "users",
			Column:     Column{Name: "email", Type: TypeText},
		}},
	}
}
