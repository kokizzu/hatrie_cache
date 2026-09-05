package hatSchema

import "testing"

func TestSchemaFingerprintIsDeterministicAndSensitive(t *testing.T) {
	base := Schema{
		Version: 7,
		Sources: map[string]Source{
			"users": {
				Name:    "users",
				Columns: []Column{{Name: "id", Type: TypeInteger}, {Name: "email", Type: TypeText, NotNull: true}},
			},
			"teams": {
				Name:    "teams",
				Columns: []Column{{Name: "id", Type: TypeUUID}},
			},
		},
	}
	reordered := Schema{
		Version: 7,
		Sources: map[string]Source{
			"teams": base.Sources["teams"],
			"users": base.Sources["users"],
		},
	}
	if got, want := base.Fingerprint(), reordered.Fingerprint(); got == "" || got != want {
		t.Fatalf("reordered schema fingerprints = %q/%q, want equal non-empty values", got, want)
	}

	changed := base.Clone()
	changed.Sources["users"] = Source{
		Name:    "users",
		Columns: []Column{{Name: "id", Type: TypeInteger}, {Name: "email", Type: TypeBinary, NotNull: true}},
	}
	if base.Fingerprint() == changed.Fingerprint() {
		t.Fatalf("schema fingerprint did not change after column type change: %q", base.Fingerprint())
	}
}
