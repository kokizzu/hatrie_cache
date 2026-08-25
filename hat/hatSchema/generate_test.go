package hatSchema

import (
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

func TestGenerateGoModelsEmitsFormattedTypedModels(t *testing.T) {
	data, err := GenerateGoModels(Schema{Sources: map[string]Source{
		"user profiles": {
			Name: "user profiles",
			Columns: []Column{
				{Name: "id", Type: TypeInteger, NotNull: true},
				{Name: "balance", Type: TypeDecimal, NotNull: true},
				{Name: "created_at", Type: TypeTimestamp, NotNull: true},
				{Name: "session_id", Type: TypeUUID, NotNull: true},
				{Name: "elapsed", Type: TypeDuration, NotNull: true},
				{Name: "payload", Type: TypeBinary, NotNull: true},
			},
		},
	}}, ModelOptions{Package: "models"})
	if err != nil {
		t.Fatalf("GenerateGoModels() error = %v", err)
	}
	if _, err := parser.ParseFile(token.NewFileSet(), "models.go", data, parser.AllErrors); err != nil {
		t.Fatalf("generated Go is invalid: %v\n%s", err, data)
	}
	text := string(data)
	for _, fragment := range []string{
		"package models",
		"type UserProfiles struct",
		"Id        int64",
		"Balance   hatSql.SQLDecimal",
		"CreatedAt time.Time",
		"`json:\"created_at\"`",
		"SessionId hatSql.SQLUUID",
		"Elapsed   time.Duration",
		"Payload   []byte",
	} {
		if !strings.Contains(text, fragment) {
			t.Fatalf("generated model missing %q:\n%s", fragment, text)
		}
	}
}
