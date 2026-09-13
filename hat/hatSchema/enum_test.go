package hatSchema

import (
	"strings"
	"testing"
)

func TestEnumSchemaValidationGenerationCloneAndCompatibility(t *testing.T) {
	schema := Schema{Version: 1, Sources: map[string]Source{
		"jobs": {
			Name:    "jobs",
			Columns: []Column{{Name: "status", Type: TypeEnum8, EnumValues: []string{"queued", "running", "done"}, NotNull: true}},
		},
	}}
	if err := schema.Validate(); err != nil {
		t.Fatalf("Schema.Validate() error = %v", err)
	}
	generated, err := GenerateGoModels(schema, ModelOptions{Package: "models"})
	if err != nil {
		t.Fatalf("GenerateGoModels() error = %v", err)
	}
	if !strings.Contains(string(generated), "hatSql.SQLEnum8") {
		t.Fatalf("generated model = %s, want hatSql.SQLEnum8", generated)
	}

	clone := schema.Clone()
	clone.Sources["jobs"].Columns[0].EnumValues[0] = "changed"
	if schema.Sources["jobs"].Columns[0].EnumValues[0] != "queued" {
		t.Fatal("Clone() did not deep-copy enum labels")
	}
	if schema.Fingerprint() == clone.Fingerprint() {
		t.Fatal("Fingerprint() did not include enum labels")
	}

	next := schema.Clone()
	next.Version = 2
	next.Sources["jobs"] = next.Sources["jobs"]
	next.Sources["jobs"].Columns[0].EnumValues = []string{"queued", "running", "finished"}
	report, err := CheckRollingCompatibility(schema, next)
	if err != nil {
		t.Fatalf("CheckRollingCompatibility() error = %v", err)
	}
	if report.Compatible {
		t.Fatalf("report = %#v, want enum label change to be incompatible", report)
	}
}

func TestEnumSchemaValidationRejectsInvalidDefinitions(t *testing.T) {
	tooMany := make([]string, 65_537)
	for index := range tooMany {
		tooMany[index] = "value"
	}
	cases := []Column{
		{Name: "status", Type: TypeEnum8},
		{Name: "status", Type: TypeEnum8, EnumValues: []string{"same", "same"}},
		{Name: "status", Type: TypeText, EnumValues: []string{"one", "two", "three"}},
		{Name: "status", Type: TypeEnum16, EnumValues: tooMany},
	}
	for index, column := range cases {
		t.Run(string(rune('a'+index)), func(t *testing.T) {
			schema := Schema{Sources: map[string]Source{"jobs": {Name: "jobs", Columns: []Column{column}}}}
			if err := schema.Validate(); err == nil {
				t.Fatal("Schema.Validate() error = nil, want error")
			}
		})
	}
}
