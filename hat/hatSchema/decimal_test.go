package hatSchema

import (
	"strings"
	"testing"
)

func TestSQLDecimalSchemaValidationGenerationAndCompatibility(t *testing.T) {
	schema := Schema{Version: 1, Sources: map[string]Source{
		"ledger": {
			Name:    "ledger",
			Columns: []Column{{Name: "amount", Type: TypeDecimal128, DecimalScale: 2, DecimalPrecision: 38, NotNull: true}},
		},
	}}
	if err := schema.Validate(); err != nil {
		t.Fatalf("Schema.Validate() error = %v", err)
	}
	generated, err := GenerateGoModels(schema, ModelOptions{Package: "models"})
	if err != nil {
		t.Fatalf("GenerateGoModels() error = %v", err)
	}
	if !strings.Contains(string(generated), "hatSql.SQLDecimal128") {
		t.Fatalf("generated model = %s, want hatSql.SQLDecimal128", generated)
	}

	clone := schema.Clone()
	clone.Sources["ledger"].Columns[0].DecimalScale = 3
	if schema.Sources["ledger"].Columns[0].DecimalScale != 2 {
		t.Fatal("Clone() changed original decimal metadata")
	}
	if schema.Fingerprint() == clone.Fingerprint() {
		t.Fatal("Fingerprint() did not include decimal metadata")
	}

	next := schema.Clone()
	next.Version = 2
	next.Sources["ledger"] = next.Sources["ledger"]
	next.Sources["ledger"].Columns[0].DecimalScale = 3
	report, err := CheckRollingCompatibility(schema, next)
	if err != nil {
		t.Fatalf("CheckRollingCompatibility() error = %v", err)
	}
	if report.Compatible {
		t.Fatalf("report = %#v, want decimal metadata change to be incompatible", report)
	}
}

func TestSQLDecimalSchemaValidationRejectsInvalidDefinitions(t *testing.T) {
	cases := []Column{
		{Name: "amount", Type: TypeDecimal128, DecimalScale: 39, DecimalPrecision: 39},
		{Name: "amount", Type: TypeDecimal128, DecimalScale: 2, DecimalPrecision: 39},
		{Name: "amount", Type: TypeDecimal128, DecimalScale: 3, DecimalPrecision: 2},
		{Name: "amount", Type: TypeDecimal256, DecimalScale: 77, DecimalPrecision: 77},
		{Name: "amount", Type: TypeText, DecimalScale: 2},
	}
	for index, column := range cases {
		t.Run(string(rune('a'+index)), func(t *testing.T) {
			schema := Schema{Sources: map[string]Source{"ledger": {Name: "ledger", Columns: []Column{column}}}}
			if err := schema.Validate(); err == nil {
				t.Fatal("Schema.Validate() error = nil, want error")
			}
		})
	}
}
