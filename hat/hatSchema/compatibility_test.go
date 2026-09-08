package hatSchema

import (
	"reflect"
	"testing"
)

func TestCheckRollingCompatibilityAllowsSafeAdditiveChanges(t *testing.T) {
	previous := schemaCompatibilityFixture(1)
	next := previous.Clone()
	next.Version = 2
	source := next.Sources["users"]
	source.Columns = append(source.Columns, Column{Name: "email", Type: TypeText})
	next.Sources["users"] = source
	report, err := CheckRollingCompatibility(previous, next)
	if err != nil {
		t.Fatalf("CheckRollingCompatibility() error = %v", err)
	}
	if !report.Compatible || report.PreviousVersion != 1 || report.NextVersion != 2 {
		t.Fatalf("compatibility report = %#v, want compatible version 1 -> 2", report)
	}
	want := []SchemaCompatibilityChange{
		{Kind: "version_advanced"},
		{Kind: "nullable_column_added", Source: "users", Column: "email"},
	}
	if !reflect.DeepEqual(report.Changes, want) {
		t.Fatalf("compatibility changes = %#v, want %#v", report.Changes, want)
	}
	next.Sources["users"].Columns[2].Name = "changed-after-report"
	if report.Changes[1].Column != "email" {
		t.Fatalf("report changed through input mutation: %#v", report)
	}
}

func TestCheckRollingCompatibilityRejectsUnsafeChanges(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Schema)
		kind   string
	}{
		{
			name: "version regression",
			mutate: func(schema *Schema) {
				schema.Version = 0
			},
			kind: "version_regressed",
		},
		{
			name: "column removed",
			mutate: func(schema *Schema) {
				source := schema.Sources["users"]
				source.Columns = source.Columns[:1]
				schema.Sources["users"] = source
			},
			kind: "column_removed",
		},
		{
			name: "column reordered",
			mutate: func(schema *Schema) {
				source := schema.Sources["users"]
				columns := source.Columns
				source.Columns = []Column{columns[1], columns[0]}
				schema.Sources["users"] = source
			},
			kind: "column_reordered",
		},
		{
			name: "type changed",
			mutate: func(schema *Schema) {
				schema.Sources["users"].Columns[0].Type = TypeText
			},
			kind: "column_type_changed",
		},
		{
			name: "column made required",
			mutate: func(schema *Schema) {
				schema.Sources["users"].Columns[1].NotNull = true
			},
			kind: "column_made_required",
		},
		{
			name: "required column added",
			mutate: func(schema *Schema) {
				source := schema.Sources["users"]
				source.Columns = append(source.Columns, Column{Name: "email", Type: TypeText, NotNull: true})
				schema.Sources["users"] = source
			},
			kind: "required_column_added",
		},
		{
			name: "source removed",
			mutate: func(schema *Schema) {
				delete(schema.Sources, "users")
			},
			kind: "source_removed",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			previous := schemaCompatibilityFixture(1)
			next := previous.Clone()
			next.Version = 2
			test.mutate(&next)
			report, err := CheckRollingCompatibility(previous, next)
			if err != nil {
				t.Fatalf("CheckRollingCompatibility() error = %v", err)
			}
			if report.Compatible {
				t.Fatalf("compatibility report = %#v, want incompatible", report)
			}
			found := false
			for _, change := range report.Changes {
				if change.Kind == test.kind {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("compatibility changes = %#v, want kind %q", report.Changes, test.kind)
			}
		})
	}
}

func TestCheckRollingCompatibilityRejectsInvalidSchemas(t *testing.T) {
	previous := schemaCompatibilityFixture(1)
	next := previous.Clone()
	next.Sources["users"].Columns[0].Type = Type("UNKNOWN")
	if _, err := CheckRollingCompatibility(previous, next); err == nil {
		t.Fatal("CheckRollingCompatibility() error = nil, want invalid schema error")
	}
}

func schemaCompatibilityFixture(version uint64) Schema {
	return Schema{
		Version: version,
		Sources: map[string]Source{
			"users": {
				Name: "users",
				Columns: []Column{
					{Name: "id", Type: TypeInteger, NotNull: true},
					{Name: "name", Type: TypeText},
				},
			},
		},
	}
}

func BenchmarkCheckRollingCompatibility(b *testing.B) {
	previous := schemaCompatibilityFixture(1)
	next := previous.Clone()
	next.Version = 2
	source := next.Sources["users"]
	source.Columns = append(source.Columns, Column{Name: "email", Type: TypeText})
	next.Sources["users"] = source
	b.ReportAllocs()
	for range b.N {
		report, err := CheckRollingCompatibility(previous, next)
		if err != nil {
			b.Fatal(err)
		}
		if !report.Compatible {
			b.Fatalf("report = %#v, want compatible", report)
		}
	}
}
