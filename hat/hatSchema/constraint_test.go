package hatSchema

import (
	"reflect"
	"strings"
	"testing"
)

func TestValidateDatasetEnforcesSchemaConstraints(t *testing.T) {
	schema := Schema{Sources: map[string]Source{
		"teams": {
			Name:        "teams",
			Columns:     []Column{{Name: "id", Type: TypeUUID, NotNull: true}, {Name: "name", Type: TypeText}},
			Constraints: []Constraint{{Name: "teams_id_unique", Kind: ConstraintUnique, Columns: []string{"id"}}},
		},
		"members": {
			Name:    "members",
			Columns: []Column{{Name: "id", Type: TypeInteger, NotNull: true}, {Name: "email", Type: TypeText}, {Name: "age", Type: TypeInteger}, {Name: "team_id", Type: TypeUUID}},
			Constraints: []Constraint{
				{Name: "members_id", Kind: ConstraintNotNull, Columns: []string{"id"}},
				{Name: "members_email_unique", Kind: ConstraintUnique, Columns: []string{"email"}},
				{Name: "members_age", Kind: ConstraintCheck, Expression: "age >= 18"},
				{Name: "members_team", Kind: ConstraintForeignKey, Columns: []string{"team_id"}, ReferenceSource: "teams", ReferenceColumns: []string{"id"}},
			},
		},
	}}
	rows := map[string][]Row{
		"teams":   {{"id": "9f315ba2-1729-4c73-92f7-c3e046f8eae3", "name": "core"}},
		"members": {{"id": 1, "email": "ada@example.test", "age": 31, "team_id": "9f315ba2-1729-4c73-92f7-c3e046f8eae3"}},
	}
	if err := ValidateDataset(schema, rows); err != nil {
		t.Fatalf("ValidateDataset(valid) error = %v", err)
	}

	for name, invalid := range map[string]map[string][]Row{
		"column not null":     {"teams": rows["teams"], "members": {{"id": nil, "email": "ada@example.test", "age": 31, "team_id": "9f315ba2-1729-4c73-92f7-c3e046f8eae3"}}},
		"constraint not null": {"teams": rows["teams"], "members": {{"email": "ada@example.test", "age": 31, "team_id": "9f315ba2-1729-4c73-92f7-c3e046f8eae3"}}},
		"unique":              {"teams": rows["teams"], "members": {{"id": 1, "email": "ada@example.test", "age": 31, "team_id": "9f315ba2-1729-4c73-92f7-c3e046f8eae3"}, {"id": 2, "email": "ada@example.test", "age": 32, "team_id": "9f315ba2-1729-4c73-92f7-c3e046f8eae3"}}},
		"check":               {"teams": rows["teams"], "members": {{"id": 1, "email": "ada@example.test", "age": 17, "team_id": "9f315ba2-1729-4c73-92f7-c3e046f8eae3"}}},
		"foreign key":         {"teams": rows["teams"], "members": {{"id": 1, "email": "ada@example.test", "age": 31, "team_id": "00000000-0000-4000-8000-000000000001"}}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidateDataset(schema, invalid); err == nil {
				t.Fatal("ValidateDataset(invalid) error = nil")
			}
		})
	}
}

func TestConstraintMigrationsAreReversibleAndSchemaValidationIsAtomic(t *testing.T) {
	initial := Schema{Version: 1, Sources: map[string]Source{
		"users": {Name: "users", Columns: []Column{{Name: "email", Type: TypeText}}},
	}}
	schema := initial.Clone()
	migration := Migration{
		Version: 2,
		Name:    "require unique email",
		Up: []Change{{Kind: ChangeAddConstraint, SourceName: "users", Constraint: Constraint{
			Name: "users_email_unique", Kind: ConstraintUnique, Columns: []string{"email"},
		}}},
		Down: []Change{{Kind: ChangeDropConstraint, SourceName: "users", Constraint: Constraint{Name: "users_email_unique"}}},
	}
	if err := Apply(&schema, migration); err != nil {
		t.Fatalf("Apply(add constraint) error = %v", err)
	}
	if got := schema.Sources["users"].Constraints; len(got) != 1 || got[0].Name != "users_email_unique" {
		t.Fatalf("applied constraints = %#v", got)
	}
	if err := Revert(&schema, migration); err != nil {
		t.Fatalf("Revert(drop constraint) error = %v", err)
	}
	if got := schema.Sources["users"].Constraints; len(got) != 0 {
		t.Fatalf("reverted constraints = %#v", got)
	}

	before := schema.Clone()
	err := Apply(&schema, Migration{
		Version: 2,
		Name:    "invalid constraint",
		Up:      []Change{{Kind: ChangeAddConstraint, SourceName: "users", Constraint: Constraint{Name: "bad", Kind: ConstraintUnique, Columns: []string{"missing"}}}},
		Down:    []Change{{Kind: ChangeDropConstraint, SourceName: "users", Constraint: Constraint{Name: "bad"}}},
	})
	if err == nil || !strings.Contains(err.Error(), "unknown column") {
		t.Fatalf("Apply(invalid constraint) error = %v, want unknown column", err)
	}
	if !reflect.DeepEqual(schema, before) {
		t.Fatalf("failed constraint migration mutated schema: got %#v, want %#v", schema, before)
	}
}

func TestConstraintMigrationsRejectRemovingReferencedSchemaObjects(t *testing.T) {
	initial := Schema{Version: 1, Sources: map[string]Source{
		"teams": {Name: "teams", Columns: []Column{{Name: "id", Type: TypeUUID}}},
		"members": {
			Name:    "members",
			Columns: []Column{{Name: "team_id", Type: TypeUUID}},
			Constraints: []Constraint{{
				Name: "members_team", Kind: ConstraintForeignKey, Columns: []string{"team_id"}, ReferenceSource: "teams", ReferenceColumns: []string{"id"},
			}},
		}}}
	for name, change := range map[string]Change{
		"source": {Kind: ChangeDropSource, SourceName: "teams"},
		"column": {Kind: ChangeDropColumn, SourceName: "teams", Column: Column{Name: "id"}},
	} {
		t.Run(name, func(t *testing.T) {
			schema := initial.Clone()
			before := schema.Clone()
			err := Apply(&schema, Migration{Version: 2, Name: "remove referenced " + name, Up: []Change{change}, Down: []Change{{Kind: ChangeCreateSource, Source: before.Sources["teams"]}}})
			if err == nil || !strings.Contains(err.Error(), "referenced") {
				t.Fatalf("Apply(remove referenced %s) error = %v, want referenced diagnostic", name, err)
			}
			if !reflect.DeepEqual(schema, before) {
				t.Fatalf("failed %s removal mutated schema: got %#v, want %#v", name, schema, before)
			}
		})
	}
}
