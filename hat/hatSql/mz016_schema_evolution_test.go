package hatSql_test

import (
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestMZ016SchemaEvolutionAdaptsAdditiveSourceColumns(t *testing.T) {
	current := []hatSql.SQLRowBinaryColumn{
		{Name: "id", Type: hatSql.SQLRowBinaryInt64},
		{Name: "name", Type: hatSql.SQLRowBinaryString, Nullable: true},
		{Name: "region", Type: hatSql.SQLRowBinaryString, Nullable: true},
	}
	expected := []hatSql.SQLRowBinaryColumn{
		{Name: "id", Type: hatSql.SQLRowBinaryInt64},
		{Name: "name", Type: hatSql.SQLRowBinaryString, Nullable: true},
	}
	plan, err := hatSql.NewSQLSchemaEvolutionPlan(current, expected, hatSql.SQLSchemaEvolutionOptions{
		AllowAddedColumns: true,
		CurrentVersion:    "v2",
		ExpectedVersion:   "v1",
	})
	if err != nil {
		t.Fatalf("NewSQLSchemaEvolutionPlan() error = %v", err)
	}
	if plan.CurrentVersion() != "v2" || plan.ExpectedVersion() != "v1" {
		t.Fatalf("versions = %q/%q, want v2/v1", plan.CurrentVersion(), plan.ExpectedVersion())
	}
	changes := plan.Changes()
	if len(changes) != 1 || changes[0].Column != "region" || changes[0].Kind != hatSql.SQLSchemaEvolutionAddedColumn {
		t.Fatalf("changes = %#v, want one added region column", changes)
	}

	got, err := plan.AdaptRow(hatSql.Row{"id": int64(7), "name": "Ada", "region": "apac"})
	if err != nil {
		t.Fatalf("AdaptRow() error = %v", err)
	}
	want := hatSql.Row{"id": int64(7), "name": "Ada"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("AdaptRow() = %#v, want %#v", got, want)
	}
}

func TestMZ016SchemaEvolutionFillsDroppedNullableColumn(t *testing.T) {
	current := []hatSql.SQLRowBinaryColumn{{Name: "id", Type: hatSql.SQLRowBinaryInt64}}
	expected := []hatSql.SQLRowBinaryColumn{
		{Name: "id", Type: hatSql.SQLRowBinaryInt64},
		{Name: "email", Type: hatSql.SQLRowBinaryString, Nullable: true},
	}
	plan, err := hatSql.NewSQLSchemaEvolutionPlan(current, expected, hatSql.SQLSchemaEvolutionOptions{AllowDroppedColumns: true})
	if err != nil {
		t.Fatalf("NewSQLSchemaEvolutionPlan() error = %v", err)
	}
	got, err := plan.AdaptRows([]hatSql.Row{{"id": int64(3)}, {"id": int64(4)}})
	if err != nil {
		t.Fatalf("AdaptRows() error = %v", err)
	}
	want := []hatSql.Row{{"id": int64(3), "email": nil}, {"id": int64(4), "email": nil}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("AdaptRows() = %#v, want %#v", got, want)
	}

	_, err = hatSql.NewSQLSchemaEvolutionPlan(current, []hatSql.SQLRowBinaryColumn{
		{Name: "id", Type: hatSql.SQLRowBinaryInt64},
		{Name: "email", Type: hatSql.SQLRowBinaryString},
	}, hatSql.SQLSchemaEvolutionOptions{AllowDroppedColumns: true})
	if !errors.Is(err, hatSql.ErrSQLSchemaEvolutionIncompatible) {
		t.Fatalf("non-nullable dropped column error = %v, want %v", err, hatSql.ErrSQLSchemaEvolutionIncompatible)
	}
}

func TestMZ016SchemaEvolutionRejectsUnsafeChanges(t *testing.T) {
	current := []hatSql.SQLRowBinaryColumn{{Name: "id", Type: hatSql.SQLRowBinaryString}}
	expected := []hatSql.SQLRowBinaryColumn{{Name: "id", Type: hatSql.SQLRowBinaryInt64}}
	if _, err := hatSql.NewSQLSchemaEvolutionPlan(current, expected, hatSql.SQLSchemaEvolutionOptions{}); !errors.Is(err, hatSql.ErrSQLSchemaEvolutionIncompatible) {
		t.Fatalf("type change error = %v, want %v", err, hatSql.ErrSQLSchemaEvolutionIncompatible)
	}

	current = []hatSql.SQLRowBinaryColumn{{Name: "id", Type: hatSql.SQLRowBinaryInt64, Nullable: true}}
	expected = []hatSql.SQLRowBinaryColumn{{Name: "id", Type: hatSql.SQLRowBinaryInt64}}
	if _, err := hatSql.NewSQLSchemaEvolutionPlan(current, expected, hatSql.SQLSchemaEvolutionOptions{}); !errors.Is(err, hatSql.ErrSQLSchemaEvolutionIncompatible) {
		t.Fatalf("nullability change error = %v, want %v", err, hatSql.ErrSQLSchemaEvolutionIncompatible)
	}

	current = []hatSql.SQLRowBinaryColumn{
		{Name: "id", Type: hatSql.SQLRowBinaryInt64},
		{Name: "extra", Type: hatSql.SQLRowBinaryInt64},
	}
	if _, err := hatSql.NewSQLSchemaEvolutionPlan(current, []hatSql.SQLRowBinaryColumn{{Name: "id", Type: hatSql.SQLRowBinaryInt64}}, hatSql.SQLSchemaEvolutionOptions{}); !errors.Is(err, hatSql.ErrSQLSchemaEvolutionIncompatible) {
		t.Fatalf("unexpected additive column error = %v, want %v", err, hatSql.ErrSQLSchemaEvolutionIncompatible)
	}
}

func TestMZ016SchemaEvolutionRejectsInvalidRows(t *testing.T) {
	plan, err := hatSql.NewSQLSchemaEvolutionPlan(
		[]hatSql.SQLRowBinaryColumn{{Name: "id", Type: hatSql.SQLRowBinaryInt64}},
		[]hatSql.SQLRowBinaryColumn{{Name: "id", Type: hatSql.SQLRowBinaryInt64}},
		hatSql.SQLSchemaEvolutionOptions{},
	)
	if err != nil {
		t.Fatalf("NewSQLSchemaEvolutionPlan() error = %v", err)
	}
	if _, err := plan.AdaptRow(hatSql.Row{}); !errors.Is(err, hatSql.ErrSQLSchemaEvolutionRowInvalid) {
		t.Fatalf("missing required row value error = %v, want %v", err, hatSql.ErrSQLSchemaEvolutionRowInvalid)
	}

	plan, err = hatSql.NewSQLSchemaEvolutionPlan(
		[]hatSql.SQLRowBinaryColumn{{Name: "id", Type: hatSql.SQLRowBinaryInt64}},
		[]hatSql.SQLRowBinaryColumn{{Name: "id", Type: hatSql.SQLRowBinaryInt64, Nullable: true}},
		hatSql.SQLSchemaEvolutionOptions{},
	)
	if err != nil {
		t.Fatalf("nullable expected plan error = %v", err)
	}
	if _, err := plan.AdaptRow(hatSql.Row{"id": nil}); !errors.Is(err, hatSql.ErrSQLSchemaEvolutionRowInvalid) {
		t.Fatalf("invalid current-schema NULL error = %v, want %v", err, hatSql.ErrSQLSchemaEvolutionRowInvalid)
	}
}
