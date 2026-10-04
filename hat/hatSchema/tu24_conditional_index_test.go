package hatSchema

import (
	"reflect"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTU24ConditionalSpaceIndexIsDeclaredMaintainedAndUsed(t *testing.T) {
	definition := SpaceDefinition{
		Name: "jobs",
		Source: Source{
			Name: "jobs",
			Columns: []Column{
				{Name: "id", Type: TypeInteger},
				{Name: "status", Type: TypeText},
			},
		},
		Indexes: []IndexDefinition{{
			Name:    "active_id",
			Kind:    IndexKindConditional,
			Columns: []string{"id"},
			Condition: &IndexCondition{
				Field: "status",
				Value: "active",
			},
		}},
	}
	catalog, err := NewSpaceCatalog([]SpaceDefinition{definition})
	if err != nil {
		t.Fatalf("NewSpaceCatalog() error = %v", err)
	}
	cloned, ok := catalog.Lookup("jobs")
	if !ok || cloned.Indexes[0].Condition == nil || cloned.Indexes[0].Condition.Field != "status" {
		t.Fatalf("catalog conditional index = %#v, want copied condition", cloned.Indexes)
	}

	source := NewMaterializedSource([]DerivedColumn{{Name: "id"}, {Name: "status"}})
	for _, row := range []Row{
		{"id": int64(1), "status": "active"},
		{"id": int64(2), "status": "inactive"},
		{"id": int64(2), "status": "active"},
	} {
		if _, err := source.Insert(row); err != nil {
			t.Fatalf("Insert(%#v) error = %v", row, err)
		}
	}
	if _, err := source.BuildConditionalIndex(cloned.Indexes[0]); err != nil {
		t.Fatalf("BuildConditionalIndex() error = %v", err)
	}
	if got := source.ConditionalIndexStats("active_id"); got.Rows != 2 {
		t.Fatalf("conditional index stats = %#v, want two admitted rows", got)
	}
	for _, row := range []Row{
		{"id": int64(3), "status": "active"},
		{"id": int64(3), "status": "inactive"},
	} {
		if _, err := source.Insert(row); err != nil {
			t.Fatalf("post-build Insert(%#v) error = %v", row, err)
		}
	}
	if got := source.ConditionalIndexStats("active_id"); got.Rows != 3 {
		t.Fatalf("post-build conditional index stats = %#v, want three admitted rows", got)
	}

	adapter := SQLResolverAdapter{Sources: map[string]*MaterializedSource{"jobs": source}}
	candidates, available, err := adapter.ResolveSQLConditionalIndexedSource("CACHE", "jobs", []string{"id", "status"}, []interface{}{int64(2), "active"})
	if err != nil || !available || len(candidates) != 1 {
		t.Fatalf("direct conditional lookup = rows=%#v available=%t err=%v, want one candidate", candidates, available, err)
	}
	result, err := hatSql.ExecuteSQLQuery("FROM CACHE('jobs') WHERE id = 2 AND status = 'active' SELECT id, status", adapter)
	if err != nil {
		t.Fatalf("conditional indexed query error = %v", err)
	}
	want := []hatSql.Row{{"id": int64(2), "status": "active"}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("conditional indexed rows = %#v, want %#v", result.Rows, want)
	}

	result, err = hatSql.ExecuteSQLQuery("FROM CACHE('jobs') WHERE id = 2 AND status = 'inactive' SELECT id, status", adapter)
	if err != nil {
		t.Fatalf("fallback query error = %v", err)
	}
	want = []hatSql.Row{{"id": int64(2), "status": "inactive"}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("fallback rows = %#v, want %#v", result.Rows, want)
	}

	explained, err := hatSql.ExecuteSQLQuery("EXPLAIN ANALYZE FROM CACHE('jobs') WHERE id = 2 AND status = 'active' SELECT id, status", adapter)
	if err != nil {
		t.Fatalf("EXPLAIN ANALYZE error = %v", err)
	}
	usedConditional := false
	for _, step := range explained.Plan {
		if strings.Contains(step.Node, "CONDITIONAL INDEX SCAN") {
			usedConditional = true
			break
		}
	}
	if !usedConditional {
		t.Fatalf("EXPLAIN ANALYZE plan = %#v, want conditional index scan", explained.Plan)
	}

	result, err = hatSql.ExecuteSQLQuery("FROM CACHE('jobs') WHERE id = 3 AND status = 'active' SELECT id, status", adapter)
	if err != nil {
		t.Fatalf("post-build indexed query error = %v", err)
	}
	want = []hatSql.Row{{"id": int64(3), "status": "active"}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("post-build indexed rows = %#v, want %#v", result.Rows, want)
	}

	if !source.DropConditionalIndex("active_id") || source.HasConditionalIndex("active_id") {
		t.Fatal("DropConditionalIndex() did not remove active_id")
	}
	result, err = hatSql.ExecuteSQLQuery("FROM CACHE('jobs') WHERE id = 3 AND status = 'active' SELECT id, status", adapter)
	if err != nil {
		t.Fatalf("post-drop fallback query error = %v", err)
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("post-drop fallback rows = %#v, want %#v", result.Rows, want)
	}
}

func TestTU24ConditionalSpaceIndexValidation(t *testing.T) {
	base := SpaceDefinition{
		Name: "jobs",
		Source: Source{
			Name: "jobs",
			Columns: []Column{
				{Name: "id", Type: TypeInteger},
				{Name: "status", Type: TypeText},
			},
		},
	}
	cases := []struct {
		name  string
		index IndexDefinition
	}{
		{name: "missing condition", index: IndexDefinition{Name: "active_id", Kind: IndexKindConditional, Columns: []string{"id"}}},
		{name: "multiple key columns", index: IndexDefinition{Name: "active_id", Kind: IndexKindConditional, Columns: []string{"id", "status"}, Condition: &IndexCondition{Field: "status", Value: "active"}}},
		{name: "unknown condition field", index: IndexDefinition{Name: "active_id", Kind: IndexKindConditional, Columns: []string{"id"}, Condition: &IndexCondition{Field: "missing", Value: "active"}}},
		{name: "condition on key field", index: IndexDefinition{Name: "active_id", Kind: IndexKindConditional, Columns: []string{"id"}, Condition: &IndexCondition{Field: "id", Value: int64(1)}}},
		{name: "non-scalar condition", index: IndexDefinition{Name: "active_id", Kind: IndexKindConditional, Columns: []string{"id"}, Condition: &IndexCondition{Field: "status", Value: []string{"active"}}}},
		{name: "condition on regular index", index: IndexDefinition{Name: "id", Kind: IndexKindHash, Columns: []string{"id"}, Condition: &IndexCondition{Field: "status", Value: "active"}}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			definition := base
			definition.Indexes = []IndexDefinition{test.index}
			if _, err := NewSpaceCatalog([]SpaceDefinition{definition}); err == nil {
				t.Fatal("NewSpaceCatalog() error = nil, want validation error")
			}
		})
	}
}
