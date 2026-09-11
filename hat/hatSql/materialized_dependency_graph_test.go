package hatSql

import (
	"context"
	"testing"
)

func TestMaterializedViewsBuildDependencyInvalidationIndex(t *testing.T) {
	resolver := SourceResolverFunc(func(_ string, key string) ([]Row, error) {
		return []Row{{"value": key}}, nil
	})
	views := NewMaterializedViews()
	for _, definition := range []MaterializedViewDefinition{
		{Name: "people_view", Query: "FROM CACHE('people') SELECT value", Dependencies: []string{"people", "teams"}},
		{Name: "teams_view", Query: "FROM CACHE('teams') SELECT value", Dependencies: []string{"teams"}},
	} {
		if _, err := views.Create(context.Background(), definition, resolver, QueryOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	views.mu.RLock()
	defer views.mu.RUnlock()
	if got := len(views.dependents["people"]); got != 1 {
		t.Fatalf("people dependents = %d, want 1", got)
	}
	if got := views.dependents["people"]; len(got) != 1 || got[0] != "people_view" {
		t.Fatalf("people dependency index = %#v, want [people_view]", got)
	}
	if got := len(views.dependents["teams"]); got != 2 {
		t.Fatalf("teams dependents = %d, want 2", got)
	}
}

func TestMaterializedViewsDependencyInvalidationDeduplicatesOverlappingChanges(t *testing.T) {
	resolver := SourceResolverFunc(func(_ string, key string) ([]Row, error) {
		return []Row{{"value": key}}, nil
	})
	views := NewMaterializedViews()
	if _, err := views.Create(context.Background(), MaterializedViewDefinition{
		Name:         "combined_view",
		Query:        "FROM CACHE('people') SELECT value",
		Dependencies: []string{"people", "teams"},
	}, resolver, QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	statuses, err := views.RefreshChanged(context.Background(), []string{"people", "teams", "people"}, resolver, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(statuses) != 1 || statuses[0].Name != "combined_view" || statuses[0].Revision != 2 {
		t.Fatalf("RefreshChanged() = %#v, want one revision-2 status", statuses)
	}
}
