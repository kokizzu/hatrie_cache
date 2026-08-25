package hatSql_test

import (
	"context"
	"fmt"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestMaterializedViewsRefreshOnlyChangedDependencies(t *testing.T) {
	rows := map[string][]hatSql.Row{"people": {{"name": "Ada"}}, "teams": {{"name": "Core"}}}
	resolver := hatSql.SourceResolverFunc(func(_ string, key string) ([]hatSql.Row, error) { return hatSql.CloneRows(rows[key]), nil })
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{Name: "people_view", Query: "FROM CACHE('people') SELECT name", Dependencies: []string{"people"}}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{Name: "teams_view", Query: "FROM CACHE('teams') SELECT name", Dependencies: []string{"teams"}}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	rows["people"] = []hatSql.Row{{"name": "Lin"}}
	statuses, err := views.RefreshChanged(context.Background(), []string{"people"}, resolver, hatSql.QueryOptions{})
	if err != nil || len(statuses) != 1 || statuses[0].Name != "people_view" || statuses[0].Revision != 2 {
		t.Fatalf("RefreshChanged() = %#v, %v", statuses, err)
	}
	people, ok := views.Get("people_view")
	if !ok || people.Result.Rows[0]["name"] != "Lin" {
		t.Fatalf("people view = %#v", people)
	}
	teams, ok := views.Get("teams_view")
	if !ok || teams.Status.Revision != 1 || teams.Result.Rows[0]["name"] != "Core" {
		t.Fatalf("teams view = %#v", teams)
	}
}

func TestMaterializedViewsKeepSnapshotsWhenRefreshFails(t *testing.T) {
	rows := map[string][]hatSql.Row{"people": {{"name": "Ada"}}, "teams": {{"name": "Core"}}}
	resolver := hatSql.SourceResolverFunc(func(_ string, key string) ([]hatSql.Row, error) {
		if _, ok := rows[key]; !ok {
			return nil, fmt.Errorf("source %q unavailable", key)
		}
		return hatSql.CloneRows(rows[key]), nil
	})
	views := hatSql.NewMaterializedViews()
	for _, definition := range []hatSql.MaterializedViewDefinition{
		{Name: "people_view", Query: "FROM CACHE('people') SELECT name", Dependencies: []string{"people"}},
		{Name: "teams_view", Query: "FROM CACHE('teams') SELECT name", Dependencies: []string{"teams"}},
	} {
		if _, err := views.Create(context.Background(), definition, resolver, hatSql.QueryOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	rows["people"] = []hatSql.Row{{"name": "Lin"}}
	delete(rows, "teams")
	if _, err := views.RefreshChanged(context.Background(), []string{"people", "teams"}, resolver, hatSql.QueryOptions{}); err == nil {
		t.Fatal("RefreshChanged() error = nil, want missing source error")
	}
	people, ok := views.Get("people_view")
	if !ok || people.Status.Revision != 1 || people.Result.Rows[0]["name"] != "Ada" {
		t.Fatalf("people view after failed refresh = %#v", people)
	}
	teams, ok := views.Get("teams_view")
	if !ok || teams.Status.Revision != 1 || teams.Result.Rows[0]["name"] != "Core" {
		t.Fatalf("teams view after failed refresh = %#v", teams)
	}
}
