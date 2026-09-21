package hatSql_test

import (
	"context"
	"fmt"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestM217MaterializedViewPointLookupIndexMaintainsCompleteRows(t *testing.T) {
	rows := map[string][]hatSql.Row{
		"people": {
			{"id": "u1", "name": "Ada", "team": "core"},
			{"id": "u2", "name": "Lin", "team": "core"},
			{"id": "u3", "name": "Kai", "team": "edge"},
		},
	}
	resolver := hatSql.SourceResolverFunc(func(_ string, key string) ([]hatSql.Row, error) {
		return hatSql.CloneRows(rows[key]), nil
	})
	views := hatSql.NewMaterializedViews()
	definition := hatSql.MaterializedViewDefinition{
		Name:         "people_view",
		Query:        "FROM CACHE('people') SELECT id, name, team",
		Dependencies: []string{"people"},
	}
	if _, err := views.Create(context.Background(), definition, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := views.CreatePointLookupIndex(hatSql.MaterializedViewPointLookupDefinition{
		Name:     "people_by_id",
		ViewName: definition.Name,
		Key: func(row hatSql.Row) (string, error) {
			id, ok := row["id"].(string)
			if !ok || id == "" {
				return "", fmt.Errorf("id is required")
			}
			return id, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := views.CreatePointLookupIndex(hatSql.MaterializedViewPointLookupDefinition{
		Name:     "people_by_team",
		ViewName: definition.Name,
		Key: func(row hatSql.Row) (string, error) {
			team, ok := row["team"].(string)
			if !ok || team == "" {
				return "", fmt.Errorf("team is required")
			}
			return team, nil
		},
	}); err != nil {
		t.Fatal(err)
	}

	result, found, err := views.LookupPoint("people_by_id", "u2")
	if err != nil || !found || len(result.Rows) != 1 || result.Rows[0]["name"] != "Lin" {
		t.Fatalf("initial point lookup = %#v, %v, %v", result, found, err)
	}
	result.Rows[0]["name"] = "mutated"
	unchanged, found, err := views.LookupPoint("people_by_id", "u2")
	if err != nil || !found || unchanged.Rows[0]["name"] != "Lin" {
		t.Fatalf("point lookup leaked mutable row = %#v, %v, %v", unchanged, found, err)
	}
	teamResult, found, err := views.LookupPoint("people_by_team", "core")
	if err != nil || !found || len(teamResult.Rows) != 2 {
		t.Fatalf("duplicate-key point lookup = %#v, %v, %v", teamResult, found, err)
	}

	rows["people"] = []hatSql.Row{
		{"id": "u2", "name": "Lin updated", "team": "core"},
		{"id": "u4", "name": "Mina", "team": "edge"},
	}
	if _, err := views.RefreshChanged(context.Background(), []string{"people"}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	updated, found, err := views.LookupPoint("people_by_id", "u2")
	if err != nil || !found || updated.Rows[0]["name"] != "Lin updated" {
		t.Fatalf("refreshed point lookup = %#v, %v, %v", updated, found, err)
	}
	if _, found, err := views.LookupPoint("people_by_id", "u1"); err != nil || found {
		t.Fatalf("removed point lookup = found %v, err %v; want false, nil", found, err)
	}

	rows["people"] = nil
	if _, err := views.RefreshChanged(context.Background(), []string{"people"}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := views.LookupPoint("people_by_id", "u2"); err != nil || found {
		t.Fatalf("empty refreshed point lookup = found %v, err %v; want old snapshot replaced", found, err)
	}
}

func TestM217MaterializedViewPointLookupRefreshKeyErrorIsAtomic(t *testing.T) {
	rows := map[string][]hatSql.Row{"people": {{"id": "u1", "value": "before"}}}
	resolver := hatSql.SourceResolverFunc(func(_ string, key string) ([]hatSql.Row, error) {
		return hatSql.CloneRows(rows[key]), nil
	})
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "people_view",
		Query:        "FROM CACHE('people') SELECT id, value",
		Dependencies: []string{"people"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := views.CreatePointLookupIndex(hatSql.MaterializedViewPointLookupDefinition{
		Name:     "people_by_id",
		ViewName: "people_view",
		Key: func(row hatSql.Row) (string, error) {
			id, ok := row["id"].(string)
			if !ok || id == "" {
				return "", fmt.Errorf("id is required")
			}
			return id, nil
		},
	}); err != nil {
		t.Fatal(err)
	}

	rows["people"] = []hatSql.Row{{"value": "invalid"}}
	if _, err := views.RefreshChanged(context.Background(), []string{"people"}, resolver, hatSql.QueryOptions{}); err == nil {
		t.Fatal("RefreshChanged() error = nil, want point lookup key error")
	}
	view, ok := views.Get("people_view")
	if !ok || view.Status.Revision != 1 || view.Result.Rows[0]["value"] != "before" {
		t.Fatalf("view after failed indexed refresh = %#v, %v", view, ok)
	}
	result, found, err := views.LookupPoint("people_by_id", "u1")
	if err != nil || !found || result.Rows[0]["value"] != "before" {
		t.Fatalf("index after failed refresh = %#v, %v, %v", result, found, err)
	}
}
