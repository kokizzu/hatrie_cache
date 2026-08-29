package hatSql_test

import (
	"context"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestManagedRefreshSchedulerRefreshesMaterializedViewsAndRollups(t *testing.T) {
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	rows := map[string][]hatSql.Row{"people": {{"name": "Ada"}}}
	resolver := hatSql.SourceResolverFunc(func(_ string, key string) ([]hatSql.Row, error) {
		return hatSql.CloneRows(rows[key]), nil
	})
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{Name: "people_view", Query: "FROM CACHE('people') SELECT name", Dependencies: []string{"people"}}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	scheduler, err := hatSql.NewManagedRefreshScheduler(hatSql.ManagedRefreshSchedulerOptions{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.AddMaterializedView("people_refresh", views, "people_view", resolver, hatSql.QueryOptions{}, time.Minute); err != nil {
		t.Fatal(err)
	}
	rollups := 0
	if err := scheduler.AddRollup("metrics_rollup", time.Minute, func(context.Context) error { rollups++; return nil }); err != nil {
		t.Fatal(err)
	}
	if runs, err := scheduler.RunDue(context.Background()); err != nil || len(runs) != 2 || rollups != 1 {
		t.Fatalf("initial RunDue() = %#v, %v, rollups=%d", runs, err, rollups)
	}
	rows["people"] = []hatSql.Row{{"name": "Lin"}}
	if runs, err := scheduler.RunDue(context.Background()); err != nil || len(runs) != 0 || rollups != 1 {
		t.Fatalf("early RunDue() = %#v, %v, rollups=%d", runs, err, rollups)
	}
	now = now.Add(time.Minute)
	if runs, err := scheduler.RunDue(context.Background()); err != nil || len(runs) != 2 || rollups != 2 {
		t.Fatalf("due RunDue() = %#v, %v, rollups=%d", runs, err, rollups)
	}
	view, ok := views.Get("people_view")
	if !ok || view.Status.Revision != 3 || view.Result.Rows[0]["name"] != "Lin" {
		t.Fatalf("view = %#v, %v", view, ok)
	}
}
