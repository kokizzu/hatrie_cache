package hatSql_test

import (
	"context"
	"errors"
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

func TestManagedRefreshSchedulerReportsFreshnessSLA(t *testing.T) {
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	scheduler, err := hatSql.NewManagedRefreshScheduler(hatSql.ManagedRefreshSchedulerOptions{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.AddRollupWithOptions("metrics", hatSql.ManagedRefreshTaskOptions{
		Every:        time.Minute,
		MaxStaleness: 2 * time.Minute,
	}, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	status, ok := scheduler.StatusAt("metrics", now)
	if !ok || !status.Stale || !status.LastSuccessAt.IsZero() {
		t.Fatalf("initial status = %#v, %v, want stale without success", status, ok)
	}
	if _, err := scheduler.RunDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, ok = scheduler.StatusAt("metrics", now)
	if !ok || status.Stale || status.LastSuccessAt.IsZero() || status.LastError != "" {
		t.Fatalf("fresh status = %#v, %v, want successful fresh task", status, ok)
	}
	now = now.Add(2 * time.Minute)
	status, ok = scheduler.StatusAt("metrics", now)
	if !ok || !status.Stale {
		t.Fatalf("expired status = %#v, %v, want stale at SLA boundary", status, ok)
	}
}

func TestManagedRefreshSchedulerStatusRetainsLastSuccessAfterFailure(t *testing.T) {
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	attempts := 0
	scheduler, err := hatSql.NewManagedRefreshScheduler(hatSql.ManagedRefreshSchedulerOptions{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.AddRollupWithOptions("metrics", hatSql.ManagedRefreshTaskOptions{
		Every:        time.Minute,
		MaxStaleness: 5 * time.Minute,
	}, func(context.Context) error {
		attempts++
		if attempts > 1 {
			return errors.New("refresh failed")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := scheduler.RunDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	first, ok := scheduler.Status("metrics")
	if !ok || first.LastSuccessAt.IsZero() {
		t.Fatalf("first status = %#v, %v, want successful refresh", first, ok)
	}
	now = now.Add(time.Minute)
	if _, err := scheduler.RunDue(context.Background()); err == nil {
		t.Fatal("failed refresh returned nil error")
	}
	second, ok := scheduler.Status("metrics")
	if !ok || second.LastSuccessAt != first.LastSuccessAt || second.LastError != "refresh failed" || second.Stale {
		t.Fatalf("failed status = %#v, %v, want retained fresh success and error", second, ok)
	}
}
