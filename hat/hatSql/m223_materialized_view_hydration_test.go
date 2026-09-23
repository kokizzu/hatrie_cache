package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestM223MaterializedViewHydrationStateTransitions(t *testing.T) {
	resolver := &m219PointBuildResolver{
		rows: []hatSql.Row{
			{"id": int64(1), "region": "sg"},
			{"id": int64(2), "region": "jp"},
		},
		version: "1",
	}
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "people_projection",
		Query:        "FROM CACHE('people') AS p SELECT p.id, p.region",
		Dependencies: []string{"people"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}

	state, ok := views.HydrationStatus("people_projection")
	if !ok || state.State != hatSql.MaterializedViewHydrationCold {
		t.Fatalf("initial hydration status = %#v/%v, want cold", state, ok)
	}

	queue, err := hatSql.NewSQLIndexRebuildQueue(hatSql.SQLIndexRebuildQueueOptions{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	if _, err := views.EnqueuePointLookupBuild(queue, hatSql.MaterializedViewPointLookupBuildRequest{
		ID:       "people-region-v1",
		ViewName: "people_projection",
		Fields:   []string{"region"},
	}); err != nil {
		t.Fatal(err)
	}
	state, ok = views.HydrationStatus("people_projection")
	if !ok || state.State != hatSql.MaterializedViewHydrationHydrating || state.TaskID != "people-region-v1" {
		t.Fatalf("queued hydration status = %#v/%v, want hydrating task", state, ok)
	}

	if err := queue.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := queue.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, ok = views.HydrationStatus("people_projection")
	if !ok || state.State != hatSql.MaterializedViewHydrationReady || state.TaskID != "people-region-v1" {
		t.Fatalf("completed hydration status = %#v/%v, want ready task", state, ok)
	}

	if err := views.DropPointLookupFields("people_projection", "region"); err != nil {
		t.Fatal(err)
	}
	state, ok = views.HydrationStatus("people_projection")
	if !ok || state.State != hatSql.MaterializedViewHydrationCold || state.TaskID != "" {
		t.Fatalf("post-drop hydration status = %#v/%v, want cold", state, ok)
	}
}

func TestM223MaterializedViewHydrationFailureLeavesColdState(t *testing.T) {
	resolver := &m219PointBuildResolver{
		rows:    []hatSql.Row{{"id": int64(1), "region": "sg"}},
		version: "1",
	}
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "people_projection",
		Query:        "FROM CACHE('people') AS p SELECT p.id, p.region",
		Dependencies: []string{"people"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	queue, err := hatSql.NewSQLIndexRebuildQueue(hatSql.SQLIndexRebuildQueueOptions{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	if _, err := views.EnqueuePointLookupBuild(queue, hatSql.MaterializedViewPointLookupBuildRequest{
		ID:       "people-region-stale",
		ViewName: "people_projection",
		Fields:   []string{"region"},
	}); err != nil {
		t.Fatal(err)
	}
	resolver.rows = []hatSql.Row{{"id": int64(2), "region": "sg"}}
	resolver.version = "2"
	if _, err := views.RefreshChanged(context.Background(), []string{"people"}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := queue.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := queue.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, ok := views.HydrationStatus("people_projection")
	if !ok || state.State != hatSql.MaterializedViewHydrationCold || state.TaskID != "" {
		t.Fatalf("failed hydration status = %#v/%v, want cold", state, ok)
	}
}

func TestM223DropInvalidatesQueuedHydration(t *testing.T) {
	resolver := &m219PointBuildResolver{
		rows:    []hatSql.Row{{"id": int64(1), "region": "sg"}},
		version: "1",
	}
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "people_projection",
		Query:        "FROM CACHE('people') AS p SELECT p.id, p.region",
		Dependencies: []string{"people"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	queue, err := hatSql.NewSQLIndexRebuildQueue(hatSql.SQLIndexRebuildQueueOptions{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	if _, err := views.EnqueuePointLookupBuild(queue, hatSql.MaterializedViewPointLookupBuildRequest{
		ID:       "people-region-dropped",
		ViewName: "people_projection",
		Fields:   []string{"region"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := views.DropPointLookupFields("people_projection"); err != nil {
		t.Fatal(err)
	}
	if err := queue.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := queue.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, ok := queue.Status("people-region-dropped")
	if !ok || status.State != hatSql.SQLIndexRebuildFailed {
		t.Fatalf("dropped hydration task = %#v/%v, want failed", status, ok)
	}
	state, ok := views.HydrationStatus("people_projection")
	if !ok || state.State != hatSql.MaterializedViewHydrationCold || state.TaskID != "" {
		t.Fatalf("dropped hydration state = %#v/%v, want cold", state, ok)
	}
}
