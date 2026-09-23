package hatSql_test

import (
	"context"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestM224HydrationReportsProgressAndEstimatedRemainingWork(t *testing.T) {
	rows := make([]hatSql.Row, 512)
	for index := range rows {
		rows[index] = hatSql.Row{"id": int64(index), "region": "sg"}
	}
	resolver := &m219PointBuildResolver{rows: rows, version: "1"}
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
	reachedHalf := make(chan struct{})
	release := make(chan struct{})
	if _, err := views.EnqueuePointLookupBuild(queue, hatSql.MaterializedViewPointLookupBuildRequest{
		ID:       "people-region-progress",
		ViewName: "people_projection",
		Fields:   []string{"region"},
		Progress: func(completed, total int) {
			if completed == 256 && total == 512 {
				close(reachedHalf)
				<-release
			}
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := queue.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-reachedHalf:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for halfway hydration progress")
	}

	state, ok := views.HydrationStatus("people_projection")
	if !ok {
		t.Fatal("hydration status missing")
	}
	if state.State != hatSql.MaterializedViewHydrationHydrating || state.Completed != 256 || state.Total != 512 {
		t.Fatalf("halfway hydration status = %#v, want hydrating 256/512", state)
	}
	if state.Progress != 0.5 {
		t.Fatalf("halfway hydration progress = %v, want 0.5", state.Progress)
	}
	if state.EstimatedRemainingSeconds <= 0 {
		t.Fatalf("halfway estimated remaining seconds = %v, want positive", state.EstimatedRemainingSeconds)
	}

	close(release)
	if err := queue.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, ok = views.HydrationStatus("people_projection")
	if !ok || state.State != hatSql.MaterializedViewHydrationReady || state.Completed != 512 || state.Total != 512 {
		t.Fatalf("completed hydration status = %#v/%v, want ready 512/512", state, ok)
	}
	if state.Progress != 1 || state.EstimatedRemainingSeconds != 0 {
		t.Fatalf("completed hydration metrics = progress %v remaining %v, want 1/0", state.Progress, state.EstimatedRemainingSeconds)
	}
}
