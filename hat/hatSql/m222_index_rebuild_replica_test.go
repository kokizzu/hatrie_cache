package hatSql_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestM222IndexRebuildReplicaSetSurvivesOneWorkerFailure(t *testing.T) {
	first, err := hatSql.NewSQLIndexRebuildQueue(hatSql.SQLIndexRebuildQueueOptions{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := hatSql.NewSQLIndexRebuildQueue(hatSql.SQLIndexRebuildQueueOptions{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	replicas, err := hatSql.NewSQLIndexRebuildReplicaSet(hatSql.SQLIndexRebuildReplicaSetOptions{
		Queues: []*hatSql.SQLIndexRebuildQueue{first, second},
		Quorum: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	var attempts atomic.Int32
	queued, err := replicas.Enqueue(hatSql.SQLIndexRebuildRequest{
		ID:   "people-index-v1",
		Name: "people_projection",
		Run: func(context.Context, hatSql.SQLIndexRebuildProgressFunc) error {
			if attempts.Add(1) == 1 {
				return errors.New("replica worker failed")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if queued.State != hatSql.SQLIndexRebuildQueued || queued.Successes != 0 {
		t.Fatalf("queued replica status = %#v, want queued with no successes", queued)
	}
	if err := replicas.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := replicas.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, ok := replicas.Status("people-index-v1")
	if !ok || status.State != hatSql.SQLIndexRebuildSucceeded || status.Successes != 1 || attempts.Load() != 2 {
		t.Fatalf("replica status = %#v/%v attempts=%d, want quorum success after two workers", status, ok, attempts.Load())
	}
}

func TestM222MaterializedViewReplicatedPointLookupBuildPublishesOnAllWorkers(t *testing.T) {
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
	first, err := hatSql.NewSQLIndexRebuildQueue(hatSql.SQLIndexRebuildQueueOptions{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := hatSql.NewSQLIndexRebuildQueue(hatSql.SQLIndexRebuildQueueOptions{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	replicas, err := hatSql.NewSQLIndexRebuildReplicaSet(hatSql.SQLIndexRebuildReplicaSetOptions{
		Queues: []*hatSql.SQLIndexRebuildQueue{first, second},
		Quorum: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	queued, err := views.EnqueueReplicatedPointLookupBuild(replicas, hatSql.MaterializedViewPointLookupBuildRequest{
		ID:       "people-region-replicated",
		ViewName: "people_projection",
		Fields:   []string{"region"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if queued.State != hatSql.SQLIndexRebuildQueued {
		t.Fatalf("queued materialized replica status = %#v, want queued", queued)
	}
	hydration, ok := views.HydrationStatus("people_projection")
	if !ok || hydration.State != hatSql.MaterializedViewHydrationHydrating {
		t.Fatalf("queued hydration status = %#v/%v, want hydrating", hydration, ok)
	}
	if err := replicas.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := replicas.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, ok := replicas.Status("people-region-replicated")
	if !ok || status.State != hatSql.SQLIndexRebuildSucceeded || status.Successes != 2 {
		t.Fatalf("replicated materialized status = %#v/%v, want two successes", status, ok)
	}
	hydration, ok = views.HydrationStatus("people_projection")
	if !ok || hydration.State != hatSql.MaterializedViewHydrationReady {
		t.Fatalf("completed hydration status = %#v/%v, want ready", hydration, ok)
	}
	rows, available, err := views.PointLookup("people_projection", "region", "sg")
	if err != nil || !available || len(rows) != 1 {
		t.Fatalf("replicated PointLookup() = %#v/%v/%v, want one row", rows, available, err)
	}
}

func TestM222ReplicaSetFlushSkipsWorkerThatCouldNotStart(t *testing.T) {
	disabled, err := hatSql.NewSQLIndexRebuildQueue(hatSql.SQLIndexRebuildQueueOptions{Workers: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer disabled.Close()
	healthy, err := hatSql.NewSQLIndexRebuildQueue(hatSql.SQLIndexRebuildQueueOptions{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer healthy.Close()
	replicas, err := hatSql.NewSQLIndexRebuildReplicaSet(hatSql.SQLIndexRebuildReplicaSetOptions{
		Queues: []*hatSql.SQLIndexRebuildQueue{disabled, healthy},
		Quorum: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := replicas.Enqueue(hatSql.SQLIndexRebuildRequest{
		ID:   "worker-start-failover",
		Name: "people_projection",
		Run:  func(context.Context, hatSql.SQLIndexRebuildProgressFunc) error { return nil },
	}); err != nil {
		t.Fatal(err)
	}
	if err := replicas.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := replicas.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, ok := replicas.Status("worker-start-failover")
	if !ok || status.State != hatSql.SQLIndexRebuildSucceeded || status.Successes != 1 {
		t.Fatalf("failover status = %#v/%v, want one healthy success", status, ok)
	}
}

func TestM222ReplicaSetCancelsPartialAcceptanceWhenQuorumUnavailable(t *testing.T) {
	disabled, err := hatSql.NewSQLIndexRebuildQueue(hatSql.SQLIndexRebuildQueueOptions{Workers: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer disabled.Close()
	healthy, err := hatSql.NewSQLIndexRebuildQueue(hatSql.SQLIndexRebuildQueueOptions{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer healthy.Close()
	replicas, err := hatSql.NewSQLIndexRebuildReplicaSet(hatSql.SQLIndexRebuildReplicaSetOptions{
		Queues: []*hatSql.SQLIndexRebuildQueue{disabled, healthy},
		Quorum: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer replicas.Close()
	if err := replicas.Start(context.Background()); !errors.Is(err, hatSql.ErrSQLIndexRebuildReplicaQuorumUnavailable) {
		t.Fatalf("start error = %v, want quorum unavailable", err)
	}

	_, err = replicas.Enqueue(hatSql.SQLIndexRebuildRequest{
		ID:   "partial-acceptance",
		Name: "partial-acceptance",
		Run: func(ctx context.Context, _ hatSql.SQLIndexRebuildProgressFunc) error {
			<-ctx.Done()
			return ctx.Err()
		},
	})
	if !errors.Is(err, hatSql.ErrSQLIndexRebuildReplicaQuorumUnavailable) {
		t.Fatalf("enqueue error = %v, want quorum unavailable", err)
	}

	flushContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := replicas.Flush(flushContext); err != nil {
		t.Fatalf("flush after rejected partial acceptance = %v", err)
	}
	status, ok := healthy.Status("partial-acceptance#replica-1")
	if !ok {
		t.Fatal("healthy partial-acceptance replica status missing")
	}
	if status.State != hatSql.SQLIndexRebuildCanceled {
		t.Fatalf("healthy partial-acceptance state = %s, want canceled", status.State)
	}
}
