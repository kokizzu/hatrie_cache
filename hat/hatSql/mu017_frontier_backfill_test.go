package hatSql_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"hatrie_cache/hat/hatSql"
)

type mu017SnapshotResolver struct {
	mu               sync.Mutex
	rows             []hatSql.Row
	snapshotFrontier uint64
	snapshotErr      error
	beginCalls       int
	releaseCalls     int
}

func (resolver *mu017SnapshotResolver) ResolveSQLSource(_ string, key string) ([]hatSql.Row, error) {
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	if key != "people" {
		return nil, nil
	}
	return hatSql.CloneRows(resolver.rows), nil
}

func (resolver *mu017SnapshotResolver) BeginSQLSnapshotAt(_ context.Context, frontier uint64) (hatSql.SQLSourceResolver, func(), error) {
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	resolver.beginCalls++
	if resolver.snapshotErr != nil {
		return nil, nil, resolver.snapshotErr
	}
	if frontier != resolver.snapshotFrontier {
		return nil, nil, errors.New("unexpected snapshot frontier")
	}
	rows := hatSql.CloneRows(resolver.rows)
	return hatSql.SourceResolverFunc(func(_ string, key string) ([]hatSql.Row, error) {
			if key != "people" {
				return nil, nil
			}
			return hatSql.CloneRows(rows), nil
		}), func() {
			resolver.mu.Lock()
			resolver.releaseCalls++
			resolver.mu.Unlock()
		}, nil
}

func (resolver *mu017SnapshotResolver) counts() (int, int) {
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	return resolver.beginCalls, resolver.releaseCalls
}

type mu017CheckpointStore struct {
	mu      sync.Mutex
	saved   uint64
	saveErr error
}

func (store *mu017CheckpointStore) LoadProjectionCheckpoint(context.Context, string) (uint64, bool, error) {
	return 0, false, nil
}

func (store *mu017CheckpointStore) SaveProjectionCheckpoint(_ context.Context, _ string, sequence uint64) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.saved = sequence
	return store.saveErr
}

func TestIncrementalProjectionBackfillAtFrontierHandoffsWithoutGap(t *testing.T) {
	resolver := &mu017SnapshotResolver{rows: []hatSql.Row{{"name": "Ada"}}, snapshotFrontier: 7}
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "people_view",
		Query:        "FROM CACHE('people') SELECT name",
		Dependencies: []string{"people"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatalf("create view: %v", err)
	}
	runner, err := hatSql.NewIncrementalProjectionRunner(views, resolver, hatSql.QueryOptions{}, hatSql.IncrementalProjectionRunnerOptions{
		Name:    "people_projection",
		Enabled: true,
	})
	if err != nil {
		t.Fatalf("NewIncrementalProjectionRunner() error = %v", err)
	}
	barrier, err := hatSql.NewSQLSourceFrontierBarrierFromPartitions([]hatSql.SQLSourceFrontierPartition{{Source: "people", Partition: "0"}})
	if err != nil {
		t.Fatalf("NewSQLSourceFrontierBarrierFromPartitions() error = %v", err)
	}
	if _, err := barrier.Observe(hatSql.SQLSourceFrontier{Source: "people", Partition: "0", Frontier: 7}); err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	resolver.mu.Lock()
	resolver.rows = []hatSql.Row{{"name": "AtFrontier"}}
	resolver.mu.Unlock()

	run, err := runner.BackfillAtFrontier(context.Background(), []string{"people"}, barrier, 7)
	if err != nil {
		t.Fatalf("BackfillAtFrontier() error = %v", err)
	}
	if !run.Enabled || run.FromSequence != 0 || run.ThroughSequence != 7 || run.Changes != 0 || len(run.Refreshed) != 1 {
		t.Fatalf("backfill run = %#v", run)
	}
	if got := runner.Checkpoint(); got != 7 {
		t.Fatalf("checkpoint after backfill = %d, want 7", got)
	}
	view, ok := views.Get("people_view")
	if !ok || len(view.Result.Rows) != 1 || view.Result.Rows[0]["name"] != "AtFrontier" {
		t.Fatalf("backfilled view = %#v, want snapshot value", view)
	}
	if begin, release := resolver.counts(); begin != 1 || release != 1 {
		t.Fatalf("snapshot lifecycle = %d/%d, want 1/1", begin, release)
	}

	resolver.mu.Lock()
	resolver.rows = []hatSql.Row{{"name": "After"}}
	resolver.mu.Unlock()
	if _, err := runner.Apply(context.Background(), []hatSql.ProjectionChange{{Sequence: 8, Dependency: "people"}}); err != nil {
		t.Fatalf("Apply() after handoff error = %v", err)
	}
	view, ok = views.Get("people_view")
	if !ok || len(view.Result.Rows) != 1 || view.Result.Rows[0]["name"] != "After" {
		t.Fatalf("post-handoff view = %#v, want live value", view)
	}
}

func TestIncrementalProjectionBackfillFailureDoesNotAdvanceCheckpoint(t *testing.T) {
	resolver := &mu017SnapshotResolver{rows: []hatSql.Row{{"name": "Ada"}}, snapshotFrontier: 4, snapshotErr: errors.New("snapshot unavailable")}
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "people_view",
		Query:        "FROM CACHE('people') SELECT name",
		Dependencies: []string{"people"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatalf("create view: %v", err)
	}
	runner, err := hatSql.NewIncrementalProjectionRunner(views, resolver, hatSql.QueryOptions{}, hatSql.IncrementalProjectionRunnerOptions{Name: "people_projection", Enabled: true})
	if err != nil {
		t.Fatalf("NewIncrementalProjectionRunner() error = %v", err)
	}
	barrier, err := hatSql.NewSQLSourceFrontierBarrierFromPartitions([]hatSql.SQLSourceFrontierPartition{{Source: "people", Partition: "0"}})
	if err != nil {
		t.Fatalf("NewSQLSourceFrontierBarrierFromPartitions() error = %v", err)
	}
	if _, err := barrier.Observe(hatSql.SQLSourceFrontier{Source: "people", Partition: "0", Frontier: 4}); err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if _, err := runner.BackfillAtFrontier(context.Background(), []string{"people"}, barrier, 4); err == nil {
		t.Fatal("BackfillAtFrontier() error = nil, want snapshot failure")
	}
	if got := runner.Checkpoint(); got != 0 {
		t.Fatalf("checkpoint after failed backfill = %d, want 0", got)
	}
	if begin, release := resolver.counts(); begin != 1 || release != 0 {
		t.Fatalf("failed snapshot lifecycle = %d/%d, want 1/0", begin, release)
	}
}

func TestIncrementalProjectionBackfillCheckpointFailureKeepsReplayBoundary(t *testing.T) {
	resolver := &mu017SnapshotResolver{rows: []hatSql.Row{{"name": "AtFrontier"}}, snapshotFrontier: 4}
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "people_view",
		Query:        "FROM CACHE('people') SELECT name",
		Dependencies: []string{"people"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatalf("create view: %v", err)
	}
	store := &mu017CheckpointStore{saveErr: errors.New("checkpoint disk full")}
	runner, err := hatSql.NewIncrementalProjectionRunner(views, resolver, hatSql.QueryOptions{}, hatSql.IncrementalProjectionRunnerOptions{
		Name:            "people_projection",
		Enabled:         true,
		CheckpointStore: store,
	})
	if err != nil {
		t.Fatalf("NewIncrementalProjectionRunner() error = %v", err)
	}
	barrier, err := hatSql.NewSQLSourceFrontierBarrierFromPartitions([]hatSql.SQLSourceFrontierPartition{{Source: "people", Partition: "0"}})
	if err != nil {
		t.Fatalf("NewSQLSourceFrontierBarrierFromPartitions() error = %v", err)
	}
	if _, err := barrier.Observe(hatSql.SQLSourceFrontier{Source: "people", Partition: "0", Frontier: 4}); err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if _, err := runner.BackfillAtFrontier(context.Background(), []string{"people"}, barrier, 4); err == nil {
		t.Fatal("BackfillAtFrontier() error = nil, want checkpoint failure")
	}
	if got := runner.Checkpoint(); got != 0 {
		t.Fatalf("checkpoint after save failure = %d, want 0", got)
	}
	if store.saved != 4 {
		t.Fatalf("saved checkpoint = %d, want attempted frontier 4", store.saved)
	}
	if begin, release := resolver.counts(); begin != 1 || release != 1 {
		t.Fatalf("checkpoint failure snapshot lifecycle = %d/%d, want 1/1", begin, release)
	}
}
