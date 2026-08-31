package hatSql_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"hatrie_cache/hat/hatSql"
)

type memoryProjectionCheckpointStore struct {
	checkpoints map[string]uint64
	saveErr     error
}

func TestFileProjectionCheckpointStoreRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "projection-checkpoints.json")
	store, err := hatSql.NewFileProjectionCheckpointStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if sequence, found, err := store.LoadProjectionCheckpoint(context.Background(), "people"); err != nil || found || sequence != 0 {
		t.Fatalf("initial LoadProjectionCheckpoint() = %d, %t, %v", sequence, found, err)
	}
	if err := store.SaveProjectionCheckpoint(context.Background(), "people", 7); err != nil {
		t.Fatal(err)
	}
	reopened, err := hatSql.NewFileProjectionCheckpointStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if sequence, found, err := reopened.LoadProjectionCheckpoint(context.Background(), "people"); err != nil || !found || sequence != 7 {
		t.Fatalf("reopened LoadProjectionCheckpoint() = %d, %t, %v", sequence, found, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if permissions := info.Mode().Perm(); permissions != 0o600 {
		t.Fatalf("checkpoint permissions = %o, want 600", permissions)
	}
}

func TestFileProjectionCheckpointStoreRejectsSymlink(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "projection-checkpoints.json")
	target := filepath.Join(directory, "target.json")
	if err := os.WriteFile(target, []byte(`{"people":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	store, err := hatSql.NewFileProjectionCheckpointStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.LoadProjectionCheckpoint(context.Background(), "people"); err == nil {
		t.Fatal("LoadProjectionCheckpoint() error = nil, want symlink rejection")
	}
	if err := store.SaveProjectionCheckpoint(context.Background(), "people", 2); err == nil {
		t.Fatal("SaveProjectionCheckpoint() error = nil, want symlink rejection")
	}
}

func TestFileProjectionCheckpointStoreConcurrentSavesPreserveNames(t *testing.T) {
	store, err := hatSql.NewFileProjectionCheckpointStore(filepath.Join(t.TempDir(), "projection-checkpoints.json"))
	if err != nil {
		t.Fatal(err)
	}
	const projections = 16
	errs := make(chan error, projections)
	var wait sync.WaitGroup
	for index := 0; index < projections; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			errs <- store.SaveProjectionCheckpoint(context.Background(), fmt.Sprintf("view-%d", index), uint64(index+1))
		}(index)
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for index := 0; index < projections; index++ {
		sequence, found, err := store.LoadProjectionCheckpoint(context.Background(), fmt.Sprintf("view-%d", index))
		if err != nil || !found || sequence != uint64(index+1) {
			t.Fatalf("checkpoint %d = %d, %t, %v", index, sequence, found, err)
		}
	}
}

func (store *memoryProjectionCheckpointStore) LoadProjectionCheckpoint(_ context.Context, name string) (uint64, bool, error) {
	sequence, ok := store.checkpoints[name]
	return sequence, ok, nil
}

func (store *memoryProjectionCheckpointStore) SaveProjectionCheckpoint(_ context.Context, name string, sequence uint64) error {
	if store.saveErr != nil {
		return store.saveErr
	}
	if store.checkpoints == nil {
		store.checkpoints = map[string]uint64{}
	}
	store.checkpoints[name] = sequence
	return nil
}

func TestIncrementalProjectionRunnerDefaultsOff(t *testing.T) {
	rows := map[string][]hatSql.Row{"people": {{"name": "Ada"}}}
	resolver := hatSql.SourceResolverFunc(func(_ string, key string) ([]hatSql.Row, error) {
		return hatSql.CloneRows(rows[key]), nil
	})
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{Name: "people_view", Query: "FROM CACHE('people') SELECT name", Dependencies: []string{"people"}}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	runner, err := hatSql.NewIncrementalProjectionRunner(views, resolver, hatSql.QueryOptions{}, hatSql.IncrementalProjectionRunnerOptions{Name: "people"})
	if err != nil {
		t.Fatal(err)
	}
	rows["people"] = []hatSql.Row{{"name": "Lin"}}
	run, err := runner.Apply(context.Background(), []hatSql.ProjectionChange{{Sequence: 1, Dependency: "people"}})
	if err != nil {
		t.Fatal(err)
	}
	if run.Enabled || run.ThroughSequence != 0 || len(run.Refreshed) != 0 {
		t.Fatalf("disabled run = %#v", run)
	}
	view, ok := views.Get("people_view")
	if !ok || view.Status.Revision != 1 || view.Result.Rows[0]["name"] != "Ada" {
		t.Fatalf("view after disabled run = %#v", view)
	}
}

func TestIncrementalProjectionRunnerRefreshesAndRecoversCheckpoint(t *testing.T) {
	rows := map[string][]hatSql.Row{"people": {{"name": "Ada"}}}
	resolver := hatSql.SourceResolverFunc(func(_ string, key string) ([]hatSql.Row, error) {
		return hatSql.CloneRows(rows[key]), nil
	})
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{Name: "people_view", Query: "FROM CACHE('people') SELECT name", Dependencies: []string{"people"}}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	store := &memoryProjectionCheckpointStore{}
	runner, err := hatSql.NewIncrementalProjectionRunner(views, resolver, hatSql.QueryOptions{}, hatSql.IncrementalProjectionRunnerOptions{Name: "people", Enabled: true, CheckpointStore: store})
	if err != nil {
		t.Fatal(err)
	}
	rows["people"] = []hatSql.Row{{"name": "Lin"}}
	run, err := runner.Apply(context.Background(), []hatSql.ProjectionChange{{Sequence: 1, Dependency: "people"}})
	if err != nil {
		t.Fatal(err)
	}
	if !run.Enabled || run.FromSequence != 0 || run.ThroughSequence != 1 || len(run.Refreshed) != 1 || run.Refreshed[0].Revision != 2 {
		t.Fatalf("run = %#v", run)
	}
	if checkpoint := runner.Checkpoint(); checkpoint != 1 || store.checkpoints["people"] != 1 {
		t.Fatalf("checkpoint = %d, store = %#v", checkpoint, store.checkpoints)
	}
	recovered, err := hatSql.NewIncrementalProjectionRunner(views, resolver, hatSql.QueryOptions{}, hatSql.IncrementalProjectionRunnerOptions{Name: "people", Enabled: true, CheckpointStore: store})
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint := recovered.Checkpoint(); checkpoint != 1 {
		t.Fatalf("recovered checkpoint = %d, want 1", checkpoint)
	}
	run, err = recovered.Apply(context.Background(), []hatSql.ProjectionChange{{Sequence: 1, Dependency: "people"}})
	if err != nil {
		t.Fatal(err)
	}
	if run.ThroughSequence != 1 || len(run.Refreshed) != 0 {
		t.Fatalf("replayed run = %#v", run)
	}
}

func TestIncrementalProjectionRunnerKeepsCheckpointWhenRefreshFails(t *testing.T) {
	rows := map[string][]hatSql.Row{"people": {{"name": "Ada"}}}
	resolver := hatSql.SourceResolverFunc(func(_ string, key string) ([]hatSql.Row, error) {
		rows, ok := rows[key]
		if !ok {
			return nil, fmt.Errorf("source %q unavailable", key)
		}
		return hatSql.CloneRows(rows), nil
	})
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{Name: "people_view", Query: "FROM CACHE('people') SELECT name", Dependencies: []string{"people"}}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	store := &memoryProjectionCheckpointStore{}
	runner, err := hatSql.NewIncrementalProjectionRunner(views, resolver, hatSql.QueryOptions{}, hatSql.IncrementalProjectionRunnerOptions{Name: "people", Enabled: true, CheckpointStore: store})
	if err != nil {
		t.Fatal(err)
	}
	delete(rows, "people")
	if _, err := runner.Apply(context.Background(), []hatSql.ProjectionChange{{Sequence: 1, Dependency: "people"}}); err == nil {
		t.Fatal("Apply() error = nil, want refresh failure")
	}
	if checkpoint := runner.Checkpoint(); checkpoint != 0 {
		t.Fatalf("checkpoint = %d, want 0", checkpoint)
	}
	view, ok := views.Get("people_view")
	if !ok || view.Status.Revision != 1 || view.Result.Rows[0]["name"] != "Ada" {
		t.Fatalf("view after failed run = %#v", view)
	}
}

func TestIncrementalProjectionRunnerRebuildsFromTrustedCheckpoint(t *testing.T) {
	rows := map[string][]hatSql.Row{"people": {{"name": "Ada"}}}
	resolver := hatSql.SourceResolverFunc(func(_ string, key string) ([]hatSql.Row, error) {
		return hatSql.CloneRows(rows[key]), nil
	})
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{Name: "people_view", Query: "FROM CACHE('people') SELECT name", Dependencies: []string{"people"}}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	store := &memoryProjectionCheckpointStore{}
	runner, err := hatSql.NewIncrementalProjectionRunner(views, resolver, hatSql.QueryOptions{}, hatSql.IncrementalProjectionRunnerOptions{Name: "people", Enabled: true, CheckpointStore: store})
	if err != nil {
		t.Fatal(err)
	}
	rows["people"] = []hatSql.Row{{"name": "Lin"}}
	run, err := runner.Rebuild(context.Background(), []string{"people"}, 7)
	if err != nil {
		t.Fatal(err)
	}
	if run.FromSequence != 0 || run.ThroughSequence != 7 || len(run.Refreshed) != 1 || runner.Checkpoint() != 7 || store.checkpoints["people"] != 7 {
		t.Fatalf("rebuild run = %#v, checkpoint = %d, store = %#v", run, runner.Checkpoint(), store.checkpoints)
	}
	view, ok := views.Get("people_view")
	if !ok || view.Result.Rows[0]["name"] != "Lin" {
		t.Fatalf("rebuilt view = %#v", view)
	}
}
