package hatCache

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func TestSQLJSONIndexRebuildCheckpointStoreRecoversAcrossRestart(t *testing.T) {
	checkpointPath := filepath.Join(t.TempDir(), "sql-index-rebuild.json")
	store, err := NewFileSQLJSONIndexRebuildCheckpointStore(checkpointPath)
	if err != nil {
		t.Fatalf("NewFileSQLJSONIndexRebuildCheckpointStore() error = %v", err)
	}

	first := newTestTrie(t)
	first.UpsertString("jobs", `[{"id":1,"state":"queued"}]`)
	if err := first.CreateSQLJSONFieldIndex("jobs", "state"); err != nil {
		t.Fatal(err)
	}
	if err := first.SetSQLJSONIndexRebuildCheckpointStore(context.Background(), store); err != nil {
		t.Fatalf("SetSQLJSONIndexRebuildCheckpointStore(first) error = %v", err)
	}
	if err := first.ScheduleSQLJSONIndexRebuild("jobs", "state"); err != nil {
		t.Fatalf("ScheduleSQLJSONIndexRebuild() error = %v", err)
	}
	checkpoints, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load() after schedule error = %v", err)
	}
	want := []SQLJSONIndexRebuildCheckpoint{{Key: "jobs", Field: "state"}}
	if !reflect.DeepEqual(checkpoints, want) {
		t.Fatalf("checkpoints after schedule = %#v, want %#v", checkpoints, want)
	}

	second := newTestTrie(t)
	second.UpsertString("jobs", `[{"id":1,"state":"queued"}]`)
	if err := second.CreateSQLJSONFieldIndex("jobs", "state"); err != nil {
		t.Fatal(err)
	}
	secondStore, err := NewFileSQLJSONIndexRebuildCheckpointStore(checkpointPath)
	if err != nil {
		t.Fatalf("NewFileSQLJSONIndexRebuildCheckpointStore(second) error = %v", err)
	}
	if err := second.SetSQLJSONIndexRebuildCheckpointStore(context.Background(), secondStore); err != nil {
		t.Fatalf("SetSQLJSONIndexRebuildCheckpointStore(second) error = %v", err)
	}
	status, available, err := second.SQLJSONIndexMaintenanceStats("jobs", "state")
	if err != nil || !available || !status.Pending || status.Current {
		t.Fatalf("recovered maintenance status = %#v, %v, %v", status, available, err)
	}
	processed, err := second.RunScheduledSQLJSONIndexRebuilds(1)
	if err != nil || processed != 1 {
		t.Fatalf("recovered rebuild = processed %d, error %v", processed, err)
	}
	status, available, err = second.SQLJSONIndexMaintenanceStats("jobs", "state")
	if err != nil || !available || status.Pending || !status.Current {
		t.Fatalf("completed maintenance status = %#v, %v, %v", status, available, err)
	}
	checkpoints, err = secondStore.Load(context.Background())
	if err != nil {
		t.Fatalf("Load() after completion error = %v", err)
	}
	if len(checkpoints) != 0 {
		t.Fatalf("checkpoints after completion = %#v, want empty", checkpoints)
	}
}

func TestSQLJSONIndexRebuildCheckpointRetainedWhenCanceled(t *testing.T) {
	checkpointPath := filepath.Join(t.TempDir(), "sql-index-rebuild.json")
	store, err := NewFileSQLJSONIndexRebuildCheckpointStore(checkpointPath)
	if err != nil {
		t.Fatalf("NewFileSQLJSONIndexRebuildCheckpointStore() error = %v", err)
	}
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `[{"id":1,"state":"queued"}]`)
	if err := trie.CreateSQLJSONFieldIndex("jobs", "state"); err != nil {
		t.Fatal(err)
	}
	if err := trie.SetSQLJSONIndexRebuildCheckpointStore(context.Background(), store); err != nil {
		t.Fatalf("SetSQLJSONIndexRebuildCheckpointStore() error = %v", err)
	}
	if err := trie.ScheduleSQLJSONIndexRebuild("jobs", "state"); err != nil {
		t.Fatalf("ScheduleSQLJSONIndexRebuild() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	processed, err := trie.RunScheduledSQLJSONIndexRebuildsWithProgress(ctx, 1, nil)
	if !errors.Is(err, context.Canceled) || processed != 0 {
		t.Fatalf("canceled rebuild = processed %d, error %v", processed, err)
	}
	checkpoints, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load() after cancellation error = %v", err)
	}
	want := []SQLJSONIndexRebuildCheckpoint{{Key: "jobs", Field: "state"}}
	if !reflect.DeepEqual(checkpoints, want) {
		t.Fatalf("checkpoints after cancellation = %#v, want %#v", checkpoints, want)
	}
}

func TestSQLJSONIndexRebuildCheckpointScheduleFailureRollsBack(t *testing.T) {
	store := &sqlIndexRebuildCheckpointTestStore{}
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `[{"id":1,"state":"queued"}]`)
	if err := trie.CreateSQLJSONFieldIndex("jobs", "state"); err != nil {
		t.Fatal(err)
	}
	if err := trie.SetSQLJSONIndexRebuildCheckpointStore(context.Background(), store); err != nil {
		t.Fatalf("SetSQLJSONIndexRebuildCheckpointStore() error = %v", err)
	}
	store.mu.Lock()
	store.saveErr = errors.New("checkpoint save failed")
	store.mu.Unlock()
	if err := trie.ScheduleSQLJSONIndexRebuild("jobs", "state"); err == nil {
		t.Fatal("ScheduleSQLJSONIndexRebuild() succeeded with a failing checkpoint store")
	}
	status, available, err := trie.SQLJSONIndexMaintenanceStats("jobs", "state")
	if err != nil || !available || status.Pending {
		t.Fatalf("rolled back maintenance status = %#v, %v, %v", status, available, err)
	}
}

func TestSQLJSONIndexRebuildCheckpointCanAttachBeforeIndexConfiguration(t *testing.T) {
	checkpointPath := filepath.Join(t.TempDir(), "sql-index-rebuild.json")
	firstStore, err := NewFileSQLJSONIndexRebuildCheckpointStore(checkpointPath)
	if err != nil {
		t.Fatalf("NewFileSQLJSONIndexRebuildCheckpointStore(first) error = %v", err)
	}
	first := newTestTrie(t)
	first.UpsertString("jobs", `[{"id":1,"state":"queued"}]`)
	if err := first.CreateSQLJSONFieldIndex("jobs", "state"); err != nil {
		t.Fatal(err)
	}
	if err := first.SetSQLJSONIndexRebuildCheckpointStore(context.Background(), firstStore); err != nil {
		t.Fatalf("SetSQLJSONIndexRebuildCheckpointStore(first) error = %v", err)
	}
	if err := first.ScheduleSQLJSONIndexRebuild("jobs", "state"); err != nil {
		t.Fatalf("ScheduleSQLJSONIndexRebuild() error = %v", err)
	}

	second := newTestTrie(t)
	second.UpsertString("jobs", `[{"id":1,"state":"queued"}]`)
	secondStore, err := NewFileSQLJSONIndexRebuildCheckpointStore(checkpointPath)
	if err != nil {
		t.Fatalf("NewFileSQLJSONIndexRebuildCheckpointStore(second) error = %v", err)
	}
	if err := second.SetSQLJSONIndexRebuildCheckpointStore(context.Background(), secondStore); err != nil {
		t.Fatalf("SetSQLJSONIndexRebuildCheckpointStore(second) error = %v", err)
	}
	if queued := second.ResumeSQLJSONIndexRebuilds(); queued != 0 {
		t.Fatalf("ResumeSQLJSONIndexRebuilds() before configuration = %d, want 0", queued)
	}
	if err := second.CreateSQLJSONFieldIndex("jobs", "state"); err != nil {
		t.Fatal(err)
	}
	if queued := second.ResumeSQLJSONIndexRebuilds(); queued != 1 {
		t.Fatalf("ResumeSQLJSONIndexRebuilds() after configuration = %d, want 1", queued)
	}
	if processed, err := second.RunScheduledSQLJSONIndexRebuilds(1); err != nil || processed != 1 {
		t.Fatalf("RunScheduledSQLJSONIndexRebuilds() = %d, %v", processed, err)
	}
}

func TestFileSQLJSONIndexRebuildCheckpointStoreValidatesFormat(t *testing.T) {
	checkpointPath := filepath.Join(t.TempDir(), "sql-index-rebuild.json")
	store, err := NewFileSQLJSONIndexRebuildCheckpointStore(checkpointPath)
	if err != nil {
		t.Fatalf("NewFileSQLJSONIndexRebuildCheckpointStore() error = %v", err)
	}
	if err := store.Save(context.Background(), []SQLJSONIndexRebuildCheckpoint{
		{Key: "z", Field: "state"}, {Key: "a", Field: "state"},
	}); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	checkpoints, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	want := []SQLJSONIndexRebuildCheckpoint{{Key: "a", Field: "state"}, {Key: "z", Field: "state"}}
	if !reflect.DeepEqual(checkpoints, want) {
		t.Fatalf("normalized checkpoints = %#v, want %#v", checkpoints, want)
	}
	if err := os.WriteFile(checkpointPath, []byte(`{"version":99,"checkpoints":[]}`), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if _, err := store.Load(context.Background()); !errors.Is(err, ErrInvalidSQLJSONIndexRebuildCheckpoint) {
		t.Fatalf("Load(unsupported version) error = %v, want invalid checkpoint", err)
	}
	linkPath := filepath.Join(t.TempDir(), "checkpoint-link.json")
	if err := os.Symlink(checkpointPath, linkPath); err == nil {
		linkStore, err := NewFileSQLJSONIndexRebuildCheckpointStore(linkPath)
		if err != nil {
			t.Fatalf("NewFileSQLJSONIndexRebuildCheckpointStore(link) error = %v", err)
		}
		if _, err := linkStore.Load(context.Background()); !errors.Is(err, ErrInvalidSQLJSONIndexRebuildCheckpoint) {
			t.Fatalf("Load(symlink) error = %v, want invalid checkpoint", err)
		}
	}
}

type sqlIndexRebuildCheckpointTestStore struct {
	mu          sync.Mutex
	checkpoints []SQLJSONIndexRebuildCheckpoint
	saveErr     error
}

func (store *sqlIndexRebuildCheckpointTestStore) Load(context.Context) ([]SQLJSONIndexRebuildCheckpoint, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return append([]SQLJSONIndexRebuildCheckpoint(nil), store.checkpoints...), nil
}

func (store *sqlIndexRebuildCheckpointTestStore) Save(_ context.Context, checkpoints []SQLJSONIndexRebuildCheckpoint) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.saveErr != nil {
		return store.saveErr
	}
	store.checkpoints = append([]SQLJSONIndexRebuildCheckpoint(nil), checkpoints...)
	return nil
}
