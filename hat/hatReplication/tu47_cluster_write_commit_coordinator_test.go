package hatReplication_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"hatrie_cache/hat/hatReplication"
)

func TestClusterWriteCommitCoordinatorFileStoreRoundTripAndCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "coordinator.bin")
	store, err := hatReplication.NewClusterWriteCommitCoordinatorFileStore(hatReplication.ClusterWriteCommitCoordinatorFileStoreOptions{
		Path: path,
	})
	if err != nil {
		t.Fatalf("NewClusterWriteCommitCoordinatorFileStore() error = %v", err)
	}

	proposal := testClusterWriteCommitCoordinatorProposal()
	want := hatReplication.ClusterWriteCommitCoordinatorSnapshot{
		Proposal: proposal,
		Nodes:    []string{"node-a", "node-b"},
		Attempts: []hatReplication.ClusterWriteCommitAttempt{
			{Node: "node-a", Prepared: true, Committed: true},
			{Node: "node-b", Prepared: true, Committed: false, CommitError: "timeout"},
		},
		Phase: hatReplication.ClusterWriteCommitCoordinatorOutcomeUnknown,
	}
	if err := store.Save(context.Background(), want); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	got, found, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !found {
		t.Fatal("Load() found = false, want true")
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Load() = %#v, want %#v", got, want)
	}

	fileInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat(state) error = %v", err)
	}
	if got := fileInfo.Mode().Perm(); got != 0o600 {
		t.Fatalf("state file mode = %o, want 600", got)
	}
	directoryInfo, err := os.Stat(filepath.Dir(path))
	if err != nil {
		t.Fatalf("Stat(directory) error = %v", err)
	}
	if got := directoryInfo.Mode().Perm(); got != 0o700 {
		t.Fatalf("state directory mode = %o, want 700", got)
	}

	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	payload[len(payload)-1] ^= 0xff
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("WriteFile(corrupt) error = %v", err)
	}
	if _, found, err := store.Load(context.Background()); !errors.Is(err, hatReplication.ErrClusterWriteCommitCoordinatorFileStoreSnapshotInvalid) || found {
		t.Fatalf("corrupt Load() = found %v, error %v; want found false and snapshot-invalid", found, err)
	}
}

func TestExecuteClusterWriteCommitWithStateStorePersistsPhaseBoundaries(t *testing.T) {
	store := &recordingClusterWriteCommitCoordinatorStateStore{}
	proposal := testClusterWriteCommitCoordinatorProposal()
	result, err := hatReplication.ExecuteClusterWriteCommitWithStateStore(
		context.Background(),
		[]string{"node-a", "node-b"},
		proposal,
		func(context.Context, string, hatReplication.ClusterWriteCommitProposal) error { return nil },
		func(context.Context, string, hatReplication.ClusterWriteCommitProposal) error { return nil },
		func(context.Context, string, hatReplication.ClusterWriteCommitProposal) error { return nil },
		store,
	)
	if err != nil {
		t.Fatalf("ExecuteClusterWriteCommitWithStateStore() error = %v", err)
	}
	if !result.Committed {
		t.Fatalf("result.Committed = false, want true: %#v", result)
	}

	got := store.phases()
	want := []hatReplication.ClusterWriteCommitCoordinatorPhase{
		hatReplication.ClusterWriteCommitCoordinatorProposed,
		hatReplication.ClusterWriteCommitCoordinatorPrepared,
		hatReplication.ClusterWriteCommitCoordinatorCommitStarted,
		hatReplication.ClusterWriteCommitCoordinatorCommitted,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("persisted phases = %v, want %v", got, want)
	}
}

func TestExecuteClusterWriteCommitWithStateStorePersistsUnknownOutcome(t *testing.T) {
	store := &recordingClusterWriteCommitCoordinatorStateStore{}
	proposal := testClusterWriteCommitCoordinatorProposal()
	result, err := hatReplication.ExecuteClusterWriteCommitWithStateStore(
		context.Background(),
		[]string{"node-a", "node-b"},
		proposal,
		func(context.Context, string, hatReplication.ClusterWriteCommitProposal) error { return nil },
		func(_ context.Context, node string, _ hatReplication.ClusterWriteCommitProposal) error {
			if node == "node-b" {
				return errors.New("commit timeout")
			}
			return nil
		},
		func(context.Context, string, hatReplication.ClusterWriteCommitProposal) error { return nil },
		store,
	)
	if !errors.Is(err, hatReplication.ErrClusterWriteCommitOutcomeUnknown) {
		t.Fatalf("error = %v, want outcome-unknown", err)
	}
	if !result.OutcomeUnknown {
		t.Fatalf("result.OutcomeUnknown = false, want true: %#v", result)
	}

	got := store.phases()
	want := []hatReplication.ClusterWriteCommitCoordinatorPhase{
		hatReplication.ClusterWriteCommitCoordinatorProposed,
		hatReplication.ClusterWriteCommitCoordinatorPrepared,
		hatReplication.ClusterWriteCommitCoordinatorCommitStarted,
		hatReplication.ClusterWriteCommitCoordinatorOutcomeUnknown,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("persisted phases = %v, want %v", got, want)
	}
}

func TestExecuteClusterWriteCommitWithStateStorePersistsAbortBoundaries(t *testing.T) {
	store := &recordingClusterWriteCommitCoordinatorStateStore{}
	proposal := testClusterWriteCommitCoordinatorProposal()
	var commitMu sync.Mutex
	commitCalled := false
	result, err := hatReplication.ExecuteClusterWriteCommitWithStateStore(
		context.Background(),
		[]string{"node-a", "node-b"},
		proposal,
		func(_ context.Context, node string, _ hatReplication.ClusterWriteCommitProposal) error {
			if node == "node-b" {
				return errors.New("prepare rejected")
			}
			return nil
		},
		func(context.Context, string, hatReplication.ClusterWriteCommitProposal) error {
			commitMu.Lock()
			commitCalled = true
			commitMu.Unlock()
			return nil
		},
		func(context.Context, string, hatReplication.ClusterWriteCommitProposal) error { return nil },
		store,
	)
	if !errors.Is(err, hatReplication.ErrClusterWriteCommitPrepareFailed) {
		t.Fatalf("error = %v, want prepare-failed", err)
	}
	if result.Prepared {
		t.Fatalf("result.Prepared = true, want false: %#v", result)
	}
	commitMu.Lock()
	gotCommitCalled := commitCalled
	commitMu.Unlock()
	if gotCommitCalled {
		t.Fatal("commit callback ran after prepare failure")
	}

	got := store.phases()
	want := []hatReplication.ClusterWriteCommitCoordinatorPhase{
		hatReplication.ClusterWriteCommitCoordinatorProposed,
		hatReplication.ClusterWriteCommitCoordinatorAbortStarted,
		hatReplication.ClusterWriteCommitCoordinatorAborted,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("persisted phases = %v, want %v", got, want)
	}
}

func TestExecuteClusterWriteCommitWithStateStoreStopsBeforeCallbacksWhenInitialSaveFails(t *testing.T) {
	store := failingClusterWriteCommitCoordinatorStateStore{err: errors.New("disk unavailable")}
	prepareCalls := 0
	_, err := hatReplication.ExecuteClusterWriteCommitWithStateStore(
		context.Background(),
		[]string{"node-a"},
		testClusterWriteCommitCoordinatorProposal(),
		func(context.Context, string, hatReplication.ClusterWriteCommitProposal) error {
			prepareCalls++
			return nil
		},
		func(context.Context, string, hatReplication.ClusterWriteCommitProposal) error { return nil },
		func(context.Context, string, hatReplication.ClusterWriteCommitProposal) error { return nil },
		store,
	)
	if !errors.Is(err, hatReplication.ErrClusterWriteCommitCoordinatorStateStoreSaveFailed) {
		t.Fatalf("error = %v, want state-store-save-failed", err)
	}
	if prepareCalls != 0 {
		t.Fatalf("prepare calls = %d, want 0", prepareCalls)
	}
}

func testClusterWriteCommitCoordinatorProposal() hatReplication.ClusterWriteCommitProposal {
	return hatReplication.ClusterWriteCommitProposal{
		TransactionID: "tx-1",
		Sequence:      42,
		FenceToken:    7,
		PayloadDigest: [32]byte{1, 2, 3, 4},
	}
}

type recordingClusterWriteCommitCoordinatorStateStore struct {
	mu        sync.Mutex
	snapshots []hatReplication.ClusterWriteCommitCoordinatorSnapshot
}

type failingClusterWriteCommitCoordinatorStateStore struct {
	err error
}

func (store failingClusterWriteCommitCoordinatorStateStore) Save(context.Context, hatReplication.ClusterWriteCommitCoordinatorSnapshot) error {
	return store.err
}

func (store *recordingClusterWriteCommitCoordinatorStateStore) Save(_ context.Context, snapshot hatReplication.ClusterWriteCommitCoordinatorSnapshot) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	snapshot.Nodes = append([]string(nil), snapshot.Nodes...)
	snapshot.Attempts = append([]hatReplication.ClusterWriteCommitAttempt(nil), snapshot.Attempts...)
	store.snapshots = append(store.snapshots, snapshot)
	return nil
}

func (store *recordingClusterWriteCommitCoordinatorStateStore) phases() []hatReplication.ClusterWriteCommitCoordinatorPhase {
	store.mu.Lock()
	defer store.mu.Unlock()
	phases := make([]hatReplication.ClusterWriteCommitCoordinatorPhase, 0, len(store.snapshots))
	for _, snapshot := range store.snapshots {
		phases = append(phases, snapshot.Phase)
	}
	return phases
}
