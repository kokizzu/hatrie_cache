package hatReplication

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestExecuteClusterWriteCommitDurablePersistsCommittedDecision(t *testing.T) {
	path := filepath.Join(t.TempDir(), "decisions.state")
	store, err := NewClusterWriteCommitDecisionFileStore(ClusterWriteCommitDecisionFileStoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	proposal := ClusterWriteCommitProposal{TransactionID: "tx-committed", Sequence: 7, FenceToken: 3}
	nodes := []string{"node-a", "node-b"}
	result, err := ExecuteClusterWriteCommitDurable(context.Background(), nodes, proposal,
		func(context.Context, string, ClusterWriteCommitProposal) error { return nil },
		func(context.Context, string, ClusterWriteCommitProposal) error { return nil },
		func(context.Context, string, ClusterWriteCommitProposal) error { return nil },
		store,
	)
	if err != nil {
		t.Fatalf("ExecuteClusterWriteCommitDurable() error = %v", err)
	}
	if !result.Committed || result.OutcomeUnknown {
		t.Fatalf("result = %#v, want committed", result)
	}
	reopened, err := NewClusterWriteCommitDecisionFileStore(ClusterWriteCommitDecisionFileStoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	loaded, ok, err := reopened.Load(context.Background(), proposal.TransactionID)
	if err != nil || !ok {
		t.Fatalf("Load() = %#v/%v/%v, want committed record", loaded, ok, err)
	}
	if loaded.Phase != ClusterWriteCommitDecisionCommitted || loaded.Proposal != proposal {
		t.Fatalf("loaded = %#v, want committed proposal %#v", loaded, proposal)
	}
	if len(loaded.Nodes) != len(nodes) || loaded.Nodes[0] != nodes[0] || loaded.Nodes[1] != nodes[1] {
		t.Fatalf("loaded nodes = %#v, want %#v", loaded.Nodes, nodes)
	}
}

func TestExecuteClusterWriteCommitDurablePersistsIndeterminateDecision(t *testing.T) {
	path := filepath.Join(t.TempDir(), "decisions.state")
	store, err := NewClusterWriteCommitDecisionFileStore(ClusterWriteCommitDecisionFileStoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	proposal := ClusterWriteCommitProposal{TransactionID: "tx-unknown", Sequence: 9}
	result, err := ExecuteClusterWriteCommitDurable(context.Background(), []string{"node-a", "node-b"}, proposal,
		func(context.Context, string, ClusterWriteCommitProposal) error { return nil },
		func(_ context.Context, node string, _ ClusterWriteCommitProposal) error {
			if node == "node-b" {
				return errors.New("commit timeout")
			}
			return nil
		},
		func(context.Context, string, ClusterWriteCommitProposal) error { return nil },
		store,
	)
	if !errors.Is(err, ErrClusterWriteCommitOutcomeUnknown) || !result.OutcomeUnknown {
		t.Fatalf("result/error = %#v/%v, want indeterminate outcome", result, err)
	}
	reopened, err := NewClusterWriteCommitDecisionFileStore(ClusterWriteCommitDecisionFileStoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	loaded, ok, err := reopened.Load(context.Background(), proposal.TransactionID)
	if err != nil || !ok {
		t.Fatalf("reopened Load() = %#v/%v/%v, want record", loaded, ok, err)
	}
	if loaded.Phase != ClusterWriteCommitDecisionIndeterminate || loaded.Proposal != proposal {
		t.Fatalf("reopened = %#v, want indeterminate proposal %#v", loaded, proposal)
	}
}

func TestClusterWriteCommitDecisionFileStoreRejectsRegressionAndIdentityConflict(t *testing.T) {
	store, err := NewClusterWriteCommitDecisionFileStore(ClusterWriteCommitDecisionFileStoreOptions{Path: filepath.Join(t.TempDir(), "decisions.state")})
	if err != nil {
		t.Fatal(err)
	}
	proposal := ClusterWriteCommitProposal{TransactionID: "tx-regression", Sequence: 1}
	decision := ClusterWriteCommitDecision{Proposal: proposal, Nodes: []string{"node-a"}, Phase: ClusterWriteCommitDecisionPreparing}
	if err := store.Record(context.Background(), decision); err != nil {
		t.Fatal(err)
	}
	if err := store.Record(context.Background(), ClusterWriteCommitDecision{Proposal: proposal, Nodes: []string{"node-a"}, Phase: ClusterWriteCommitDecisionPrepared}); err != nil {
		t.Fatal(err)
	}
	if err := store.Record(context.Background(), decision); !errors.Is(err, ErrClusterWriteCommitDecisionTransition) {
		t.Fatalf("regression error = %v, want phase transition error", err)
	}
	if err := store.Record(context.Background(), ClusterWriteCommitDecision{Proposal: proposal, Nodes: []string{"node-b"}, Phase: ClusterWriteCommitDecisionCommitting}); !errors.Is(err, ErrClusterWriteCommitDecisionConflict) {
		t.Fatalf("identity conflict error = %v, want conflict", err)
	}
}

func TestClusterWriteCommitDecisionFileStoreRejectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "decisions.state")
	store, err := NewClusterWriteCommitDecisionFileStore(ClusterWriteCommitDecisionFileStoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Record(context.Background(), ClusterWriteCommitDecision{
		Proposal: ClusterWriteCommitProposal{TransactionID: "tx-corrupt"},
		Nodes:    []string{"node-a"},
		Phase:    ClusterWriteCommitDecisionPreparing,
	}); err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	encoded[len(encoded)-1] ^= 0xff
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewClusterWriteCommitDecisionFileStore(ClusterWriteCommitDecisionFileStoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := reopened.Load(context.Background(), "tx-corrupt"); !errors.Is(err, ErrClusterWriteCommitDecisionChecksum) {
		t.Fatalf("corrupt Load() error = %v, want checksum error", err)
	}
}

func TestClusterWriteCommitDecisionFileStoreRejectsOversizedFileBeforeDecode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "decisions.state")
	if err := os.WriteFile(path, make([]byte, 257), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := NewClusterWriteCommitDecisionFileStore(ClusterWriteCommitDecisionFileStoreOptions{Path: path, MaxBytes: 256})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Load(context.Background(), "tx-large"); !errors.Is(err, ErrClusterWriteCommitDecisionSnapshotInvalid) {
		t.Fatalf("oversized Load() error = %v, want snapshot limit error", err)
	}
}

func TestClusterWriteCommitDecisionFileStoreRejectsSymlink(t *testing.T) {
	directory := t.TempDir()
	target := filepath.Join(directory, "target.state")
	path := filepath.Join(directory, "decisions.state")
	if err := os.WriteFile(target, []byte("not a decision store"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	store, err := NewClusterWriteCommitDecisionFileStore(ClusterWriteCommitDecisionFileStoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Load(context.Background(), "tx-link"); !errors.Is(err, ErrClusterWriteCommitDecisionSnapshotInvalid) {
		t.Fatalf("symlink Load() error = %v, want snapshot error", err)
	}
}

func TestExecuteClusterWriteCommitDurablePersistsAbortedPrepareFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "decisions.state")
	store, err := NewClusterWriteCommitDecisionFileStore(ClusterWriteCommitDecisionFileStoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	proposal := ClusterWriteCommitProposal{TransactionID: "tx-aborted"}
	result, err := ExecuteClusterWriteCommitDurable(context.Background(), []string{"node-a", "node-b"}, proposal,
		func(_ context.Context, node string, _ ClusterWriteCommitProposal) error {
			if node == "node-b" {
				return errors.New("prepare rejected")
			}
			return nil
		},
		func(context.Context, string, ClusterWriteCommitProposal) error {
			t.Fatal("commit callback invoked after prepare failure")
			return nil
		},
		func(context.Context, string, ClusterWriteCommitProposal) error { return nil },
		store,
	)
	if !errors.Is(err, ErrClusterWriteCommitPrepareFailed) || result.Prepared || result.Committed {
		t.Fatalf("result/error = %#v/%v, want aborted prepare failure", result, err)
	}
	loaded, ok, err := store.Load(context.Background(), proposal.TransactionID)
	if err != nil || !ok || loaded.Phase != ClusterWriteCommitDecisionAborted {
		t.Fatalf("loaded = %#v/%v/%v, want aborted record", loaded, ok, err)
	}
}
