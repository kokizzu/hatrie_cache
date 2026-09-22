package hatReplication

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestExecuteClusterWriteCommitWithLedgerPersistsUnknownOutcome(t *testing.T) {
	path := filepath.Join(t.TempDir(), "quorum-ledger.bin")
	ledger, err := OpenClusterWriteCommitLedger(path, ClusterWriteCommitLedgerOptions{})
	if err != nil {
		t.Fatalf("OpenClusterWriteCommitLedger() error = %v", err)
	}
	proposal := ClusterWriteCommitProposal{TransactionID: "tx-ledger-unknown", Sequence: 9, FenceToken: 4}
	result, err := ExecuteClusterWriteCommitWithLedger(
		context.Background(),
		[]string{"node-a", "node-b", "node-c"},
		proposal,
		func(context.Context, string, ClusterWriteCommitProposal) error { return nil },
		func(_ context.Context, node string, _ ClusterWriteCommitProposal) error {
			if node == "node-b" {
				return errors.New("commit acknowledgement lost")
			}
			return nil
		},
		func(context.Context, string, ClusterWriteCommitProposal) error {
			t.Fatal("abort callback invoked after commit phase started")
			return nil
		},
		ledger,
	)
	if !errors.Is(err, ErrClusterWriteCommitOutcomeUnknown) || !result.OutcomeUnknown {
		t.Fatalf("result = %#v, error = %v, want unknown outcome", result, err)
	}
	if err := ledger.Close(); err != nil {
		t.Fatalf("ledger.Close() error = %v", err)
	}

	reopened, err := OpenClusterWriteCommitLedger(path, ClusterWriteCommitLedgerOptions{})
	if err != nil {
		t.Fatalf("reopen ledger error = %v", err)
	}
	record, ok := reopened.Record(proposal.TransactionID)
	if !ok {
		t.Fatalf("Record(%q) not found after reopen", proposal.TransactionID)
	}
	if record.Phase != ClusterWriteCommitLedgerOutcomeUnknown {
		t.Fatalf("record phase = %q, want unknown", record.Phase)
	}
	if len(record.Participants) != 3 || !record.Participants[0].Committed || record.Participants[1].Committed || !record.Participants[2].Committed {
		t.Fatalf("record participants = %#v, want committed/unfinished/committed", record.Participants)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("reopened.Close() error = %v", err)
	}
}

func TestClusterWriteCommitLedgerRejectsBlindReplayAndIsIdempotentAfterCommit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "quorum-ledger.bin")
	ledger, err := OpenClusterWriteCommitLedger(path, ClusterWriteCommitLedgerOptions{MaxEntries: 2})
	if err != nil {
		t.Fatalf("OpenClusterWriteCommitLedger() error = %v", err)
	}
	proposal := ClusterWriteCommitProposal{TransactionID: "tx-ledger-committed", Sequence: 11}
	var callbackCalls atomic.Int32
	callback := func(context.Context, string, ClusterWriteCommitProposal) error {
		callbackCalls.Add(1)
		return nil
	}
	result, err := ExecuteClusterWriteCommitWithLedger(context.Background(), []string{"node-a"}, proposal, callback, callback, callback, ledger)
	if err != nil || !result.Committed {
		t.Fatalf("first result = %#v, error = %v", result, err)
	}
	firstCalls := callbackCalls.Load()
	result, err = ExecuteClusterWriteCommitWithLedger(context.Background(), []string{"node-a"}, proposal, callback, callback, callback, ledger)
	if err != nil || !result.Committed || callbackCalls.Load() != firstCalls {
		t.Fatalf("idempotent result = %#v, error = %v, callback calls = %d want %d", result, err, callbackCalls.Load(), firstCalls)
	}

	unknown := ClusterWriteCommitProposal{TransactionID: "tx-ledger-unknown-replay", Sequence: 12}
	if _, err := ExecuteClusterWriteCommitWithLedger(context.Background(), []string{"node-a"}, unknown,
		func(context.Context, string, ClusterWriteCommitProposal) error { return nil },
		func(context.Context, string, ClusterWriteCommitProposal) error { return errors.New("unknown") },
		callback,
		ledger,
	); !errors.Is(err, ErrClusterWriteCommitOutcomeUnknown) {
		t.Fatalf("unknown setup error = %v", err)
	}
	if _, err := ExecuteClusterWriteCommitWithLedger(context.Background(), []string{"node-a"}, unknown, callback, callback, callback, ledger); !errors.Is(err, ErrClusterWriteCommitLedgerReconcileRequired) {
		t.Fatalf("unknown replay error = %v, want reconcile-required", err)
	}
	if err := ledger.Close(); err != nil {
		t.Fatalf("ledger.Close() error = %v", err)
	}
}

func TestClusterWriteCommitLedgerRejectsCorruptState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "quorum-ledger.bin")
	ledger, err := OpenClusterWriteCommitLedger(path, ClusterWriteCommitLedgerOptions{})
	if err != nil {
		t.Fatalf("OpenClusterWriteCommitLedger() error = %v", err)
	}
	if err := ledger.Close(); err != nil {
		t.Fatalf("ledger.Close() error = %v", err)
	}
	if err := os.WriteFile(path, []byte("not-a-ledger"), 0o600); err != nil {
		t.Fatalf("write corrupt ledger: %v", err)
	}
	if _, err := OpenClusterWriteCommitLedger(path, ClusterWriteCommitLedgerOptions{}); !errors.Is(err, ErrClusterWriteCommitLedgerCorrupt) {
		t.Fatalf("corrupt open error = %v, want corrupt error", err)
	}
}

func TestClusterWriteCommitLedgerValidatesBoundsConflictsAndSymlinks(t *testing.T) {
	if _, err := OpenClusterWriteCommitLedger("", ClusterWriteCommitLedgerOptions{}); !errors.Is(err, ErrClusterWriteCommitLedgerInvalid) {
		t.Fatalf("empty path error = %v, want invalid", err)
	}
	for name, options := range map[string]ClusterWriteCommitLedgerOptions{
		"negative entries":  {MaxEntries: -1},
		"oversized entries": {MaxEntries: MaxClusterWriteCommitLedgerEntries + 1},
		"negative bytes":    {MaxFileBytes: -1},
		"oversized bytes":   {MaxFileBytes: MaxClusterWriteCommitLedgerFileBytes + 1},
	} {
		if _, err := OpenClusterWriteCommitLedger(filepath.Join(t.TempDir(), "ledger.bin"), options); !errors.Is(err, ErrClusterWriteCommitLedgerInvalid) {
			t.Fatalf("%s error = %v, want invalid", name, err)
		}
	}

	directory := t.TempDir()
	path := filepath.Join(directory, "ledger.bin")
	ledger, err := OpenClusterWriteCommitLedger(path, ClusterWriteCommitLedgerOptions{MaxEntries: 1})
	if err != nil {
		t.Fatalf("OpenClusterWriteCommitLedger() error = %v", err)
	}
	callback := func(context.Context, string, ClusterWriteCommitProposal) error { return nil }
	proposal := ClusterWriteCommitProposal{TransactionID: "tx-boundary", Sequence: 1}
	if _, err := ExecuteClusterWriteCommitWithLedger(context.Background(), []string{"node-a"}, proposal, callback, callback, callback, ledger); err != nil {
		t.Fatalf("first ledger transaction error = %v", err)
	}
	conflicting := proposal
	conflicting.Sequence++
	if _, err := ExecuteClusterWriteCommitWithLedger(context.Background(), []string{"node-a"}, conflicting, callback, callback, callback, ledger); !errors.Is(err, ErrClusterWriteCommitLedgerConflict) {
		t.Fatalf("conflicting transaction error = %v, want conflict", err)
	}
	if _, err := ExecuteClusterWriteCommitWithLedger(context.Background(), []string{"node-b"}, ClusterWriteCommitProposal{TransactionID: "tx-full"}, callback, callback, callback, ledger); !errors.Is(err, ErrClusterWriteCommitLedgerFull) {
		t.Fatalf("full ledger error = %v, want full", err)
	}
	if err := ledger.Forget(proposal.TransactionID); err != nil {
		t.Fatalf("Forget() error = %v", err)
	}
	if _, err := ExecuteClusterWriteCommitWithLedger(context.Background(), []string{"node-b"}, ClusterWriteCommitProposal{TransactionID: "tx-full"}, callback, callback, callback, ledger); err != nil {
		t.Fatalf("transaction after Forget() error = %v", err)
	}
	if err := ledger.Close(); err != nil {
		t.Fatalf("ledger.Close() error = %v", err)
	}

	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	for _, entry := range entries {
		if strings.Contains(entry.Name(), ".tmp-") {
			t.Fatalf("temporary ledger file remains: %s", entry.Name())
		}
	}
	linkPath := filepath.Join(directory, "ledger-link.bin")
	if err := os.Symlink(path, linkPath); err != nil {
		t.Fatalf("create ledger symlink: %v", err)
	}
	if _, err := OpenClusterWriteCommitLedger(linkPath, ClusterWriteCommitLedgerOptions{}); !errors.Is(err, ErrClusterWriteCommitLedgerCorrupt) {
		t.Fatalf("symlink ledger error = %v, want corrupt", err)
	}
}
