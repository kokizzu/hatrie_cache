package hatReplication

import (
	"bytes"
	"errors"
	"testing"
)

func TestClusterWriteCommitParticipantIsIdempotentAndReconciliable(t *testing.T) {
	participant, err := NewClusterWriteCommitParticipant(ClusterWriteCommitParticipantOptions{MaxRecords: 4})
	if err != nil {
		t.Fatal(err)
	}
	proposal := ClusterWriteCommitProposal{TransactionID: " tx-1 ", Sequence: 7, FenceToken: 9}
	prepared, err := participant.Prepare(proposal)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if prepared.Phase != ClusterWriteCommitParticipantPrepared || prepared.Proposal.TransactionID != "tx-1" {
		t.Fatalf("prepared record = %#v", prepared)
	}
	if repeated, err := participant.Prepare(proposal); err != nil || repeated != prepared {
		t.Fatalf("repeated Prepare() = %#v, %v; want idempotent result", repeated, err)
	}
	if _, err := participant.Prepare(ClusterWriteCommitProposal{TransactionID: "tx-1", Sequence: 8}); !errors.Is(err, ErrClusterWriteCommitParticipantConflict) {
		t.Fatalf("conflicting Prepare() error = %v, want conflict", err)
	}
	committed, err := participant.Commit(proposal)
	if err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if committed.Phase != ClusterWriteCommitParticipantCommitted {
		t.Fatalf("committed record = %#v", committed)
	}
	if repeated, err := participant.Commit(proposal); err != nil || repeated != committed {
		t.Fatalf("repeated Commit() = %#v, %v; want idempotent result", repeated, err)
	}
	if _, err := participant.Abort(proposal); !errors.Is(err, ErrClusterWriteCommitParticipantCommitted) {
		t.Fatalf("Abort() after commit error = %v, want committed error", err)
	}
	if got, ok := participant.Status("tx-1"); !ok || got != committed {
		t.Fatalf("Status() = %#v, %t; want committed record", got, ok)
	}
}

func TestClusterWriteCommitParticipantSnapshotIsDeterministicAndAtomic(t *testing.T) {
	participant, err := NewClusterWriteCommitParticipant(ClusterWriteCommitParticipantOptions{MaxRecords: 4})
	if err != nil {
		t.Fatal(err)
	}
	committed := ClusterWriteCommitProposal{TransactionID: "tx-committed", Sequence: 2}
	aborted := ClusterWriteCommitProposal{TransactionID: "tx-aborted", Sequence: 1}
	if _, err := participant.Prepare(committed); err != nil {
		t.Fatal(err)
	}
	if _, err := participant.Commit(committed); err != nil {
		t.Fatal(err)
	}
	if _, err := participant.Prepare(aborted); err != nil {
		t.Fatal(err)
	}
	if _, err := participant.Abort(aborted); err != nil {
		t.Fatal(err)
	}
	first, err := participant.MarshalSnapshot()
	if err != nil {
		t.Fatalf("MarshalSnapshot() error = %v", err)
	}
	second, err := participant.MarshalSnapshot()
	if err != nil {
		t.Fatalf("second MarshalSnapshot() error = %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("snapshot is not deterministic: %x != %x", first, second)
	}

	restored, err := NewClusterWriteCommitParticipant(ClusterWriteCommitParticipantOptions{MaxRecords: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.RestoreSnapshot(first); err != nil {
		t.Fatalf("RestoreSnapshot() error = %v", err)
	}
	if got, ok := restored.Status("tx-committed"); !ok || got.Phase != ClusterWriteCommitParticipantCommitted {
		t.Fatalf("restored committed status = %#v, %t", got, ok)
	}
	if got, ok := restored.Status("tx-aborted"); !ok || got.Phase != ClusterWriteCommitParticipantAborted {
		t.Fatalf("restored aborted status = %#v, %t", got, ok)
	}
	before, err := restored.MarshalSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	invalid := append(append([]byte(nil), first...), 0xff)
	if err := restored.RestoreSnapshot(invalid); !errors.Is(err, ErrClusterWriteCommitParticipantSnapshotInvalid) {
		t.Fatalf("invalid RestoreSnapshot() error = %v, want snapshot-invalid", err)
	}
	after, err := restored.MarshalSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("invalid snapshot restore changed participant state")
	}
}

func TestClusterWriteCommitParticipantRejectsMissingAndCapacity(t *testing.T) {
	participant, err := NewClusterWriteCommitParticipant(ClusterWriteCommitParticipantOptions{MaxRecords: 1})
	if err != nil {
		t.Fatal(err)
	}
	proposal := ClusterWriteCommitProposal{TransactionID: "tx-1"}
	if _, err := participant.Commit(proposal); !errors.Is(err, ErrClusterWriteCommitParticipantNotPrepared) {
		t.Fatalf("Commit() before Prepare() error = %v, want not-prepared", err)
	}
	if _, err := participant.Prepare(proposal); err != nil {
		t.Fatal(err)
	}
	if _, err := participant.Prepare(ClusterWriteCommitProposal{TransactionID: "tx-2"}); !errors.Is(err, ErrClusterWriteCommitParticipantCapacity) {
		t.Fatalf("capacity Prepare() error = %v, want capacity", err)
	}
}
