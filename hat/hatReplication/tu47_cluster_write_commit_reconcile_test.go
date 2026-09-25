package hatReplication

import (
	"errors"
	"testing"
)

func TestClusterWriteCommitParticipantReconcileBatch(t *testing.T) {
	participant, err := NewClusterWriteCommitParticipant(ClusterWriteCommitParticipantOptions{MaxRecords: 8})
	if err != nil {
		t.Fatal(err)
	}
	proposals := []ClusterWriteCommitProposal{
		{TransactionID: "tx-a", Sequence: 1},
		{TransactionID: "tx-b", Sequence: 2},
		{TransactionID: "tx-c", Sequence: 3},
	}
	for _, proposal := range proposals {
		if _, err := participant.Prepare(proposal); err != nil {
			t.Fatal(err)
		}
	}
	pending := participant.PreparedRecords()
	if len(pending) != len(proposals) || pending[0].Proposal.TransactionID != "tx-a" || pending[2].Proposal.TransactionID != "tx-c" {
		t.Fatalf("PreparedRecords() = %#v, want sorted prepared records", pending)
	}

	decisions := []ClusterWriteCommitParticipantReconcileDecision{
		{Proposal: proposals[0], Phase: ClusterWriteCommitParticipantCommitted},
		{Proposal: proposals[1], Phase: ClusterWriteCommitParticipantAborted},
	}
	result, err := participant.Reconcile(decisions)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.Requested != 2 || result.Committed != 1 || result.Aborted != 1 || result.AlreadyTerminal != 0 {
		t.Fatalf("Reconcile() result = %#v", result)
	}
	if record, ok := participant.Status("tx-a"); !ok || record.Phase != ClusterWriteCommitParticipantCommitted {
		t.Fatalf("committed status = %#v/%v", record, ok)
	}
	if record, ok := participant.Status("tx-b"); !ok || record.Phase != ClusterWriteCommitParticipantAborted {
		t.Fatalf("aborted status = %#v/%v", record, ok)
	}

	repeated, err := participant.Reconcile(decisions)
	if err != nil {
		t.Fatalf("repeated Reconcile() error = %v", err)
	}
	if repeated.AlreadyTerminal != 2 || repeated.Committed != 0 || repeated.Aborted != 0 {
		t.Fatalf("repeated Reconcile() result = %#v", repeated)
	}

	atomic, err := NewClusterWriteCommitParticipant(ClusterWriteCommitParticipantOptions{MaxRecords: 8})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := atomic.Prepare(proposals[2]); err != nil {
		t.Fatal(err)
	}
	_, err = atomic.Reconcile([]ClusterWriteCommitParticipantReconcileDecision{
		{Proposal: proposals[2], Phase: ClusterWriteCommitParticipantCommitted},
		{Proposal: ClusterWriteCommitProposal{TransactionID: "tx-z"}, Phase: ClusterWriteCommitParticipantAborted},
	})
	if !errors.Is(err, ErrClusterWriteCommitParticipantUnknownTransaction) {
		t.Fatalf("atomic invalid Reconcile() error = %v, want unknown transaction", err)
	}
	if record, ok := atomic.Status("tx-c"); !ok || record.Phase != ClusterWriteCommitParticipantPrepared {
		t.Fatalf("atomic failure changed state = %#v/%v", record, ok)
	}

	_, err = participant.Reconcile([]ClusterWriteCommitParticipantReconcileDecision{
		{Proposal: proposals[1], Phase: ClusterWriteCommitParticipantAborted},
		{Proposal: proposals[0], Phase: ClusterWriteCommitParticipantCommitted},
	})
	if !errors.Is(err, ErrClusterWriteCommitParticipantReconcileOrder) {
		t.Fatalf("out-of-order Reconcile() error = %v, want order error", err)
	}
}
