package hatReplication

import "testing"

func newT047PreparedParticipant(b *testing.B, count int) (*ClusterWriteCommitParticipant, []ClusterWriteCommitProposal, []byte) {
	b.Helper()
	participant, err := NewClusterWriteCommitParticipant(ClusterWriteCommitParticipantOptions{MaxRecords: count + 1})
	if err != nil {
		b.Fatal(err)
	}
	proposals := make([]ClusterWriteCommitProposal, count)
	for index := range proposals {
		proposals[index] = ClusterWriteCommitProposal{TransactionID: "tx-" + string(rune('a'+index))}
		if _, err := participant.Prepare(proposals[index]); err != nil {
			b.Fatal(err)
		}
	}
	snapshot, err := participant.MarshalSnapshot()
	if err != nil {
		b.Fatal(err)
	}
	return participant, proposals, snapshot
}

func BenchmarkTU047ParticipantStatusLoop(b *testing.B) {
	participant, proposals, snapshot := newT047PreparedParticipant(b, 32)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		b.StopTimer()
		if err := participant.RestoreSnapshot(snapshot); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		for _, proposal := range proposals {
			record, ok := participant.Status(proposal.TransactionID)
			if !ok || record.Phase != ClusterWriteCommitParticipantPrepared {
				b.Fatalf("status = %#v/%v", record, ok)
			}
			if _, err := participant.Commit(proposal); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkTU047ParticipantBatchReconcile(b *testing.B) {
	participant, proposals, snapshot := newT047PreparedParticipant(b, 32)
	decisions := make([]ClusterWriteCommitParticipantReconcileDecision, len(proposals))
	for index, proposal := range proposals {
		decisions[index] = ClusterWriteCommitParticipantReconcileDecision{
			Proposal: proposal,
			Phase:    ClusterWriteCommitParticipantCommitted,
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		b.StopTimer()
		if err := participant.RestoreSnapshot(snapshot); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		result, err := participant.Reconcile(decisions)
		if err != nil || result.Committed != len(decisions) {
			b.Fatalf("result = %#v/%v", result, err)
		}
	}
}
