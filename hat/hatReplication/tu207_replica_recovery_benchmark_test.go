package hatReplication

import "testing"

var tu207PlanSink ReplicaRejoinPlan
var tu207SnapshotSink ReplicaRecoverySnapshot

func BenchmarkTU207EvaluateRejoin(b *testing.B) {
	protocol := newTU207BenchmarkProtocol(b)
	request := ReplicaRejoinRequest{
		NodeID:              "node-a",
		Incarnation:         1,
		LastAppliedSequence: 100,
		StorageGeneration:   7,
	}
	source := ReplicaRecoverySource{
		CurrentJournalSequence: 110,
		RetainedFromSequence:   90,
		StorageGeneration:      7,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		plan, err := protocol.EvaluateRejoin(request, source)
		if err != nil {
			b.Fatal(err)
		}
		tu207PlanSink = plan
	}
}

func BenchmarkTU207RecoverySnapshot(b *testing.B) {
	protocol := newTU207BenchmarkProtocol(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		tu207SnapshotSink = protocol.Snapshot()
	}
}

func newTU207BenchmarkProtocol(b testing.TB) *ReplicaRecoveryProtocol {
	b.Helper()
	protocol, err := NewReplicaRecoveryProtocol(ReplicaRecoveryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := protocol.Admit(ReplicaRecoveryNode{
		NodeID:              "node-a",
		Incarnation:         1,
		LastAppliedSequence: 100,
		StorageGeneration:   7,
	}, 0, 1); err != nil {
		b.Fatal(err)
	}
	return protocol
}
