package hatReplication

import (
	"errors"
	"reflect"
	"testing"
)

func TestT207ReplicaEvictionRejoinRejectsStaleState(t *testing.T) {
	admission := newT207Admission(t, 4)
	member := addT207Member(t, admission, "node-a", "127.0.0.1:9001")

	eviction, err := admission.Evict(member.NodeID, member.Generation, "operator removal")
	if err != nil {
		t.Fatalf("evict: %v", err)
	}
	if eviction.EvictionEpoch == 0 || eviction.TopologyGeneration != member.Generation+1 {
		t.Fatalf("unexpected eviction: %+v", eviction)
	}

	stale := t207RejoinRequest(member.NodeID, member.Address, eviction.EvictionEpoch+1)
	if _, err := admission.PrepareRejoin(stale); !errors.Is(err, ErrReplicaJoinAdmissionStaleEviction) {
		t.Fatalf("stale rejoin error = %v, want %v", err, ErrReplicaJoinAdmissionStaleEviction)
	}
	if _, err := admission.Prepare(stale.ReplicaJoinRequest); !errors.Is(err, ErrReplicaJoinAdmissionEvictionRequired) {
		t.Fatalf("normal join after eviction error = %v, want %v", err, ErrReplicaJoinAdmissionEvictionRequired)
	}

	rejoin := t207RejoinRequest(member.NodeID, member.Address, eviction.EvictionEpoch)
	decision, err := admission.PrepareRejoin(rejoin)
	if err != nil {
		t.Fatalf("prepare rejoin: %v", err)
	}
	retry, err := admission.PrepareRejoin(rejoin)
	if err != nil {
		t.Fatalf("retry prepare rejoin: %v", err)
	}
	if !reflect.DeepEqual(decision, retry) {
		t.Fatalf("retry decision differs:\nfirst=%+v\nretry=%+v", decision, retry)
	}

	if _, err := admission.CommitRejoin(decision, SnapshotWALBootstrapState{
		Phase: SnapshotWALBootstrapPhaseReady,
		Plan:  decision.BootstrapPlan,
	}); !errors.Is(err, ErrReplicaJoinAdmissionBootstrapNotActive) {
		t.Fatalf("commit before activation error = %v, want %v", err, ErrReplicaJoinAdmissionBootstrapNotActive)
	}

	rejoined := commitT207Rejoin(t, admission, decision)
	if rejoined.NodeID != member.NodeID || rejoined.Generation != eviction.TopologyGeneration+1 {
		t.Fatalf("unexpected rejoined member: %+v", rejoined)
	}
	if _, err := admission.PrepareRejoin(rejoin); !errors.Is(err, ErrReplicaJoinAdmissionAlreadyPresent) {
		t.Fatalf("rejoin after commit error = %v, want %v", err, ErrReplicaJoinAdmissionAlreadyPresent)
	}
	if snapshot := admission.RecoverySnapshot(); len(snapshot.Evictions) != 0 || snapshot.PendingRejoins != 0 {
		t.Fatalf("recovery snapshot after commit = %+v", snapshot)
	}
}

func TestT207ReplicaRejoinGenerationFenceAndAbortPreserveTombstone(t *testing.T) {
	admission := newT207Admission(t, 4)
	member := addT207Member(t, admission, "node-a", "127.0.0.1:9001")
	eviction, err := admission.Evict(member.NodeID, member.Generation, "replace disk")
	if err != nil {
		t.Fatalf("evict: %v", err)
	}
	rejoin := t207RejoinRequest(member.NodeID, member.Address, eviction.EvictionEpoch)
	decision, err := admission.PrepareRejoin(rejoin)
	if err != nil {
		t.Fatalf("prepare rejoin: %v", err)
	}
	if err := admission.AbortRejoin(decision); err != nil {
		t.Fatalf("abort rejoin: %v", err)
	}
	if snapshot := admission.RecoverySnapshot(); len(snapshot.Evictions) != 1 || snapshot.PendingRejoins != 0 {
		t.Fatalf("recovery snapshot after abort = %+v", snapshot)
	}

	decision, err = admission.PrepareRejoin(rejoin)
	if err != nil {
		t.Fatalf("prepare rejoin after abort: %v", err)
	}
	other := addT207Member(t, admission, "node-b", "127.0.0.1:9002")
	if other.NodeID != "node-b" {
		t.Fatalf("unexpected other member: %+v", other)
	}
	if _, err := admission.CommitRejoin(decision, t207ActiveBootstrap(t, decision.BootstrapPlan)); !errors.Is(err, ErrReplicaJoinAdmissionGeneration) {
		t.Fatalf("stale rejoin commit error = %v, want %v", err, ErrReplicaJoinAdmissionGeneration)
	}
}

func TestT207ReplicaEvictionHistoryIsBoundedAndDetached(t *testing.T) {
	admission := newT207AdmissionWithLimits(t, 4, 1)
	first := addT207Member(t, admission, "node-a", "127.0.0.1:9001")
	second := addT207Member(t, admission, "node-b", "127.0.0.1:9002")
	if _, err := admission.Evict(first.NodeID, first.Generation, "first"); err != nil {
		t.Fatalf("first eviction: %v", err)
	}
	if _, err := admission.Evict(second.NodeID, second.Generation, "second"); !errors.Is(err, ErrReplicaJoinAdmissionEvictionLimit) {
		t.Fatalf("second eviction error = %v, want %v", err, ErrReplicaJoinAdmissionEvictionLimit)
	}

	snapshot := admission.RecoverySnapshot()
	if len(snapshot.Evictions) != 1 || snapshot.Evictions[0].NodeID != first.NodeID {
		t.Fatalf("unexpected recovery snapshot: %+v", snapshot)
	}
	snapshot.Evictions[0].Reason = "mutated"
	if got := admission.RecoverySnapshot().Evictions[0].Reason; got != "first" {
		t.Fatalf("recovery snapshot was not detached: %q", got)
	}
}

func newT207Admission(t *testing.T, maxMembers int) *ReplicaJoinAdmission {
	t.Helper()
	return newT207AdmissionWithLimits(t, maxMembers, 4)
}

func newT207AdmissionWithLimits(t *testing.T, maxMembers, maxEvictions int) *ReplicaJoinAdmission {
	t.Helper()
	admission, err := NewReplicaJoinAdmission(ReplicaJoinAdmissionOptions{
		MaxMembers:    maxMembers,
		MaxCandidates: 4,
		MaxEvictions:  maxEvictions,
	})
	if err != nil {
		t.Fatalf("new admission: %v", err)
	}
	return admission
}

func addT207Member(t *testing.T, admission *ReplicaJoinAdmission, nodeID, address string) ReplicaJoinMember {
	t.Helper()
	request := ReplicaJoinRequest{
		JoinerID:              nodeID,
		Address:               address,
		SnapshotID:            "snapshot-1",
		StorageGeneration:     7,
		TargetJournalSequence: 120,
		FencingToken:          9,
		Candidates: []ReplicaJoinCandidate{{
			NodeID:                  "source-a",
			Address:                 "127.0.0.1:8001",
			Healthy:                 true,
			StorageGeneration:       7,
			SnapshotJournalSequence: 100,
			AppliedJournalSequence:  120,
			AvailableThrough:        120,
		}},
	}
	decision, err := admission.Prepare(request)
	if err != nil {
		t.Fatalf("prepare %s: %v", nodeID, err)
	}
	member, err := admission.Commit(decision, t207ActiveBootstrap(t, decision.BootstrapPlan))
	if err != nil {
		t.Fatalf("commit %s: %v", nodeID, err)
	}
	return member
}

func t207RejoinRequest(nodeID, address string, evictionEpoch uint64) ReplicaRejoinRequest {
	return ReplicaRejoinRequest{
		ReplicaJoinRequest: ReplicaJoinRequest{
			JoinerID:              nodeID,
			Address:               address,
			SnapshotID:            "snapshot-2",
			StorageGeneration:     7,
			TargetJournalSequence: 140,
			FencingToken:          10,
			Candidates: []ReplicaJoinCandidate{{
				NodeID:                  "source-a",
				Address:                 "127.0.0.1:8001",
				Healthy:                 true,
				StorageGeneration:       7,
				SnapshotJournalSequence: 120,
				AppliedJournalSequence:  140,
				AvailableThrough:        140,
			}},
		},
		EvictionEpoch: evictionEpoch,
	}
}

func commitT207Rejoin(t *testing.T, admission *ReplicaJoinAdmission, decision ReplicaRejoinDecision) ReplicaJoinMember {
	t.Helper()
	member, err := admission.CommitRejoin(decision, t207ActiveBootstrap(t, decision.BootstrapPlan))
	if err != nil {
		t.Fatalf("commit rejoin: %v", err)
	}
	return member
}

func t207ActiveBootstrap(t *testing.T, plan SnapshotWALBootstrapPlan) SnapshotWALBootstrapState {
	t.Helper()
	coordinator, err := NewSnapshotWALBootstrapCoordinator(SnapshotWALBootstrapOptions{
		MaxWALGap: 100,
	})
	if err != nil {
		t.Fatalf("new bootstrap coordinator: %v", err)
	}
	state, err := coordinator.Begin(plan)
	if err != nil {
		t.Fatalf("begin bootstrap: %v", err)
	}
	state, err = coordinator.InstallSnapshot(plan.SnapshotID, plan.StorageGeneration, plan.SnapshotJournalSequence, plan.FencingToken)
	if err != nil {
		t.Fatalf("install snapshot: %v", err)
	}
	state, err = coordinator.AdvanceWAL(plan.TargetJournalSequence, plan.FencingToken)
	if err != nil {
		t.Fatalf("advance WAL: %v", err)
	}
	state, err = coordinator.MarkReady(state.Generation, plan.FencingToken)
	if err != nil {
		t.Fatalf("mark ready: %v", err)
	}
	state, err = coordinator.Activate(state.Generation, plan.FencingToken)
	if err != nil {
		t.Fatalf("activate bootstrap: %v", err)
	}
	return state
}
