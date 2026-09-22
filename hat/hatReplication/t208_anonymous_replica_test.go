//go:build t208
// +build t208

package hatReplication

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestT208AnonymousReplicaIsExcludedFromQuorum(t *testing.T) {
	admission := newT208Admission(t)
	voter := addT208Member(t, admission, "voter-a", "127.0.0.1:9101", ReplicaRoleVoter)
	anonymous := addT208Member(t, admission, "reader-a", "127.0.0.1:9102", ReplicaRoleAnonymous)
	if voter.Role != ReplicaRoleVoter || anonymous.Role != ReplicaRoleAnonymous {
		t.Fatalf("unexpected member roles: voter=%+v anonymous=%+v", voter, anonymous)
	}

	roster := admission.RoleRoster()
	if !reflect.DeepEqual(roster.Voters, []string{"voter-a"}) || !reflect.DeepEqual(roster.Anonymous, []string{"reader-a"}) {
		t.Fatalf("unexpected role roster: %+v", roster)
	}

	result, err := ExecuteVoterWriteQuorum(context.Background(), roster, []string{"voter-a"}, 1, func(context.Context, string) error {
		return nil
	})
	if err != nil || !result.Decision.Satisfied || result.Decision.Acknowledged != 1 {
		t.Fatalf("voter quorum result=%+v err=%v", result, err)
	}
	if _, err := ExecuteVoterWriteQuorum(context.Background(), roster, []string{"voter-a", "reader-a"}, 1, func(context.Context, string) error {
		return nil
	}); !errors.Is(err, ErrReplicaQuorumAnonymousTarget) {
		t.Fatalf("anonymous quorum error=%v, want %v", err, ErrReplicaQuorumAnonymousTarget)
	}
	if _, err := ExecuteVoterWriteQuorum(context.Background(), roster, []string{"unknown"}, 1, func(context.Context, string) error {
		return nil
	}); !errors.Is(err, ErrReplicaQuorumUnknownTarget) {
		t.Fatalf("unknown quorum error=%v, want %v", err, ErrReplicaQuorumUnknownTarget)
	}
}

func TestT208AnonymousRoleIsFencedAcrossRejoin(t *testing.T) {
	admission := newT208Admission(t)
	member := addT208Member(t, admission, "reader-a", "127.0.0.1:9102", ReplicaRoleAnonymous)
	eviction, err := admission.Evict(member.NodeID, member.Generation, "replace reader")
	if err != nil {
		t.Fatalf("evict anonymous replica: %v", err)
	}
	request := t208RejoinRequest(member.NodeID, member.Address, eviction.EvictionEpoch, ReplicaRoleVoter)
	if _, err := admission.PrepareRejoin(request); !errors.Is(err, ErrReplicaJoinAdmissionRoleMismatch) {
		t.Fatalf("role-changing rejoin error=%v, want %v", err, ErrReplicaJoinAdmissionRoleMismatch)
	}
	request.Role = ReplicaRoleAnonymous
	decision, err := admission.PrepareRejoin(request)
	if err != nil {
		t.Fatalf("anonymous rejoin: %v", err)
	}
	rejoined, err := admission.CommitRejoin(decision, t208ActiveBootstrap(t, decision.BootstrapPlan))
	if err != nil {
		t.Fatalf("commit anonymous rejoin: %v", err)
	}
	if rejoined.Role != ReplicaRoleAnonymous {
		t.Fatalf("rejoined role=%v, want anonymous", rejoined.Role)
	}
}

func TestT208RoleRosterIsDetachedAndRejectsOverlap(t *testing.T) {
	admission := newT208Admission(t)
	addT208Member(t, admission, "voter-a", "127.0.0.1:9101", ReplicaRoleVoter)
	addT208Member(t, admission, "reader-a", "127.0.0.1:9102", ReplicaRoleAnonymous)
	roster := admission.RoleRoster()
	roster.Voters[0] = "mutated"
	if _, err := ExecuteVoterWriteQuorum(context.Background(), admission.RoleRoster(), []string{"voter-a"}, 1, func(context.Context, string) error {
		return nil
	}); err != nil {
		t.Fatalf("detached caller-owned roster should remain usable: %v", err)
	}
	if _, err := ExecuteVoterWriteQuorum(context.Background(), ReplicaRoleRoster{
		Voters:    []string{"same"},
		Anonymous: []string{"same"},
	}, []string{"same"}, 1, func(context.Context, string) error {
		return nil
	}); !errors.Is(err, ErrReplicaRoleRosterInvalid) {
		t.Fatalf("overlapping roster error=%v, want %v", err, ErrReplicaRoleRosterInvalid)
	}
}

func newT208Admission(t *testing.T) *ReplicaJoinAdmission {
	t.Helper()
	admission, err := NewReplicaJoinAdmission(ReplicaJoinAdmissionOptions{
		MaxMembers:    8,
		MaxCandidates: 4,
		MaxEvictions:  4,
	})
	if err != nil {
		t.Fatalf("new admission: %v", err)
	}
	return admission
}

func addT208Member(t *testing.T, admission *ReplicaJoinAdmission, nodeID, address string, role ReplicaRole) ReplicaJoinMember {
	t.Helper()
	request := ReplicaJoinRequest{
		JoinerID:              nodeID,
		Address:               address,
		SnapshotID:            "snapshot-1",
		StorageGeneration:     7,
		TargetJournalSequence: 120,
		FencingToken:          9,
		Role:                  role,
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
	member, err := admission.Commit(decision, t208ActiveBootstrap(t, decision.BootstrapPlan))
	if err != nil {
		t.Fatalf("commit %s: %v", nodeID, err)
	}
	return member
}

func t208RejoinRequest(nodeID, address string, evictionEpoch uint64, role ReplicaRole) ReplicaRejoinRequest {
	return ReplicaRejoinRequest{
		ReplicaJoinRequest: ReplicaJoinRequest{
			JoinerID:              nodeID,
			Address:               address,
			SnapshotID:            "snapshot-2",
			StorageGeneration:     7,
			TargetJournalSequence: 140,
			FencingToken:          10,
			Role:                  role,
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

func t208ActiveBootstrap(t *testing.T, plan SnapshotWALBootstrapPlan) SnapshotWALBootstrapState {
	t.Helper()
	coordinator, err := NewSnapshotWALBootstrapCoordinator(SnapshotWALBootstrapOptions{MaxWALGap: 100})
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
