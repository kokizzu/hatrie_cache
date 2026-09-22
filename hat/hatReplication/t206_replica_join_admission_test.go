package hatReplication

import (
	"errors"
	"testing"
)

func TestReplicaJoinAdmissionSelectsDeterministicSourceAndRequiresActiveBootstrap(t *testing.T) {
	admission, err := NewReplicaJoinAdmission(ReplicaJoinAdmissionOptions{
		MaxMembers:    4,
		MaxCandidates: 4,
	})
	if err != nil {
		t.Fatalf("NewReplicaJoinAdmission() error = %v", err)
	}
	request := ReplicaJoinRequest{
		JoinerID:              "node-c",
		Address:               "https://node-c",
		SnapshotID:            "snapshot-9",
		StorageGeneration:     7,
		TargetJournalSequence: 120,
		FencingToken:          11,
		Candidates: []ReplicaJoinCandidate{
			{NodeID: "node-b", Address: "https://node-b", Healthy: true, StorageGeneration: 7, SnapshotJournalSequence: 100, AppliedJournalSequence: 115, AvailableThrough: 130},
			{NodeID: "node-a", Address: "https://node-a", Healthy: true, StorageGeneration: 7, SnapshotJournalSequence: 100, AppliedJournalSequence: 115, AvailableThrough: 130},
			{NodeID: "node-d", Address: "https://node-d", Healthy: false, StorageGeneration: 7, SnapshotJournalSequence: 100, AppliedJournalSequence: 120, AvailableThrough: 130},
		},
	}
	decision, err := admission.Prepare(request)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if decision.SourceID != "node-a" || decision.SourceAddress != "https://node-a" {
		t.Fatalf("decision source = %+v, want deterministic node-a", decision)
	}
	if decision.BootstrapPlan.SnapshotJournalSequence != 100 || decision.BootstrapPlan.TargetJournalSequence != 120 {
		t.Fatalf("bootstrap plan = %+v", decision.BootstrapPlan)
	}
	if _, err := admission.Commit(decision, SnapshotWALBootstrapState{}); !errors.Is(err, ErrReplicaJoinAdmissionBootstrapNotActive) {
		t.Fatalf("inactive bootstrap error = %v, want bootstrap-not-active", err)
	}
	active := SnapshotWALBootstrapState{
		Phase:                  SnapshotWALBootstrapPhaseActive,
		Plan:                   decision.BootstrapPlan,
		AppliedJournalSequence: 120,
	}
	member, err := admission.Commit(decision, active)
	if err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if member.NodeID != "node-c" || member.Address != "https://node-c" || member.Generation != 1 {
		t.Fatalf("member = %+v, want node-c at generation 1", member)
	}
	if _, err := admission.Prepare(request); !errors.Is(err, ErrReplicaJoinAdmissionAlreadyPresent) {
		t.Fatalf("duplicate join error = %v, want already-present", err)
	}
}

func TestReplicaJoinAdmissionRejectsDuplicateIdentityAndStaleGeneration(t *testing.T) {
	admission, err := NewReplicaJoinAdmission(ReplicaJoinAdmissionOptions{MaxMembers: 4})
	if err != nil {
		t.Fatalf("NewReplicaJoinAdmission() error = %v", err)
	}
	first := validReplicaJoinRequest("node-a", "https://node-a", 1)
	firstDecision, err := admission.Prepare(first)
	if err != nil {
		t.Fatal(err)
	}
	second := validReplicaJoinRequest("node-b", "https://node-b", 2)
	secondDecision, err := admission.Prepare(second)
	if err != nil {
		t.Fatal(err)
	}
	activeFirst := activeReplicaJoinState(firstDecision)
	if _, err := admission.Commit(firstDecision, activeFirst); err != nil {
		t.Fatalf("first Commit() error = %v", err)
	}
	if _, err := admission.Prepare(validReplicaJoinRequest("node-a", "https://other", 3)); !errors.Is(err, ErrReplicaJoinAdmissionAlreadyPresent) {
		t.Fatalf("duplicate node error = %v, want already-present", err)
	}
	if _, err := admission.Prepare(validReplicaJoinRequest("node-c", "https://node-a", 4)); !errors.Is(err, ErrReplicaJoinAdmissionAddressInUse) {
		t.Fatalf("duplicate address error = %v, want address-in-use", err)
	}
	if _, err := admission.Commit(secondDecision, activeReplicaJoinState(secondDecision)); !errors.Is(err, ErrReplicaJoinAdmissionGeneration) {
		t.Fatalf("stale generation error = %v, want generation error", err)
	}
	if err := admission.Abort(secondDecision); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
}

func TestReplicaJoinAdmissionPrepareRetryIsIdempotent(t *testing.T) {
	admission, err := NewReplicaJoinAdmission(ReplicaJoinAdmissionOptions{})
	if err != nil {
		t.Fatalf("NewReplicaJoinAdmission() error = %v", err)
	}
	request := validReplicaJoinRequest("node-a", "https://node-a", 1)
	first, err := admission.Prepare(request)
	if err != nil {
		t.Fatal(err)
	}
	request.Candidates = []ReplicaJoinCandidate{request.Candidates[0]}
	second, err := admission.Prepare(request)
	if err != nil {
		t.Fatalf("retry Prepare() error = %v", err)
	}
	if second != first {
		t.Fatalf("retry decision = %+v, want exact original %+v", second, first)
	}
}

func TestReplicaJoinAdmissionRejectsInvalidRequestsAndNoSource(t *testing.T) {
	admission, err := NewReplicaJoinAdmission(ReplicaJoinAdmissionOptions{MaxCandidates: 2})
	if err != nil {
		t.Fatalf("NewReplicaJoinAdmission() error = %v", err)
	}
	invalid := validReplicaJoinRequest("node-a", "https://node-a", 1)
	invalid.JoinerID = ""
	if _, err := admission.Prepare(invalid); !errors.Is(err, ErrReplicaJoinAdmissionInvalid) {
		t.Fatalf("blank node error = %v, want invalid", err)
	}
	noSource := validReplicaJoinRequest("node-a", "https://node-a", 1)
	noSource.Candidates = []ReplicaJoinCandidate{{
		NodeID: "node-source", Address: "https://source", Healthy: true,
		StorageGeneration: 2, SnapshotJournalSequence: 10, AppliedJournalSequence: 10, AvailableThrough: 20,
	}}
	if _, err := admission.Prepare(noSource); !errors.Is(err, ErrReplicaJoinAdmissionNoSource) {
		t.Fatalf("no source error = %v, want no-source", err)
	}
	duplicateCandidates := validReplicaJoinRequest("node-b", "https://node-b", 2)
	duplicateCandidates.Candidates = append(duplicateCandidates.Candidates, duplicateCandidates.Candidates[0])
	if _, err := admission.Prepare(duplicateCandidates); !errors.Is(err, ErrReplicaJoinAdmissionInvalid) {
		t.Fatalf("duplicate candidate error = %v, want invalid", err)
	}
}

func TestReplicaJoinAdmissionSnapshotIsSortedAndDetached(t *testing.T) {
	admission, err := NewReplicaJoinAdmission(ReplicaJoinAdmissionOptions{})
	if err != nil {
		t.Fatalf("NewReplicaJoinAdmission() error = %v", err)
	}
	for _, request := range []ReplicaJoinRequest{
		validReplicaJoinRequest("node-z", "https://node-z", 1),
		validReplicaJoinRequest("node-a", "https://node-a", 2),
	} {
		decision, err := admission.Prepare(request)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := admission.Commit(decision, activeReplicaJoinState(decision)); err != nil {
			t.Fatal(err)
		}
	}
	snapshot := admission.Snapshot()
	if len(snapshot.Members) != 2 || snapshot.Members[0].NodeID != "node-a" || snapshot.Members[1].NodeID != "node-z" {
		t.Fatalf("snapshot = %+v, want node-a then node-z", snapshot)
	}
	snapshot.Members[0].NodeID = "tampered"
	if admission.Snapshot().Members[0].NodeID != "node-a" {
		t.Fatal("Snapshot() exposed mutable member state")
	}
}

func validReplicaJoinRequest(joinerID, address string, token uint64) ReplicaJoinRequest {
	return ReplicaJoinRequest{
		JoinerID:              joinerID,
		Address:               address,
		SnapshotID:            "snapshot-1",
		StorageGeneration:     1,
		TargetJournalSequence: 20,
		FencingToken:          token,
		Candidates: []ReplicaJoinCandidate{
			{NodeID: "source", Address: "https://source", Healthy: true, StorageGeneration: 1, SnapshotJournalSequence: 10, AppliedJournalSequence: 20, AvailableThrough: 20},
		},
	}
}

func activeReplicaJoinState(decision ReplicaJoinDecision) SnapshotWALBootstrapState {
	return SnapshotWALBootstrapState{
		Phase:                  SnapshotWALBootstrapPhaseActive,
		Plan:                   decision.BootstrapPlan,
		AppliedJournalSequence: decision.BootstrapPlan.TargetJournalSequence,
	}
}
