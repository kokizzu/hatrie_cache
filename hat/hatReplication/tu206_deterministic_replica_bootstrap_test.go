package hatReplication

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func TestPlanReplicaBootstrapIsDeterministicAndPrefersFreshLocalSource(t *testing.T) {
	request := ReplicaBootstrapRequest{
		JoinerID:                  " node-z ",
		RequiredStorageGeneration: 7,
		MinimumJournalSequence:    100,
		PreferredRegions:          []string{"sg"},
		MaxWALGap:                 20,
	}
	sources := []ReplicaBootstrapSource{
		{
			NodeID:                  "node-c",
			Address:                 "10.0.0.3:9000",
			Region:                  "us",
			Ready:                   true,
			SnapshotID:              "snapshot-c",
			StorageGeneration:       7,
			SnapshotJournalSequence: 101,
			CurrentJournalSequence:  112,
			FencingToken:            3,
		},
		{
			NodeID:                  "node-b",
			Address:                 "10.0.0.2:9000",
			Region:                  "sg",
			Ready:                   true,
			SnapshotID:              "snapshot-b",
			StorageGeneration:       7,
			SnapshotJournalSequence: 96,
			CurrentJournalSequence:  112,
			FencingToken:            2,
		},
		{
			NodeID:                  "node-a",
			Address:                 "10.0.0.1:9000",
			Region:                  "sg",
			Ready:                   true,
			SnapshotID:              "snapshot-a",
			StorageGeneration:       7,
			SnapshotJournalSequence: 100,
			CurrentJournalSequence:  112,
			FencingToken:            1,
		},
	}

	plan, err := PlanReplicaBootstrap(request, sources)
	if err != nil {
		t.Fatalf("PlanReplicaBootstrap() error = %v", err)
	}
	if plan.Source.NodeID != "node-a" {
		t.Fatalf("selected source = %q, want node-a", plan.Source.NodeID)
	}
	if plan.Bootstrap.JoinerID != "node-z" || plan.Bootstrap.SourceID != "node-a" {
		t.Fatalf("bootstrap identities = %+v", plan.Bootstrap)
	}
	if plan.Bootstrap.SnapshotJournalSequence != 100 || plan.Bootstrap.TargetJournalSequence != 112 || plan.WALGap != 12 {
		t.Fatalf("bootstrap boundary = %+v, gap = %d", plan.Bootstrap, plan.WALGap)
	}

	reversed := []ReplicaBootstrapSource{sources[2], sources[1], sources[0]}
	repeated, err := PlanReplicaBootstrap(request, reversed)
	if err != nil {
		t.Fatalf("reordered PlanReplicaBootstrap() error = %v", err)
	}
	if !reflect.DeepEqual(plan, repeated) {
		t.Fatalf("plan changed with source order:\nfirst=%+v\nsecond=%+v", plan, repeated)
	}
}

func TestPlanReplicaBootstrapRejectsUnsafeOrAmbiguousSources(t *testing.T) {
	base := ReplicaBootstrapRequest{JoinerID: "node-z", RequiredStorageGeneration: 7, MaxWALGap: 10}
	valid := ReplicaBootstrapSource{
		NodeID:                  "node-a",
		Ready:                   true,
		SnapshotID:              "snapshot-a",
		StorageGeneration:       7,
		SnapshotJournalSequence: 10,
		CurrentJournalSequence:  15,
		FencingToken:            1,
	}

	tests := []struct {
		name    string
		sources []ReplicaBootstrapSource
		wantErr error
	}{
		{name: "no source", sources: nil, wantErr: ErrReplicaBootstrapNoSource},
		{name: "joiner cannot source itself", sources: []ReplicaBootstrapSource{{NodeID: "node-z"}}, wantErr: ErrReplicaBootstrapNoSource},
		{name: "duplicate source identity", sources: []ReplicaBootstrapSource{valid, valid}, wantErr: ErrReplicaBootstrapDuplicateSource},
		{name: "gap exceeds bound", sources: []ReplicaBootstrapSource{{
			NodeID:                  "node-a",
			Ready:                   true,
			SnapshotID:              "snapshot-a",
			StorageGeneration:       7,
			SnapshotJournalSequence: 1,
			CurrentJournalSequence:  15,
			FencingToken:            1,
		}}, wantErr: ErrReplicaBootstrapNoSource},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := PlanReplicaBootstrap(base, test.sources)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestReplicaBootstrapWorkflowBindsPlanToLifecycleCoordinator(t *testing.T) {
	plan, err := PlanReplicaBootstrap(
		ReplicaBootstrapRequest{JoinerID: "node-b", RequiredStorageGeneration: 2},
		[]ReplicaBootstrapSource{{
			NodeID:                  "node-a",
			Address:                 "10.0.0.1:9000",
			Ready:                   true,
			SnapshotID:              "snapshot-1",
			StorageGeneration:       2,
			SnapshotJournalSequence: 20,
			CurrentJournalSequence:  20,
			FencingToken:            8,
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	workflow, err := NewReplicaBootstrapWorkflow(plan)
	if err != nil {
		t.Fatal(err)
	}
	state, err := workflow.Begin()
	if err != nil || state.Phase != SnapshotWALBootstrapPhaseSnapshotPending {
		t.Fatalf("Begin() = %+v, %v", state, err)
	}
	state, err = workflow.Coordinator().InstallSnapshot("snapshot-1", 2, 20, 8)
	if err != nil || state.Phase != SnapshotWALBootstrapPhaseCatchingUp {
		t.Fatalf("InstallSnapshot() = %+v, %v", state, err)
	}
	state, err = workflow.Coordinator().MarkReady(state.Generation, 8)
	if err != nil || state.Phase != SnapshotWALBootstrapPhaseReady {
		t.Fatalf("MarkReady() = %+v, %v", state, err)
	}
}

func TestPlanReplicaBootstrapOverflowSetStillRejectsDuplicateIdentity(t *testing.T) {
	sources := make([]ReplicaBootstrapSource, replicaBootstrapInlineSourceLimit+1)
	for index := range sources {
		sources[index] = ReplicaBootstrapSource{
			NodeID:                  fmt.Sprintf("node-%d", index),
			Ready:                   true,
			SnapshotID:              "snapshot-1",
			StorageGeneration:       1,
			SnapshotJournalSequence: 20,
			CurrentJournalSequence:  21,
			FencingToken:            uint64(index + 1),
		}
	}
	request := ReplicaBootstrapRequest{JoinerID: "joiner", RequiredStorageGeneration: 1}
	if _, err := PlanReplicaBootstrap(request, sources); err != nil {
		t.Fatalf("unique overflow source set error = %v", err)
	}
	sources[len(sources)-1].NodeID = sources[0].NodeID
	if _, err := PlanReplicaBootstrap(request, sources); !errors.Is(err, ErrReplicaBootstrapDuplicateSource) {
		t.Fatalf("duplicate overflow source error = %v", err)
	}
}
