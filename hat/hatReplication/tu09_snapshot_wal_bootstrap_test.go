package hatReplication

import (
	"errors"
	"sync"
	"testing"
)

func TestSnapshotWALBootstrapLifecycleFencesAndActivates(t *testing.T) {
	coordinator, err := NewSnapshotWALBootstrapCoordinator(SnapshotWALBootstrapOptions{})
	if err != nil {
		t.Fatal(err)
	}
	plan := SnapshotWALBootstrapPlan{
		JoinerID:                "node-b",
		SourceID:                "node-a",
		SnapshotID:              "snapshot-7",
		StorageGeneration:       4,
		SnapshotJournalSequence: 100,
		TargetJournalSequence:   103,
		FencingToken:            9,
	}
	state, err := coordinator.Begin(plan)
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	if state.Phase != SnapshotWALBootstrapPhaseSnapshotPending || state.Generation != 1 {
		t.Fatalf("Begin() state = %+v", state)
	}
	if _, err := coordinator.Begin(plan); !errors.Is(err, ErrSnapshotWALBootstrapAlreadyStarted) {
		t.Fatalf("second Begin() error = %v", err)
	}
	if _, err := coordinator.InstallSnapshot("snapshot-7", 4, 100, 8); !errors.Is(err, ErrSnapshotWALBootstrapFencing) {
		t.Fatalf("stale snapshot token error = %v", err)
	}
	state, err = coordinator.InstallSnapshot("snapshot-7", 4, 100, 9)
	if err != nil || state.Phase != SnapshotWALBootstrapPhaseCatchingUp || state.AppliedJournalSequence != 100 {
		t.Fatalf("InstallSnapshot() = %+v, %v", state, err)
	}
	if _, err := coordinator.AdvanceWAL(102, 9); err != nil {
		t.Fatalf("AdvanceWAL() error = %v", err)
	}
	if _, err := coordinator.MarkReady(state.Generation, 9); !errors.Is(err, ErrSnapshotWALBootstrapGeneration) {
		t.Fatalf("stale MarkReady() error = %v", err)
	}
	state = coordinator.Snapshot()
	state, err = coordinator.AdvanceWAL(103, 9)
	if err != nil || state.AppliedJournalSequence != 103 {
		t.Fatalf("final AdvanceWAL() = %+v, %v", state, err)
	}
	state, err = coordinator.MarkReady(state.Generation, 9)
	if err != nil || state.Phase != SnapshotWALBootstrapPhaseReady {
		t.Fatalf("MarkReady() = %+v, %v", state, err)
	}
	state, err = coordinator.Activate(state.Generation, 9)
	if err != nil || state.Phase != SnapshotWALBootstrapPhaseActive {
		t.Fatalf("Activate() = %+v, %v", state, err)
	}
	if _, err := coordinator.Activate(state.Generation, 9); !errors.Is(err, ErrSnapshotWALBootstrapPhase) {
		t.Fatalf("second Activate() error = %v", err)
	}
}

func TestSnapshotWALBootstrapRejectsInvalidPlansAndGaps(t *testing.T) {
	coordinator, err := NewSnapshotWALBootstrapCoordinator(SnapshotWALBootstrapOptions{MaxWALGap: 2})
	if err != nil {
		t.Fatal(err)
	}
	invalid := []SnapshotWALBootstrapPlan{
		{},
		{JoinerID: "node-b", SourceID: "node-a", SnapshotID: "id", TargetJournalSequence: 1, FencingToken: 1},
		{JoinerID: "node-b", SourceID: "node-a", SnapshotID: "id", StorageGeneration: 1, SnapshotJournalSequence: 4, TargetJournalSequence: 3, FencingToken: 1},
		{JoinerID: "node-b", SourceID: "node-a", SnapshotID: "id", StorageGeneration: 1, SnapshotJournalSequence: 1, TargetJournalSequence: 4, FencingToken: 1},
		{JoinerID: "node-b", SourceID: "node-a", SnapshotID: "id", StorageGeneration: 1, SnapshotJournalSequence: 1, TargetJournalSequence: 1},
	}
	for index, plan := range invalid {
		if _, err := coordinator.Begin(plan); !errors.Is(err, ErrSnapshotWALBootstrapInvalid) && !errors.Is(err, ErrSnapshotWALBootstrapLimit) {
			t.Fatalf("invalid plan %d error = %v", index, err)
		}
	}
	if _, err := coordinator.Begin(SnapshotWALBootstrapPlan{
		JoinerID:                "node-b",
		SourceID:                "node-a",
		SnapshotID:              "snapshot-1",
		StorageGeneration:       1,
		SnapshotJournalSequence: 1,
		TargetJournalSequence:   3,
		FencingToken:            1,
	}); err != nil {
		t.Fatalf("valid plan error = %v", err)
	}
	if _, err := coordinator.InstallSnapshot("other", 1, 1, 1); !errors.Is(err, ErrSnapshotWALBootstrapInvalid) {
		t.Fatalf("wrong snapshot ID error = %v", err)
	}
}

func TestSnapshotWALBootstrapRejectsOutOfOrderAndStaleOperations(t *testing.T) {
	coordinator, err := NewSnapshotWALBootstrapCoordinator(SnapshotWALBootstrapOptions{})
	if err != nil {
		t.Fatal(err)
	}
	plan := SnapshotWALBootstrapPlan{
		JoinerID:                "node-b",
		SourceID:                "node-a",
		SnapshotID:              "snapshot-1",
		StorageGeneration:       1,
		SnapshotJournalSequence: 10,
		TargetJournalSequence:   12,
		FencingToken:            5,
	}
	if _, err := coordinator.Begin(plan); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.InstallSnapshot("snapshot-1", 2, 10, 5); !errors.Is(err, ErrSnapshotWALBootstrapInvalid) {
		t.Fatalf("wrong storage generation error = %v", err)
	}
	state, err := coordinator.InstallSnapshot("snapshot-1", 1, 10, 5)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.AdvanceWAL(9, 5); !errors.Is(err, ErrSnapshotWALBootstrapSequence) {
		t.Fatalf("backward WAL error = %v", err)
	}
	if _, err := coordinator.AdvanceWAL(13, 5); !errors.Is(err, ErrSnapshotWALBootstrapSequence) {
		t.Fatalf("past-target WAL error = %v", err)
	}
	if _, err := coordinator.MarkReady(state.Generation, 5); !errors.Is(err, ErrSnapshotWALBootstrapNotReady) {
		t.Fatalf("early MarkReady() error = %v", err)
	}
	if _, err := coordinator.Abort("stale operator", state.Generation, 4); !errors.Is(err, ErrSnapshotWALBootstrapFencing) {
		t.Fatalf("stale Abort() error = %v", err)
	}
	state, err = coordinator.Abort("operator cancel", state.Generation, 5)
	if err != nil || state.Phase != SnapshotWALBootstrapPhaseAborted {
		t.Fatalf("Abort() = %+v, %v", state, err)
	}
	if _, err := coordinator.AdvanceWAL(11, 5); !errors.Is(err, ErrSnapshotWALBootstrapPhase) {
		t.Fatalf("post-abort AdvanceWAL() error = %v", err)
	}
}

func TestSnapshotWALBootstrapConcurrentSnapshotsRemainConsistent(t *testing.T) {
	coordinator, err := NewSnapshotWALBootstrapCoordinator(SnapshotWALBootstrapOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Begin(SnapshotWALBootstrapPlan{
		JoinerID:                "node-b",
		SourceID:                "node-a",
		SnapshotID:              "snapshot-1",
		StorageGeneration:       1,
		SnapshotJournalSequence: 0,
		TargetJournalSequence:   1,
		FencingToken:            1,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.InstallSnapshot("snapshot-1", 1, 0, 1); err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for index := 0; index < 32; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			_ = coordinator.Snapshot()
			_, _ = coordinator.AdvanceWAL(1, 1)
		}()
	}
	group.Wait()
	state := coordinator.Snapshot()
	if state.AppliedJournalSequence != 1 || state.Phase != SnapshotWALBootstrapPhaseCatchingUp {
		t.Fatalf("concurrent state = %+v", state)
	}
}
