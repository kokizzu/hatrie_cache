package hatReplication

import (
	"bytes"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
)

func TestT206SnapshotWALBootstrapCheckpointRoundTripsAndResumes(t *testing.T) {
	plan := t206BootstrapPlan()
	coordinator, err := NewSnapshotWALBootstrapCoordinator(SnapshotWALBootstrapOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Begin(plan); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.InstallSnapshot(plan.SnapshotID, plan.StorageGeneration, plan.SnapshotJournalSequence, plan.FencingToken); err != nil {
		t.Fatal(err)
	}
	state, err := coordinator.AdvanceWAL(102, plan.FencingToken)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := coordinator.MarshalSnapshot()
	if err != nil {
		t.Fatalf("MarshalSnapshot() error = %v", err)
	}
	repeated, err := coordinator.MarshalSnapshot()
	if err != nil {
		t.Fatalf("repeated MarshalSnapshot() error = %v", err)
	}
	if !bytes.Equal(encoded, repeated) {
		t.Fatal("MarshalSnapshot() is not deterministic")
	}

	restored, err := NewSnapshotWALBootstrapCoordinator(SnapshotWALBootstrapOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.RestoreSnapshot(encoded); err != nil {
		t.Fatalf("RestoreSnapshot() error = %v", err)
	}
	if got := restored.Snapshot(); !reflect.DeepEqual(got, state) {
		t.Fatalf("restored state = %#v, want %#v", got, state)
	}
	if _, err := restored.AdvanceWAL(103, plan.FencingToken); err != nil {
		t.Fatal(err)
	}
	state, err = restored.MarkReady(restored.Snapshot().Generation, plan.FencingToken)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restored.Activate(state.Generation, plan.FencingToken); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(t.TempDir(), "bootstrap.checkpoint")
	checkpointState := coordinator.Snapshot()
	if err := coordinator.Save(path); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	loaded, err := LoadSnapshotWALBootstrapCoordinator(path, SnapshotWALBootstrapOptions{})
	if err != nil {
		t.Fatalf("LoadSnapshotWALBootstrapCoordinator() error = %v", err)
	}
	if got := loaded.Snapshot(); !reflect.DeepEqual(got, checkpointState) {
		t.Fatalf("loaded state = %#v, want %#v", got, checkpointState)
	}
}

func TestT206SnapshotWALBootstrapCheckpointRejectsCorruptionAndOverwrite(t *testing.T) {
	plan := t206BootstrapPlan()
	coordinator, err := NewSnapshotWALBootstrapCoordinator(SnapshotWALBootstrapOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Begin(plan); err != nil {
		t.Fatal(err)
	}
	payload, err := coordinator.MarshalSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), payload...)
	corrupt[len(corrupt)-1] ^= 0xff
	restored, err := NewSnapshotWALBootstrapCoordinator(SnapshotWALBootstrapOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.RestoreSnapshot(corrupt); !errors.Is(err, ErrSnapshotWALBootstrapSnapshotChecksum) {
		t.Fatalf("corrupt RestoreSnapshot() error = %v, want checksum error", err)
	}
	if got := restored.Snapshot(); got.Phase != SnapshotWALBootstrapPhaseIdle {
		t.Fatalf("failed restore mutated state = %#v", got)
	}
	if err := restored.RestoreSnapshot(append(payload, 0)); !errors.Is(err, ErrSnapshotWALBootstrapSnapshotInvalid) {
		t.Fatalf("trailing RestoreSnapshot() error = %v, want invalid error", err)
	}
	if err := coordinator.RestoreSnapshot(payload); !errors.Is(err, ErrSnapshotWALBootstrapAlreadyStarted) {
		t.Fatalf("overwrite RestoreSnapshot() error = %v, want already-started error", err)
	}
	if got := coordinator.Snapshot(); got.Phase != SnapshotWALBootstrapPhaseSnapshotPending {
		t.Fatalf("rejected overwrite changed state = %#v", got)
	}
}

func t206BootstrapPlan() SnapshotWALBootstrapPlan {
	return SnapshotWALBootstrapPlan{
		JoinerID:                "node-b",
		SourceID:                "node-a",
		SnapshotID:              "snapshot-206",
		StorageGeneration:       4,
		SnapshotJournalSequence: 100,
		TargetJournalSequence:   103,
		FencingToken:            9,
	}
}
