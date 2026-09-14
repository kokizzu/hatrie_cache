package hatPipeline_test

import (
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatPipeline"
)

func TestLateDataReclockPublishesRemapAndClassifiesLateUpdates(t *testing.T) {
	reclock, err := hatPipeline.NewLateDataReclock(hatPipeline.LateDataReclockOptions{MaxBindings: 8})
	if err != nil {
		t.Fatalf("NewLateDataReclock() error = %v", err)
	}
	if changed, err := reclock.ObserveSourceFrontier(10); err != nil || !changed {
		t.Fatalf("ObserveSourceFrontier(10) = changed %v err %v", changed, err)
	}
	binding, changed, err := reclock.AdvanceProcessingFrontier(5)
	if err != nil || !changed || binding != (hatPipeline.LateDataReclockBinding{ProcessingFrontier: 5, SourceFrontier: 10}) {
		t.Fatalf("AdvanceProcessingFrontier(5) = %#v changed %v err %v", binding, changed, err)
	}
	if changed, err := reclock.ObserveSourceFrontier(20); err != nil || !changed {
		t.Fatalf("ObserveSourceFrontier(20) = changed %v err %v", changed, err)
	}
	if _, changed, err := reclock.AdvanceProcessingFrontier(9); err != nil || !changed {
		t.Fatalf("AdvanceProcessingFrontier(9) = changed %v err %v", changed, err)
	}

	if source, err := reclock.SourceFrontierAt(8); err != nil || source != 10 {
		t.Fatalf("SourceFrontierAt(8) = %d err %v, want 10", source, err)
	}
	if processing, err := reclock.ProcessingFrontierAt(15); err != nil || processing != 9 {
		t.Fatalf("ProcessingFrontierAt(15) = %d err %v, want 9", processing, err)
	}

	held, err := reclock.Assign(15, 7)
	if err != nil {
		t.Fatalf("Assign(15, 7) error = %v", err)
	}
	if held.ProcessingFrontier != 9 || held.CoveredBySourceFrontier != 20 || held.Late || !held.HeldUntilProcessing {
		t.Fatalf("held assignment = %#v", held)
	}
	late, err := reclock.Assign(15, 12)
	if err != nil {
		t.Fatalf("Assign(15, 12) error = %v", err)
	}
	if late.ProcessingFrontier != 12 || late.CoveredBySourceFrontier != 20 || !late.Late || late.HeldUntilProcessing {
		t.Fatalf("late assignment = %#v", late)
	}

	snapshot := reclock.Snapshot()
	if snapshot.Generation != 2 || snapshot.ProcessingFrontier != 9 || snapshot.SourceFrontier != 20 || len(snapshot.Bindings) != 3 {
		t.Fatalf("Snapshot() = %#v", snapshot)
	}
	snapshot.Bindings[1].SourceFrontier = 999
	if reclock.Snapshot().Bindings[1].SourceFrontier != 10 {
		t.Fatal("Snapshot() exposed mutable binding storage")
	}
}

func TestLateDataReclockBoundsHistoryAndRestoresSnapshots(t *testing.T) {
	reclock, err := hatPipeline.NewLateDataReclock(hatPipeline.LateDataReclockOptions{MaxBindings: 4})
	if err != nil {
		t.Fatalf("NewLateDataReclock() error = %v", err)
	}
	for _, step := range []struct {
		source     uint64
		processing uint64
	}{
		{source: 10, processing: 5},
		{source: 20, processing: 9},
		{source: 30, processing: 12},
	} {
		if _, err := reclock.ObserveSourceFrontier(step.source); err != nil {
			t.Fatalf("ObserveSourceFrontier(%d) error = %v", step.source, err)
		}
		if _, _, err := reclock.AdvanceProcessingFrontier(step.processing); err != nil {
			t.Fatalf("AdvanceProcessingFrontier(%d) error = %v", step.processing, err)
		}
	}
	if err := reclock.CompactBefore(9); err != nil {
		t.Fatalf("CompactBefore(9) error = %v", err)
	}
	if _, err := reclock.SourceFrontierAt(5); !errors.Is(err, hatPipeline.ErrLateDataReclockHistoryCompacted) {
		t.Fatalf("SourceFrontierAt(5) error = %v, want history compacted", err)
	}
	if source, err := reclock.SourceFrontierAt(9); err != nil || source != 20 {
		t.Fatalf("SourceFrontierAt(9) = %d err %v, want 20", source, err)
	}
	if processing, err := reclock.ProcessingFrontierAt(5); err != nil || processing != 9 {
		t.Fatalf("ProcessingFrontierAt(5) = %d err %v, want retained anchor 9", processing, err)
	}

	payload, err := reclock.MarshalSnapshot()
	if err != nil {
		t.Fatalf("MarshalSnapshot() error = %v", err)
	}
	snapshot, err := hatPipeline.UnmarshalLateDataReclockSnapshot(payload, 4)
	if err != nil {
		t.Fatalf("UnmarshalLateDataReclockSnapshot() error = %v", err)
	}
	restored, err := hatPipeline.NewLateDataReclockFromSnapshot(snapshot, hatPipeline.LateDataReclockOptions{MaxBindings: 4})
	if err != nil {
		t.Fatalf("NewLateDataReclockFromSnapshot() error = %v", err)
	}
	if got := restored.Snapshot(); !reflect.DeepEqual(got, reclock.Snapshot()) {
		t.Fatalf("restored snapshot = %#v, want %#v", got, reclock.Snapshot())
	}
	payload[len(payload)-1] ^= 1
	if _, err := hatPipeline.UnmarshalLateDataReclockSnapshot(payload, 4); !errors.Is(err, hatPipeline.ErrLateDataReclockSnapshotInvalid) {
		t.Fatalf("corrupt snapshot error = %v, want invalid snapshot", err)
	}
}

func TestLateDataReclockRejectsRegressionsAndCapacityOverflowAtomically(t *testing.T) {
	if _, err := hatPipeline.NewLateDataReclock(hatPipeline.LateDataReclockOptions{MaxBindings: 0}); err != nil {
		t.Fatalf("zero MaxBindings error = %v", err)
	}
	reclock, err := hatPipeline.NewLateDataReclock(hatPipeline.LateDataReclockOptions{MaxBindings: 2})
	if err != nil {
		t.Fatalf("NewLateDataReclock() error = %v", err)
	}
	if _, err := reclock.ObserveSourceFrontier(10); err != nil {
		t.Fatal(err)
	}
	if _, _, err := reclock.AdvanceProcessingFrontier(5); err != nil {
		t.Fatal(err)
	}
	if _, err := reclock.ObserveSourceFrontier(9); !errors.Is(err, hatPipeline.ErrLateDataReclockSourceRegression) {
		t.Fatalf("source regression error = %v", err)
	}
	if _, _, err := reclock.AdvanceProcessingFrontier(4); !errors.Is(err, hatPipeline.ErrLateDataReclockProcessingRegression) {
		t.Fatalf("processing regression error = %v", err)
	}
	if _, err := reclock.ObserveSourceFrontier(20); err != nil {
		t.Fatal(err)
	}
	before := reclock.Snapshot()
	if _, _, err := reclock.AdvanceProcessingFrontier(5); !errors.Is(err, hatPipeline.ErrLateDataReclockBindingConflict) {
		t.Fatalf("same-processing conflict error = %v", err)
	}
	if _, _, err := reclock.AdvanceProcessingFrontier(9); !errors.Is(err, hatPipeline.ErrLateDataReclockBindingLimit) {
		t.Fatalf("capacity error = %v", err)
	}
	if got := reclock.Snapshot(); !reflect.DeepEqual(got, before) {
		t.Fatalf("state changed after rejected operations = %#v, want %#v", got, before)
	}
}

func TestLateDataReclockNilReceiverAndUncoveredSource(t *testing.T) {
	var reclock *hatPipeline.LateDataReclock
	if changed, err := reclock.ObserveSourceFrontier(1); !errors.Is(err, hatPipeline.ErrLateDataReclockNil) || changed {
		t.Fatalf("nil ObserveSourceFrontier() = changed %v err %v", changed, err)
	}
	if _, _, err := reclock.AdvanceProcessingFrontier(1); !errors.Is(err, hatPipeline.ErrLateDataReclockNil) {
		t.Fatalf("nil AdvanceProcessingFrontier() error = %v", err)
	}
	if _, err := reclock.SourceFrontierAt(1); !errors.Is(err, hatPipeline.ErrLateDataReclockNil) {
		t.Fatalf("nil SourceFrontierAt() error = %v", err)
	}
	if _, err := reclock.ProcessingFrontierAt(1); !errors.Is(err, hatPipeline.ErrLateDataReclockNil) {
		t.Fatalf("nil ProcessingFrontierAt() error = %v", err)
	}
	if _, err := reclock.Assign(1, 1); !errors.Is(err, hatPipeline.ErrLateDataReclockNil) {
		t.Fatalf("nil Assign() error = %v", err)
	}
	if !reflect.DeepEqual(reclock.Snapshot(), hatPipeline.LateDataReclockSnapshot{}) {
		t.Fatalf("nil Snapshot() = %#v", reclock.Snapshot())
	}

	active, err := hatPipeline.NewLateDataReclock(hatPipeline.LateDataReclockOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := active.Assign(0, 0); !errors.Is(err, hatPipeline.ErrLateDataReclockSourceNotCovered) {
		t.Fatalf("uncovered Assign() error = %v", err)
	}

	var zero hatPipeline.LateDataReclock
	if changed, err := zero.ObserveSourceFrontier(1); !errors.Is(err, hatPipeline.ErrLateDataReclockOptionsInvalid) || changed {
		t.Fatalf("zero ObserveSourceFrontier() = changed %v err %v", changed, err)
	}
	if _, _, err := zero.AdvanceProcessingFrontier(1); !errors.Is(err, hatPipeline.ErrLateDataReclockOptionsInvalid) {
		t.Fatalf("zero AdvanceProcessingFrontier() error = %v", err)
	}
}
