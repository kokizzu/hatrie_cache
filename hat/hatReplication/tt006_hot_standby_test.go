package hatReplication_test

import (
	"errors"
	"testing"

	"hatrie_cache/hat/hatReplication"
)

func TestTT006HotStandbyReplaysContiguouslyAndPromotes(t *testing.T) {
	coordinator, err := hatReplication.NewHotStandbyCoordinator(hatReplication.HotStandbyOptions{MaxLag: 16})
	if err != nil {
		t.Fatalf("NewHotStandbyCoordinator() error = %v", err)
	}

	state, err := coordinator.Start(hatReplication.HotStandbyPlan{
		StandbyID:         "standby-a",
		PrimaryID:         "primary-a",
		StorageGeneration: 4,
		SnapshotSequence:  100,
		SourceTerm:        7,
		FencingToken:      99,
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if state.Phase != hatReplication.HotStandbyPhaseReplaying || state.AppliedSequence != 100 {
		t.Fatalf("initial state = %#v", state)
	}

	state, err = coordinator.ObserveHead(7, 102, 99)
	if err != nil {
		t.Fatalf("ObserveHead() error = %v", err)
	}
	if state.Phase != hatReplication.HotStandbyPhaseReplaying || state.AdvertisedSequence != 102 {
		t.Fatalf("observed state = %#v", state)
	}

	state, err = coordinator.ApplyWAL(7, 101, 102, 99)
	if err != nil {
		t.Fatalf("ApplyWAL() error = %v", err)
	}
	if state.Phase != hatReplication.HotStandbyPhaseCaughtUp || state.AppliedSequence != 102 {
		t.Fatalf("caught-up state = %#v", state)
	}

	state, err = coordinator.Promote(state.Generation, 99, 8)
	if err != nil {
		t.Fatalf("Promote() error = %v", err)
	}
	if state.Phase != hatReplication.HotStandbyPhasePrimary || state.Term != 8 {
		t.Fatalf("promoted state = %#v", state)
	}
}

func TestTT006HotStandbyRejectsGapsStaleFencesAndEarlyPromotion(t *testing.T) {
	coordinator, err := hatReplication.NewHotStandbyCoordinator(hatReplication.HotStandbyOptions{MaxLag: 4})
	if err != nil {
		t.Fatalf("NewHotStandbyCoordinator() error = %v", err)
	}
	if _, err := coordinator.Start(hatReplication.HotStandbyPlan{
		StandbyID:         "standby-a",
		PrimaryID:         "primary-a",
		StorageGeneration: 4,
		SnapshotSequence:  10,
		SourceTerm:        3,
		FencingToken:      11,
	}); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	before := coordinator.Snapshot()
	if _, err := coordinator.ObserveHead(3, 15, 11); !errors.Is(err, hatReplication.ErrHotStandbyLag) {
		t.Fatalf("ObserveHead() error = %v, want lag error", err)
	}
	if got := coordinator.Snapshot(); got != before {
		t.Fatalf("lag rejection changed state: before=%#v after=%#v", before, got)
	}
	if _, err := coordinator.ObserveHead(3, 12, 999); !errors.Is(err, hatReplication.ErrHotStandbyFencing) {
		t.Fatalf("stale fence error = %v", err)
	}
	if _, err := coordinator.ObserveHead(3, 12, 11); err != nil {
		t.Fatalf("ObserveHead(valid) error = %v", err)
	}
	if _, err := coordinator.ApplyWAL(3, 12, 12, 11); !errors.Is(err, hatReplication.ErrHotStandbySequence) {
		t.Fatalf("WAL gap error = %v", err)
	}
	state := coordinator.Snapshot()
	if _, err := coordinator.Promote(state.Generation, 11, 4); !errors.Is(err, hatReplication.ErrHotStandbyNotCaughtUp) {
		t.Fatalf("early Promote() error = %v", err)
	}
}

func TestTT006HotStandbyDuplicateReplayIsIdempotent(t *testing.T) {
	coordinator, err := hatReplication.NewHotStandbyCoordinator(hatReplication.HotStandbyOptions{})
	if err != nil {
		t.Fatalf("NewHotStandbyCoordinator() error = %v", err)
	}
	if _, err := coordinator.Start(hatReplication.HotStandbyPlan{
		StandbyID:         "standby-a",
		PrimaryID:         "primary-a",
		StorageGeneration: 1,
		SnapshotSequence:  20,
		SourceTerm:        2,
		FencingToken:      5,
	}); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if _, err := coordinator.ObserveHead(2, 22, 5); err != nil {
		t.Fatalf("ObserveHead() error = %v", err)
	}
	first, err := coordinator.ApplyWAL(2, 21, 22, 5)
	if err != nil {
		t.Fatalf("first ApplyWAL() error = %v", err)
	}
	second, err := coordinator.ApplyWAL(2, 21, 22, 5)
	if err != nil {
		t.Fatalf("duplicate ApplyWAL() error = %v", err)
	}
	if second != first {
		t.Fatalf("duplicate replay state = %#v, want %#v", second, first)
	}
}

func TestTT006HotStandbyValidatesPlans(t *testing.T) {
	if _, err := hatReplication.NewHotStandbyCoordinator(hatReplication.HotStandbyOptions{MaxLag: ^uint64(0)}); !errors.Is(err, hatReplication.ErrHotStandbyInvalid) {
		t.Fatalf("invalid options error = %v", err)
	}
	coordinator, err := hatReplication.NewHotStandbyCoordinator(hatReplication.HotStandbyOptions{})
	if err != nil {
		t.Fatalf("NewHotStandbyCoordinator() error = %v", err)
	}
	if _, err := coordinator.Start(hatReplication.HotStandbyPlan{StandbyID: "same", PrimaryID: "same", SourceTerm: 1, FencingToken: 1}); !errors.Is(err, hatReplication.ErrHotStandbyInvalid) {
		t.Fatalf("invalid plan error = %v", err)
	}
}

func TestTT006HotStandbyAcceptsIdempotentMaximumSequence(t *testing.T) {
	maximum := ^uint64(0)
	coordinator, err := hatReplication.NewHotStandbyCoordinator(hatReplication.HotStandbyOptions{})
	if err != nil {
		t.Fatalf("NewHotStandbyCoordinator() error = %v", err)
	}
	if _, err := coordinator.Start(hatReplication.HotStandbyPlan{
		StandbyID:         "standby-a",
		PrimaryID:         "primary-a",
		StorageGeneration: 1,
		SnapshotSequence:  maximum,
		SourceTerm:        1,
		FencingToken:      1,
	}); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if _, err := coordinator.ObserveHead(1, maximum, 1); err != nil {
		t.Fatalf("ObserveHead() error = %v", err)
	}
	if _, err := coordinator.ApplyWAL(1, maximum, maximum, 1); err != nil {
		t.Fatalf("duplicate maximum ApplyWAL() error = %v", err)
	}
}
