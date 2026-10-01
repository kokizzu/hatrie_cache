package hatReplication

import (
	"errors"
	"testing"
)

var benchmarkSnapshotWALBootstrapSink uint64

func TestTU09SnapshotWALBootstrapLifecycle(t *testing.T) {
	coordinator, err := NewSnapshotWALBootstrapCoordinator(SnapshotWALBootstrapOptions{MaxSessions: 2})
	if err != nil {
		t.Fatalf("new coordinator: %v", err)
	}

	status, err := coordinator.Begin(SnapshotWALBootstrapRequest{
		ID:               "join-1",
		Node:             "node-b",
		Source:           "node-a",
		FencingToken:     7,
		SnapshotID:       "snapshot-7",
		SnapshotSequence: 100,
	})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if status.Phase != SnapshotWALBootstrapPhasePending || status.AppliedSequence != 0 || status.SourceSequence != 100 {
		t.Fatalf("unexpected initial status: %+v", status)
	}

	if _, err := coordinator.Activate("join-1", 7); !errors.Is(err, ErrSnapshotWALBootstrapSnapshotNotInstalled) {
		t.Fatalf("activate before snapshot error = %v, want snapshot-not-installed", err)
	}
	if err := coordinator.MarkSnapshotInstalled("join-1", "snapshot-7", 100); err != nil {
		t.Fatalf("install snapshot: %v", err)
	}
	if err := coordinator.AdvanceSource("join-1", 7, 120); err != nil {
		t.Fatalf("advance source: %v", err)
	}
	if err := coordinator.AdvanceApplied("join-1", 7, 110); err != nil {
		t.Fatalf("advance applied: %v", err)
	}

	if _, err := coordinator.Activate("join-1", 7); !errors.Is(err, ErrSnapshotWALBootstrapSourceNotFenced) {
		t.Fatalf("activate before fencing error = %v, want source-not-fenced", err)
	}
	if _, err := coordinator.FenceSource("join-1", 99); !errors.Is(err, ErrSnapshotWALBootstrapFenceMismatch) {
		t.Fatalf("fence with stale token error = %v, want fence-mismatch", err)
	}
	if _, err := coordinator.FenceSource("join-1", 7); err != nil {
		t.Fatalf("fence source: %v", err)
	}
	if _, err := coordinator.Activate("join-1", 7); !errors.Is(err, ErrSnapshotWALBootstrapNotCaughtUp) {
		t.Fatalf("activate before catch-up error = %v, want not-caught-up", err)
	}
	if err := coordinator.AdvanceApplied("join-1", 7, 120); err != nil {
		t.Fatalf("finish catch-up: %v", err)
	}

	activation, err := coordinator.Activate("join-1", 7)
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	if activation.ID != "join-1" || activation.Sequence != 120 || activation.FencingToken != 7 {
		t.Fatalf("unexpected activation: %+v", activation)
	}
	secondActivation, err := coordinator.Activate("join-1", 7)
	if err != nil {
		t.Fatalf("repeat activate: %v", err)
	}
	if secondActivation != activation {
		t.Fatalf("repeat activation changed result: first=%+v second=%+v", activation, secondActivation)
	}

	status, err = coordinator.Status("join-1")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.Phase != SnapshotWALBootstrapPhaseActivated || status.AppliedSequence != 120 || status.SourceSequence != 120 {
		t.Fatalf("unexpected final status: %+v", status)
	}
	if _, err := coordinator.Begin(SnapshotWALBootstrapRequest{
		ID:               "join-2",
		Node:             "node-c",
		Source:           "node-a",
		FencingToken:     8,
		SnapshotID:       "snapshot-8",
		SnapshotSequence: 200,
	}); err != nil {
		t.Fatalf("begin second session: %v", err)
	}
	statuses := coordinator.Statuses()
	if len(statuses) != 2 || statuses[0].ID != "join-1" || statuses[1].ID != "join-2" {
		t.Fatalf("statuses are not sorted: %+v", statuses)
	}
	statuses[0].Node = "mutated"
	status, err = coordinator.Status("join-1")
	if err != nil {
		t.Fatalf("status after detached mutation: %v", err)
	}
	if status.Node != "node-b" {
		t.Fatalf("status was not detached: %+v", status)
	}
}

func TestTU09SnapshotWALBootstrapRejectsStaleAndUnboundedWork(t *testing.T) {
	coordinator, err := NewSnapshotWALBootstrapCoordinator(SnapshotWALBootstrapOptions{MaxSessions: 1})
	if err != nil {
		t.Fatalf("new coordinator: %v", err)
	}
	_, err = coordinator.Begin(SnapshotWALBootstrapRequest{
		ID:               "join-1",
		Node:             "node-b",
		Source:           "node-a",
		FencingToken:     7,
		SnapshotID:       "snapshot-7",
		SnapshotSequence: 100,
	})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := coordinator.Begin(SnapshotWALBootstrapRequest{ID: "join-2", Node: "node-c", Source: "node-a", FencingToken: 8, SnapshotID: "snapshot-8", SnapshotSequence: 200}); !errors.Is(err, ErrSnapshotWALBootstrapLimitReached) {
		t.Fatalf("second session error = %v, want limit-reached", err)
	}
	if err := coordinator.MarkSnapshotInstalled("join-1", "wrong-snapshot", 100); !errors.Is(err, ErrSnapshotWALBootstrapSnapshotMismatch) {
		t.Fatalf("wrong snapshot error = %v, want snapshot-mismatch", err)
	}
	if err := coordinator.MarkSnapshotInstalled("join-1", "snapshot-7", 100); err != nil {
		t.Fatalf("install snapshot: %v", err)
	}
	if err := coordinator.AdvanceSource("join-1", 7, 99); !errors.Is(err, ErrSnapshotWALBootstrapSequenceRegression) {
		t.Fatalf("source regression error = %v, want sequence-regression", err)
	}
	if err := coordinator.AdvanceSource("join-1", 8, 101); !errors.Is(err, ErrSnapshotWALBootstrapFenceMismatch) {
		t.Fatalf("stale source token error = %v, want fence-mismatch", err)
	}
	if err := coordinator.AdvanceApplied("join-1", 7, 101); !errors.Is(err, ErrSnapshotWALBootstrapSequenceAhead) {
		t.Fatalf("applied ahead error = %v, want sequence-ahead", err)
	}
	if err := coordinator.Abort("join-1", "operator cancelled"); err != nil {
		t.Fatalf("abort: %v", err)
	}
	if err := coordinator.Abort("join-1", "operator cancelled"); err != nil {
		t.Fatalf("repeat abort: %v", err)
	}
	if _, err := coordinator.Activate("join-1", 7); !errors.Is(err, ErrSnapshotWALBootstrapInvalidTransition) {
		t.Fatalf("activate aborted session error = %v, want invalid-transition", err)
	}
}

func BenchmarkTU09SnapshotWALBootstrap(b *testing.B) {
	b.Run("direct_sequence_checks", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sourceSequence := uint64(100)
			appliedSequence := sourceSequence
			sourceSequence = 120
			fenced := true
			appliedSequence = sourceSequence
			if !fenced || appliedSequence < sourceSequence {
				b.Fatal("direct check did not activate")
			}
			benchmarkSnapshotWALBootstrapSink = appliedSequence
		}
	})

	b.Run("coordinator_workflow", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			coordinator, err := NewSnapshotWALBootstrapCoordinator(SnapshotWALBootstrapOptions{MaxSessions: 1})
			if err != nil {
				b.Fatal(err)
			}
			if _, err := coordinator.Begin(SnapshotWALBootstrapRequest{
				ID:               "join-1",
				Node:             "node-b",
				Source:           "node-a",
				FencingToken:     7,
				SnapshotID:       "snapshot-7",
				SnapshotSequence: 100,
			}); err != nil {
				b.Fatal(err)
			}
			if err := coordinator.MarkSnapshotInstalled("join-1", "snapshot-7", 100); err != nil {
				b.Fatal(err)
			}
			if err := coordinator.AdvanceSource("join-1", 7, 120); err != nil {
				b.Fatal(err)
			}
			if err := coordinator.AdvanceApplied("join-1", 7, 120); err != nil {
				b.Fatal(err)
			}
			if _, err := coordinator.FenceSource("join-1", 7); err != nil {
				b.Fatal(err)
			}
			activation, err := coordinator.Activate("join-1", 7)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkSnapshotWALBootstrapSink = activation.Sequence
		}
	})
}
