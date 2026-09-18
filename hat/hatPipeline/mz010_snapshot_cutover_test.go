package hatPipeline

import (
	"errors"
	"reflect"
	"testing"
)

func TestMZ010SnapshotCutoverRequiresAllFencedSources(t *testing.T) {
	coordinator, err := NewSnapshotCutoverCoordinator(SnapshotCutoverOptions{MaxCutovers: 4, MaxSources: 4})
	if err != nil {
		t.Fatalf("NewSnapshotCutoverCoordinator() error = %v", err)
	}
	prepared, err := coordinator.Prepare(SnapshotCutoverSpec{
		ID:        "snapshot-1",
		Timestamp: 100,
		Sources: []SnapshotCutoverSource{
			{ID: "zeta", Generation: 9},
			{ID: "alpha", Generation: 3},
			{ID: "beta", Generation: 4},
		},
	})
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if prepared.State != SnapshotCutoverPrepared || prepared.Remaining != 3 || len(prepared.Sources) != 3 {
		t.Fatalf("prepared status = %#v", prepared)
	}
	if got := []string{prepared.Sources[0].ID, prepared.Sources[1].ID, prepared.Sources[2].ID}; !reflect.DeepEqual(got, []string{"alpha", "beta", "zeta"}) {
		t.Fatalf("prepared source order = %#v", got)
	}
	if _, err := coordinator.Commit("snapshot-1"); !errors.Is(err, ErrSnapshotCutoverNotReady) {
		t.Fatalf("early Commit() error = %v, want %v", err, ErrSnapshotCutoverNotReady)
	}
	if _, err := coordinator.Acknowledge("snapshot-1", SnapshotCutoverAcknowledgement{SourceID: "alpha", Generation: 3, Lower: 99, Upper: 120}); !errors.Is(err, ErrSnapshotCutoverNotReady) {
		t.Fatalf("behind Acknowledge() error = %v, want %v", err, ErrSnapshotCutoverNotReady)
	}
	if _, err := coordinator.Acknowledge("snapshot-1", SnapshotCutoverAcknowledgement{SourceID: "alpha", Generation: 8, Lower: 100, Upper: 120}); !errors.Is(err, ErrSnapshotCutoverGenerationMismatch) {
		t.Fatalf("stale-generation Acknowledge() error = %v, want %v", err, ErrSnapshotCutoverGenerationMismatch)
	}
	if _, err := coordinator.Acknowledge("snapshot-1", SnapshotCutoverAcknowledgement{SourceID: "unknown", Generation: 1, Lower: 100, Upper: 100}); !errors.Is(err, ErrSnapshotCutoverSourceUnknown) {
		t.Fatalf("unknown-source Acknowledge() error = %v, want %v", err, ErrSnapshotCutoverSourceUnknown)
	}
	for _, acknowledgement := range []SnapshotCutoverAcknowledgement{
		{SourceID: "zeta", Generation: 9, Lower: 100, Upper: 110},
		{SourceID: "alpha", Generation: 3, Lower: 100, Upper: 120},
		{SourceID: "beta", Generation: 4, Lower: 101, Upper: 140},
	} {
		status, err := coordinator.Acknowledge("snapshot-1", acknowledgement)
		if err != nil {
			t.Fatalf("Acknowledge(%q) error = %v", acknowledgement.SourceID, err)
		}
		if status.Acknowledged > status.TotalSources {
			t.Fatalf("invalid acknowledgement count in %#v", status)
		}
	}
	committed, err := coordinator.Commit("snapshot-1")
	if err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if committed.State != SnapshotCutoverCommitted || committed.Remaining != 0 || committed.Acknowledged != 3 {
		t.Fatalf("committed status = %#v", committed)
	}
	if got := []string{committed.Acknowledgements[0].SourceID, committed.Acknowledgements[1].SourceID, committed.Acknowledgements[2].SourceID}; !reflect.DeepEqual(got, []string{"alpha", "beta", "zeta"}) {
		t.Fatalf("committed acknowledgement order = %#v", got)
	}
	if _, err := coordinator.Commit("snapshot-1"); err != nil {
		t.Fatalf("idempotent Commit() error = %v", err)
	}
}

func TestMZ010SnapshotCutoverRejectsRegressionsAndDetachedState(t *testing.T) {
	coordinator, err := NewSnapshotCutoverCoordinator(SnapshotCutoverOptions{})
	if err != nil {
		t.Fatalf("NewSnapshotCutoverCoordinator() error = %v", err)
	}
	if _, err := coordinator.Prepare(SnapshotCutoverSpec{
		ID:        "cutover",
		Timestamp: 10,
		Sources:   []SnapshotCutoverSource{{ID: "events", Generation: 2}},
	}); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if _, err := coordinator.Acknowledge("cutover", SnapshotCutoverAcknowledgement{SourceID: "events", Generation: 2, Lower: 10, Upper: 20}); err != nil {
		t.Fatalf("first Acknowledge() error = %v", err)
	}
	if _, err := coordinator.Acknowledge("cutover", SnapshotCutoverAcknowledgement{SourceID: "events", Generation: 2, Lower: 10, Upper: 19}); !errors.Is(err, ErrSnapshotCutoverAcknowledgementRegression) {
		t.Fatalf("regressed Acknowledge() error = %v, want %v", err, ErrSnapshotCutoverAcknowledgementRegression)
	}
	status, ok := coordinator.Status("cutover")
	if !ok || len(status.Acknowledgements) != 1 || status.Acknowledgements[0].Upper != 20 {
		t.Fatalf("status after rejected regression = %#v, ok %v", status, ok)
	}
	status.Acknowledgements[0].Upper = 999
	fresh, ok := coordinator.Status("cutover")
	if !ok || fresh.Acknowledgements[0].Upper != 20 {
		t.Fatal("Status() returned aliased acknowledgement state")
	}

	if _, err := coordinator.Prepare(SnapshotCutoverSpec{
		ID:        "duplicate-source",
		Timestamp: 1,
		Sources:   []SnapshotCutoverSource{{ID: "x"}, {ID: "x"}},
	}); !errors.Is(err, ErrSnapshotCutoverSpecInvalid) {
		t.Fatalf("duplicate source Prepare() error = %v, want %v", err, ErrSnapshotCutoverSpecInvalid)
	}
	if _, err := coordinator.Prepare(SnapshotCutoverSpec{ID: "cutover", Timestamp: 10, Sources: []SnapshotCutoverSource{{ID: "events", Generation: 2}}}); !errors.Is(err, ErrSnapshotCutoverAlreadyExists) {
		t.Fatalf("duplicate cutover Prepare() error = %v, want %v", err, ErrSnapshotCutoverAlreadyExists)
	}
}

func TestMZ010SnapshotCutoverAbortForgetAndBounds(t *testing.T) {
	coordinator, err := NewSnapshotCutoverCoordinator(SnapshotCutoverOptions{MaxCutovers: 1, MaxSources: 1})
	if err != nil {
		t.Fatalf("NewSnapshotCutoverCoordinator() error = %v", err)
	}
	if _, err := coordinator.Prepare(SnapshotCutoverSpec{ID: "first", Timestamp: 1, Sources: []SnapshotCutoverSource{{ID: "source"}}}); err != nil {
		t.Fatalf("Prepare(first) error = %v", err)
	}
	if _, err := coordinator.Prepare(SnapshotCutoverSpec{ID: "second", Timestamp: 1, Sources: []SnapshotCutoverSource{{ID: "source"}}}); !errors.Is(err, ErrSnapshotCutoverCapacity) {
		t.Fatalf("Prepare(second) error = %v, want %v", err, ErrSnapshotCutoverCapacity)
	}
	aborted, err := coordinator.Abort("first", "source unavailable")
	if err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	if aborted.State != SnapshotCutoverAborted || aborted.Reason != "source unavailable" {
		t.Fatalf("aborted status = %#v", aborted)
	}
	if _, err := coordinator.Commit("first"); !errors.Is(err, ErrSnapshotCutoverTerminal) {
		t.Fatalf("Commit(aborted) error = %v, want %v", err, ErrSnapshotCutoverTerminal)
	}
	if err := coordinator.Forget("first"); err != nil {
		t.Fatalf("Forget() error = %v", err)
	}
	if _, err := coordinator.Prepare(SnapshotCutoverSpec{ID: "second", Timestamp: 1, Sources: []SnapshotCutoverSource{{ID: "source"}}}); err != nil {
		t.Fatalf("Prepare(second after Forget) error = %v", err)
	}
	if err := coordinator.Forget("second"); !errors.Is(err, ErrSnapshotCutoverNotTerminal) {
		t.Fatalf("Forget(prepared) error = %v, want %v", err, ErrSnapshotCutoverNotTerminal)
	}
	if _, err := NewSnapshotCutoverCoordinator(SnapshotCutoverOptions{MaxCutovers: -1}); !errors.Is(err, ErrSnapshotCutoverOptionsInvalid) {
		t.Fatalf("invalid options error = %v, want %v", err, ErrSnapshotCutoverOptionsInvalid)
	}
}

func TestMZ010SnapshotCutoverProgressFastPath(t *testing.T) {
	coordinator, err := NewSnapshotCutoverCoordinator(SnapshotCutoverOptions{MaxCutovers: 1, MaxSources: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Prepare(SnapshotCutoverSpec{
		ID:        "cutover",
		Timestamp: 100,
		Sources: []SnapshotCutoverSource{
			{ID: "alpha", Generation: 7},
			{ID: "beta", Generation: 8},
		},
	}); err != nil {
		t.Fatal(err)
	}

	progress, err := coordinator.AcknowledgeProgress("cutover", SnapshotCutoverAcknowledgement{
		SourceID:   "alpha",
		Generation: 7,
		Lower:      100,
		Upper:      110,
	})
	if err != nil {
		t.Fatal(err)
	}
	if progress.State != SnapshotCutoverPrepared || progress.Acknowledged != 1 || progress.Remaining != 1 || progress.Ready {
		t.Fatalf("unexpected partial progress: %+v", progress)
	}

	progress, err = coordinator.AcknowledgeProgress("cutover", SnapshotCutoverAcknowledgement{
		SourceID:   "beta",
		Generation: 8,
		Lower:      100,
		Upper:      120,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !progress.Ready || progress.Acknowledged != 2 || progress.Remaining != 0 {
		t.Fatalf("unexpected ready progress: %+v", progress)
	}

	status, ok := coordinator.Status("cutover")
	if !ok || status.Acknowledged != 2 || len(status.Acknowledgements) != 2 {
		t.Fatalf("compact acknowledgement was not reflected in status: ok=%v status=%+v", ok, status)
	}
	if _, err := coordinator.AcknowledgeProgress("cutover", SnapshotCutoverAcknowledgement{
		SourceID:   "alpha",
		Generation: 6,
		Lower:      100,
		Upper:      111,
	}); err != ErrSnapshotCutoverGenerationMismatch {
		t.Fatalf("expected generation mismatch, got %v", err)
	}
}
