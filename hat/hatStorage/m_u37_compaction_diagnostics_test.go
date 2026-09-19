//go:build mu37

package hatStorage

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestMU37CompactionDiagnosticsTracksBoundedHistory(t *testing.T) {
	diagnostics, err := NewCompactionDiagnostics(CompactionDiagnosticsOptions{
		MaxArrangements:       2,
		HistoryPerArrangement: 2,
	})
	if err != nil {
		t.Fatalf("NewCompactionDiagnostics: %v", err)
	}
	if err := diagnostics.Register("orders"); err != nil {
		t.Fatalf("Register: %v", err)
	}
	for index := uint64(1); index <= 3; index++ {
		if err := diagnostics.Record(CompactionObservation{
			Arrangement:         "orders",
			LogicalBytes:        index * 100,
			PhysicalBytes:       index * 140,
			CompactionDebtBytes: index * 40,
			InputBytes:          index * 160,
			OutputBytes:         index * 120,
			Duration:            time.Duration(index) * time.Millisecond,
			Outcome:             CompactionSucceeded,
		}); err != nil {
			t.Fatalf("Record(%d): %v", index, err)
		}
	}

	snapshots := diagnostics.Snapshot()
	if len(snapshots) != 1 {
		t.Fatalf("Snapshot length = %d, want 1", len(snapshots))
	}
	snapshot := snapshots[0]
	if snapshot.Arrangement != "orders" || snapshot.TotalObservations != 3 || snapshot.SuccessfulCompactions != 3 {
		t.Fatalf("snapshot summary = %#v", snapshot)
	}
	if snapshot.FailedCompactions != 0 || snapshot.Last.Sequence != 3 || snapshot.Last.CompactionDebtBytes != 120 {
		t.Fatalf("snapshot counters = %#v", snapshot)
	}
	if len(snapshot.History) != 2 || snapshot.History[0].Sequence != 2 || snapshot.History[1].Sequence != 3 {
		t.Fatalf("snapshot history = %#v", snapshot.History)
	}

	snapshot.History[0].LogicalBytes = 999
	snapshotAgain := diagnostics.Snapshot()
	if snapshotAgain[0].History[0].LogicalBytes == 999 {
		t.Fatal("Snapshot returned mutable internal history")
	}
}

func TestMU37CompactionDiagnosticsIsBoundedAndDeterministic(t *testing.T) {
	diagnostics, err := NewCompactionDiagnostics(CompactionDiagnosticsOptions{
		MaxArrangements:       2,
		HistoryPerArrangement: 1,
	})
	if err != nil {
		t.Fatalf("NewCompactionDiagnostics: %v", err)
	}
	if err := diagnostics.Register("zeta"); err != nil {
		t.Fatalf("Register zeta: %v", err)
	}
	if err := diagnostics.Register("alpha"); err != nil {
		t.Fatalf("Register alpha: %v", err)
	}
	if err := diagnostics.Register("alpha"); !errors.Is(err, ErrCompactionDiagnosticsAlreadyRegistered) {
		t.Fatalf("duplicate Register error = %v", err)
	}
	if err := diagnostics.Register("overflow"); !errors.Is(err, ErrCompactionDiagnosticsCapacity) {
		t.Fatalf("capacity Register error = %v", err)
	}
	if err := diagnostics.Record(CompactionObservation{Arrangement: "zeta", Outcome: CompactionFailed}); err != nil {
		t.Fatalf("Record zeta: %v", err)
	}
	if err := diagnostics.Record(CompactionObservation{Arrangement: "missing", Outcome: CompactionSucceeded}); !errors.Is(err, ErrCompactionDiagnosticsUnregistered) {
		t.Fatalf("unregistered Record error = %v", err)
	}

	snapshots := diagnostics.Snapshot()
	if len(snapshots) != 2 || snapshots[0].Arrangement != "alpha" || snapshots[1].Arrangement != "zeta" {
		t.Fatalf("sorted snapshots = %#v", snapshots)
	}
	if snapshots[1].FailedCompactions != 1 {
		t.Fatalf("failed count = %#v", snapshots[1])
	}
}

func TestMU37CompactionDiagnosticsValidatesConfigurationAndObservations(t *testing.T) {
	invalidOptions := []CompactionDiagnosticsOptions{
		{MaxArrangements: -1},
		{HistoryPerArrangement: -1},
		{MaxArrangementNameBytes: -1},
	}
	for _, options := range invalidOptions {
		if _, err := NewCompactionDiagnostics(options); !errors.Is(err, ErrCompactionDiagnosticsInvalid) {
			t.Fatalf("options %#v error = %v", options, err)
		}
	}

	diagnostics, err := NewCompactionDiagnostics(CompactionDiagnosticsOptions{MaxArrangements: 1, HistoryPerArrangement: 1})
	if err != nil {
		t.Fatalf("NewCompactionDiagnostics: %v", err)
	}
	if err := diagnostics.Register(""); !errors.Is(err, ErrCompactionDiagnosticsInvalid) {
		t.Fatalf("empty arrangement error = %v", err)
	}
	if err := diagnostics.Register("orders"); err != nil {
		t.Fatalf("Register orders: %v", err)
	}
	for _, observation := range []CompactionObservation{
		{Arrangement: "", Outcome: CompactionSucceeded},
		{Arrangement: "orders", Duration: -time.Nanosecond, Outcome: CompactionSucceeded},
		{Arrangement: "orders", Outcome: CompactionOutcome(99)},
	} {
		if err := diagnostics.Record(observation); !errors.Is(err, ErrCompactionDiagnosticsInvalid) {
			t.Fatalf("observation %#v error = %v", observation, err)
		}
	}
}

func TestMU37CompactionDiagnosticsSupportsConcurrentRecording(t *testing.T) {
	diagnostics, err := NewCompactionDiagnostics(CompactionDiagnosticsOptions{MaxArrangements: 1, HistoryPerArrangement: 4})
	if err != nil {
		t.Fatalf("NewCompactionDiagnostics: %v", err)
	}
	if err := diagnostics.Register("orders"); err != nil {
		t.Fatalf("Register orders: %v", err)
	}
	const (
		workers               = 8
		observationsPerWorker = 100
	)
	var group sync.WaitGroup
	group.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func() {
			defer group.Done()
			for index := 0; index < observationsPerWorker; index++ {
				if err := diagnostics.Record(CompactionObservation{Arrangement: "orders", Outcome: CompactionSucceeded}); err != nil {
					t.Errorf("Record: %v", err)
					return
				}
			}
		}()
	}
	group.Wait()

	snapshots := diagnostics.Snapshot()
	if len(snapshots) != 1 || snapshots[0].TotalObservations != workers*observationsPerWorker {
		t.Fatalf("concurrent snapshot = %#v", snapshots)
	}
	if len(snapshots[0].History) != 4 {
		t.Fatalf("concurrent history length = %d, want 4", len(snapshots[0].History))
	}
	if !diagnostics.Unregister("orders") || diagnostics.Len() != 0 {
		t.Fatalf("Unregister/Len did not remove the arrangement")
	}
}
