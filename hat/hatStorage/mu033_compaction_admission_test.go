package hatStorage

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

type mu33TestPermit struct{ released atomic.Int32 }

func (permit *mu33TestPermit) Release() error {
	permit.released.Add(1)
	return nil
}

type mu33TestAdmission struct {
	permits atomic.Int32
	permit  *mu33TestPermit
}

func (admission *mu33TestAdmission) BeginCompaction(context.Context, string, uint64) (CompactionBoundaryPermit, error) {
	admission.permits.Add(1)
	admission.permit = &mu33TestPermit{}
	return admission.permit, nil
}

func TestMU33CompactionControllerAdmitsAndReleasesBoundaryPermit(t *testing.T) {
	controller, err := NewCompactionController(CompactionControllerOptions{MaxPending: 1})
	if err != nil {
		t.Fatal(err)
	}
	admission := &mu33TestAdmission{}
	var ran atomic.Int32
	_, queued, err := controller.SubmitWithBoundaryAdmission(context.Background(), CompactionRequest{
		Target: "events",
		Run: func(context.Context) error {
			ran.Add(1)
			return nil
		},
	}, admission, "events", 8)
	if err != nil || !queued {
		t.Fatalf("SubmitWithBoundaryAdmission() = queued %v, err %v", queued, err)
	}
	if _, err := controller.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if admission.permits.Load() != 1 || admission.permit == nil || admission.permit.released.Load() != 1 || ran.Load() != 1 {
		t.Fatalf("admission state = permits %d, permit %#v, ran %d", admission.permits.Load(), admission.permit, ran.Load())
	}
}

func TestMU33CompactionControllerReleasesPermitAfterFailure(t *testing.T) {
	controller, err := NewCompactionController(CompactionControllerOptions{MaxPending: 1})
	if err != nil {
		t.Fatal(err)
	}
	admission := &mu33TestAdmission{}
	want := errors.New("compaction failed")
	if _, queued, err := controller.SubmitWithBoundaryAdmission(context.Background(), CompactionRequest{
		Target: "events",
		Run:    func(context.Context) error { return want },
	}, admission, "events", 8); err != nil || !queued {
		t.Fatalf("SubmitWithBoundaryAdmission() = queued %v, err %v", queued, err)
	}
	if _, err := controller.Run(context.Background()); !errors.Is(err, want) {
		t.Fatalf("Run() error = %v, want %v", err, want)
	}
	if admission.permit == nil || admission.permit.released.Load() != 1 {
		t.Fatalf("failed compaction permit = %#v", admission.permit)
	}
}
