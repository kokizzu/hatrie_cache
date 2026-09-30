package hatReplication

import (
	"errors"
	"testing"
)

func TestT209ReplicaBackpressurePausesAndResumesWithHysteresis(t *testing.T) {
	controller, err := NewReplicaBackpressureController(ReplicaBackpressureOptions{MaxLag: 10, ResumeLag: 4})
	if err != nil {
		t.Fatalf("NewReplicaBackpressureController() error = %v", err)
	}
	state, err := controller.Observe(10, 0)
	if err != nil || !state.Paused || !state.Changed || state.LagLSN != 10 {
		t.Fatalf("pause state = %#v/%v, want paused transition at lag 10", state, err)
	}
	state, err = controller.Observe(13, 4)
	if err != nil || !state.Paused || state.Changed || state.LagLSN != 9 {
		t.Fatalf("hysteresis state = %#v/%v, want still paused above resume lag", state, err)
	}
	state, err = controller.Observe(13, 9)
	if err != nil || state.Paused || !state.Changed || state.LagLSN != 4 {
		t.Fatalf("resume state = %#v/%v, want resumed at resume lag", state, err)
	}
	allowed, err := controller.Admit(13, 9)
	if err != nil || !allowed {
		t.Fatalf("Admit(resumed) = %v/%v, want allowed", allowed, err)
	}
}

func TestT209ReplicaBackpressureRejectsRegressingObservations(t *testing.T) {
	controller, err := NewReplicaBackpressureController(ReplicaBackpressureOptions{MaxLag: 10})
	if err != nil {
		t.Fatalf("NewReplicaBackpressureController() error = %v", err)
	}
	if _, err := controller.Observe(10, 2); err != nil {
		t.Fatalf("first Observe() error = %v", err)
	}
	if _, err := controller.Observe(9, 2); !errors.Is(err, ErrReplicaBackpressureSequenceRegression) {
		t.Fatalf("source regression error = %v, want ErrReplicaBackpressureSequenceRegression", err)
	}
	if _, err := controller.Observe(10, 1); !errors.Is(err, ErrReplicaBackpressureSequenceRegression) {
		t.Fatalf("applied regression error = %v, want ErrReplicaBackpressureSequenceRegression", err)
	}
	state := controller.Snapshot()
	if state.SourceLSN != 10 || state.AppliedLSN != 2 || state.Paused {
		t.Fatalf("state after rejected observations = %#v, want unchanged and unpaused", state)
	}
}

func TestT209ReplicaBackpressureDefaultsAndValidatesOptions(t *testing.T) {
	controller, err := NewReplicaBackpressureController(ReplicaBackpressureOptions{})
	if err != nil {
		t.Fatalf("default options error = %v", err)
	}
	state, err := controller.Observe(DefaultReplicaBackpressureMaxLag, 0)
	if err != nil || !state.Paused || state.MaxLag != DefaultReplicaBackpressureMaxLag || state.ResumeLag != DefaultReplicaBackpressureMaxLag/2 {
		t.Fatalf("default state = %#v/%v, want sane default thresholds", state, err)
	}
	if _, err := NewReplicaBackpressureController(ReplicaBackpressureOptions{MaxLag: 10, ResumeLag: 10}); !errors.Is(err, ErrReplicaBackpressureInvalidOptions) {
		t.Fatalf("equal thresholds error = %v, want ErrReplicaBackpressureInvalidOptions", err)
	}
}
