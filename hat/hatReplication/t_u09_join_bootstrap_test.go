package hatReplication

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

func TestJoinBootstrapTracksSnapshotAndWALProgress(t *testing.T) {
	state, err := NewJoinBootstrapState("join-1", "leader", "replica", 11, 100)
	if err != nil {
		t.Fatalf("NewJoinBootstrapState() error = %v", err)
	}
	state, err = state.BeginCatchUp()
	if err != nil {
		t.Fatalf("BeginCatchUp() error = %v", err)
	}
	state, err = state.RecordApplied(100)
	if err != nil {
		t.Fatalf("RecordApplied(snapshot) error = %v", err)
	}
	repeated, err := state.RecordApplied(100)
	if err != nil {
		t.Fatalf("RecordApplied(equal) error = %v", err)
	}
	if !reflect.DeepEqual(repeated, state) {
		t.Fatalf("RecordApplied(equal) = %#v, want %#v", repeated, state)
	}
	if _, err := state.RecordApplied(99); !errors.Is(err, ErrJoinBootstrapProgressRegressed) {
		t.Fatalf("RecordApplied(regressed) error = %v, want progress regression", err)
	}

	state, err = state.PrepareActivation(11, 100)
	if err != nil {
		t.Fatalf("PrepareActivation() error = %v", err)
	}
	if state.Phase != JoinBootstrapReady || state.SourceSequence != 100 {
		t.Fatalf("prepared state = %#v, want ready at sequence 100", state)
	}
	state, err = state.Activate(11)
	if err != nil {
		t.Fatalf("Activate() error = %v", err)
	}
	if state.Phase != JoinBootstrapActivated {
		t.Fatalf("activated phase = %s, want activated", state.Phase)
	}
	repeated, err = state.Activate(11)
	if err != nil {
		t.Fatalf("Activate(equal) error = %v", err)
	}
	if !reflect.DeepEqual(repeated, state) {
		t.Fatalf("Activate(equal) = %#v, want %#v", repeated, state)
	}
}

func TestJoinBootstrapRejectsStaleFenceAndIncompleteCatchUp(t *testing.T) {
	state, err := NewJoinBootstrapState("join-2", "leader", "replica", 21, 10)
	if err != nil {
		t.Fatalf("NewJoinBootstrapState() error = %v", err)
	}
	state, err = state.BeginCatchUp()
	if err != nil {
		t.Fatalf("BeginCatchUp() error = %v", err)
	}
	state, err = state.RecordApplied(10)
	if err != nil {
		t.Fatalf("RecordApplied() error = %v", err)
	}
	if _, err := state.PrepareActivation(21, 11); !errors.Is(err, ErrJoinBootstrapNotCaughtUp) {
		t.Fatalf("PrepareActivation(ahead) error = %v, want not caught up", err)
	}
	if _, err := state.PrepareActivation(20, 10); !errors.Is(err, ErrJoinBootstrapStaleFence) {
		t.Fatalf("PrepareActivation(stale) error = %v, want stale fence", err)
	}
	if _, err := state.Activate(20); !errors.Is(err, ErrJoinBootstrapStaleFence) {
		t.Fatalf("Activate(stale) error = %v, want stale fence", err)
	}
	if _, err := state.Retry(20); !errors.Is(err, ErrJoinBootstrapStaleFence) {
		t.Fatalf("Retry(stale) error = %v, want stale fence", err)
	}
}

func TestJoinBootstrapAbortAndRetryIsIdempotent(t *testing.T) {
	state, err := NewJoinBootstrapState("join-3", "leader", "replica", 31, 50)
	if err != nil {
		t.Fatalf("NewJoinBootstrapState() error = %v", err)
	}
	state, err = state.BeginCatchUp()
	if err != nil {
		t.Fatalf("BeginCatchUp() error = %v", err)
	}
	state, err = state.RecordApplied(50)
	if err != nil {
		t.Fatalf("RecordApplied() error = %v", err)
	}
	state, err = state.Abort("target stopped")
	if err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	repeated, err := state.Abort("target stopped")
	if err != nil {
		t.Fatalf("Abort(equal) error = %v", err)
	}
	if !reflect.DeepEqual(repeated, state) {
		t.Fatalf("Abort(equal) = %#v, want %#v", repeated, state)
	}
	state, err = state.Retry(31)
	if err != nil {
		t.Fatalf("Retry() error = %v", err)
	}
	if state.Phase != JoinBootstrapCatchingUp || state.AbortReason != "" {
		t.Fatalf("retry state = %#v, want clean catch-up state", state)
	}
	state, err = state.RecordApplied(52)
	if err != nil {
		t.Fatalf("RecordApplied(retry) error = %v", err)
	}
	state, err = state.PrepareActivation(31, 52)
	if err != nil {
		t.Fatalf("PrepareActivation(retry) error = %v", err)
	}
	state, err = state.Activate(31)
	if err != nil {
		t.Fatalf("Activate(retry) error = %v", err)
	}
	if _, err := state.Abort("too late"); !errors.Is(err, ErrJoinBootstrapPhase) {
		t.Fatalf("Abort(after activation) error = %v, want phase error", err)
	}
}

func TestJoinBootstrapValidatesIdentityAndPhase(t *testing.T) {
	for _, input := range [][5]interface{}{
		{"", "leader", "replica", uint64(1), uint64(0)},
		{"join", "", "replica", uint64(1), uint64(0)},
		{"join", "leader", "", uint64(1), uint64(0)},
		{"join", "leader", "replica", uint64(0), uint64(0)},
		{"join", "leader", " replica", uint64(1), uint64(0)},
	} {
		if _, err := NewJoinBootstrapState(input[0].(string), input[1].(string), input[2].(string), input[3].(uint64), input[4].(uint64)); !errors.Is(err, ErrJoinBootstrapInvalid) {
			t.Fatalf("NewJoinBootstrapState(%#v) error = %v, want invalid state", input, err)
		}
	}
	if got := JoinBootstrapPhase(99).String(); got != "unknown" {
		t.Fatalf("unknown phase string = %q, want unknown", got)
	}
	if err := (JoinBootstrapState{}).Validate(); !errors.Is(err, ErrJoinBootstrapInvalid) {
		t.Fatalf("zero state Validate() error = %v, want invalid state", err)
	}
}

func TestJoinBootstrapStateRoundTripsAsDurableJSON(t *testing.T) {
	state, err := NewJoinBootstrapState("join-json", "leader", "replica", 41, 70)
	if err != nil {
		t.Fatalf("NewJoinBootstrapState() error = %v", err)
	}
	state, err = state.BeginCatchUp()
	if err != nil {
		t.Fatalf("BeginCatchUp() error = %v", err)
	}
	state, err = state.RecordApplied(73)
	if err != nil {
		t.Fatalf("RecordApplied() error = %v", err)
	}
	encoded, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	var restored JoinBootstrapState
	if err := json.Unmarshal(encoded, &restored); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if err := restored.Validate(); err != nil {
		t.Fatalf("restored Validate() error = %v", err)
	}
	if !reflect.DeepEqual(restored, state) {
		t.Fatalf("restored state = %#v, want %#v", restored, state)
	}
}
