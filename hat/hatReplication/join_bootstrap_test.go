package hatReplication

import (
	"errors"
	"testing"
)

func TestJoinBootstrapRequiresValidatedIdentityAndFence(t *testing.T) {
	cases := []JoinBootstrapOptions{
		{},
		{SourceNode: "source", TargetNode: "source", SnapshotSequence: 7, FencingToken: 1},
		{SourceNode: "source", TargetNode: "target", SnapshotSequence: 7},
	}
	for _, options := range cases {
		if _, err := NewJoinBootstrap(options); err == nil {
			t.Fatalf("NewJoinBootstrap(%#v) error = nil, want validation error", options)
		}
	}

	bootstrap, err := NewJoinBootstrap(JoinBootstrapOptions{
		SourceNode:       "source",
		TargetNode:       "target",
		SnapshotSequence: 7,
		FencingToken:     11,
	})
	if err != nil {
		t.Fatal(err)
	}
	state := bootstrap.Snapshot()
	if state.Phase != JoinBootstrapPhasePending || state.AppliedThrough != 0 {
		t.Fatalf("initial state = %#v, want pending", state)
	}
}

func TestJoinBootstrapRequiresExactSnapshotBeforeCatchUpAndActivation(t *testing.T) {
	bootstrap, err := NewJoinBootstrap(JoinBootstrapOptions{
		SourceNode:       "source",
		TargetNode:       "target",
		SnapshotSequence: 7,
		FencingToken:     11,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := bootstrap.CatchUp(7); !errors.Is(err, ErrJoinBootstrapSnapshotRequired) {
		t.Fatalf("CatchUp before snapshot error = %v, want snapshot-required", err)
	}
	if _, err := bootstrap.Activate(7, 11); !errors.Is(err, ErrJoinBootstrapCatchUpRequired) {
		t.Fatalf("Activate before catch-up error = %v, want catch-up-required", err)
	}
	if err := bootstrap.SnapshotInstalled(6); !errors.Is(err, ErrJoinBootstrapSnapshotMismatch) {
		t.Fatalf("SnapshotInstalled(6) error = %v, want snapshot-mismatch", err)
	}
	if err := bootstrap.SnapshotInstalled(7); err != nil {
		t.Fatal(err)
	}
	if err := bootstrap.CatchUp(6); !errors.Is(err, ErrJoinBootstrapCatchUpBehind) {
		t.Fatalf("CatchUp(6) error = %v, want catch-up-behind", err)
	}
	if err := bootstrap.CatchUp(7); err != nil {
		t.Fatal(err)
	}
	if _, err := bootstrap.Activate(7, 10); !errors.Is(err, ErrJoinBootstrapFenceMismatch) {
		t.Fatalf("Activate with stale fence error = %v, want fence-mismatch", err)
	}
	state, err := bootstrap.Activate(7, 11)
	if err != nil {
		t.Fatal(err)
	}
	if state.Phase != JoinBootstrapPhaseActivated || state.AppliedThrough != 7 {
		t.Fatalf("activated state = %#v, want activated at 7", state)
	}
}

func TestJoinBootstrapIsMonotonicAndRetrySafe(t *testing.T) {
	bootstrap, err := NewJoinBootstrap(JoinBootstrapOptions{
		SourceNode:       "source",
		TargetNode:       "target",
		SnapshotSequence: 7,
		FencingToken:     11,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := bootstrap.SnapshotInstalled(7); err != nil {
		t.Fatal(err)
	}
	if err := bootstrap.SnapshotInstalled(7); err != nil {
		t.Fatalf("retry SnapshotInstalled() error = %v", err)
	}
	if err := bootstrap.CatchUp(9); err != nil {
		t.Fatal(err)
	}
	if err := bootstrap.CatchUp(8); !errors.Is(err, ErrJoinBootstrapCatchUpRegression) {
		t.Fatalf("CatchUp(8) error = %v, want regression", err)
	}
	if err := bootstrap.CatchUp(9); err != nil {
		t.Fatalf("retry CatchUp() error = %v", err)
	}
	state, err := bootstrap.Activate(9, 11)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := bootstrap.Activate(9, 11)
	if err != nil {
		t.Fatalf("retry Activate() error = %v", err)
	}
	if retry != state {
		t.Fatalf("retry state = %#v, want %#v", retry, state)
	}
	if err := bootstrap.Abort("too late"); !errors.Is(err, ErrJoinBootstrapTerminal) {
		t.Fatalf("Abort after activation error = %v, want terminal", err)
	}
}

func TestJoinBootstrapAbortIsTerminalAndRetrySafe(t *testing.T) {
	bootstrap, err := NewJoinBootstrap(JoinBootstrapOptions{
		SourceNode:       "source",
		TargetNode:       "target",
		SnapshotSequence: 7,
		FencingToken:     11,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := bootstrap.Abort("source unavailable"); err != nil {
		t.Fatal(err)
	}
	if err := bootstrap.Abort("source unavailable"); err != nil {
		t.Fatalf("retry Abort() error = %v", err)
	}
	if err := bootstrap.SnapshotInstalled(7); !errors.Is(err, ErrJoinBootstrapTerminal) {
		t.Fatalf("SnapshotInstalled after abort error = %v, want terminal", err)
	}
	state := bootstrap.Snapshot()
	if state.Phase != JoinBootstrapPhaseAborted || state.AbortReason != "source unavailable" {
		t.Fatalf("aborted state = %#v", state)
	}
}
