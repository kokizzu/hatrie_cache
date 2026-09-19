package hatSql_test

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestDifferentialDataflowPolicyAcceptsWithinLatenessAndDropsTooLate(t *testing.T) {
	var received []hatSql.DifferentialRow
	flow, err := hatSql.NewDifferentialDataflow(hatSql.DifferentialDataflowOptions{
		Policy: hatSql.DifferentialDataflowPolicy{
			AllowedLateness: 2,
			Correction:      hatSql.DifferentialDataflowDrop,
			Frontier:        hatSql.DifferentialDataflowManual,
		},
		InitialFrontier: 10,
		Sink: func(rows []hatSql.DifferentialRow) error {
			received = append(received, rows...)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewDifferentialDataflow() error = %v", err)
	}
	input := []hatSql.DifferentialRow{
		{Key: "within-1", Time: 9, Diff: 1, Row: hatSql.Row{"value": "one"}},
		{Key: "within-2", Time: 8, Diff: 1, Row: hatSql.Row{"value": "two"}},
		{Key: "too-late", Time: 7, Diff: 1, Row: hatSql.Row{"value": "drop"}},
		{Key: "on-time", Time: 10, Diff: 1, Row: hatSql.Row{"value": "on"}},
	}
	if err := flow.Apply(input); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if !reflect.DeepEqual(input[0].Row, hatSql.Row{"value": "one"}) {
		t.Fatalf("Apply() mutated input row: %#v", input[0].Row)
	}
	want := []hatSql.DifferentialRow{input[0], input[1], input[3]}
	if !reflect.DeepEqual(received, want) {
		t.Fatalf("received = %#v, want %#v", received, want)
	}
	stats := flow.Stats()
	if stats.Frontier != 10 || stats.AppliedBatches != 1 || stats.AcceptedRows != 3 || stats.LateRows != 3 || stats.AcceptedLateRows != 2 || stats.TooLateRows != 1 || stats.DroppedRows != 1 {
		t.Fatalf("Stats() = %#v, want frontier 10, applied 1, accepted 3, late 3, accepted late 2, too late 1, dropped 1", stats)
	}
	received[0].Row["value"] = "sink mutation"
	if input[0].Row["value"] != "one" {
		t.Fatal("sink received an aliased input row")
	}
}

func TestDifferentialDataflowPolicyRejectsTooLateBatchAtomically(t *testing.T) {
	sinkCalls := 0
	flow, err := hatSql.NewDifferentialDataflow(hatSql.DifferentialDataflowOptions{
		Policy: hatSql.DifferentialDataflowPolicy{
			AllowedLateness: 1,
			Correction:      hatSql.DifferentialDataflowReject,
			Frontier:        hatSql.DifferentialDataflowManual,
		},
		InitialFrontier: 10,
		Sink: func([]hatSql.DifferentialRow) error {
			sinkCalls++
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewDifferentialDataflow() error = %v", err)
	}
	if err := flow.Apply([]hatSql.DifferentialRow{
		{Key: "on-time", Time: 10, Diff: 1},
		{Key: "too-late", Time: 8, Diff: 1},
	}); !errors.Is(err, hatSql.ErrDifferentialDataflowTooLate) {
		t.Fatalf("Apply() error = %v, want ErrDifferentialDataflowTooLate", err)
	}
	if sinkCalls != 0 {
		t.Fatalf("sink calls = %d, want 0", sinkCalls)
	}
	stats := flow.Stats()
	if stats.AppliedBatches != 0 || stats.AcceptedRows != 0 || stats.RejectedBatches != 1 || stats.RejectedRows != 1 || stats.Frontier != 10 {
		t.Fatalf("Stats() after rejection = %#v, want atomic state with one rejected batch", stats)
	}
}

func TestDifferentialDataflowBatchMaxFrontierAdvancesOnlyAfterSinkSuccess(t *testing.T) {
	var received []hatSql.DifferentialRow
	flow, err := hatSql.NewDifferentialDataflow(hatSql.DifferentialDataflowOptions{
		Policy: hatSql.DifferentialDataflowPolicy{
			Correction: hatSql.DifferentialDataflowDrop,
			Frontier:   hatSql.DifferentialDataflowBatchMax,
		},
		Sink: func(rows []hatSql.DifferentialRow) error {
			received = append(received, rows...)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewDifferentialDataflow() error = %v", err)
	}
	if err := flow.Apply([]hatSql.DifferentialRow{{Key: "first", Time: 5, Diff: 1}}); err != nil {
		t.Fatalf("first Apply() error = %v", err)
	}
	if got := flow.Frontier(); got != 5 {
		t.Fatalf("Frontier() after first batch = %d, want 5", got)
	}
	if err := flow.Apply([]hatSql.DifferentialRow{
		{Key: "late", Time: 3, Diff: 1},
		{Key: "new", Time: 7, Diff: 1},
	}); err != nil {
		t.Fatalf("second Apply() error = %v", err)
	}
	if got := flow.Frontier(); got != 7 {
		t.Fatalf("Frontier() after second batch = %d, want 7", got)
	}
	if len(received) != 2 || received[1].Key != "new" {
		t.Fatalf("received = %#v, want first and new rows only", received)
	}
}

func TestDifferentialDataflowSinkFailureDoesNotCommitBatch(t *testing.T) {
	wantErr := errors.New("sink unavailable")
	fail := true
	flow, err := hatSql.NewDifferentialDataflow(hatSql.DifferentialDataflowOptions{
		Policy: hatSql.DifferentialDataflowPolicy{Frontier: hatSql.DifferentialDataflowBatchMax},
		Sink: func([]hatSql.DifferentialRow) error {
			if fail {
				return wantErr
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewDifferentialDataflow() error = %v", err)
	}
	if err := flow.Apply([]hatSql.DifferentialRow{{Key: "first", Time: 5, Diff: 1}}); !errors.Is(err, wantErr) {
		t.Fatalf("first Apply() error = %v, want sink error", err)
	}
	if stats := flow.Stats(); stats.AppliedBatches != 0 || stats.Frontier != 0 || stats.SinkErrors != 1 {
		t.Fatalf("Stats() after sink failure = %#v, want no applied batch and one sink error", stats)
	}
	fail = false
	if err := flow.Apply([]hatSql.DifferentialRow{{Key: "first", Time: 5, Diff: 1}}); err != nil {
		t.Fatalf("retry Apply() error = %v", err)
	}
	if got := flow.Frontier(); got != 5 {
		t.Fatalf("Frontier() after retry = %d, want 5", got)
	}
}

func TestDifferentialDataflowPolicyDrivesDifferentialWindow(t *testing.T) {
	window, err := hatSql.NewDifferentialWindow(hatSql.DifferentialWindowOptions{
		Mode:  hatSql.DifferentialWindowFrameRows,
		Start: -1,
		End:   0,
	})
	if err != nil {
		t.Fatalf("NewDifferentialWindow() error = %v", err)
	}
	flow, err := hatSql.NewDifferentialDataflow(hatSql.DifferentialDataflowOptions{
		Policy: hatSql.DifferentialDataflowPolicy{
			AllowedLateness: 1,
			Correction:      hatSql.DifferentialDataflowDrop,
			Frontier:        hatSql.DifferentialDataflowManual,
		},
		InitialFrontier: 5,
		Sink: func(rows []hatSql.DifferentialRow) error {
			_, err := window.Apply(rows)
			return err
		},
	})
	if err != nil {
		t.Fatalf("NewDifferentialDataflow() error = %v", err)
	}
	if err := flow.Apply([]hatSql.DifferentialRow{
		{Key: "on-time", Time: 5, Diff: 1, Row: hatSql.Row{"value": int64(5)}},
		{Key: "within", Time: 4, Diff: 1, Row: hatSql.Row{"value": int64(4)}},
		{Key: "too-late", Time: 3, Diff: 1, Row: hatSql.Row{"value": int64(3)}},
	}); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if got := window.Len(); got != 2 {
		t.Fatalf("window Len() = %d, want 2 accepted rows", got)
	}
}

func TestDifferentialDataflowValidatesOptionsAndFrontier(t *testing.T) {
	sink := func([]hatSql.DifferentialRow) error { return nil }
	for _, testCase := range []struct {
		name   string
		policy hatSql.DifferentialDataflowPolicy
	}{
		{name: "correction", policy: hatSql.DifferentialDataflowPolicy{Correction: 99}},
		{name: "frontier", policy: hatSql.DifferentialDataflowPolicy{Frontier: 99}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := hatSql.NewDifferentialDataflow(hatSql.DifferentialDataflowOptions{Policy: testCase.policy, Sink: sink}); !errors.Is(err, hatSql.ErrDifferentialDataflowPolicyInvalid) {
				t.Fatalf("error = %v, want ErrDifferentialDataflowPolicyInvalid", err)
			}
		})
	}
	if _, err := hatSql.NewDifferentialDataflow(hatSql.DifferentialDataflowOptions{}); !errors.Is(err, hatSql.ErrDifferentialDataflowSinkRequired) {
		t.Fatalf("missing sink error = %v, want ErrDifferentialDataflowSinkRequired", err)
	}
	flow, err := hatSql.NewDifferentialDataflow(hatSql.DifferentialDataflowOptions{Sink: sink})
	if err != nil {
		t.Fatalf("default NewDifferentialDataflow() error = %v", err)
	}
	if err := flow.Advance(5); err != nil {
		t.Fatalf("Advance() error = %v", err)
	}
	if err := flow.Advance(4); !errors.Is(err, hatSql.ErrDifferentialDataflowFrontierRegression) {
		t.Fatalf("regressive Advance() error = %v, want ErrDifferentialDataflowFrontierRegression", err)
	}
	var nilFlow *hatSql.DifferentialDataflow
	if err := nilFlow.Apply(nil); !errors.Is(err, hatSql.ErrDifferentialDataflowNil) {
		t.Fatalf("nil Apply() error = %v, want ErrDifferentialDataflowNil", err)
	}
}

func TestDifferentialDataflowEmptyBatchDoesNotCallSink(t *testing.T) {
	calls := 0
	flow, err := hatSql.NewDifferentialDataflow(hatSql.DifferentialDataflowOptions{
		Sink: func([]hatSql.DifferentialRow) error {
			calls++
			return fmt.Errorf("unexpected sink call")
		},
	})
	if err != nil {
		t.Fatalf("NewDifferentialDataflow() error = %v", err)
	}
	if err := flow.Apply(nil); err != nil {
		t.Fatalf("empty Apply() error = %v", err)
	}
	if calls != 0 {
		t.Fatalf("sink calls = %d, want 0", calls)
	}
}
