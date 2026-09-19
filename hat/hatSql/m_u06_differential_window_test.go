package hatSql_test

import (
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestDifferentialWindowRowsFrameSupportsRetractionsAndLateRows(t *testing.T) {
	window, err := hatSql.NewDifferentialWindow(hatSql.DifferentialWindowOptions{
		Mode:  hatSql.DifferentialWindowFrameRows,
		Start: -1,
		End:   0,
		Value: mU06WindowValue,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := window.Apply([]hatSql.DifferentialRow{
		{Key: "a", Time: 1, Diff: 1, Row: hatSql.Row{"value": int64(10)}},
		{Key: "b", Time: 2, Diff: 1, Row: hatSql.Row{"value": int64(20)}},
		{Key: "c", Time: 3, Diff: 1, Row: hatSql.Row{"value": int64(30)}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := window.Apply([]hatSql.DifferentialRow{{Key: "aa", Time: 2, Diff: 1, Row: hatSql.Row{"value": int64(5)}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := window.Apply([]hatSql.DifferentialRow{{Key: "b", Time: 2, Diff: -1}}); err != nil {
		t.Fatal(err)
	}

	want := []hatSql.DifferentialWindowRow{
		{Key: "a", Time: 1, Diff: 1, FrameCount: 1, FrameSum: 10, HasFrameSum: true, Row: hatSql.Row{"value": int64(10)}},
		{Key: "aa", Time: 2, Diff: 1, FrameCount: 2, FrameSum: 15, HasFrameSum: true, Row: hatSql.Row{"value": int64(5)}},
		{Key: "c", Time: 3, Diff: 1, FrameCount: 2, FrameSum: 35, HasFrameSum: true, Row: hatSql.Row{"value": int64(30)}},
	}
	if got := window.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Snapshot() = %#v, want %#v", got, want)
	}
}

func TestDifferentialWindowRangeFrameSupportsLateAndWeightedUpdates(t *testing.T) {
	window, err := hatSql.NewDifferentialWindow(hatSql.DifferentialWindowOptions{
		Mode:  hatSql.DifferentialWindowFrameRange,
		Start: -1,
		End:   0,
		Value: mU06WindowValue,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := window.Apply([]hatSql.DifferentialRow{
		{Key: "c", Time: 12, Diff: 2, Row: hatSql.Row{"value": int64(3)}},
		{Key: "a", Time: 10, Diff: 1, Row: hatSql.Row{"value": int64(5)}},
		{Key: "b", Time: 11, Diff: 1, Row: hatSql.Row{"value": int64(7)}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := window.Apply([]hatSql.DifferentialRow{{Key: "aa", Time: 10, Diff: 1, Row: hatSql.Row{"value": int64(2)}}}); err != nil {
		t.Fatal(err)
	}
	want := []hatSql.DifferentialWindowRow{
		{Key: "a", Time: 10, Diff: 1, FrameCount: 2, FrameSum: 7, HasFrameSum: true, Row: hatSql.Row{"value": int64(5)}},
		{Key: "aa", Time: 10, Diff: 1, FrameCount: 2, FrameSum: 7, HasFrameSum: true, Row: hatSql.Row{"value": int64(2)}},
		{Key: "b", Time: 11, Diff: 1, FrameCount: 3, FrameSum: 14, HasFrameSum: true, Row: hatSql.Row{"value": int64(7)}},
		{Key: "c", Time: 12, Diff: 2, FrameCount: 3, FrameSum: 13, HasFrameSum: true, Row: hatSql.Row{"value": int64(3)}},
	}
	if got := window.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("late range Snapshot() = %#v, want %#v", got, want)
	}
	if _, err := window.Apply([]hatSql.DifferentialRow{{Key: "c", Time: 12, Diff: -1}}); err != nil {
		t.Fatal(err)
	}
	want[3].Diff = 1
	want[3].FrameCount = 2
	want[3].FrameSum = 10
	if got := window.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("weighted retraction Snapshot() = %#v, want %#v", got, want)
	}
}

func TestDifferentialWindowRejectsInvalidUpdatesAtomically(t *testing.T) {
	window, err := hatSql.NewDifferentialWindow(hatSql.DifferentialWindowOptions{MaxRows: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := window.Apply([]hatSql.DifferentialRow{
		{Key: "a", Time: 1, Diff: 1, Row: hatSql.Row{"value": int64(1)}},
		{Key: "b", Time: 2, Diff: 1, Row: hatSql.Row{"value": int64(2)}},
	}); !errors.Is(err, hatSql.ErrDifferentialWindowMaxRows) {
		t.Fatalf("max rows error = %v", err)
	}
	if got := window.Snapshot(); got != nil {
		t.Fatalf("failed max rows update changed state: %#v", got)
	}
	if _, err := window.Apply([]hatSql.DifferentialRow{{Key: "missing", Time: 1, Diff: -1}}); !errors.Is(err, hatSql.ErrDifferentialWindowRetractionMissing) {
		t.Fatalf("missing retraction error = %v", err)
	}
	if _, err := window.Apply([]hatSql.DifferentialRow{{Key: "a", Time: 1, Diff: 1, Row: hatSql.Row{"value": int64(1)}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := window.Apply([]hatSql.DifferentialRow{{Key: "a", Time: 1, Diff: 1, Row: hatSql.Row{"value": int64(2)}}}); !errors.Is(err, hatSql.ErrDifferentialWindowRowMismatch) {
		t.Fatalf("row mismatch error = %v", err)
	}
	if got := window.Snapshot(); len(got) != 1 || got[0].Diff != 1 || got[0].Row["value"] != int64(1) {
		t.Fatalf("row mismatch changed state: %#v", got)
	}
}

func TestDifferentialWindowValueCallbackCannotMutateState(t *testing.T) {
	window, err := hatSql.NewDifferentialWindow(hatSql.DifferentialWindowOptions{
		Value: func(row hatSql.SQLRow) (float64, bool, error) {
			row["value"] = int64(99)
			return 1, true, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := window.Apply([]hatSql.DifferentialRow{{Key: "a", Time: 1, Diff: 1, Row: hatSql.Row{"value": int64(1)}}}); err != nil {
		t.Fatal(err)
	}
	rows := window.Snapshot()
	if len(rows) != 1 || rows[0].Row["value"] != int64(1) {
		t.Fatalf("value callback mutated retained row: %#v", rows)
	}
}

func TestDifferentialWindowRejectsMinimumIntRetraction(t *testing.T) {
	window, err := hatSql.NewDifferentialWindow(hatSql.DifferentialWindowOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := window.Apply([]hatSql.DifferentialRow{{Key: "a", Time: 1, Diff: 1, Row: hatSql.Row{"value": int64(1)}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := window.Apply([]hatSql.DifferentialRow{{Key: "a", Time: 1, Diff: -1 << 63}}); !errors.Is(err, hatSql.ErrDifferentialWindowRetractionMissing) {
		t.Fatalf("minimum-int retraction error = %v", err)
	}
	if got := window.Len(); got != 1 {
		t.Fatalf("minimum-int retraction changed length to %d", got)
	}
}

func TestDifferentialWindowSnapshotWithErrorReportsCallbackFailure(t *testing.T) {
	fail := false
	wantErr := errors.New("value callback failed")
	window, err := hatSql.NewDifferentialWindow(hatSql.DifferentialWindowOptions{
		Value: func(row hatSql.SQLRow) (float64, bool, error) {
			if fail {
				return 0, false, wantErr
			}
			return 1, true, nil
		},
	})
	if err != nil {
		t.Fatalf("NewDifferentialWindow() error = %v", err)
	}
	if _, err := window.Apply([]hatSql.DifferentialRow{{Key: "one", Time: 1, Diff: 1, Row: hatSql.Row{"value": 1}}}); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	fail = true
	if got, err := window.SnapshotWithError(); !errors.Is(err, wantErr) || got != nil {
		t.Fatalf("SnapshotWithError() = %#v, error %v; want nil and callback error", got, err)
	}
	if got := window.Snapshot(); got != nil {
		t.Fatalf("Snapshot() after callback error = %#v, want nil", got)
	}
	var nilWindow *hatSql.DifferentialWindow
	if got, err := nilWindow.SnapshotWithError(); !errors.Is(err, hatSql.ErrDifferentialWindowNil) || got != nil {
		t.Fatalf("nil SnapshotWithError() = %#v, error %v; want nil receiver error", got, err)
	}
}

func mU06WindowValue(row hatSql.SQLRow) (float64, bool, error) {
	value, ok := row["value"].(int64)
	return float64(value), ok, nil
}
