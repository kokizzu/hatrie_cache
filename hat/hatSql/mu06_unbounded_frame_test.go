package hatSql

import (
	"errors"
	"reflect"
	"testing"
)

func TestMU06UnboundedRowsFramesProduceCumulativeAndTrailingResults(t *testing.T) {
	value := func(row SQLRow) (float64, bool, error) {
		return float64(row["value"].(int64)), true, nil
	}
	window, err := NewDifferentialWindowWithFrame(DifferentialWindowOptions{
		Mode:  DifferentialWindowFrameRows,
		Value: value,
	}, DifferentialWindowFrame{
		Start: DifferentialWindowFrameBound{Kind: DifferentialWindowBoundUnboundedPreceding},
		End:   DifferentialWindowFrameBound{Kind: DifferentialWindowBoundCurrentRow},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := window.Apply([]DifferentialRow{
		{Key: "a", Time: 1, Diff: 1, Row: Row{"value": int64(1)}},
		{Key: "b", Time: 2, Diff: 1, Row: Row{"value": int64(2)}},
		{Key: "c", Time: 3, Diff: 1, Row: Row{"value": int64(3)}},
	}); err != nil {
		t.Fatal(err)
	}
	got := window.Snapshot()
	want := []DifferentialWindowRow{
		{Key: "a", Time: 1, Diff: 1, Row: Row{"value": int64(1)}, FrameCount: 1, FrameSum: 1, HasFrameSum: true},
		{Key: "b", Time: 2, Diff: 1, Row: Row{"value": int64(2)}, FrameCount: 2, FrameSum: 3, HasFrameSum: true},
		{Key: "c", Time: 3, Diff: 1, Row: Row{"value": int64(3)}, FrameCount: 3, FrameSum: 6, HasFrameSum: true},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("cumulative rows = %#v, want %#v", got, want)
	}

	trailing, err := NewDifferentialWindowWithFrame(DifferentialWindowOptions{
		Mode:  DifferentialWindowFrameRows,
		Value: value,
	}, DifferentialWindowFrame{
		Start: DifferentialWindowFrameBound{Kind: DifferentialWindowBoundCurrentRow},
		End:   DifferentialWindowFrameBound{Kind: DifferentialWindowBoundUnboundedFollowing},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := trailing.Apply([]DifferentialRow{
		{Key: "a", Time: 1, Diff: 1, Row: Row{"value": int64(1)}},
		{Key: "b", Time: 2, Diff: 1, Row: Row{"value": int64(2)}},
		{Key: "c", Time: 3, Diff: 1, Row: Row{"value": int64(3)}},
	}); err != nil {
		t.Fatal(err)
	}
	got = trailing.Snapshot()
	for index, row := range got {
		wantCount := int64(len(got) - index)
		if row.FrameCount != wantCount {
			t.Fatalf("trailing row %q count = %d, want %d", row.Key, row.FrameCount, wantCount)
		}
	}
}

func TestMU06UnboundedRangeFrameUsesCurrentTimeBoundary(t *testing.T) {
	window, err := NewDifferentialWindowWithFrame(DifferentialWindowOptions{Mode: DifferentialWindowFrameRange}, DifferentialWindowFrame{
		Start: DifferentialWindowFrameBound{Kind: DifferentialWindowBoundUnboundedPreceding},
		End:   DifferentialWindowFrameBound{Kind: DifferentialWindowBoundCurrentRow},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := window.Apply([]DifferentialRow{
		{Key: "old", Time: 10, Diff: 1, Row: Row{"value": int64(1)}},
		{Key: "same", Time: 20, Diff: 1, Row: Row{"value": int64(2)}},
		{Key: "new", Time: 20, Diff: 1, Row: Row{"value": int64(3)}},
	}); err != nil {
		t.Fatal(err)
	}
	rows := window.Snapshot()
	if rows[0].FrameCount != 1 || rows[1].FrameCount != 3 || rows[2].FrameCount != 3 {
		t.Fatalf("range cumulative counts = %#v, want 1,3,3", rows)
	}
}

func TestMU06UnboundedFrameRejectsImpossibleBounds(t *testing.T) {
	invalid := []DifferentialWindowFrame{
		{Start: DifferentialWindowFrameBound{Kind: DifferentialWindowBoundUnboundedFollowing}, End: DifferentialWindowFrameBound{Kind: DifferentialWindowBoundUnboundedFollowing}},
		{Start: DifferentialWindowFrameBound{Kind: DifferentialWindowBoundCurrentRow}, End: DifferentialWindowFrameBound{Kind: DifferentialWindowBoundUnboundedPreceding}},
		{Start: DifferentialWindowFrameBound{Kind: DifferentialWindowBoundCurrentRow, Offset: 1}, End: DifferentialWindowFrameBound{Kind: DifferentialWindowBoundUnboundedFollowing}},
	}
	for _, frame := range invalid {
		if _, err := NewDifferentialWindowWithFrame(DifferentialWindowOptions{}, frame); !errors.Is(err, ErrDifferentialWindowFrameInvalid) {
			t.Fatalf("frame %#v error = %v, want %v", frame, err, ErrDifferentialWindowFrameInvalid)
		}
	}
}
