package hatSql

import (
	"errors"
	"testing"
)

func TestM250TemporalJoinAlignmentWaitsForBothInputFrontiers(t *testing.T) {
	aligned, err := NewDifferentialTemporalJoinAligned(DifferentialTemporalJoinDefinition{
		MaxTimeDistance: 3,
		LeftKey:         func(row SQLRow) string { return row["join"].(string) },
		RightKey:        func(row SQLRow) string { return row["join"].(string) },
	}, DifferentialTemporalJoinAlignmentOptions{})
	if err != nil {
		t.Fatalf("NewDifferentialTemporalJoinAligned() error = %v", err)
	}
	left := DifferentialRow{Key: "left-1", Time: 10, Diff: 1, Row: Row{"join": "a", "left": 1}}
	right := DifferentialRow{Key: "right-1", Time: 12, Diff: 1, Row: Row{"join": "a", "right": 2}}
	if got, err := aligned.ApplyLeft(20, []DifferentialRow{left}); err != nil || got != nil {
		t.Fatalf("ApplyLeft() = %#v, error = %v; want nil, nil", got, err)
	}
	if stats := aligned.Stats(); stats.LeftFrontier != 20 || stats.RightFrontier != 0 || stats.AlignedFrontier != 0 || stats.PendingLeft != 1 || stats.PendingRight != 0 {
		t.Fatalf("stats after left frontier = %#v", stats)
	}
	got, err := aligned.ApplyRight(20, []DifferentialRow{right})
	if err != nil {
		t.Fatalf("ApplyRight() error = %v", err)
	}
	if len(got) != 1 || got[0].Diff != 1 || got[0].Time != 12 {
		t.Fatalf("aligned output = %#v, want one joined row", got)
	}
	if stats := aligned.Stats(); stats.LeftFrontier != 20 || stats.RightFrontier != 20 || stats.AlignedFrontier != 20 || stats.PendingLeft != 0 || stats.PendingRight != 0 {
		t.Fatalf("stats after alignment = %#v", stats)
	}
	if _, err := aligned.ApplyLeft(19, nil); !errors.Is(err, ErrDifferentialTemporalJoinAlignmentFrontierRegression) {
		t.Fatalf("frontier regression error = %v", err)
	}
}

func TestM250TemporalJoinAlignmentHoldsFutureRowsUntilBothAdvance(t *testing.T) {
	aligned, err := NewDifferentialTemporalJoinAligned(DifferentialTemporalJoinDefinition{
		MaxTimeDistance: 0,
		LeftKey:         func(row SQLRow) string { return row["join"].(string) },
		RightKey:        func(row SQLRow) string { return row["join"].(string) },
	}, DifferentialTemporalJoinAlignmentOptions{})
	if err != nil {
		t.Fatal(err)
	}
	left := DifferentialRow{Key: "left", Time: 30, Diff: 1, Row: Row{"join": "a"}}
	right := DifferentialRow{Key: "right", Time: 30, Diff: 1, Row: Row{"join": "a"}}
	if got, err := aligned.ApplyLeft(20, []DifferentialRow{left}); err != nil || got != nil {
		t.Fatalf("future ApplyLeft() = %#v, error = %v", got, err)
	}
	if got, err := aligned.ApplyRight(20, []DifferentialRow{right}); err != nil || got != nil {
		t.Fatalf("future ApplyRight() = %#v, error = %v", got, err)
	}
	if stats := aligned.Stats(); stats.AlignedFrontier != 20 || stats.PendingLeft != 1 || stats.PendingRight != 1 {
		t.Fatalf("future rows were not retained = %#v", stats)
	}
	if got, err := aligned.ApplyLeft(40, nil); err != nil || got != nil {
		t.Fatalf("left frontier advance = %#v, error = %v", got, err)
	}
	got, err := aligned.ApplyRight(40, nil)
	if err != nil {
		t.Fatalf("right frontier advance error = %v", err)
	}
	if len(got) != 1 || got[0].Diff != 1 {
		t.Fatalf("future aligned output = %#v, want one row", got)
	}
	if stats := aligned.Stats(); stats.AlignedFrontier != 40 || stats.PendingLeft != 0 || stats.PendingRight != 0 {
		t.Fatalf("stats after future release = %#v", stats)
	}
}

func TestM250TemporalJoinAlignmentBoundsPendingRowsAndClonesInput(t *testing.T) {
	newAligned := func(t *testing.T, options DifferentialTemporalJoinAlignmentOptions) *DifferentialTemporalJoinAligned {
		t.Helper()
		aligned, err := NewDifferentialTemporalJoinAligned(DifferentialTemporalJoinDefinition{
			MaxTimeDistance: 0,
			LeftKey:         func(row SQLRow) string { return row["join"].(string) },
			RightKey:        func(row SQLRow) string { return row["join"].(string) },
		}, options)
		if err != nil {
			t.Fatal(err)
		}
		return aligned
	}

	bounded := newAligned(t, DifferentialTemporalJoinAlignmentOptions{MaxPendingChanges: 1})
	left := DifferentialRow{Key: "left", Time: 10, Diff: 1, Row: Row{"join": "a", "value": "original"}}
	if got, err := bounded.ApplyLeft(20, []DifferentialRow{left}); err != nil || got != nil {
		t.Fatalf("bounded ApplyLeft() = %#v, error = %v", got, err)
	}
	if _, err := bounded.ApplyRight(20, []DifferentialRow{{Key: "right", Time: 10, Diff: 1, Row: Row{"join": "a"}}}); !errors.Is(err, ErrDifferentialTemporalJoinAlignmentPendingLimit) {
		t.Fatalf("pending limit error = %v", err)
	}
	if stats := bounded.Stats(); stats.RightFrontier != 0 || stats.PendingLeft != 1 || stats.PendingRight != 0 {
		t.Fatalf("state changed after pending-limit error = %#v", stats)
	}

	aligned := newAligned(t, DifferentialTemporalJoinAlignmentOptions{})
	mutable := DifferentialRow{Key: "left", Time: 10, Diff: 1, Row: Row{"join": "a", "value": "original"}}
	if _, err := aligned.ApplyLeft(20, []DifferentialRow{mutable}); err != nil {
		t.Fatal(err)
	}
	mutable.Row["value"] = "mutated"
	got, err := aligned.ApplyRight(20, []DifferentialRow{{Key: "right", Time: 10, Diff: 1, Row: Row{"join": "a"}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Row["left.value"] != "original" {
		t.Fatalf("aligned row clone = %#v, want original value", got)
	}
	if _, err := NewDifferentialTemporalJoinAligned(DifferentialTemporalJoinDefinition{
		LeftKey:  func(SQLRow) string { return "a" },
		RightKey: func(SQLRow) string { return "a" },
	}, DifferentialTemporalJoinAlignmentOptions{MaxPendingChanges: -1}); !errors.Is(err, ErrDifferentialTemporalJoinAlignmentInvalidOptions) {
		t.Fatalf("invalid options error = %v", err)
	}
}
