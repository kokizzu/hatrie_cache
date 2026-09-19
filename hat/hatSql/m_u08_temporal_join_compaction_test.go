package hatSql

import (
	"errors"
	"math"
	"testing"
)

func TestDifferentialTemporalJoinCompactsOnlyRowsPastBothCounterpartFrontiers(t *testing.T) {
	join, err := NewDifferentialTemporalJoin(DifferentialTemporalJoinDefinition{
		MaxTimeDistance: 2,
		LeftKey:         func(row SQLRow) string { return row["group"].(string) },
		RightKey:        func(row SQLRow) string { return row["group"].(string) },
	})
	if err != nil {
		t.Fatalf("NewDifferentialTemporalJoin() error = %v", err)
	}
	left := []DifferentialRow{
		{Key: "left-old", Time: 1, Diff: 1, Row: Row{"group": "a"}},
		{Key: "left-retained", Time: 4, Diff: 1, Row: Row{"group": "a"}},
	}
	right := []DifferentialRow{
		{Key: "right-old", Time: 1, Diff: 1, Row: Row{"group": "a"}},
		{Key: "right-retained", Time: 4, Diff: 1, Row: Row{"group": "a"}},
	}
	if _, err := join.ApplyLeft(left); err != nil {
		t.Fatalf("ApplyLeft() error = %v", err)
	}
	if _, err := join.ApplyRight(right); err != nil {
		t.Fatalf("ApplyRight() error = %v", err)
	}
	stats, err := join.Compact(5, 5)
	if err != nil {
		t.Fatalf("Compact() error = %v", err)
	}
	if stats.LeftFrontier != 5 || stats.RightFrontier != 5 || stats.RemovedLeft != 1 || stats.RemovedRight != 1 || stats.RetainedLeft != 1 || stats.RetainedRight != 1 {
		t.Fatalf("Compact() stats = %#v, want one removed and one retained per side", stats)
	}
	if _, err := join.ApplyLeft([]DifferentialRow{{Key: "left-future", Time: 5, Diff: 1, Row: Row{"group": "a"}}}); err != nil {
		t.Fatalf("future ApplyLeft() error = %v", err)
	}
	got, err := join.ApplyRight([]DifferentialRow{{Key: "right-future", Time: 6, Diff: 1, Row: Row{"group": "a"}}})
	if err != nil {
		t.Fatalf("future ApplyRight() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("future matches = %#v, want retained and future left rows", got)
	}
	if _, err := join.ApplyLeft([]DifferentialRow{{Key: "left-old", Time: 1, Diff: -1}}); !errors.Is(err, ErrDifferentialTemporalJoinCompacted) {
		t.Fatalf("compacted retraction error = %v, want ErrDifferentialTemporalJoinCompacted", err)
	}
}

func TestDifferentialTemporalJoinCompactionRejectsRegressiveFrontiersAtomically(t *testing.T) {
	join, err := NewDifferentialTemporalJoin(DifferentialTemporalJoinDefinition{
		MaxTimeDistance: 1,
		LeftKey:         func(row SQLRow) string { return row["group"].(string) },
		RightKey:        func(row SQLRow) string { return row["group"].(string) },
	})
	if err != nil {
		t.Fatalf("NewDifferentialTemporalJoin() error = %v", err)
	}
	if _, err := join.ApplyLeft([]DifferentialRow{{Key: "left", Time: 10, Diff: 1, Row: Row{"group": "a"}}}); err != nil {
		t.Fatalf("ApplyLeft() error = %v", err)
	}
	if _, err := join.ApplyRight([]DifferentialRow{{Key: "right", Time: 10, Diff: 1, Row: Row{"group": "a"}}}); err != nil {
		t.Fatalf("ApplyRight() error = %v", err)
	}
	if _, err := join.Compact(8, 8); err != nil {
		t.Fatalf("initial Compact() error = %v", err)
	}
	if got, err := join.Compact(7, 9); !errors.Is(err, ErrDifferentialTemporalJoinFrontierRegression) || got != (DifferentialTemporalJoinCompactionStats{}) {
		t.Fatalf("regressive Compact() = %#v, error %v; want zero stats and frontier error", got, err)
	}
	if _, err := join.ApplyLeft([]DifferentialRow{{Key: "left", Time: 10, Diff: -1}}); err != nil {
		t.Fatalf("state changed after regressive Compact(): %v", err)
	}
}

func TestDifferentialTemporalJoinCompactionDoesNotEvictUnsealedSide(t *testing.T) {
	join, err := NewDifferentialTemporalJoin(DifferentialTemporalJoinDefinition{
		MaxTimeDistance: 2,
		LeftKey:         func(row SQLRow) string { return row["group"].(string) },
		RightKey:        func(row SQLRow) string { return row["group"].(string) },
	})
	if err != nil {
		t.Fatalf("NewDifferentialTemporalJoin() error = %v", err)
	}
	if _, err := join.ApplyLeft([]DifferentialRow{{Key: "left", Time: 1, Diff: 1, Row: Row{"group": "a"}}}); err != nil {
		t.Fatalf("ApplyLeft() error = %v", err)
	}
	if _, err := join.ApplyRight([]DifferentialRow{{Key: "right", Time: 1, Diff: 1, Row: Row{"group": "a"}}}); err != nil {
		t.Fatalf("ApplyRight() error = %v", err)
	}
	stats, err := join.Compact(1, 10)
	if err != nil {
		t.Fatalf("unsealed Compact() error = %v", err)
	}
	if stats.RemovedLeft != 0 || stats.RemovedRight != 0 {
		t.Fatalf("unsealed Compact() stats = %#v, want no rows removed", stats)
	}
	stats, err = join.Compact(2, 10)
	if err != nil {
		t.Fatalf("sealed Compact() error = %v", err)
	}
	if stats.RemovedLeft != 1 || stats.RemovedRight != 0 {
		t.Fatalf("sealed Compact() stats = %#v, want only left row removed", stats)
	}
}

func TestDifferentialTemporalJoinCompactionSaturatesTimeUpperBound(t *testing.T) {
	join, err := NewDifferentialTemporalJoin(DifferentialTemporalJoinDefinition{
		MaxTimeDistance: 2,
		LeftKey:         func(row SQLRow) string { return row["group"].(string) },
		RightKey:        func(row SQLRow) string { return row["group"].(string) },
	})
	if err != nil {
		t.Fatalf("NewDifferentialTemporalJoin() error = %v", err)
	}
	if _, err := join.ApplyLeft([]DifferentialRow{{Key: "left", Time: math.MaxUint64 - 1, Diff: 1, Row: Row{"group": "a"}}}); err != nil {
		t.Fatalf("ApplyLeft() error = %v", err)
	}
	stats, err := join.Compact(0, math.MaxUint64)
	if err != nil {
		t.Fatalf("Compact() error = %v", err)
	}
	if stats.RemovedLeft != 0 || stats.RetainedLeft != 1 {
		t.Fatalf("overflow-bound Compact() stats = %#v, want row retained", stats)
	}
}

func TestDifferentialTemporalJoinCompactionValidatesNilReceiver(t *testing.T) {
	var join *DifferentialTemporalJoin
	if _, err := join.Compact(1, 1); !errors.Is(err, ErrDifferentialTemporalJoinNil) {
		t.Fatalf("nil Compact() error = %v, want ErrDifferentialTemporalJoinNil", err)
	}
}
