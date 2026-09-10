package hatPartition

import (
	"slices"
	"testing"
)

func TestPlanSplitProducesDeterministicMovesAndKeyRouting(t *testing.T) {
	plan, err := PlanSplit(4)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := plan.SourceCount(), 4; got != want {
		t.Fatalf("SourceCount() = %d, want %d", got, want)
	}
	if got, want := plan.TargetCount(), 8; got != want {
		t.Fatalf("TargetCount() = %d, want %d", got, want)
	}
	if got, want := plan.Operation(), ResizeSplit; got != want {
		t.Fatalf("Operation() = %q, want %q", got, want)
	}
	wantMoves := []ResizeMove{
		{Source: 0, Target: 0}, {Source: 0, Target: 4},
		{Source: 1, Target: 1}, {Source: 1, Target: 5},
		{Source: 2, Target: 2}, {Source: 2, Target: 6},
		{Source: 3, Target: 3}, {Source: 3, Target: 7},
	}
	if got := plan.Moves(); !slices.Equal(got, wantMoves) {
		t.Fatalf("Moves() = %#v, want %#v", got, wantMoves)
	}
	for _, key := range []string{"", "a", "customer/1", "customer/2", "region/ap-southeast-1"} {
		if got, want := plan.SourcePartition(key), Index(key, 4); got != want {
			t.Fatalf("SourcePartition(%q) = %d, want %d", key, got, want)
		}
		if got, want := plan.TargetPartition(key), Index(key, 8); got != want {
			t.Fatalf("TargetPartition(%q) = %d, want %d", key, got, want)
		}
		if got, want := plan.MovesKey(key), plan.SourcePartition(key) != plan.TargetPartition(key); got != want {
			t.Fatalf("MovesKey(%q) = %v, want %v", key, got, want)
		}
		source, target, ok := plan.RouteKey(key)
		if !ok || source != plan.SourcePartition(key) || target != plan.TargetPartition(key) {
			t.Fatalf("RouteKey(%q) = (%d, %d, %v), want (%d, %d, true)", key, source, target, ok, plan.SourcePartition(key), plan.TargetPartition(key))
		}
	}
}

func TestPlanMergeProducesDeterministicMoves(t *testing.T) {
	plan, err := PlanMerge(8)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := plan.TargetCount(), 4; got != want {
		t.Fatalf("TargetCount() = %d, want %d", got, want)
	}
	if got, want := plan.Operation(), ResizeMerge; got != want {
		t.Fatalf("Operation() = %q, want %q", got, want)
	}
	wantMoves := []ResizeMove{
		{Source: 0, Target: 0}, {Source: 4, Target: 0},
		{Source: 1, Target: 1}, {Source: 5, Target: 1},
		{Source: 2, Target: 2}, {Source: 6, Target: 2},
		{Source: 3, Target: 3}, {Source: 7, Target: 3},
	}
	if got := plan.Moves(); !slices.Equal(got, wantMoves) {
		t.Fatalf("Moves() = %#v, want %#v", got, wantMoves)
	}
	for _, key := range []string{"", "a", "customer/1", "customer/2", "region/ap-southeast-1"} {
		source, target, ok := plan.RouteKey(key)
		if !ok || source != Index(key, 8) || target != Index(key, 4) {
			t.Fatalf("RouteKey(%q) = (%d, %d, %v), want (%d, %d, true)", key, source, target, ok, Index(key, 8), Index(key, 4))
		}
	}
}

func TestPlanResizeValidatesAdjacentLayouts(t *testing.T) {
	for _, test := range []struct {
		name   string
		from   int
		to     int
		wantOp ResizeOperation
	}{
		{name: "split", from: 2, to: 4, wantOp: ResizeSplit},
		{name: "merge", from: 16, to: 8, wantOp: ResizeMerge},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan, err := PlanResize(test.from, test.to)
			if err != nil {
				t.Fatal(err)
			}
			if got := plan.Operation(); got != test.wantOp {
				t.Fatalf("Operation() = %q, want %q", got, test.wantOp)
			}
		})
	}
	for _, test := range []struct {
		name string
		from int
		to   int
	}{
		{name: "disabled source", from: 0, to: 2},
		{name: "disabled target", from: 2, to: 0},
		{name: "one source", from: 1, to: 2},
		{name: "one target", from: 2, to: 1},
		{name: "not power of two", from: 4, to: 6},
		{name: "not adjacent", from: 2, to: 8},
		{name: "same count", from: 4, to: 4},
		{name: "split beyond maximum", from: MaxCount, to: MaxCount * 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := PlanResize(test.from, test.to); err == nil {
				t.Fatalf("PlanResize(%d, %d) unexpectedly succeeded", test.from, test.to)
			}
		})
	}
}

func TestPlanSplitAndMergeConvenienceValidation(t *testing.T) {
	if _, err := PlanSplit(0); err == nil {
		t.Fatal("PlanSplit(0) unexpectedly succeeded")
	}
	if _, err := PlanMerge(2); err == nil {
		t.Fatal("PlanMerge(2) unexpectedly succeeded")
	}
	if got := (ResizePlan{}).SourcePartition("key"); got != -1 {
		t.Fatalf("zero SourcePartition() = %d, want -1", got)
	}
	if got := (ResizePlan{}).TargetPartition("key"); got != -1 {
		t.Fatalf("zero TargetPartition() = %d, want -1", got)
	}
	if got := (ResizePlan{}).Moves(); got != nil {
		t.Fatalf("zero Moves() = %#v, want nil", got)
	}
	if source, target, ok := (ResizePlan{}).RouteKey("key"); ok || source != -1 || target != -1 {
		t.Fatalf("zero RouteKey() = (%d, %d, %v), want (-1, -1, false)", source, target, ok)
	}
}
