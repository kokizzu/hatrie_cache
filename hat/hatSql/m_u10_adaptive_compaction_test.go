package hatSql

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestAdaptiveTemporalJoinCompactionIsDisabledByDefault(t *testing.T) {
	if _, err := NewDifferentialTemporalJoin(DifferentialTemporalJoinDefinition{
		LeftKey:          func(row SQLRow) string { return row["group"].(string) },
		RightKey:         func(row SQLRow) string { return row["group"].(string) },
		CompactionPolicy: &DifferentialTemporalJoinCompactionPolicy{MaxFrontierAge: -time.Second},
	}); !errors.Is(err, ErrDifferentialTemporalJoinCompactionPolicyInvalid) {
		t.Fatalf("negative compaction age error = %v, want policy validation error", err)
	}
	join := newMUTenJoin(t, nil)
	now := time.Unix(100, 0)
	if _, err := join.ApplyLeft([]DifferentialRow{{Key: "left", Time: 1, Diff: 1, Row: Row{"group": "g"}}}); err != nil {
		t.Fatal(err)
	}
	recommendation := join.CompactionRecommendation(now)
	if recommendation.ShouldCompact {
		t.Fatalf("default recommendation = %#v, want disabled", recommendation)
	}
}

func TestAdaptiveTemporalJoinCompactionTriggersOnUpdatesAndBytes(t *testing.T) {
	policy := &DifferentialTemporalJoinCompactionPolicy{
		MinUpdates:           2,
		MaxStateBytes:        1 << 20,
		MaxFrontierAge:       time.Hour,
		EstimatedBytesPerRow: 64,
	}
	join := newMUTenJoin(t, policy)
	changes := []DifferentialRow{
		{Key: "left-a", Time: 1, Diff: 1, Row: Row{"group": "a"}},
		{Key: "left-b", Time: 2, Diff: 1, Row: Row{"group": "b"}},
	}
	if _, err := join.ApplyLeft(changes); err != nil {
		t.Fatal(err)
	}
	recommendation := join.CompactionRecommendation(time.Unix(100, 0))
	if !recommendation.ShouldCompact || recommendation.Reason != DifferentialTemporalJoinCompactionReasonUpdates {
		t.Fatalf("update recommendation = %#v, want update trigger", recommendation)
	}

	bytesPolicy := &DifferentialTemporalJoinCompactionPolicy{
		MinUpdates:           100,
		MaxStateBytes:        64,
		MaxFrontierAge:       time.Hour,
		EstimatedBytesPerRow: 64,
	}
	bytesJoin := newMUTenJoin(t, bytesPolicy)
	if _, err := bytesJoin.ApplyLeft([]DifferentialRow{{Key: "left", Time: 1, Diff: 1, Row: Row{"group": "g"}}}); err != nil {
		t.Fatal(err)
	}
	recommendation = bytesJoin.CompactionRecommendation(time.Unix(100, 0))
	if !recommendation.ShouldCompact || recommendation.Reason != DifferentialTemporalJoinCompactionReasonBytes {
		t.Fatalf("byte recommendation = %#v, want byte trigger", recommendation)
	}
}

func TestAdaptiveTemporalJoinCompactionCancellationPreservesState(t *testing.T) {
	policy := &DifferentialTemporalJoinCompactionPolicy{
		MinUpdates:           1,
		MaxStateBytes:        1 << 20,
		MaxFrontierAge:       time.Hour,
		EstimatedBytesPerRow: 64,
	}
	join := newMUTenJoin(t, policy)
	rows := []DifferentialRow{
		{Key: "left", Time: 1, Diff: 1, Row: Row{"group": "g"}},
		{Key: "right", Time: 1, Diff: 1, Row: Row{"group": "g"}},
	}
	if _, err := join.ApplyLeft(rows[:1]); err != nil {
		t.Fatal(err)
	}
	if _, err := join.ApplyRight(rows[1:]); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stats, recommendation, err := join.CompactIfNeeded(ctx, 5, 5, time.Unix(100, 0))
	if !errors.Is(err, context.Canceled) || !recommendation.ShouldCompact || stats != (DifferentialTemporalJoinCompactionStats{}) {
		t.Fatalf("canceled CompactIfNeeded() = stats %#v, recommendation %#v, error %v", stats, recommendation, err)
	}
	if stats, err := join.Compact(5, 5); err != nil || stats.RemovedLeft != 1 || stats.RemovedRight != 1 {
		t.Fatalf("post-cancellation Compact() = %#v, %v", stats, err)
	}
}

func TestAdaptiveTemporalJoinCompactionTriggersOnFrontierAge(t *testing.T) {
	policy := &DifferentialTemporalJoinCompactionPolicy{
		MinUpdates:           100,
		MaxStateBytes:        1 << 20,
		MaxFrontierAge:       time.Nanosecond,
		EstimatedBytesPerRow: 64,
	}
	join := newMUTenJoin(t, policy)
	if _, err := join.ApplyLeft([]DifferentialRow{{Key: "left", Time: 1, Diff: 1, Row: Row{"group": "g"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := join.Compact(0, 0); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	recommendation := join.CompactionRecommendation(time.Now())
	if !recommendation.ShouldCompact || recommendation.Reason != DifferentialTemporalJoinCompactionReasonAge {
		t.Fatalf("age recommendation = %#v, want age trigger", recommendation)
	}
}

func newMUTenJoin(t *testing.T, policy *DifferentialTemporalJoinCompactionPolicy) *DifferentialTemporalJoin {
	t.Helper()
	join, err := NewDifferentialTemporalJoin(DifferentialTemporalJoinDefinition{
		MaxTimeDistance:  1,
		LeftKey:          func(row SQLRow) string { return row["group"].(string) },
		RightKey:         func(row SQLRow) string { return row["group"].(string) },
		CompactionPolicy: policy,
	})
	if err != nil {
		t.Fatal(err)
	}
	return join
}
