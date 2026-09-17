package hatStorage_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"hatrie_cache/hat/hatStorage"
)

func newCH015StorageTierMovePolicy(t *testing.T) hatStorage.StorageTierPolicy {
	t.Helper()
	rules := []struct {
		name string
		path string
	}{
		{name: "hot", path: "/data/hot"},
		{name: "warm", path: "/data/warm"},
		{name: "cold", path: "/data/cold"},
	}
	tierRules := make([]hatStorage.StorageTierRule, 0, len(rules))
	for index, rule := range rules {
		placement, err := hatStorage.NewDiskPlacementPolicy(rule.name, []hatStorage.DiskPlacementRule{{Path: rule.path, Weight: 1}})
		if err != nil {
			t.Fatal(err)
		}
		tierRules = append(tierRules, hatStorage.StorageTierRule{
			Name:      rule.name,
			MinAge:    time.Duration(index) * time.Hour,
			Placement: placement,
		})
	}
	policy, err := hatStorage.NewStorageTierPolicy(tierRules)
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func TestCH015StorageTierMovePlanIsDeterministicAndSkipsCurrentTier(t *testing.T) {
	policy := newCH015StorageTierMovePolicy(t)
	parts := []hatStorage.StorageTierPart{
		{Key: "part-a", CurrentTier: "hot", Age: 90 * time.Minute},
		{Key: "part-b", CurrentTier: "warm", Age: 30 * time.Minute},
		{Key: "part-c", CurrentTier: "cold", Age: 48 * time.Hour},
	}
	moves, err := policy.PlanStorageTierMoves(parts)
	if err != nil {
		t.Fatal(err)
	}
	want := []hatStorage.StorageTierMove{
		{Key: "part-a", SourceTier: "hot", SourcePath: "/data/hot", DestinationTier: "warm", DestinationPath: "/data/warm"},
		{Key: "part-b", SourceTier: "warm", SourcePath: "/data/warm", DestinationTier: "hot", DestinationPath: "/data/hot"},
	}
	if !reflect.DeepEqual(moves, want) {
		t.Fatalf("moves = %#v, want %#v", moves, want)
	}
	repeat, err := policy.PlanStorageTierMoves(parts)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(moves, repeat) {
		t.Fatalf("repeated plan changed: %#v != %#v", moves, repeat)
	}
}

func TestCH015StorageTierMoveExecutorReportsPartialProgressAndHonorsCancellation(t *testing.T) {
	policy := newCH015StorageTierMovePolicy(t)
	parts := []hatStorage.StorageTierPart{
		{Key: "part-a", CurrentTier: "hot", Age: 2 * time.Hour},
		{Key: "part-b", CurrentTier: "warm", Age: 30 * time.Minute},
	}
	var keys []string
	report, err := hatStorage.ExecuteStorageTierMoves(context.Background(), policy, parts, func(_ context.Context, move hatStorage.StorageTierMove) error {
		keys = append(keys, move.Key)
		if move.Key == "part-b" {
			return errors.New("destination unavailable")
		}
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "destination unavailable") {
		t.Fatalf("executor error = %v, want destination failure", err)
	}
	if report.Planned != 2 || report.Moved != 1 {
		t.Fatalf("report = %#v, want planned=2 moved=1", report)
	}
	if !reflect.DeepEqual(keys, []string{"part-a", "part-b"}) {
		t.Fatalf("callback order = %#v", keys)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	report, err = hatStorage.ExecuteStorageTierMoves(canceled, policy, parts, func(context.Context, hatStorage.StorageTierMove) error {
		t.Fatal("canceled executor invoked callback")
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled error = %v, want context.Canceled", err)
	}
	if report.Planned != 2 || report.Moved != 0 {
		t.Fatalf("canceled report = %#v, want planned=2 moved=0", report)
	}
}

func TestCH015StorageTierMoveRejectsAmbiguousInput(t *testing.T) {
	policy := newCH015StorageTierMovePolicy(t)
	for name, parts := range map[string][]hatStorage.StorageTierPart{
		"empty key":    {{CurrentTier: "hot", Age: time.Hour}},
		"unknown tier": {{Key: "part", CurrentTier: "archive", Age: time.Hour}},
		"negative age": {{Key: "part", CurrentTier: "hot", Age: -time.Second}},
		"duplicate key": {
			{Key: "part", CurrentTier: "hot", Age: time.Hour},
			{Key: "part", CurrentTier: "hot", Age: time.Hour},
		},
	} {
		if _, err := policy.PlanStorageTierMoves(parts); !errors.Is(err, hatStorage.ErrStorageTierMoveInvalid) {
			t.Errorf("%s error = %v, want ErrStorageTierMoveInvalid", name, err)
		}
	}

	parts := []hatStorage.StorageTierPart{{Key: "part", CurrentTier: "hot", Age: time.Hour}}
	if _, err := hatStorage.ExecuteStorageTierMoves(context.Background(), policy, parts, nil); !errors.Is(err, hatStorage.ErrStorageTierMoveInvalid) {
		t.Fatalf("nil callback error = %v, want ErrStorageTierMoveInvalid", err)
	}
	if _, err := hatStorage.ExecuteStorageTierMoves(nil, policy, parts, func(context.Context, hatStorage.StorageTierMove) error {
		return nil
	}); !errors.Is(err, hatStorage.ErrStorageTierMoveInvalid) {
		t.Fatalf("nil context error = %v, want ErrStorageTierMoveInvalid", err)
	}
}
