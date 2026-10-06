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

func TestCHU29NamespaceTierLifecycle(t *testing.T) {
	policy := newCHU29TestPolicy(t)
	registry, err := hatStorage.NewStorageTierNamespaceRegistry(
		hatStorage.StorageTierNamespacePolicy{Namespace: " eu ", Policy: policy},
	)
	if err != nil {
		t.Fatal(err)
	}

	stored, err := registry.Policy("eu")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(stored.Rules(), policy.Rules()) {
		t.Fatalf("stored policy = %#v, want %#v", stored.Rules(), policy.Rules())
	}

	now := time.Date(2026, time.January, 2, 0, 0, 0, 0, time.UTC)
	parts := []hatStorage.StorageTierLifecyclePart{
		{Key: "part-hot", CurrentTier: "hot", LifecycleTime: now.Add(-30 * time.Minute)},
		{Key: "part-warm", CurrentTier: "hot", LifecycleTime: now.Add(-2 * time.Hour)},
		{Key: "part-cold", CurrentTier: "warm", LifecycleTime: now.Add(-48 * time.Hour)},
	}
	moves, err := registry.Plan(" eu ", now, parts)
	if err != nil {
		t.Fatal(err)
	}
	wantMoves := []hatStorage.StorageTierMove{
		{
			Key:             "part-warm",
			SourceTier:      "hot",
			SourcePath:      "/data/hot",
			DestinationTier: "warm",
			DestinationPath: "/data/warm",
		},
		{
			Key:             "part-cold",
			SourceTier:      "warm",
			SourcePath:      "/data/warm",
			DestinationTier: "cold",
			DestinationPath: "/data/cold",
		},
	}
	if !reflect.DeepEqual(moves, wantMoves) {
		t.Fatalf("moves = %#v, want %#v", moves, wantMoves)
	}

	var executed []string
	report, err := registry.Execute(context.Background(), "eu", now, parts, func(_ context.Context, move hatStorage.StorageTierMove) error {
		executed = append(executed, move.Key)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if report != (hatStorage.StorageTierMoveReport{Planned: 2, Moved: 2}) {
		t.Fatalf("report = %#v, want planned=2 moved=2", report)
	}
	if !reflect.DeepEqual(executed, []string{"part-warm", "part-cold"}) {
		t.Fatalf("executed = %#v", executed)
	}

	if _, err := registry.Plan("missing", now, parts); !errors.Is(err, hatStorage.ErrStorageTierNamespaceNotFound) {
		t.Fatalf("missing namespace error = %v", err)
	}
	if _, err := registry.Plan("eu", time.Time{}, parts); !errors.Is(err, hatStorage.ErrStorageTierNamespaceInvalid) {
		t.Fatalf("zero now error = %v", err)
	}
	future := append([]hatStorage.StorageTierLifecyclePart(nil), parts...)
	future[0].LifecycleTime = now.Add(time.Second)
	if _, err := registry.Plan("eu", now, future); !errors.Is(err, hatStorage.ErrStorageTierNamespaceInvalid) {
		t.Fatalf("future lifecycle error = %v", err)
	}
	zero := append([]hatStorage.StorageTierLifecyclePart(nil), parts...)
	zero[0].LifecycleTime = time.Time{}
	if _, err := registry.Plan("eu", now, zero); !errors.Is(err, hatStorage.ErrStorageTierNamespaceInvalid) {
		t.Fatalf("zero lifecycle error = %v", err)
	}

	if err := registry.Register("eu", policy); !errors.Is(err, hatStorage.ErrStorageTierNamespaceInvalid) {
		t.Fatalf("duplicate namespace error = %v", err)
	}
	if err := registry.Register("us", policy); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("bad\x00namespace", policy); !errors.Is(err, hatStorage.ErrStorageTierNamespaceInvalid) {
		t.Fatalf("invalid namespace error = %v", err)
	}
	if err := registry.Register(string([]byte{0xff}), policy); !errors.Is(err, hatStorage.ErrStorageTierNamespaceInvalid) {
		t.Fatalf("invalid UTF-8 namespace error = %v", err)
	}
	if err := registry.Register(strings.Repeat("x", 257), policy); !errors.Is(err, hatStorage.ErrStorageTierNamespaceInvalid) {
		t.Fatalf("oversized namespace error = %v", err)
	}

	var zeroRegistry hatStorage.StorageTierNamespaceRegistry
	if err := zeroRegistry.Register("zero", policy); err != nil {
		t.Fatalf("zero-value registry registration error = %v", err)
	}
	if _, err := zeroRegistry.Policy("zero"); err != nil {
		t.Fatalf("zero-value registry lookup error = %v", err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := registry.Execute(canceled, "eu", now, parts, func(context.Context, hatStorage.StorageTierMove) error {
		t.Fatal("canceled execution invoked executor")
		return nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled execution error = %v", err)
	}
}

type chu29TestingT interface {
	Helper()
	Fatal(...any)
}

func newCHU29TestPolicy(t chu29TestingT) hatStorage.StorageTierPolicy {
	t.Helper()
	placements := make(map[string]hatStorage.DiskPlacementPolicy, 3)
	for _, name := range []string{"hot", "warm", "cold"} {
		placement, err := hatStorage.NewDiskPlacementPolicy(name, []hatStorage.DiskPlacementRule{{Path: "/data/" + name, Weight: 1}})
		if err != nil {
			t.Fatal(err)
		}
		placements[name] = placement
	}
	policy, err := hatStorage.NewStorageTierPolicy([]hatStorage.StorageTierRule{
		{Name: "cold", MinAge: 24 * time.Hour, Placement: placements["cold"]},
		{Name: "hot", MinAge: 0, Placement: placements["hot"]},
		{Name: "warm", MinAge: time.Hour, Placement: placements["warm"]},
	})
	if err != nil {
		t.Fatal(err)
	}
	return policy
}
