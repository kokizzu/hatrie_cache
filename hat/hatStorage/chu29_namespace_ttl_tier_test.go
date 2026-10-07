package hatStorage_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
	"hatrie_cache/hat/hatStorage"
)

func newCHU29TierPolicy(t *testing.T) hatStorage.StorageTierPolicy {
	t.Helper()
	placements := make([]hatStorage.StorageTierRule, 0, 2)
	for _, rule := range []struct {
		name string
		path string
		age  time.Duration
	}{
		{name: "hot", path: "/data/hot", age: 0},
		{name: "warm", path: "/data/warm", age: time.Hour},
	} {
		placement, err := hatStorage.NewDiskPlacementPolicy(rule.name, []hatStorage.DiskPlacementRule{{Path: rule.path, Weight: 1}})
		if err != nil {
			t.Fatal(err)
		}
		placements = append(placements, hatStorage.StorageTierRule{Name: rule.name, MinAge: rule.age, Placement: placement})
	}
	policy, err := hatStorage.NewStorageTierPolicy(placements)
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func TestCHU29NamespaceTierMovementRespectsTTLAndLifecycle(t *testing.T) {
	registry, err := hatStorage.NewSQLAdapterRegistry(nil,
		hatStorage.SQLNamespaceAdapter{
			NamespaceName: "active",
			Store:         testEngine{},
			Resolver:      hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) { return nil, nil }),
		},
		hatStorage.SQLNamespaceAdapter{
			NamespaceName: "expired",
			Store:         testEngine{},
			Resolver:      hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) { return nil, nil }),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	policy := newCHU29TierPolicy(t)
	archived := 0
	controller, err := hatStorage.NewNamespaceLifecycleController(registry, map[string]hatStorage.NamespaceLifecyclePolicy{
		"active": {TierPolicy: &policy},
		"expired": {
			TierPolicy:   &policy,
			ExpiresAt:    time.Unix(100, 0),
			ExpiryAction: hatStorage.NamespaceExpiryArchive,
			Archive: func(context.Context, string, hatStorage.SQLAdapter) error {
				archived++
				return nil
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	parts := []hatStorage.StorageTierPart{
		{Key: "part-a", CurrentTier: "hot", Age: 2 * time.Hour},
		{Key: "part-b", CurrentTier: "warm", Age: 30 * time.Minute},
	}
	moves, err := controller.PlanNamespaceStorageTierMoves(context.Background(), "active", parts)
	if err != nil {
		t.Fatal(err)
	}
	if len(moves) != 2 || moves[0].DestinationTier != "warm" || moves[1].DestinationTier != "hot" {
		t.Fatalf("moves = %#v, want both age-based tier changes", moves)
	}

	var executed []string
	report, err := controller.ExecuteNamespaceStorageTierMoves(context.Background(), "active", parts, func(_ context.Context, move hatStorage.StorageTierMove) error {
		executed = append(executed, move.Key)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Planned != 2 || report.Moved != 2 || len(executed) != 2 {
		t.Fatalf("report = %#v, executed = %#v, want two completed moves", report, executed)
	}

	if err := controller.Freeze("active"); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.PlanNamespaceStorageTierMoves(context.Background(), "active", parts); !errors.Is(err, hatStorage.ErrNamespaceFrozen) {
		t.Fatalf("frozen planning error = %v, want ErrNamespaceFrozen", err)
	}

	if _, err := controller.PlanNamespaceStorageTierMoves(context.Background(), "expired", parts); !errors.Is(err, hatStorage.ErrNamespaceArchived) {
		t.Fatalf("expired planning error = %v, want ErrNamespaceArchived", err)
	}
	if archived != 1 {
		t.Fatalf("archive calls = %d, want 1", archived)
	}
}

func TestCHU29NamespaceTierMovementRequiresOptInPolicy(t *testing.T) {
	registry, err := hatStorage.NewSQLAdapterRegistry(nil, hatStorage.SQLNamespaceAdapter{
		NamespaceName: "plain",
		Store:         testEngine{},
		Resolver:      hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) { return nil, nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	controller, err := hatStorage.NewNamespaceLifecycleController(registry, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = controller.PlanNamespaceStorageTierMoves(context.Background(), "plain", []hatStorage.StorageTierPart{{Key: "part", CurrentTier: "hot"}})
	if !errors.Is(err, hatStorage.ErrNamespaceTierPolicyDisabled) {
		t.Fatalf("disabled policy error = %v, want ErrNamespaceTierPolicyDisabled", err)
	}
}

func TestCHU29NamespaceTierMovementRejectsEmptyOptInPolicy(t *testing.T) {
	registry, err := hatStorage.NewSQLAdapterRegistry(nil, hatStorage.SQLNamespaceAdapter{
		NamespaceName: "invalid",
		Store:         testEngine{},
		Resolver:      hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) { return nil, nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	empty := hatStorage.StorageTierPolicy{}
	if _, err := hatStorage.NewNamespaceLifecycleController(registry, map[string]hatStorage.NamespaceLifecyclePolicy{
		"invalid": {TierPolicy: &empty},
	}); err == nil {
		t.Fatal("empty tier policy registration succeeded, want validation error")
	}
}
