package hatReplication_test

import (
	"errors"
	"testing"
	"time"

	hatReplication "hatrie_cache/hat/hatReplication"
)

func TestTT003RouteCacheUsesStableHealthAwareLookup(t *testing.T) {
	cache, err := hatReplication.NewFailoverRouteCache(hatReplication.FailoverRouteCacheOptions{
		MaxRoutes:        4,
		FailureThreshold: 1,
		Cooldown:         time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Replace(1, []hatReplication.FailoverRoute{
		{NodeID: "node-b", Address: "b.internal:9000"},
		{NodeID: "node-a", Address: "a.internal:9000"},
	}); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0)
	first, err := cache.Lookup("tenant-a", now)
	if err != nil {
		t.Fatal(err)
	}
	second, err := cache.Lookup("tenant-a", now)
	if err != nil || first != second {
		t.Fatalf("repeated Lookup() = %#v/%#v/%v, want stable route", first, second, err)
	}
	snapshot := cache.Snapshot(now)
	if snapshot.Generation != 1 || len(snapshot.Routes) != 2 || !snapshot.Routes[0].Healthy || !snapshot.Routes[1].Healthy {
		t.Fatalf("Snapshot() = %#v, want generation 1 with two healthy routes", snapshot)
	}
	if err := cache.Replace(1, []hatReplication.FailoverRoute{{NodeID: "node-a", Address: "a.internal:9000"}}); !errors.Is(err, hatReplication.ErrFailoverRouteCacheGeneration) {
		t.Fatalf("stale Replace() error = %v, want generation error", err)
	}
}

func TestTT003RouteCacheFailureInvalidationAndCooldown(t *testing.T) {
	cache, err := hatReplication.NewFailoverRouteCache(hatReplication.FailoverRouteCacheOptions{
		FailureThreshold: 1,
		Cooldown:         10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Replace(1, []hatReplication.FailoverRoute{
		{NodeID: "node-a", Address: "a.internal:9000"},
		{NodeID: "node-b", Address: "b.internal:9000"},
	}); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(200, 0)
	first, err := cache.Lookup("tenant-a", now)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.ReportFailure(first.NodeID, now); err != nil {
		t.Fatal(err)
	}
	second, err := cache.Lookup("tenant-a", now)
	if err != nil || second.NodeID == first.NodeID {
		t.Fatalf("post-failure Lookup() = %#v/%v, want alternate route", second, err)
	}
	if err := cache.ReportFailure(second.NodeID, now); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Lookup("tenant-a", now); !errors.Is(err, hatReplication.ErrFailoverRouteCacheNoHealthyRoute) {
		t.Fatalf("all-unhealthy Lookup() error = %v, want no healthy route", err)
	}
	if _, err := cache.Lookup("tenant-a", now.Add(11*time.Second)); err != nil {
		t.Fatalf("cooldown Lookup() error = %v, want recovered route", err)
	}
	if err := cache.ReportSuccess(first.NodeID); err != nil {
		t.Fatal(err)
	}
}

func TestTT003RouteCacheValidatesBounds(t *testing.T) {
	if _, err := hatReplication.NewFailoverRouteCache(hatReplication.FailoverRouteCacheOptions{MaxRoutes: -1}); !errors.Is(err, hatReplication.ErrFailoverRouteCacheInvalid) {
		t.Fatalf("invalid options error = %v, want invalid", err)
	}
	cache, err := hatReplication.NewFailoverRouteCache(hatReplication.FailoverRouteCacheOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Replace(1, []hatReplication.FailoverRoute{{NodeID: "node-a", Address: "a"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Lookup("", time.Unix(300, 0)); err != nil {
		t.Fatalf("empty-key Lookup() error = %v", err)
	}
	if err := cache.ReportFailure("missing", time.Unix(300, 0)); !errors.Is(err, hatReplication.ErrFailoverRouteCacheNodeNotFound) {
		t.Fatalf("missing ReportFailure() error = %v, want node-not-found", err)
	}
}
