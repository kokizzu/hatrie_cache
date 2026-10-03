package hatReplication

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPeerRouteCacheSelectsHealthyRoutesAndRetries(t *testing.T) {
	cache, err := NewPeerRouteCache(PeerRouteCacheOptions{
		FailureCooldown: 10 * time.Second,
		MaxAttempts:     3,
	})
	if err != nil {
		t.Fatalf("NewPeerRouteCache() error = %v", err)
	}
	if err := cache.Replace("cluster", []PeerRoute{
		{Node: "node-a", Address: "a:3301"},
		{Node: "node-b", Address: "b:3301"},
	}); err != nil {
		t.Fatalf("Replace() error = %v", err)
	}

	now := time.Now()
	first, ok := cache.Next("cluster", now)
	if !ok || first.Node != "node-a" {
		t.Fatalf("first Next() = %#v/%v, want node-a/true", first, ok)
	}
	cache.ReportFailure("cluster", first.Node, now)
	second, ok := cache.Next("cluster", now)
	if !ok || second.Node != "node-b" {
		t.Fatalf("Next() after failure = %#v/%v, want node-b/true", second, ok)
	}

	attempts := make([]string, 0, 2)
	selected, err := cache.Do(context.Background(), "cluster", func(_ context.Context, route PeerRoute) error {
		attempts = append(attempts, route.Node)
		if route.Node == "node-b" {
			return nil
		}
		return errors.New("unreachable")
	})
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	if selected.Node != "node-b" || len(attempts) != 1 || attempts[0] != "node-b" {
		t.Fatalf("Do() selected %#v after attempts %#v, want node-b without a cooled retry", selected, attempts)
	}
}

func TestPeerRouteCacheCooldownAndInvalidation(t *testing.T) {
	cache, err := NewPeerRouteCache(PeerRouteCacheOptions{FailureCooldown: time.Second})
	if err != nil {
		t.Fatalf("NewPeerRouteCache() error = %v", err)
	}
	if err := cache.Replace("cluster", []PeerRoute{{Node: "node-a", Address: "a:3301"}}); err != nil {
		t.Fatalf("Replace() error = %v", err)
	}
	now := time.Unix(200, 0)
	cache.ReportFailure("cluster", "node-a", now)
	if route, ok := cache.Next("cluster", now.Add(500*time.Millisecond)); ok || route != (PeerRoute{}) {
		t.Fatalf("Next() during cooldown = %#v/%v, want no route", route, ok)
	}
	if route, ok := cache.Next("cluster", now.Add(time.Second)); !ok || route.Node != "node-a" {
		t.Fatalf("Next() after cooldown = %#v/%v, want node-a/true", route, ok)
	}
	cache.ReportFailure("cluster", "node-a", now)
	cache.ReportSuccess("cluster", "node-a")
	if route, ok := cache.Next("cluster", now); !ok || route.Node != "node-a" {
		t.Fatalf("Next() after success = %#v/%v, want node-a/true", route, ok)
	}
	if !cache.Invalidate("cluster") {
		t.Fatal("Invalidate() = false, want true")
	}
	if _, ok := cache.Next("cluster", now.Add(time.Second)); ok {
		t.Fatal("Next() after invalidation = true, want false")
	}
}

func TestPeerRouteCachePreservesCancellationAndRejectsBadRoutes(t *testing.T) {
	cache, err := NewPeerRouteCache(PeerRouteCacheOptions{MaxRoutes: 1})
	if err != nil {
		t.Fatalf("NewPeerRouteCache() error = %v", err)
	}
	if err := cache.Replace("cluster", []PeerRoute{{Node: "node-a", Address: "a:3301"}, {Node: "node-b", Address: "b:3301"}}); err == nil {
		t.Fatal("Replace() error = nil, want max-route validation")
	}
	if err := cache.Replace("cluster", []PeerRoute{{Node: "node-a", Address: "a:3301"}}); err != nil {
		t.Fatalf("valid Replace() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = cache.Do(ctx, "cluster", func(context.Context, PeerRoute) error {
		t.Fatal("attempt called after context cancellation")
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Do() error = %v, want context.Canceled", err)
	}
}

func TestPeerRouteCacheHonorsAttemptLimit(t *testing.T) {
	cache, err := NewPeerRouteCache(PeerRouteCacheOptions{MaxRoutes: 2, MaxAttempts: 1})
	if err != nil {
		t.Fatalf("NewPeerRouteCache() error = %v", err)
	}
	if err := cache.Replace("cluster", []PeerRoute{
		{Node: "node-a", Address: "a:3301"},
		{Node: "node-b", Address: "b:3301"},
	}); err != nil {
		t.Fatalf("Replace() error = %v", err)
	}
	attempts := 0
	_, err = cache.Do(context.Background(), "cluster", func(context.Context, PeerRoute) error {
		attempts++
		return errors.New("unreachable")
	})
	if err == nil || attempts != 1 {
		t.Fatalf("Do() error/attempts = %v/%d, want an error after one attempt", err, attempts)
	}
}

var peerRouteBenchmarkSink PeerRoute

func BenchmarkPeerRouteCacheNext(b *testing.B) {
	routes := make([]PeerRoute, 32)
	for index := range routes {
		routes[index] = PeerRoute{Node: "node-" + string(rune('a'+index)), Address: "127.0.0.1:3301"}
	}
	cache, err := NewPeerRouteCache(PeerRouteCacheOptions{MaxRoutes: len(routes)})
	if err != nil {
		b.Fatal(err)
	}
	if err := cache.Replace("cluster", routes); err != nil {
		b.Fatal(err)
	}
	now := time.Unix(300, 0)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		route, ok := cache.Next("cluster", now)
		if !ok {
			b.Fatal("cache returned no route")
		}
		peerRouteBenchmarkSink = route
	}
}

func BenchmarkPeerRouteLinearSelection(b *testing.B) {
	routes := make([]PeerRoute, 32)
	for index := range routes {
		routes[index] = PeerRoute{Node: "node-" + string(rune('a'+index)), Address: "127.0.0.1:3301"}
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		peerRouteBenchmarkSink = routes[index%len(routes)]
	}
}

func BenchmarkPeerRouteCacheFailover(b *testing.B) {
	cache, err := NewPeerRouteCache(PeerRouteCacheOptions{MaxRoutes: 2})
	if err != nil {
		b.Fatal(err)
	}
	if err := cache.Replace("cluster", []PeerRoute{
		{Node: "node-a", Address: "a:3301"},
		{Node: "node-b", Address: "b:3301"},
	}); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	attempt := func(_ context.Context, route PeerRoute) error {
		if route.Node == "node-b" {
			return nil
		}
		return errors.New("unreachable")
	}
	cache.ReportFailure("cluster", "node-a", time.Now())
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		selected, err := cache.Do(ctx, "cluster", attempt)
		if err != nil || selected.Node != "node-b" {
			b.Fatalf("Do() = %#v/%v, want node-b/nil", selected, err)
		}
		cache.ReportFailure("cluster", "node-a", time.Now())
	}
	b.ReportMetric(1, "attempts/op")
}

func BenchmarkPeerRouteLinearFailover(b *testing.B) {
	routes := []PeerRoute{
		{Node: "node-a", Address: "a:3301"},
		{Node: "node-b", Address: "b:3301"},
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		for _, route := range routes {
			if route.Node == "node-b" {
				peerRouteBenchmarkSink = route
				break
			}
		}
	}
	b.ReportMetric(2, "attempts/op")
}
