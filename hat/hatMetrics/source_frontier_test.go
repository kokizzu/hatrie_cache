package hatMetrics_test

import (
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatMetrics"
)

func TestSourceFrontierRegistryTracksMonotoneProgressAndLag(t *testing.T) {
	registry := hatMetrics.NewSourceFrontierRegistry()
	if err := registry.Advance("orders", 4); err != nil {
		t.Fatalf("Advance(orders) error = %v", err)
	}
	if err := registry.Advance("accounts", 8); err != nil {
		t.Fatalf("Advance(accounts) error = %v", err)
	}

	want := []hatMetrics.SourceFrontier{
		{Source: "accounts", Frontier: 8, Observed: 10, Lag: 2},
		{Source: "orders", Frontier: 4, Observed: 10, Lag: 6},
	}
	if got := registry.Snapshot(10); !reflect.DeepEqual(got, want) {
		t.Fatalf("Snapshot(10) = %#v, want %#v", got, want)
	}
	if got := registry.Snapshot(3); got[0].Lag != 0 || got[1].Lag != 0 {
		t.Fatalf("Snapshot(3) = %#v, want clamped lag", got)
	}
}

func TestSourceFrontierRegistryRejectsInvalidAndRegressedUpdates(t *testing.T) {
	registry := hatMetrics.NewSourceFrontierRegistry()
	if err := registry.Advance(" ", 1); !errors.Is(err, hatMetrics.ErrSourceNameRequired) {
		t.Fatalf("Advance(empty) error = %v, want ErrSourceNameRequired", err)
	}
	if err := registry.Advance("orders", 5); err != nil {
		t.Fatalf("Advance(orders) error = %v", err)
	}
	if err := registry.Advance("orders", 4); !errors.Is(err, hatMetrics.ErrSourceFrontierRegressed) {
		t.Fatalf("Advance(regressed) error = %v, want ErrSourceFrontierRegressed", err)
	}
	if got, ok := registry.Frontier("orders"); !ok || got != 5 {
		t.Fatalf("Frontier(orders) = %d/%v, want 5/true", got, ok)
	}
}

func TestSourceFrontierRegistrySnapshotsAreIndependent(t *testing.T) {
	registry := hatMetrics.NewSourceFrontierRegistry()
	if err := registry.Advance("orders", 5); err != nil {
		t.Fatalf("Advance() error = %v", err)
	}
	first := registry.Snapshot(10)
	first[0].Source = "changed"
	first[0].Frontier = 99
	second := registry.Snapshot(10)
	if second[0].Source != "orders" || second[0].Frontier != 5 {
		t.Fatalf("snapshot mutation leaked into registry: %#v", second)
	}
}

func TestSourceFrontierRegistrySerializesConcurrentAdvances(t *testing.T) {
	registry := hatMetrics.NewSourceFrontierRegistry()
	finished := make(chan struct{}, 8)
	for worker := 0; worker < 8; worker++ {
		go func() {
			for frontier := uint64(0); frontier < 1000; frontier++ {
				if err := registry.Advance("orders", frontier); err != nil && !errors.Is(err, hatMetrics.ErrSourceFrontierRegressed) {
					t.Errorf("Advance() error = %v", err)
				}
			}
			finished <- struct{}{}
		}()
	}
	for worker := 0; worker < 8; worker++ {
		<-finished
	}
	if got, ok := registry.Frontier("orders"); !ok || got != 999 {
		t.Fatalf("Frontier(orders) = %d/%v, want 999/true", got, ok)
	}
}

func BenchmarkSourceFrontierRegistrySnapshot(b *testing.B) {
	registry := hatMetrics.NewSourceFrontierRegistry()
	for source := 0; source < 1024; source++ {
		if err := registry.Advance("source-"+string(rune(source)), uint64(source)); err != nil {
			b.Fatalf("Advance() error = %v", err)
		}
	}
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		_ = registry.Snapshot(2048)
	}
}
