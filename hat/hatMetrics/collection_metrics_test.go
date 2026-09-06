package hatMetrics_test

import (
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatMetrics"
)

func TestCollectionMetricsRegistryTracksSizeAndCompactions(t *testing.T) {
	registry := hatMetrics.NewCollectionMetricsRegistry()
	if err := registry.SetSize(" orders ", 10, 100); err != nil {
		t.Fatalf("SetSize(orders) error = %v", err)
	}
	if err := registry.RecordCompaction("orders"); err != nil {
		t.Fatalf("RecordCompaction(orders) error = %v", err)
	}
	if err := registry.RecordCompaction("orders"); err != nil {
		t.Fatalf("RecordCompaction(orders) second error = %v", err)
	}
	if err := registry.SetSize("orders", 5, 50); err != nil {
		t.Fatalf("SetSize(orders update) error = %v", err)
	}
	if err := registry.RecordCompaction("cache"); err != nil {
		t.Fatalf("RecordCompaction(cache) error = %v", err)
	}

	want := []hatMetrics.CollectionMetrics{
		{Collection: "cache", CompactionsTotal: 1},
		{Collection: "orders", Entries: 5, Bytes: 50, CompactionsTotal: 2},
	}
	if got := registry.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Snapshot() = %#v, want %#v", got, want)
	}
}

func TestCollectionMetricsRegistryRejectsEmptyNamesAndOwnsSnapshots(t *testing.T) {
	registry := hatMetrics.NewCollectionMetricsRegistry()
	if err := registry.SetSize(" ", 1, 2); !errors.Is(err, hatMetrics.ErrCollectionNameRequired) {
		t.Fatalf("SetSize(empty) error = %v, want ErrCollectionNameRequired", err)
	}
	if err := registry.RecordCompaction(""); !errors.Is(err, hatMetrics.ErrCollectionNameRequired) {
		t.Fatalf("RecordCompaction(empty) error = %v, want ErrCollectionNameRequired", err)
	}
	if err := registry.SetSize("orders", 1, 2); err != nil {
		t.Fatalf("SetSize(orders) error = %v", err)
	}
	first := registry.Snapshot()
	first[0].Collection = "changed"
	first[0].Bytes = 99
	second := registry.Snapshot()
	if second[0].Collection != "orders" || second[0].Bytes != 2 {
		t.Fatalf("snapshot mutation leaked into registry: %#v", second)
	}
}

func TestCollectionMetricsRegistrySerializesConcurrentUpdates(t *testing.T) {
	registry := hatMetrics.NewCollectionMetricsRegistry()
	finished := make(chan struct{}, 8)
	for worker := 0; worker < 8; worker++ {
		go func() {
			for update := uint64(0); update < 1000; update++ {
				if err := registry.SetSize("orders", update, update*2); err != nil {
					t.Errorf("SetSize() error = %v", err)
				}
				if err := registry.RecordCompaction("orders"); err != nil {
					t.Errorf("RecordCompaction() error = %v", err)
				}
			}
			finished <- struct{}{}
		}()
	}
	for worker := 0; worker < 8; worker++ {
		<-finished
	}
	rows := registry.Snapshot()
	if len(rows) != 1 || rows[0].Collection != "orders" || rows[0].CompactionsTotal != 8000 {
		t.Fatalf("Snapshot() after concurrent updates = %#v, want one collection with 8000 compactions", rows)
	}
}

func BenchmarkCollectionMetricsRegistrySnapshot(b *testing.B) {
	registry := hatMetrics.NewCollectionMetricsRegistry()
	for collection := 0; collection < 1024; collection++ {
		if err := registry.SetSize("collection-"+string(rune(collection)), uint64(collection), uint64(collection*2)); err != nil {
			b.Fatalf("SetSize() error = %v", err)
		}
	}
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		_ = registry.Snapshot()
	}
}
