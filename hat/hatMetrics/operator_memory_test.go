package hatMetrics_test

import (
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatMetrics"
)

func TestOperatorMemoryRegistryTracksSortedGaugeSnapshots(t *testing.T) {
	registry := hatMetrics.NewOperatorMemoryRegistry()
	if err := registry.Set(" sort ", 4096); err != nil {
		t.Fatalf("Set(sort) error = %v", err)
	}
	if err := registry.Set("scan", 1024); err != nil {
		t.Fatalf("Set(scan) error = %v", err)
	}
	if err := registry.Set("sort", 2048); err != nil {
		t.Fatalf("Set(sort update) error = %v", err)
	}

	want := []hatMetrics.OperatorMemory{
		{Operator: "scan", RetainedBytes: 1024},
		{Operator: "sort", RetainedBytes: 2048},
	}
	if got := registry.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Snapshot() = %#v, want %#v", got, want)
	}
}

func TestOperatorMemoryRegistryRejectsEmptyNamesAndOwnsSnapshots(t *testing.T) {
	registry := hatMetrics.NewOperatorMemoryRegistry()
	if err := registry.Set(" ", 1); !errors.Is(err, hatMetrics.ErrOperatorNameRequired) {
		t.Fatalf("Set(empty) error = %v, want ErrOperatorNameRequired", err)
	}
	if err := registry.Set("scan", 1024); err != nil {
		t.Fatalf("Set(scan) error = %v", err)
	}
	first := registry.Snapshot()
	first[0].Operator = "changed"
	first[0].RetainedBytes = 99
	second := registry.Snapshot()
	if second[0].Operator != "scan" || second[0].RetainedBytes != 1024 {
		t.Fatalf("snapshot mutation leaked into registry: %#v", second)
	}
}

func TestOperatorMemoryRegistrySerializesConcurrentSets(t *testing.T) {
	registry := hatMetrics.NewOperatorMemoryRegistry()
	finished := make(chan struct{}, 8)
	for worker := 0; worker < 8; worker++ {
		go func() {
			for update := uint64(0); update < 1000; update++ {
				if err := registry.Set("scan", update); err != nil {
					t.Errorf("Set() error = %v", err)
				}
			}
			finished <- struct{}{}
		}()
	}
	for worker := 0; worker < 8; worker++ {
		<-finished
	}
	if got := registry.Snapshot(); len(got) != 1 || got[0].Operator != "scan" || got[0].RetainedBytes >= 1000 {
		t.Fatalf("Snapshot() after concurrent sets = %#v, want one scan gauge", got)
	}
}

func BenchmarkOperatorMemoryRegistrySnapshot(b *testing.B) {
	registry := hatMetrics.NewOperatorMemoryRegistry()
	for operator := 0; operator < 1024; operator++ {
		if err := registry.Set("operator-"+string(rune(operator)), uint64(operator)); err != nil {
			b.Fatalf("Set() error = %v", err)
		}
	}
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		_ = registry.Snapshot()
	}
}
