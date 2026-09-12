package hatMetrics_test

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"hatrie_cache/hat/hatMetrics"
)

func TestSpaceOperationMetricsRecordsOperationsAndBytes(t *testing.T) {
	metrics, err := hatMetrics.NewSpaceOperationMetrics(" orders ")
	if err != nil {
		t.Fatalf("NewSpaceOperationMetrics() error = %v", err)
	}
	if err := metrics.Record(hatMetrics.SpaceOperationRead, hatMetrics.SpaceOperationHit, 100, 10*time.Nanosecond); err != nil {
		t.Fatalf("read hit Record() error = %v", err)
	}
	if err := metrics.Record(hatMetrics.SpaceOperationRead, hatMetrics.SpaceOperationMiss, 50, 20*time.Nanosecond); err != nil {
		t.Fatalf("read miss Record() error = %v", err)
	}
	if err := metrics.Record(hatMetrics.SpaceOperationWrite, hatMetrics.SpaceOperationSuccess, 20, 30*time.Nanosecond); err != nil {
		t.Fatalf("write Record() error = %v", err)
	}
	if err := metrics.Record(hatMetrics.SpaceOperationDelete, hatMetrics.SpaceOperationSuccess, 5, 40*time.Nanosecond); err != nil {
		t.Fatalf("delete Record() error = %v", err)
	}
	if err := metrics.Record(hatMetrics.SpaceOperationScan, hatMetrics.SpaceOperationError, 30, 50*time.Nanosecond); err != nil {
		t.Fatalf("scan error Record() error = %v", err)
	}
	snapshot := metrics.Snapshot()
	if snapshot.Name != "orders" || snapshot.Operations != 5 || snapshot.ReadOperations != 2 || snapshot.WriteOperations != 1 || snapshot.DeleteOperations != 1 || snapshot.ScanOperations != 1 {
		t.Fatalf("operation snapshot = %#v", snapshot)
	}
	if snapshot.Hits != 1 || snapshot.Misses != 1 || snapshot.Errors != 1 || snapshot.BytesRead != 180 || snapshot.BytesWritten != 25 || snapshot.LatencyNanos != 150 || snapshot.LatencySamples != 5 {
		t.Fatalf("counter snapshot = %#v", snapshot)
	}
}

func TestSpaceOperationMetricsRejectInvalidRecordsWithoutMutation(t *testing.T) {
	metrics, err := hatMetrics.NewSpaceOperationMetrics("orders")
	if err != nil {
		t.Fatalf("NewSpaceOperationMetrics() error = %v", err)
	}
	cases := []struct {
		name    string
		kind    hatMetrics.SpaceOperationKind
		outcome hatMetrics.SpaceOperationOutcome
		latency time.Duration
		want    error
	}{
		{name: "kind", kind: hatMetrics.SpaceOperationKind(99), outcome: hatMetrics.SpaceOperationSuccess, want: hatMetrics.ErrSpaceOperationKindInvalid},
		{name: "outcome", kind: hatMetrics.SpaceOperationRead, outcome: hatMetrics.SpaceOperationOutcome(99), want: hatMetrics.ErrSpaceOperationOutcomeInvalid},
		{name: "latency", kind: hatMetrics.SpaceOperationRead, outcome: hatMetrics.SpaceOperationSuccess, latency: -time.Nanosecond, want: hatMetrics.ErrSpaceOperationLatencyInvalid},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if err := metrics.Record(test.kind, test.outcome, 100, test.latency); !errors.Is(err, test.want) {
				t.Fatalf("Record() error = %v, want %v", err, test.want)
			}
		})
	}
	if snapshot := metrics.Snapshot(); snapshot.Operations != 0 || snapshot.BytesRead != 0 || snapshot.BytesWritten != 0 {
		t.Fatalf("invalid records mutated counters: %#v", snapshot)
	}
}

func TestSpaceOperationStatsRegistrySortsAndOwnsSnapshots(t *testing.T) {
	registry := hatMetrics.NewSpaceOperationStatsRegistry()
	users, err := registry.Register(" users ")
	if err != nil {
		t.Fatalf("Register(users) error = %v", err)
	}
	orders, err := registry.Register("orders")
	if err != nil {
		t.Fatalf("Register(orders) error = %v", err)
	}
	if _, err := registry.Register("orders"); !errors.Is(err, hatMetrics.ErrSpaceOperationStatsDuplicate) {
		t.Fatalf("duplicate Register() error = %v, want %v", err, hatMetrics.ErrSpaceOperationStatsDuplicate)
	}
	if err := registry.Record("orders", hatMetrics.SpaceOperationRead, hatMetrics.SpaceOperationHit, 8, time.Nanosecond); err != nil {
		t.Fatalf("registry Record(orders) error = %v", err)
	}
	if err := users.Record(hatMetrics.SpaceOperationWrite, hatMetrics.SpaceOperationSuccess, 4, 2*time.Nanosecond); err != nil {
		t.Fatalf("users Record() error = %v", err)
	}
	if err := registry.Record("missing", hatMetrics.SpaceOperationRead, hatMetrics.SpaceOperationMiss, 1, 0); !errors.Is(err, hatMetrics.ErrSpaceOperationStatsNotFound) {
		t.Fatalf("missing Record() error = %v, want %v", err, hatMetrics.ErrSpaceOperationStatsNotFound)
	}
	if got, ok := registry.Lookup(" orders "); !ok || got != orders {
		t.Fatalf("Lookup(orders) = %p/%v, want %p/true", got, ok, orders)
	}
	first := registry.Snapshot()
	if len(first) != 2 || first[0].Name != "orders" || first[1].Name != "users" {
		t.Fatalf("snapshot order = %#v, want orders/users", first)
	}
	first[0].Name = "changed"
	second := registry.Snapshot()
	if len(second) != 2 || second[0].Name != "orders" {
		t.Fatalf("snapshot mutation leaked: %#v", second)
	}
}

func TestSpaceOperationMetricsRecordsConcurrentUpdates(t *testing.T) {
	metrics, err := hatMetrics.NewSpaceOperationMetrics("orders")
	if err != nil {
		t.Fatalf("NewSpaceOperationMetrics() error = %v", err)
	}
	var group sync.WaitGroup
	for i := 0; i < 32; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for j := 0; j < 100; j++ {
				if err := metrics.Record(hatMetrics.SpaceOperationRead, hatMetrics.SpaceOperationHit, 2, time.Nanosecond); err != nil {
					t.Errorf("Record() error = %v", err)
					return
				}
			}
		}()
	}
	group.Wait()
	snapshot := metrics.Snapshot()
	if snapshot.Operations != 3200 || snapshot.Hits != 3200 || snapshot.BytesRead != 6400 || snapshot.LatencySamples != 3200 {
		t.Fatalf("concurrent snapshot = %#v", snapshot)
	}
}

func BenchmarkSpaceOperationMetricsRecord(b *testing.B) {
	metrics, err := hatMetrics.NewSpaceOperationMetrics("orders")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := metrics.Record(hatMetrics.SpaceOperationRead, hatMetrics.SpaceOperationHit, 64, time.Nanosecond); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSpaceOperationStatsRegistryRecord(b *testing.B) {
	registry := hatMetrics.NewSpaceOperationStatsRegistry()
	if _, err := registry.Register("orders"); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := registry.Record("orders", hatMetrics.SpaceOperationRead, hatMetrics.SpaceOperationHit, 64, time.Nanosecond); err != nil {
			b.Fatal(err)
		}
	}
}

func TestSpaceOperationMetricsRejectsLongNames(t *testing.T) {
	if _, err := hatMetrics.NewSpaceOperationMetrics(strings.Repeat("x", hatMetrics.MaxSpaceOperationStatsNameBytes+1)); !errors.Is(err, hatMetrics.ErrSpaceOperationStatsInvalid) {
		t.Fatalf("long name error = %v, want %v", err, hatMetrics.ErrSpaceOperationStatsInvalid)
	}
}
