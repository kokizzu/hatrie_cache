package hatStorage_test

import (
	"errors"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"hatrie_cache/hat/hatStorage"
)

func TestSpaceMemoryQuotaReservesAndReleasesAtomically(t *testing.T) {
	quota, err := hatStorage.NewSpaceMemoryQuota(" orders ", 100)
	if err != nil {
		t.Fatalf("NewSpaceMemoryQuota() error = %v", err)
	}
	if quota.Name() != "orders" || quota.Limit() != 100 || quota.Used() != 0 || quota.Available() != 100 {
		t.Fatalf("initial quota = name %q limit %d used %d available %d", quota.Name(), quota.Limit(), quota.Used(), quota.Available())
	}
	if err := quota.Reserve(70); err != nil {
		t.Fatalf("Reserve(70) error = %v", err)
	}
	if err := quota.Reserve(31); !errors.Is(err, hatStorage.ErrSpaceMemoryQuotaExceeded) {
		t.Fatalf("Reserve(31) error = %v, want %v", err, hatStorage.ErrSpaceMemoryQuotaExceeded)
	}
	if quota.Used() != 70 || quota.Available() != 30 {
		t.Fatalf("quota after rejected reserve = used %d available %d", quota.Used(), quota.Available())
	}
	if err := quota.Release(20); err != nil {
		t.Fatalf("Release(20) error = %v", err)
	}
	if quota.Used() != 50 || quota.Available() != 50 {
		t.Fatalf("quota after release = used %d available %d", quota.Used(), quota.Available())
	}
	if err := quota.Release(51); !errors.Is(err, hatStorage.ErrSpaceMemoryQuotaUnderflow) {
		t.Fatalf("Release(51) error = %v, want %v", err, hatStorage.ErrSpaceMemoryQuotaUnderflow)
	}
	if err := quota.Release(50); err != nil {
		t.Fatalf("Release(50) error = %v", err)
	}
	if quota.Used() != 0 || quota.Available() != 100 {
		t.Fatalf("quota after full release = used %d available %d", quota.Used(), quota.Available())
	}
}

func TestSpaceMemoryQuotaRegistryOwnsNamesAndSnapshots(t *testing.T) {
	registry := hatStorage.NewSpaceMemoryQuotaRegistry()
	east, err := registry.Register(" east ", 64)
	if err != nil {
		t.Fatalf("Register(east) error = %v", err)
	}
	if east.Name() != "east" {
		t.Fatalf("registered name = %q, want east", east.Name())
	}
	if _, err := registry.Register("east", 128); !errors.Is(err, hatStorage.ErrSpaceMemoryQuotaDuplicate) {
		t.Fatalf("duplicate Register() error = %v, want %v", err, hatStorage.ErrSpaceMemoryQuotaDuplicate)
	}
	if got, ok := registry.Lookup(" east "); !ok || got != east {
		t.Fatalf("Lookup(east) = %p/%v, want %p/true", got, ok, east)
	}
	if err := registry.Reserve("east", 32); err != nil {
		t.Fatalf("registry Reserve() error = %v", err)
	}
	if err := registry.Release("east", 12); err != nil {
		t.Fatalf("registry Release() error = %v", err)
	}
	if err := registry.Reserve("missing", 1); !errors.Is(err, hatStorage.ErrSpaceMemoryQuotaNotFound) {
		t.Fatalf("missing Reserve() error = %v, want %v", err, hatStorage.ErrSpaceMemoryQuotaNotFound)
	}
	first := registry.Snapshot()
	if len(first) != 1 || first[0].Name != "east" || first[0].UsedBytes != 20 || first[0].AvailableBytes != 44 {
		t.Fatalf("snapshot = %#v, want east used=20 available=44", first)
	}
	first[0].Name = "changed"
	second := registry.Snapshot()
	if len(second) != 1 || second[0].Name != "east" {
		t.Fatalf("snapshot mutation leaked: %#v", second)
	}
}

func TestSpaceMemoryQuotaHandlesUnlimitedAndOverflow(t *testing.T) {
	unlimited, err := hatStorage.NewSpaceMemoryQuota("unlimited", 0)
	if err != nil {
		t.Fatalf("NewSpaceMemoryQuota(unlimited) error = %v", err)
	}
	if err := unlimited.Reserve(math.MaxUint64); err != nil {
		t.Fatalf("Reserve(MaxUint64) error = %v", err)
	}
	if unlimited.Available() != math.MaxUint64-math.MaxUint64 {
		t.Fatalf("unlimited Available() = %d, want 0 after max reservation", unlimited.Available())
	}
	if err := unlimited.Reserve(1); !errors.Is(err, hatStorage.ErrSpaceMemoryQuotaOverflow) {
		t.Fatalf("overflow Reserve() error = %v, want %v", err, hatStorage.ErrSpaceMemoryQuotaOverflow)
	}
	if err := unlimited.Release(math.MaxUint64); err != nil {
		t.Fatalf("unlimited Release() error = %v", err)
	}
	if _, err := hatStorage.NewSpaceMemoryQuota(strings.Repeat("x", hatStorage.MaxSpaceMemoryQuotaNameBytes+1), 1); !errors.Is(err, hatStorage.ErrSpaceMemoryQuotaInvalid) {
		t.Fatalf("long name error = %v, want %v", err, hatStorage.ErrSpaceMemoryQuotaInvalid)
	}
}

func TestSpaceMemoryQuotaRejectsConcurrentOversubscription(t *testing.T) {
	quota, err := hatStorage.NewSpaceMemoryQuota("orders", 1000)
	if err != nil {
		t.Fatalf("NewSpaceMemoryQuota() error = %v", err)
	}
	var success atomic.Int64
	var group sync.WaitGroup
	for i := 0; i < 20; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			if err := quota.Reserve(100); err == nil {
				success.Add(1)
			}
		}()
	}
	group.Wait()
	if success.Load() != 10 || quota.Used() != 1000 {
		t.Fatalf("concurrent reservations = success %d used %d, want 10/1000", success.Load(), quota.Used())
	}
	if err := quota.Release(1000); err != nil {
		t.Fatalf("Release(all) error = %v", err)
	}
}

func BenchmarkSpaceMemoryQuotaReserveRelease(b *testing.B) {
	quota, err := hatStorage.NewSpaceMemoryQuota("orders", 1<<30)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := quota.Reserve(64); err != nil {
			b.Fatal(err)
		}
		if err := quota.Release(64); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSpaceMemoryQuotaRegistryReserveRelease(b *testing.B) {
	registry := hatStorage.NewSpaceMemoryQuotaRegistry()
	if _, err := registry.Register("orders", 1<<30); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := registry.Reserve("orders", 64); err != nil {
			b.Fatal(err)
		}
		if err := registry.Release("orders", 64); err != nil {
			b.Fatal(err)
		}
	}
}
