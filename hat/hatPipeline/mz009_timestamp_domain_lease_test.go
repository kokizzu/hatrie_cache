package hatPipeline

import (
	"errors"
	"testing"
	"time"
)

func TestMZ009TimestampDomainLeaseFencesOwnersAndKeepsMonotonicTime(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	registry, err := NewTimestampDomainLeaseRegistry(TimestampDomainLeaseOptions{
		MaxDomains: 2,
		Clock:      func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewTimestampDomainLeaseRegistry() error = %v", err)
	}
	first, err := registry.Acquire("orders", "reader-a", time.Minute)
	if err != nil {
		t.Fatalf("Acquire(first) error = %v", err)
	}
	if got, err := registry.Next(first); err != nil || got != 1 {
		t.Fatalf("Next(first) = %d/%v, want 1/nil", got, err)
	}
	if err := registry.Observe(first, 9); err != nil {
		t.Fatalf("Observe(9) error = %v", err)
	}
	if got, err := registry.Next(first); err != nil || got != 10 {
		t.Fatalf("Next(after Observe) = %d/%v, want 10/nil", got, err)
	}
	if _, err := registry.Acquire("orders", "reader-b", time.Minute); !errors.Is(err, ErrTimestampDomainLeaseActive) {
		t.Fatalf("second Acquire() error = %v, want %v", err, ErrTimestampDomainLeaseActive)
	}
	if err := registry.Observe(first, 8); !errors.Is(err, ErrTimestampDomainLeaseRegression) {
		t.Fatalf("regressing Observe() error = %v, want %v", err, ErrTimestampDomainLeaseRegression)
	}
	if err := registry.Release(first); err != nil {
		t.Fatalf("Release(first) error = %v", err)
	}
	second, err := registry.Acquire("orders", "reader-b", time.Minute)
	if err != nil {
		t.Fatalf("Acquire(second) error = %v", err)
	}
	if second.Epoch <= first.Epoch {
		t.Fatalf("second epoch = %d, first = %d; want fencing epoch increase", second.Epoch, first.Epoch)
	}
	if _, err := registry.Next(first); !errors.Is(err, ErrTimestampDomainLeaseStale) {
		t.Fatalf("Next(stale first) error = %v, want %v", err, ErrTimestampDomainLeaseStale)
	}
	if got, err := registry.Next(second); err != nil || got != 11 {
		t.Fatalf("Next(second) = %d/%v, want preserved monotonic 11/nil", got, err)
	}
}

func TestMZ009TimestampDomainLeaseExpiryAndBounds(t *testing.T) {
	now := time.Unix(200, 0).UTC()
	registry, err := NewTimestampDomainLeaseRegistry(TimestampDomainLeaseOptions{
		MaxDomains: 1,
		Clock:      func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewTimestampDomainLeaseRegistry() error = %v", err)
	}
	if _, err := registry.Acquire("", "owner", time.Minute); !errors.Is(err, ErrTimestampDomainLeaseDomainEmpty) {
		t.Fatalf("empty domain error = %v, want %v", err, ErrTimestampDomainLeaseDomainEmpty)
	}
	if _, err := registry.Acquire("orders", "owner", 0); !errors.Is(err, ErrTimestampDomainLeaseTTLInvalid) {
		t.Fatalf("zero TTL error = %v, want %v", err, ErrTimestampDomainLeaseTTLInvalid)
	}
	lease, err := registry.Acquire("orders", "owner", time.Minute)
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	now = now.Add(time.Minute)
	if _, err := registry.Next(lease); !errors.Is(err, ErrTimestampDomainLeaseExpired) {
		t.Fatalf("expired Next() error = %v, want %v", err, ErrTimestampDomainLeaseExpired)
	}
	replacement, err := registry.Acquire("orders", "replacement", time.Minute)
	if err != nil {
		t.Fatalf("Acquire(replacement) error = %v", err)
	}
	if _, err := registry.Acquire("other", "owner", time.Minute); !errors.Is(err, ErrTimestampDomainLeaseCapacity) {
		t.Fatalf("capacity error = %v, want %v", err, ErrTimestampDomainLeaseCapacity)
	}
	if err := registry.Release(replacement); err != nil {
		t.Fatalf("Release(replacement) error = %v", err)
	}
	if err := registry.Forget("orders"); err != nil {
		t.Fatalf("Forget() error = %v", err)
	}
	if got := registry.Snapshot(); len(got) != 0 {
		t.Fatalf("Snapshot() after Forget = %#v, want empty", got)
	}
}
