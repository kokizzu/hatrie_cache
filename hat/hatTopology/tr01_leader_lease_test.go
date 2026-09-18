package hatTopology

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestTR01LeaderLeaseAcquireValidateAndFence(t *testing.T) {
	now := time.Unix(1000, 0)
	store, err := NewLeaderLeaseStore(LeaderLeaseOptions{
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewLeaderLeaseStore() error = %v", err)
	}

	first, err := store.Acquire("shard-0", "node-a", 10*time.Second)
	if err != nil {
		t.Fatalf("Acquire(first) error = %v", err)
	}
	if first.Token != 1 || first.Holder != "node-a" || !first.ExpiresAt.Equal(now.Add(10*time.Second)) {
		t.Fatalf("first lease = %#v, want token 1 and 10-second expiry", first)
	}
	if err := store.Validate(first.Name, first.Holder, first.Token); err != nil {
		t.Fatalf("Validate(first) error = %v", err)
	}
	if _, err := store.Acquire(first.Name, "node-b", time.Second); !errors.Is(err, ErrLeaderLeaseHeld) {
		t.Fatalf("Acquire(while held) error = %v, want ErrLeaderLeaseHeld", err)
	}

	now = now.Add(11 * time.Second)
	if err := store.Validate(first.Name, first.Holder, first.Token); !errors.Is(err, ErrLeaderLeaseExpired) {
		t.Fatalf("Validate(expired) error = %v, want ErrLeaderLeaseExpired", err)
	}
	second, err := store.Acquire(first.Name, "node-b", time.Second)
	if err != nil {
		t.Fatalf("Acquire(after expiry) error = %v", err)
	}
	if second.Token != 2 {
		t.Fatalf("second token = %d, want 2", second.Token)
	}
	if err := store.Validate(first.Name, first.Holder, first.Token); !errors.Is(err, ErrLeaderLeaseFenced) {
		t.Fatalf("Validate(old token) error = %v, want ErrLeaderLeaseFenced", err)
	}
}

func TestTR01LeaderLeaseRenewAndReleaseRequireCurrentFence(t *testing.T) {
	now := time.Unix(2000, 0)
	store, err := NewLeaderLeaseStore(LeaderLeaseOptions{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatalf("NewLeaderLeaseStore() error = %v", err)
	}
	lease, err := store.Acquire("shard-1", "node-a", 5*time.Second)
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}

	now = now.Add(4 * time.Second)
	renewed, err := store.Renew(lease.Name, lease.Holder, lease.Token, 5*time.Second)
	if err != nil {
		t.Fatalf("Renew() error = %v", err)
	}
	if !renewed.ExpiresAt.Equal(now.Add(5 * time.Second)) {
		t.Fatalf("renewed expiry = %v, want %v", renewed.ExpiresAt, now.Add(5*time.Second))
	}
	if _, err := store.Renew(lease.Name, "node-b", lease.Token, time.Second); !errors.Is(err, ErrLeaderLeaseFenced) {
		t.Fatalf("Renew(wrong holder) error = %v, want ErrLeaderLeaseFenced", err)
	}
	if err := store.Release(lease.Name, "node-b", lease.Token); !errors.Is(err, ErrLeaderLeaseFenced) {
		t.Fatalf("Release(wrong holder) error = %v, want ErrLeaderLeaseFenced", err)
	}
	if err := store.Release(lease.Name, lease.Holder, lease.Token); err != nil {
		t.Fatalf("Release(current) error = %v", err)
	}
	if err := store.Validate(lease.Name, lease.Holder, lease.Token); !errors.Is(err, ErrLeaderLeaseNotFound) {
		t.Fatalf("Validate(released) error = %v, want ErrLeaderLeaseNotFound", err)
	}
}

func TestTR01LeaderLeaseValidatesDefaultsAndConcurrentAcquire(t *testing.T) {
	if _, err := NewLeaderLeaseStore(LeaderLeaseOptions{MaxTTL: -time.Second}); !errors.Is(err, ErrLeaderLeaseTTL) {
		t.Fatalf("negative max TTL error = %v, want ErrLeaderLeaseTTL", err)
	}
	now := time.Unix(3000, 0)
	defaultStore, err := NewLeaderLeaseStore(LeaderLeaseOptions{
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewLeaderLeaseStore() error = %v", err)
	}
	lease, err := defaultStore.Acquire("shard-2", "node-a", 0)
	if err != nil {
		t.Fatalf("Acquire(default TTL) error = %v", err)
	}
	if !lease.ExpiresAt.Equal(now.Add(DefaultLeaderLeaseTTL)) {
		t.Fatalf("default expiry = %v, want %v", lease.ExpiresAt, now.Add(DefaultLeaderLeaseTTL))
	}
	store, err := NewLeaderLeaseStore(LeaderLeaseOptions{
		Now:    func() time.Time { return now },
		MaxTTL: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewLeaderLeaseStore(max TTL) error = %v", err)
	}
	if _, err := store.Acquire("", "node-b", time.Second); !errors.Is(err, ErrLeaderLeaseName) {
		t.Fatalf("empty name error = %v, want ErrLeaderLeaseName", err)
	}
	if _, err := store.Acquire("shard-3", "node-b", 3*time.Second); !errors.Is(err, ErrLeaderLeaseTTL) {
		t.Fatalf("overlong TTL error = %v, want ErrLeaderLeaseTTL", err)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	acquired := 0
	for index := 0; index < 16; index++ {
		index := index
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := store.Acquire("shard-4", "node-"+string(rune('a'+index)), time.Second); err == nil {
				mu.Lock()
				acquired++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if acquired != 1 {
		t.Fatalf("concurrent acquisitions = %d, want exactly 1", acquired)
	}
}
