package hatTopology

import (
	"errors"
	"testing"
	"time"
)

func TestT203LeaderLeaseWithFenceRejectsStaleWriters(t *testing.T) {
	now := time.Unix(5000, 0)
	store, err := NewLeaderLeaseStore(LeaderLeaseOptions{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatalf("NewLeaderLeaseStore() error = %v", err)
	}
	first, err := store.Acquire("shard-0", "node-a", 10*time.Second)
	if err != nil {
		t.Fatalf("Acquire(first) error = %v", err)
	}
	var writes int
	if err := store.WithFence(first.Name, first.Holder, first.Token, func(got LeaderLease) error {
		writes++
		if got != first {
			t.Errorf("callback lease = %#v, want %#v", got, first)
		}
		return nil
	}); err != nil {
		t.Fatalf("WithFence(current) error = %v", err)
	}
	if writes != 1 {
		t.Fatalf("current callback count = %d, want 1", writes)
	}

	callbackErr := errors.New("write rejected")
	if err := store.WithFence(first.Name, first.Holder, first.Token, func(LeaderLease) error { return callbackErr }); !errors.Is(err, callbackErr) {
		t.Fatalf("WithFence(callback error) = %v, want callback error", err)
	}
	if err := store.Validate(first.Name, first.Holder, first.Token); err != nil {
		t.Fatalf("Validate(after callback error) = %v, want current lease", err)
	}

	now = now.Add(11 * time.Second)
	second, err := store.Acquire(first.Name, "node-b", time.Second)
	if err != nil {
		t.Fatalf("Acquire(second) error = %v", err)
	}
	writes = 0
	if err := store.WithFence(first.Name, first.Holder, first.Token, func(LeaderLease) error {
		writes++
		return nil
	}); !errors.Is(err, ErrLeaderLeaseFenced) {
		t.Fatalf("WithFence(stale) = %v, want ErrLeaderLeaseFenced", err)
	}
	if writes != 0 {
		t.Fatalf("stale callback count = %d, want 0", writes)
	}
	if err := store.WithFence(second.Name, second.Holder, second.Token, nil); !errors.Is(err, ErrLeaderLeaseWriteCallback) {
		t.Fatalf("WithFence(nil callback) = %v, want ErrLeaderLeaseWriteCallback", err)
	}
}

func TestT203LeaderLeaseWithFenceSerializesRotation(t *testing.T) {
	store, err := NewLeaderLeaseStore(LeaderLeaseOptions{Now: time.Now})
	if err != nil {
		t.Fatalf("NewLeaderLeaseStore() error = %v", err)
	}
	lease, err := store.Acquire("shard-1", "node-a", time.Minute)
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	entered := make(chan struct{})
	allowReturn := make(chan struct{})
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- store.WithFence(lease.Name, lease.Holder, lease.Token, func(LeaderLease) error {
			close(entered)
			<-allowReturn
			return nil
		})
	}()
	<-entered
	rotationDone := make(chan error, 1)
	go func() { rotationDone <- store.Release(lease.Name, lease.Holder, lease.Token) }()
	select {
	case err := <-rotationDone:
		t.Fatalf("lease rotation completed during fenced write: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(allowReturn)
	if err := <-writeDone; err != nil {
		t.Fatalf("WithFence(serialized) error = %v", err)
	}
	if err := <-rotationDone; err != nil {
		t.Fatalf("lease rotation after fenced write = %v", err)
	}
}

func BenchmarkT203LeaderLeaseWithFence(b *testing.B) {
	now := time.Unix(4000, 0)
	store, err := NewLeaderLeaseStore(LeaderLeaseOptions{Now: func() time.Time { return now }})
	if err != nil {
		b.Fatal(err)
	}
	lease, err := store.Acquire("shard-0", "node-a", DefaultLeaderLeaseMaxTTL)
	if err != nil {
		b.Fatal(err)
	}
	write := func(LeaderLease) error { return nil }
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := store.WithFence(lease.Name, lease.Holder, lease.Token, write); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkT203LeaderLeaseValidateControl(b *testing.B) {
	now := time.Unix(4000, 0)
	store, err := NewLeaderLeaseStore(LeaderLeaseOptions{Now: func() time.Time { return now }})
	if err != nil {
		b.Fatal(err)
	}
	lease, err := store.Acquire("shard-0", "node-a", DefaultLeaderLeaseMaxTTL)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := store.Validate(lease.Name, lease.Holder, lease.Token); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkT203ValidateThenWriteControl(b *testing.B) {
	now := time.Unix(4000, 0)
	store, err := NewLeaderLeaseStore(LeaderLeaseOptions{Now: func() time.Time { return now }})
	if err != nil {
		b.Fatal(err)
	}
	lease, err := store.Acquire("shard-0", "node-a", DefaultLeaderLeaseMaxTTL)
	if err != nil {
		b.Fatal(err)
	}
	write := func(LeaderLease) error { return nil }
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := store.Validate(lease.Name, lease.Holder, lease.Token); err != nil {
			b.Fatal(err)
		}
		if err := write(lease); err != nil {
			b.Fatal(err)
		}
	}
}
