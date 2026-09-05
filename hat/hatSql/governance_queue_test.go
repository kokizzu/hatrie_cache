package hatSql

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNamespaceQueryGovernorRejectsExcessQueuedQueries(t *testing.T) {
	gate := newNamespaceQueryGate(1, 1)
	if err := gate.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}

	queued := make(chan error, 1)
	go func() {
		queued <- gate.acquire(context.Background())
	}()
	waitForQueuedGovernorWaiter(t, gate)

	if err := gate.acquire(context.Background()); !errors.Is(err, ErrNamespaceQueryQueueFull) {
		t.Fatalf("third acquire error = %v, want %v", err, ErrNamespaceQueryQueueFull)
	}

	gate.release()
	select {
	case err := <-queued:
		if err != nil {
			t.Fatalf("queued acquire error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued acquire was not released")
	}
	gate.release()
}

func TestNamespaceQueryGovernorValidatesQueueLimit(t *testing.T) {
	if _, err := NewNamespaceQueryGovernor(NamespaceResourceLimits{MaxQueuedQueries: -1}, nil); err == nil {
		t.Fatal("negative MaxQueuedQueries was accepted")
	}

	governor, err := NewNamespaceQueryGovernor(
		NamespaceResourceLimits{MaxConcurrentQueries: 1, MaxQueuedQueries: 4},
		map[string]NamespaceResourceLimits{"tenant": {MaxQueuedQueries: 1}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := governor.limitsFor("tenant").MaxQueuedQueries; got != 1 {
		t.Fatalf("tenant MaxQueuedQueries = %d, want 1", got)
	}
	if got := governor.limitsFor("other").MaxQueuedQueries; got != 4 {
		t.Fatalf("default MaxQueuedQueries = %d, want 4", got)
	}
}

func waitForQueuedGovernorWaiter(t *testing.T, gate *namespaceQueryGate) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		gate.mu.Lock()
		queued := len(gate.waiters)
		gate.mu.Unlock()
		if queued == 1 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("governor waiter was not queued")
}

func BenchmarkNamespaceQueryGateFastPath(b *testing.B) {
	gate := newNamespaceQueryGate(1, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if err := gate.acquire(context.Background()); err != nil {
			b.Fatal(err)
		}
		gate.release()
	}
}
