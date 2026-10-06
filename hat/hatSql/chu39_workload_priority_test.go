package hatSql

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestCHU39PriorityAdmissionPrefersHigherPriority(t *testing.T) {
	gate := newNamespaceQueryGate(1)
	if err := gate.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}

	lowReady := make(chan struct{})
	lowDone := make(chan error, 1)
	go func() {
		close(lowReady)
		lowDone <- gate.acquireWithPriority(context.Background(), -1)
	}()
	<-lowReady
	waitForCHU39Waiters(t, gate, 1)

	highReady := make(chan struct{})
	highDone := make(chan error, 1)
	go func() {
		close(highReady)
		highDone <- gate.acquireWithPriority(context.Background(), 1)
	}()
	<-highReady
	waitForCHU39Waiters(t, gate, 2)

	gate.release()
	select {
	case err := <-highDone:
		if err != nil {
			t.Fatalf("high-priority acquire error = %v", err)
		}
	case <-lowDone:
		t.Fatal("low-priority waiter acquired before high-priority waiter")
	case <-time.After(2 * time.Second):
		t.Fatal("high-priority waiter was not admitted")
	}

	gate.release()
	if err := <-lowDone; err != nil {
		t.Fatalf("low-priority acquire error = %v", err)
	}
	gate.release()
}

func TestCHU39ZeroPriorityPreservesFIFO(t *testing.T) {
	gate := newNamespaceQueryGate(1)
	if err := gate.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}

	firstDone := make(chan error, 1)
	go func() { firstDone <- gate.acquireWithPriority(context.Background(), 0) }()
	waitForCHU39Waiters(t, gate, 1)
	secondDone := make(chan error, 1)
	go func() { secondDone <- gate.acquireWithPriority(context.Background(), 0) }()
	waitForCHU39Waiters(t, gate, 2)

	gate.release()
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first FIFO acquire error = %v", err)
		}
	case <-secondDone:
		t.Fatal("second zero-priority waiter acquired before the first")
	case <-time.After(2 * time.Second):
		t.Fatal("first zero-priority waiter was not admitted")
	}

	gate.release()
	if err := <-secondDone; err != nil {
		t.Fatalf("second FIFO acquire error = %v", err)
	}
	gate.release()
}

func TestCHU39PriorityAdmissionPreventsStarvation(t *testing.T) {
	gate := newNamespaceQueryGate(1)
	if err := gate.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}

	oldReady := make(chan struct{})
	oldDone := make(chan error, 1)
	go func() {
		close(oldReady)
		oldDone <- gate.acquireWithPriority(context.Background(), -100)
	}()
	<-oldReady
	waitForCHU39Waiters(t, gate, 1)

	oldAdmitted := false
	for index := 0; index < 256; index++ {
		highDone := make(chan error, 1)
		go func() { highDone <- gate.acquireWithPriority(context.Background(), 100) }()
		waitForCHU39Waiters(t, gate, 2)
		gate.release()
		select {
		case err := <-oldDone:
			if err != nil {
				t.Fatalf("old waiter acquire error = %v", err)
			}
			oldAdmitted = true
		case err := <-highDone:
			if err != nil {
				t.Fatalf("high-priority acquire error = %v", err)
			}
			gate.release()
			select {
			case err := <-oldDone:
				if err != nil {
					t.Fatalf("old waiter acquire error = %v", err)
				}
				oldAdmitted = true
			case <-time.After(2 * time.Second):
				t.Fatal("old waiter was not admitted after high-priority release")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("priority waiters did not make progress")
		}
		if oldAdmitted {
			break
		}
	}
	if !oldAdmitted {
		t.Fatal("old low-priority waiter starved after 256 admissions")
	}
	gate.release()
}

func TestCHU39GovernorUsesWorkloadPriority(t *testing.T) {
	resolver := &chu39PriorityResolver{
		firstStarted: make(chan struct{}),
		releaseFirst: make(chan struct{}),
	}
	governor, err := NewNamespaceQueryGovernor(NamespaceResourceLimits{MaxConcurrentQueries: 1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer governor.Close()

	firstDone := make(chan error, 1)
	go func() {
		_, firstErr := governor.Execute(context.Background(), "tenant", "SELECT value FROM CACHE('first')", resolver, nil, SQLQueryOptions{})
		firstDone <- firstErr
	}()
	<-resolver.firstStarted

	lowDone := make(chan error, 1)
	go func() {
		_, lowErr := governor.Execute(context.Background(), "tenant", "SELECT value FROM CACHE('low')", resolver, nil, SQLQueryOptions{WorkloadPriority: -1})
		lowDone <- lowErr
	}()
	waitForCHU39GovernorWaiters(t, governor, "tenant", 1)

	highDone := make(chan error, 1)
	go func() {
		_, highErr := governor.Execute(context.Background(), "tenant", "SELECT value FROM CACHE('high')", resolver, nil, SQLQueryOptions{WorkloadPriority: 1})
		highDone <- highErr
	}()
	waitForCHU39GovernorWaiters(t, governor, "tenant", 2)

	close(resolver.releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatalf("first query error = %v", err)
	}
	if err := <-highDone; err != nil {
		t.Fatalf("high-priority query error = %v", err)
	}
	if err := <-lowDone; err != nil {
		t.Fatalf("low-priority query error = %v", err)
	}

	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	if len(resolver.keys) != 3 || resolver.keys[0] != "first" || resolver.keys[1] != "high" || resolver.keys[2] != "low" {
		t.Fatalf("resolver order = %#v, want [first high low]", resolver.keys)
	}
}

type chu39PriorityResolver struct {
	mu           sync.Mutex
	keys         []string
	firstStarted chan struct{}
	releaseFirst chan struct{}
}

func (resolver *chu39PriorityResolver) ResolveSQLSource(_ string, key string) ([]Row, error) {
	resolver.mu.Lock()
	resolver.keys = append(resolver.keys, key)
	resolver.mu.Unlock()
	if key == "first" {
		close(resolver.firstStarted)
		<-resolver.releaseFirst
	}
	return []Row{{"value": int64(1)}}, nil
}

func waitForCHU39Waiters(t *testing.T, gate *namespaceQueryGate, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	last := 0
	for time.Now().Before(deadline) {
		gate.mu.Lock()
		got := len(gate.waiters)
		gate.mu.Unlock()
		last = got
		if got >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	gate.mu.Lock()
	priorities := make([]int, len(gate.waiters))
	for index, waiter := range gate.waiters {
		priorities[index] = waiter.priority
	}
	running := gate.running
	gate.mu.Unlock()
	t.Fatalf("waiters = %d, want at least %d, running=%d priorities=%v", last, want, running, priorities)
}

func waitForCHU39GovernorWaiters(t *testing.T, governor *NamespaceQueryGovernor, namespace string, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		governor.mu.Lock()
		gate := governor.gates[namespace]
		governor.mu.Unlock()
		if gate != nil {
			gate.mu.Lock()
			got := len(gate.waiters)
			gate.mu.Unlock()
			if got >= want {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("namespace %q waiters = less than %d", namespace, want)
}
