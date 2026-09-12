package hatDataStructure

import (
	"fmt"
	"testing"
	"time"
)

func ExampleVisibilityQueue() {
	now := time.Unix(100, 0).UTC()
	queue := NewVisibilityQueue[string](1024, 30*time.Second)
	queue.Enqueue("job-42")
	item, ok := queue.Lease(now)
	fmt.Printf("%d %s %d %t\n", item.ID, item.Value, item.Attempts, ok)
	// Output:
	// 1 job-42 1 true
}

func TestVisibilityQueueLeaseAckAndNack(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	queue := NewVisibilityQueue[string](4, 10*time.Second)

	if !queue.Enqueue("ready") {
		t.Fatal("Enqueue(ready) returned false")
	}
	if !queue.EnqueueAt(now.Add(5*time.Second), "delayed") {
		t.Fatal("EnqueueAt(delayed) returned false")
	}

	item, ok := queue.Lease(now)
	if !ok {
		t.Fatal("Lease returned no item")
	}
	if item.ID != 1 || item.Value != "ready" || item.Attempts != 1 {
		t.Fatalf("unexpected first lease: %+v", item)
	}
	if want := now.Add(10 * time.Second); !item.LeaseUntil.Equal(want) {
		t.Fatalf("lease deadline = %v, want %v", item.LeaseUntil, want)
	}
	if !queue.Ack(item.ID) {
		t.Fatal("Ack returned false")
	}
	if queue.Ack(item.ID) {
		t.Fatal("duplicate Ack returned true")
	}

	if _, ok := queue.Lease(now); ok {
		t.Fatal("Lease returned delayed item too early")
	}
	delayed, ok := queue.Lease(now.Add(5 * time.Second))
	if !ok {
		t.Fatal("Lease returned no delayed item")
	}
	if delayed.ID != 2 || delayed.Value != "delayed" || delayed.Attempts != 1 {
		t.Fatalf("unexpected delayed lease: %+v", delayed)
	}
	if !queue.Nack(delayed.ID, now.Add(7*time.Second)) {
		t.Fatal("Nack returned false")
	}
	if _, ok := queue.Lease(now.Add(6 * time.Second)); ok {
		t.Fatal("Lease returned nacked item too early")
	}
	retry, ok := queue.Lease(now.Add(7 * time.Second))
	if !ok {
		t.Fatal("Lease returned no nacked item")
	}
	if retry.ID != delayed.ID || retry.Value != delayed.Value || retry.Attempts != 2 {
		t.Fatalf("unexpected retry lease: %+v", retry)
	}
	if !queue.Ack(retry.ID) {
		t.Fatal("Ack(retry) returned false")
	}
	if queue.Len() != 0 {
		t.Fatalf("Len = %d, want 0", queue.Len())
	}
}

func TestVisibilityQueueRequeuesExpiredLease(t *testing.T) {
	now := time.Unix(200, 0).UTC()
	queue := NewVisibilityQueue[string](0, time.Second)
	if !queue.Enqueue("recoverable") {
		t.Fatal("Enqueue returned false")
	}
	item, ok := queue.Lease(now)
	if !ok {
		t.Fatal("Lease returned no item")
	}
	if got := queue.RequeueExpired(now.Add(999 * time.Millisecond)); got != 0 {
		t.Fatalf("RequeueExpired before deadline = %d, want 0", got)
	}
	if got := queue.RequeueExpired(now.Add(time.Second)); got != 1 {
		t.Fatalf("RequeueExpired at deadline = %d, want 1", got)
	}
	if queue.LeaseLen() != 0 || queue.PendingLen() != 1 {
		t.Fatalf("queue lengths after expiry = pending %d, leases %d", queue.PendingLen(), queue.LeaseLen())
	}
	retry, ok := queue.Lease(now.Add(time.Second))
	if !ok {
		t.Fatal("Lease returned no expired item")
	}
	if retry.ID != item.ID || retry.Attempts != 2 {
		t.Fatalf("unexpected expired retry: %+v", retry)
	}
	if !queue.Ack(retry.ID) {
		t.Fatal("Ack(expired retry) returned false")
	}
	if got := queue.RequeueExpired(now.Add(10 * time.Second)); got != 0 {
		t.Fatalf("RequeueExpired after ack = %d, want 0", got)
	}
}

func TestVisibilityQueueRemovesArbitraryLeaseDeadline(t *testing.T) {
	now := time.Unix(250, 0).UTC()
	queue := NewVisibilityQueue[int](4, time.Minute)
	for value := 1; value <= 3; value++ {
		if !queue.Enqueue(value) {
			t.Fatalf("Enqueue(%d) returned false", value)
		}
	}
	first, ok := queue.LeaseFor(now, time.Second)
	if !ok {
		t.Fatal("first Lease returned no item")
	}
	second, ok := queue.LeaseFor(now, 2*time.Second)
	if !ok {
		t.Fatal("second Lease returned no item")
	}
	third, ok := queue.LeaseFor(now, 3*time.Second)
	if !ok {
		t.Fatal("third Lease returned no item")
	}
	if !queue.Ack(second.ID) {
		t.Fatal("Ack(second) returned false")
	}
	if got := queue.RequeueExpired(now.Add(2 * time.Second)); got != 1 {
		t.Fatalf("RequeueExpired at second deadline = %d, want 1", got)
	}
	retry, ok := queue.Lease(now.Add(2 * time.Second))
	if !ok || retry.ID != first.ID {
		t.Fatalf("first retry = %+v, ok %v", retry, ok)
	}
	if !queue.Ack(retry.ID) {
		t.Fatal("Ack(first retry) returned false")
	}
	if got := queue.RequeueExpired(now.Add(3 * time.Second)); got != 1 {
		t.Fatalf("RequeueExpired at third deadline = %d, want 1", got)
	}
	retry, ok = queue.Lease(now.Add(3 * time.Second))
	if !ok || retry.ID != third.ID {
		t.Fatalf("third retry = %+v, ok %v", retry, ok)
	}
	if !queue.Ack(retry.ID) {
		t.Fatal("Ack(third retry) returned false")
	}
	if queue.Len() != 0 {
		t.Fatalf("Len = %d, want 0", queue.Len())
	}
}

func TestVisibilityQueueCapacityCountsLeases(t *testing.T) {
	queue := NewVisibilityQueue[int](1, time.Minute)
	if !queue.Enqueue(1) {
		t.Fatal("first Enqueue returned false")
	}
	if queue.Enqueue(2) {
		t.Fatal("second Enqueue returned true at capacity")
	}
	item, ok := queue.Lease(time.Unix(300, 0).UTC())
	if !ok {
		t.Fatal("Lease returned no item")
	}
	if queue.Enqueue(2) {
		t.Fatal("Enqueue returned true while item was leased at capacity")
	}
	if !queue.Ack(item.ID) {
		t.Fatal("Ack returned false")
	}
	if !queue.Enqueue(2) {
		t.Fatal("Enqueue returned false after capacity was released")
	}
}

func TestVisibilityQueueClearAndZeroValue(t *testing.T) {
	now := time.Unix(400, 0).UTC()
	var queue VisibilityQueue[int]
	if !queue.Enqueue(1) || !queue.Enqueue(2) {
		t.Fatal("zero-value Enqueue failed")
	}
	item, ok := queue.Lease(now)
	if !ok {
		t.Fatal("zero-value Lease returned no item")
	}
	if want := now.Add(DefaultVisibilityQueueTimeout); !item.LeaseUntil.Equal(want) {
		t.Fatalf("default lease deadline = %v, want %v", item.LeaseUntil, want)
	}
	queue.Clear()
	if queue.Len() != 0 || queue.PendingLen() != 0 || queue.LeaseLen() != 0 {
		t.Fatalf("queue not empty after Clear: pending %d, leases %d", queue.PendingLen(), queue.LeaseLen())
	}
	if queue.Ack(item.ID) {
		t.Fatal("Ack returned true for cleared lease")
	}
	if _, ok := queue.Lease(now); ok {
		t.Fatal("Lease returned an item after Clear")
	}
	if !queue.Enqueue(3) {
		t.Fatal("Enqueue failed after Clear")
	}
}
