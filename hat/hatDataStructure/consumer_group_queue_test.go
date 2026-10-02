package hatDataStructure

import (
	"testing"
	"time"
)

func TestConsumerGroupQueuePartitionsWorkAndFencesOwnership(t *testing.T) {
	now := time.Unix(900, 0).UTC()
	queue := NewConsumerGroupQueue[string](8, time.Second)
	if !queue.Register("payments", "worker-a") || !queue.Register("payments", "worker-b") {
		t.Fatal("Register(payments) failed")
	}
	if !queue.Register("emails", "worker-c") {
		t.Fatal("Register(emails) failed")
	}
	if !queue.Enqueue("payments", "p1") || !queue.Enqueue("payments", "p2") {
		t.Fatal("Enqueue(payments) failed")
	}
	if !queue.Enqueue("emails", "e1") {
		t.Fatal("Enqueue(emails) failed")
	}

	first, ok := queue.Lease("payments", "worker-a", now)
	if !ok {
		t.Fatal("worker-a did not receive a payment")
	}
	second, ok := queue.Lease("payments", "worker-b", now)
	if !ok || second.Value == first.Value {
		t.Fatalf("worker-b lease = %+v, want the other payment", second)
	}
	if _, ok := queue.Lease("payments", "worker-a", now); ok {
		t.Fatal("payments work was duplicated before acknowledgement")
	}
	email, ok := queue.Lease("emails", "worker-c", now)
	if !ok || email.Value != "e1" {
		t.Fatalf("email lease = %+v, want e1", email)
	}

	staleOwner := first.Token
	staleOwner.Consumer = "worker-b"
	if queue.Ack(staleOwner) {
		t.Fatal("acknowledgement from the wrong consumer succeeded")
	}
	if !queue.Ack(first.Token) || !queue.Ack(email.Token) {
		t.Fatal("acknowledgement from the owning consumers failed")
	}

	if !queue.Nack(second.Token, now.Add(2*time.Second)) {
		t.Fatal("Nack failed")
	}
	retry, ok := queue.Lease("payments", "worker-a", now.Add(2*time.Second))
	if !ok || retry.Value != second.Value || retry.Attempts != 2 {
		t.Fatalf("retry lease = %+v, want value %q at attempt 2", retry, second.Value)
	}
	if !queue.Ack(retry.Token) {
		t.Fatal("retry acknowledgement failed")
	}
}

func TestConsumerGroupQueueExpiredAndReleasedLeasesReturnToGroup(t *testing.T) {
	now := time.Unix(950, 0).UTC()
	queue := NewConsumerGroupQueue[int](4, time.Second)
	if !queue.Register("jobs", "worker-a") || !queue.Register("jobs", "worker-b") {
		t.Fatal("Register(jobs) failed")
	}
	if !queue.Enqueue("jobs", 1) {
		t.Fatal("Enqueue(jobs) failed")
	}

	lease, ok := queue.Lease("jobs", "worker-a", now)
	if !ok {
		t.Fatal("initial lease failed")
	}
	if got := queue.RequeueExpired(now.Add(time.Second)); got != 1 {
		t.Fatalf("RequeueExpired = %d, want 1", got)
	}
	retry, ok := queue.Lease("jobs", "worker-a", now.Add(time.Second))
	if !ok || retry.Value != lease.Value || retry.Attempts != 2 {
		t.Fatalf("expired retry = %+v, want value %d at attempt 2", retry, lease.Value)
	}
	if queue.Ack(lease.Token) {
		t.Fatal("stale pre-expiry token acknowledged the retry")
	}
	if !queue.Ack(retry.Token) {
		t.Fatal("retry acknowledgement failed")
	}

	if !queue.Enqueue("jobs", 3) {
		t.Fatal("Enqueue(3) failed")
	}
	owned, ok := queue.Lease("jobs", "worker-a", now)
	if !ok || owned.Value != 3 {
		t.Fatalf("owned lease = %+v, want value 3", owned)
	}
	if !queue.Unregister("jobs", "worker-a", now) {
		t.Fatal("Unregister failed")
	}
	released, ok := queue.Lease("jobs", "worker-b", now)
	if !ok || released.Value != 3 {
		t.Fatalf("released lease = %+v, want value 3", released)
	}
	if !queue.Ack(released.Token) {
		t.Fatal("released lease acknowledgement failed")
	}
}
