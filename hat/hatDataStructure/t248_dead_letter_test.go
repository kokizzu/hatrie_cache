package hatDataStructure

import (
	"errors"
	"testing"
	"time"
)

func TestT248VisibilityQueueRoutesExhaustedRetriesToDeadLetter(t *testing.T) {
	now := time.Unix(100, 0)
	queue, err := NewVisibilityQueueWithRetryPolicy[int](4, time.Minute, 1, VisibilityQueueRetryOptions{
		MaxAttempts:    2,
		MaxDeadLetters: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !queue.Enqueue(7) {
		t.Fatal("Enqueue() rejected item")
	}
	first, ok := queue.Lease(now)
	if !ok || first.Attempts != 1 {
		t.Fatalf("first Lease() = %#v/%t, want attempt 1", first, ok)
	}
	if !queue.Nack(first.ID, now) {
		t.Fatal("first Nack() failed")
	}
	second, ok := queue.Lease(now)
	if !ok || second.Attempts != 2 {
		t.Fatalf("second Lease() = %#v/%t, want attempt 2", second, ok)
	}
	if !queue.Nack(second.ID, now) {
		t.Fatal("terminal Nack() failed")
	}
	if queue.PendingLen() != 0 || queue.LeaseLen() != 0 || queue.DeadLetterLen() != 1 {
		t.Fatalf("queue lengths = pending %d lease %d dead %d, want 0/0/1", queue.PendingLen(), queue.LeaseLen(), queue.DeadLetterLen())
	}
	dead, ok := queue.PopDeadLetter()
	if !ok || dead.ID != second.ID || dead.Value != 7 || dead.Attempts != 2 {
		t.Fatalf("dead letter = %#v/%t, want id %d value 7 attempts 2", dead, ok, second.ID)
	}
	if _, ok := queue.PopDeadLetter(); ok {
		t.Fatal("PopDeadLetter() returned an empty queue item")
	}
}

func TestT248VisibilityQueueDeadLetterCapacityPreservesLease(t *testing.T) {
	now := time.Unix(200, 0)
	queue, err := NewVisibilityQueueWithRetryPolicy[int](4, time.Minute, 1, VisibilityQueueRetryOptions{
		MaxAttempts:    1,
		MaxDeadLetters: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !queue.Enqueue(1) || !queue.Enqueue(2) {
		t.Fatal("Enqueue() rejected fixture")
	}
	first, _ := queue.Lease(now)
	if !queue.Nack(first.ID, now) {
		t.Fatal("first terminal Nack() failed")
	}
	second, _ := queue.Lease(now)
	if queue.Nack(second.ID, now) {
		t.Fatal("Nack() succeeded with a full dead-letter queue")
	}
	if queue.LeaseLen() != 1 || queue.DeadLetterLen() != 1 {
		t.Fatalf("queue lengths after full dead-letter Nack = lease %d dead %d, want 1/1", queue.LeaseLen(), queue.DeadLetterLen())
	}
	if _, ok := queue.PopDeadLetter(); !ok {
		t.Fatal("PopDeadLetter() failed to free capacity")
	}
	if !queue.Nack(second.ID, now) {
		t.Fatal("Nack() failed after dead-letter capacity was freed")
	}
}

func TestT248VisibilityQueueRoutesExpiredLeaseToDeadLetter(t *testing.T) {
	now := time.Unix(300, 0)
	queue, err := NewVisibilityQueueWithRetryPolicy[int](2, time.Second, 1, VisibilityQueueRetryOptions{
		MaxAttempts:    1,
		MaxDeadLetters: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !queue.Enqueue(9) {
		t.Fatal("Enqueue() rejected item")
	}
	item, ok := queue.Lease(now)
	if !ok {
		t.Fatal("Lease() failed")
	}
	if recovered := queue.RequeueExpired(now.Add(time.Second)); recovered != 1 {
		t.Fatalf("RequeueExpired() recovered %d items, want 1", recovered)
	}
	dead, ok := queue.PopDeadLetter()
	if !ok || dead.ID != item.ID || dead.Value != 9 || dead.Attempts != 1 {
		t.Fatalf("expired dead letter = %#v/%t, want id %d value 9 attempts 1", dead, ok, item.ID)
	}
}

func TestT248VisibilityQueueRetryPolicyValidatesBounds(t *testing.T) {
	_, err := NewVisibilityQueueWithRetryPolicy[int](1, time.Second, 1, VisibilityQueueRetryOptions{
		MaxAttempts:    2,
		MaxDeadLetters: 0,
	})
	if !errors.Is(err, ErrVisibilityQueueRetryPolicyInvalid) {
		t.Fatalf("invalid retry policy error = %v, want ErrVisibilityQueueRetryPolicyInvalid", err)
	}
}
func TestT248VisibilityQueueNackTokenRoutesExhaustedRetry(t *testing.T) {
	queue, err := NewVisibilityQueueWithRetryPolicy[int](
		4,
		time.Minute,
		9,
		VisibilityQueueRetryOptions{MaxAttempts: 1, MaxDeadLetters: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(40, 0)
	if !queue.Enqueue(23) {
		t.Fatal("enqueue failed")
	}
	item, ok := queue.LeaseWithToken(now)
	if !ok {
		t.Fatal("lease failed")
	}
	if !queue.NackToken(item.Token, now) {
		t.Fatal("terminal token nack failed")
	}
	deadLetter, ok := queue.PopDeadLetter()
	if !ok {
		t.Fatal("dead letter missing")
	}
	if deadLetter.ID != item.Token.ID || deadLetter.Value != item.Value || deadLetter.Attempts != item.Attempts {
		t.Fatalf("dead letter = %#v, want id=%d value=%d attempts=%d", deadLetter, item.Token.ID, item.Value, item.Attempts)
	}
}
