package hatDataStructure

import (
	"testing"
	"time"
)

func TestVisibilityQueueEpochTokenFencesRestoredQueue(t *testing.T) {
	now := time.Unix(500, 0).UTC()
	oldQueue := NewVisibilityQueueWithEpoch[string](1, time.Minute, 41)
	if !oldQueue.Enqueue("old") {
		t.Fatal("old queue Enqueue returned false")
	}
	oldLease, ok := oldQueue.LeaseWithToken(now)
	if !ok {
		t.Fatal("old queue Lease returned no item")
	}

	restoredQueue := NewVisibilityQueueWithEpoch[string](1, time.Minute, 42)
	if restoredQueue.Epoch() != 42 {
		t.Fatalf("restored queue epoch = %d, want 42", restoredQueue.Epoch())
	}
	if !restoredQueue.Enqueue("new") {
		t.Fatal("restored queue Enqueue returned false")
	}
	newLease, ok := restoredQueue.LeaseWithToken(now)
	if !ok || newLease.Token.ID != oldLease.Token.ID {
		t.Fatalf("new lease = %+v, ok %v; want reused sequence ID for epoch-fence test", newLease, ok)
	}
	if restoredQueue.AckToken(oldLease.Token) {
		t.Fatal("AckToken accepted a token from a previous queue epoch")
	}
	if restoredQueue.NackToken(oldLease.Token, now) {
		t.Fatal("NackToken accepted a token from a previous queue epoch")
	}
	if restoredQueue.LeaseLen() != 1 {
		t.Fatalf("lease count after stale tokens = %d, want 1", restoredQueue.LeaseLen())
	}
	if !restoredQueue.AckToken(newLease.Token) {
		t.Fatal("AckToken rejected the current queue epoch")
	}
}

func TestVisibilityQueueEpochTokenSupportsRetryAndLegacyIDs(t *testing.T) {
	now := time.Unix(600, 0).UTC()
	queue := NewVisibilityQueueWithEpoch[int](2, time.Minute, 9)
	if !queue.Enqueue(7) {
		t.Fatal("Enqueue returned false")
	}
	lease, ok := queue.LeaseWithToken(now)
	if !ok || lease.Token.Epoch != 9 || lease.Token.ID == 0 {
		t.Fatalf("leased item = %+v; want epoch-bearing token", lease)
	}
	if !queue.NackToken(lease.Token, now) {
		t.Fatal("NackToken rejected the current token")
	}
	retry, ok := queue.LeaseWithToken(now)
	if !ok || retry.Token.ID != lease.Token.ID || retry.Attempts != 2 {
		t.Fatalf("retry = %+v, ok %v; want same ID and incremented attempts", retry, ok)
	}
	if !queue.AckToken(retry.Token) {
		t.Fatal("AckToken rejected a current token")
	}
}

func TestVisibilityQueueEpochZeroUsesDefaultEpoch(t *testing.T) {
	queue := NewVisibilityQueueWithEpoch[int](0, time.Second, 0)
	if queue.Epoch() != 1 {
		t.Fatalf("zero epoch = %d, want default epoch 1", queue.Epoch())
	}
}
