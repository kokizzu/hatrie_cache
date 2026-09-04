package hatDataStructure

import (
	"testing"
	"time"
)

func TestDeadLetterQueueRetainsAndReplaysByID(t *testing.T) {
	now := time.Unix(1000, 0)
	queue := NewDeadLetterQueue[string](4, 4)
	queue.EnqueueAt(now, "job")

	item, ok := queue.PopReady(now)
	if !ok || item.Value != "job" {
		t.Fatalf("PopReady() = %#v/%v, want job/true", item, ok)
	}
	id := queue.FailAt(item, now.Add(time.Second), 3, "target unavailable")
	if id == 0 {
		t.Fatal("FailAt() returned zero ID")
	}
	dead, ok := queue.DeadLetter(id)
	if !ok || dead.Value != "job" || dead.Attempts != 3 || dead.Reason != "target unavailable" {
		t.Fatalf("DeadLetter() = %#v/%v, want retained failure", dead, ok)
	}

	if !queue.ReplayAt(id, now.Add(5*time.Second)) {
		t.Fatal("ReplayAt() = false, want true")
	}
	if queue.DeadLetterLen() != 0 {
		t.Fatalf("dead-letter length after replay = %d, want 0", queue.DeadLetterLen())
	}
	item, ok = queue.PopReady(now.Add(5 * time.Second))
	if !ok || item.Value != "job" || !item.ReadyAt.Equal(now.Add(5*time.Second)) {
		t.Fatalf("replayed PopReady() = %#v/%v, want job at replay deadline", item, ok)
	}
}

func TestDeadLetterQueueBoundsOldestFailuresAndDiscard(t *testing.T) {
	now := time.Unix(2000, 0)
	queue := NewDeadLetterQueue[string](0, 2)
	for index := 0; index < 3; index++ {
		item := DelayQueueItem[string]{ReadyAt: now, Value: string(rune('a' + index))}
		queue.FailAt(item, now.Add(time.Duration(index)*time.Second), 1, "failed")
	}
	if queue.DeadLetterLen() != 2 {
		t.Fatalf("dead-letter length = %d, want 2", queue.DeadLetterLen())
	}
	if _, ok := queue.DeadLetter(1); ok {
		t.Fatal("oldest bounded dead letter was retained")
	}
	if dead, ok := queue.DeadLetter(3); !ok || dead.Value != "c" {
		t.Fatalf("latest dead letter = %#v/%v, want c/true", dead, ok)
	}
	if !queue.Discard(3) {
		t.Fatal("Discard() = false, want true")
	}
	if queue.Discard(3) {
		t.Fatal("second Discard() = true, want false")
	}
}

func TestDeadLetterQueueZeroValueAndClear(t *testing.T) {
	var queue DeadLetterQueue[int]
	now := time.Unix(3000, 0)
	queue.SetDeadLetterLimit(2)
	queue.EnqueueAt(now, 1)
	item, ok := queue.PopReady(now)
	if !ok {
		t.Fatal("zero-value PopReady() returned false")
	}
	queue.FailAt(item, now, 1, "retry exhausted")
	if queue.DeadLetterLen() != 1 {
		t.Fatalf("zero-value dead-letter length = %d, want 1", queue.DeadLetterLen())
	}
	queue.Clear()
	if queue.Len() != 0 || queue.DeadLetterLen() != 0 {
		t.Fatalf("Clear() lengths = %d/%d, want 0/0", queue.Len(), queue.DeadLetterLen())
	}
}
