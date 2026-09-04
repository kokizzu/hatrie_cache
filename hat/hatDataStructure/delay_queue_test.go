package hatDataStructure

import (
	"testing"
	"time"
)

func TestDelayQueueReturnsReadyItemsInDeadlineAndInsertionOrder(t *testing.T) {
	now := time.Unix(100, 0)
	var queue DelayQueue[string]
	queue.Push(now.Add(2*time.Second), "later")
	queue.Push(now.Add(time.Second), "first")
	queue.Push(now.Add(time.Second), "second")

	if _, ok := queue.PopReady(now); ok {
		t.Fatal("PopReady returned an item before its deadline")
	}
	if got, ok := queue.PopReady(now.Add(time.Second)); !ok || got != "first" {
		t.Fatalf("first PopReady = %#v/%v, want first/true", got, ok)
	}
	if got, ok := queue.PopReady(now.Add(time.Second)); !ok || got != "second" {
		t.Fatalf("second PopReady = %#v/%v, want second/true", got, ok)
	}
	if got, ok := queue.PopReady(now.Add(2 * time.Second)); !ok || got != "later" {
		t.Fatalf("third PopReady = %#v/%v, want later/true", got, ok)
	}
	if queue.Len() != 0 {
		t.Fatalf("queue length = %d, want 0", queue.Len())
	}
}

func TestDelayQueuePeekNextReadyAtAndPushAfter(t *testing.T) {
	now := time.Unix(200, 0)
	var queue DelayQueue[int]
	queue.PushAfter(now, 3*time.Second, 7)

	item, ok := queue.Peek()
	if !ok || item.Value != 7 || !item.ReadyAt.Equal(now.Add(3*time.Second)) {
		t.Fatalf("Peek = %#v/%v, want value 7 at deadline", item, ok)
	}
	deadline, ok := queue.NextReadyAt()
	if !ok || !deadline.Equal(now.Add(3*time.Second)) {
		t.Fatalf("NextReadyAt = %v/%v, want deadline/true", deadline, ok)
	}
	if got, ok := queue.Pop(); !ok || got.Value != 7 {
		t.Fatalf("Pop = %#v/%v, want value 7/true", got, ok)
	}
	if _, ok := queue.NextReadyAt(); ok {
		t.Fatal("NextReadyAt returned a deadline for an empty queue")
	}
}

func TestDelayQueueZeroValueAndClear(t *testing.T) {
	var queue DelayQueue[int]
	if queue.Len() != 0 {
		t.Fatalf("zero-value length = %d, want 0", queue.Len())
	}
	if _, ok := queue.Peek(); ok {
		t.Fatal("zero-value Peek returned an item")
	}
	queue.Push(time.Unix(300, 0), 1)
	queue.Push(time.Unix(301, 0), 2)
	queue.Clear()
	if queue.Len() != 0 {
		t.Fatalf("cleared length = %d, want 0", queue.Len())
	}
}

func TestDelayQueuePopReadyHandlesPastDeadlinesAndEqualTimes(t *testing.T) {
	now := time.Unix(400, 0)
	var queue DelayQueue[string]
	queue.Push(now.Add(-time.Second), "past")
	queue.Push(now, "now")

	if got, ok := queue.PopReady(now); !ok || got != "past" {
		t.Fatalf("past PopReady = %#v/%v, want past/true", got, ok)
	}
	if got, ok := queue.PopReady(now); !ok || got != "now" {
		t.Fatalf("now PopReady = %#v/%v, want now/true", got, ok)
	}
}
