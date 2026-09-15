package hatDataStructure

import (
	"testing"
	"time"
)

func TestDelayQueuePopReadyPreservesOrderingAndReferenceClearing(t *testing.T) {
	now := time.Unix(900, 0).UTC()
	queue := NewDelayQueue[string](3)
	queue.Push(now.Add(-time.Second), "ready")
	queue.Push(now.Add(time.Second), "future")
	queue.Push(now, "now")

	if got, ok := queue.PopReady(now); !ok || got != "ready" {
		t.Fatalf("first PopReady() = %q, %t; want ready, true", got, ok)
	}
	if got, ok := queue.Peek(); !ok || got.Value != "now" {
		t.Fatalf("next Peek() = %#v, %t; want now, true", got, ok)
	}
	if got, ok := queue.PopReady(now); !ok || got != "now" {
		t.Fatalf("second PopReady() = %q, %t; want now, true", got, ok)
	}
	if got, ok := queue.PopReady(now); ok || got != "" {
		t.Fatalf("future PopReady() = %q, %t; want empty, false", got, ok)
	}
	if got, ok := queue.Pop(); !ok || got.Value != "future" {
		t.Fatalf("future Pop() = %#v, %t; want future, true", got, ok)
	}
	if queue.Len() != 0 {
		t.Fatalf("queue length = %d, want 0", queue.Len())
	}
}

var delayQueuePopReadyFastPathSink string

func BenchmarkDelayQueuePopReadyC215(b *testing.B) {
	now := time.Unix(901, 0).UTC()
	queue := NewDelayQueue[string](1)
	queue.Push(now, "value")
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		value, ok := queue.PopReady(now)
		if !ok {
			b.Fatal("PopReady returned false")
		}
		delayQueuePopReadyFastPathSink = value
		queue.items = append(queue.items, DelayQueueItem[string]{ReadyAt: now, Value: "value"})
	}
}
