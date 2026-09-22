package hatReplication

import (
	"errors"
	"testing"
	"time"
)

func TestSinkRetryBackpressureIsDisabledByDefault(t *testing.T) {
	queue, err := NewSinkRetryQueue(SinkRetryOptions{Source: "orders", MaxPending: 2, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(600, 0)
	for index := uint64(1); index <= 2; index++ {
		if _, err := queue.Enqueue(retryTestRecord(index, "order-"+string(rune('0'+index)), "value"), now); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := queue.Enqueue(retryTestRecord(3, "order-3", "value"), now); !errors.Is(err, ErrSinkRetryQueueFull) {
		t.Fatalf("default-off overflow error = %v, want queue full", err)
	}
	if stats := queue.Stats(); stats.Backpressured || stats.BackpressureEvents != 0 {
		t.Fatalf("default-off stats = %+v", stats)
	}
}

func TestSinkRetryBackpressureUsesHighAndLowWatermarks(t *testing.T) {
	queue, err := NewSinkRetryQueue(SinkRetryOptions{
		Source:     "orders",
		MaxPending: 4,
		MaxBytes:   1 << 20,
		Backpressure: SinkRetryBackpressureOptions{
			Enabled:     true,
			HighPending: 2,
			LowPending:  1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(610, 0)
	first := retryTestRecord(1, "order-1", "one")
	second := retryTestRecord(2, "order-2", "two")
	third := retryTestRecord(3, "order-3", "three")
	if _, err := queue.Enqueue(first, now); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(second, now); err != nil {
		t.Fatal(err)
	}
	if stats := queue.Stats(); !stats.Backpressured || stats.BackpressureEvents != 0 {
		t.Fatalf("high-watermark stats = %+v", stats)
	}
	if _, err := queue.Enqueue(third, now); !errors.Is(err, ErrSinkRetryBackpressure) {
		t.Fatalf("throttled enqueue error = %v", err)
	}
	if stats := queue.Stats(); stats.BackpressureEvents != 1 {
		t.Fatalf("backpressure event stats = %+v", stats)
	}

	duplicate, err := queue.Enqueue(second, now.Add(time.Second))
	if err != nil || duplicate.Action != SinkRetryDuplicate {
		t.Fatalf("duplicate while throttled = %+v err=%v", duplicate, err)
	}
	replacement, err := queue.Enqueue(retryTestRecord(4, "order-2", "newer"), now.Add(time.Second))
	if err != nil || replacement.Action != SinkRetryReplaced {
		t.Fatalf("replacement while throttled = %+v err=%v", replacement, err)
	}

	delivery, ok, err := queue.Next(now.Add(time.Second))
	if err != nil || !ok {
		t.Fatalf("first delivery = %+v ok=%v err=%v", delivery, ok, err)
	}
	if err := queue.Ack(delivery.Record.OutputID, delivery.Record.Sequence); err != nil {
		t.Fatal(err)
	}
	if stats := queue.Stats(); stats.Backpressured {
		t.Fatalf("low-watermark release stats = %+v", stats)
	}
	if _, err := queue.Enqueue(third, now.Add(2*time.Second)); err != nil {
		t.Fatalf("enqueue after release = %v", err)
	}
}

func TestSinkRetryBackpressureUsesByteWatermarkAndRestoresState(t *testing.T) {
	now := time.Unix(620, 0)
	first := retryTestRecord(1, "order-1", "one")
	firstSize := sinkRetryRecordSize(first)
	queue, err := NewSinkRetryQueue(SinkRetryOptions{
		Source:     "orders",
		MaxPending: 4,
		MaxBytes:   firstSize * 4,
		Backpressure: SinkRetryBackpressureOptions{
			Enabled:   true,
			HighBytes: firstSize,
			LowBytes:  firstSize / 2,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(first, now); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(retryTestRecord(2, "order-2", "two"), now); !errors.Is(err, ErrSinkRetryBackpressure) {
		t.Fatalf("byte-throttled enqueue error = %v", err)
	}

	encoded, err := queue.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := NewSinkRetryQueueFromBinary(queue.options, encoded)
	if err != nil {
		t.Fatal(err)
	}
	if stats := restored.Stats(); !stats.Backpressured {
		t.Fatalf("restored byte-throttled stats = %+v", stats)
	}
	delivery, ok, err := restored.Next(now)
	if err != nil || !ok {
		t.Fatalf("restored delivery = %+v ok=%v err=%v", delivery, ok, err)
	}
	if err := restored.Ack(delivery.Record.OutputID, delivery.Record.Sequence); err != nil {
		t.Fatal(err)
	}
	if stats := restored.Stats(); stats.Backpressured {
		t.Fatalf("restored release stats = %+v", stats)
	}
}
