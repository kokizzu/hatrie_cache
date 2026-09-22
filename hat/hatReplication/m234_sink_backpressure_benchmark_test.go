package hatReplication

import (
	"errors"
	"testing"
	"time"
)

func BenchmarkSinkRetryBackpressureEnqueueOff(b *testing.B) {
	queue, err := NewSinkRetryQueue(SinkRetryOptions{Source: "orders", MaxPending: 1024, MaxBytes: 1 << 20})
	if err != nil {
		b.Fatal(err)
	}
	record := retryTestRecord(1, "order-1", "value")
	now := time.Unix(700, 0)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := queue.Enqueue(record, now); err != nil {
			b.Fatal(err)
		}
		delivery, ok, err := queue.Next(now)
		if err != nil || !ok {
			b.Fatalf("next: delivery=%+v ok=%v err=%v", delivery, ok, err)
		}
		if err := queue.Ack(record.OutputID, record.Sequence); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSinkRetryBackpressureEnqueueOnHealthy(b *testing.B) {
	queue, err := NewSinkRetryQueue(SinkRetryOptions{
		Source:     "orders",
		MaxPending: 1024,
		MaxBytes:   1 << 20,
		Backpressure: SinkRetryBackpressureOptions{
			Enabled:     true,
			HighPending: 900,
			LowPending:  450,
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	record := retryTestRecord(1, "order-1", "value")
	now := time.Unix(700, 0)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := queue.Enqueue(record, now); err != nil {
			b.Fatal(err)
		}
		delivery, ok, err := queue.Next(now)
		if err != nil || !ok {
			b.Fatalf("next: delivery=%+v ok=%v err=%v", delivery, ok, err)
		}
		if err := queue.Ack(record.OutputID, record.Sequence); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSinkRetryBackpressureReject(b *testing.B) {
	queue, err := NewSinkRetryQueue(SinkRetryOptions{
		Source:     "orders",
		MaxPending: 1024,
		MaxBytes:   1 << 20,
		Backpressure: SinkRetryBackpressureOptions{
			Enabled:     true,
			HighPending: 1,
			LowPending:  0,
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	now := time.Unix(700, 0)
	if _, err := queue.Enqueue(retryTestRecord(1, "order-1", "value"), now); err != nil {
		b.Fatal(err)
	}
	record := retryTestRecord(2, "order-2", "value")
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := queue.Enqueue(record, now); !errors.Is(err, ErrSinkRetryBackpressure) {
			b.Fatalf("enqueue error = %v", err)
		}
	}
}
