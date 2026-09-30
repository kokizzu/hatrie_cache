package hatPipeline

import (
	"runtime"
	"testing"
)

const m234BenchmarkBatch = 256

func BenchmarkM234ExistingUnboundedPendingAppendAck(b *testing.B) {
	payload := []byte("payload-0123456789")
	key := "event-key"
	pending := make([]SinkPendingOutput, 0, m234BenchmarkBatch)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		pending = pending[:0]
		for index := 0; index < m234BenchmarkBatch; index++ {
			pending = append(pending, SinkPendingOutput{
				Frontier:       uint64(index),
				IdempotencyKey: string(append([]byte(nil), key...)),
				Payload:        append([]byte(nil), payload...),
			})
		}
		runtime.KeepAlive(pending)
	}
}

func BenchmarkM234BoundedQueueTryEnqueueAck(b *testing.B) {
	payload := []byte("payload-0123456789")
	key := "event-key"
	queue, err := NewSinkPendingOutputQueue(SinkPendingOutputQueueOptions{
		MaxItems: m234BenchmarkBatch,
		MaxBytes: m234BenchmarkBatch * 64,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		for index := 0; index < m234BenchmarkBatch; index++ {
			sequence, err := queue.TryEnqueue(SinkPendingOutput{
				Frontier:       uint64(index),
				IdempotencyKey: key,
				Payload:        payload,
			})
			if err != nil {
				b.Fatal(err)
			}
			if err := queue.Acknowledge(sequence); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkM234ExistingUnboundedRetained256(b *testing.B) {
	payload := []byte("payload-0123456789")
	key := "event-key"
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		pending := make([]SinkPendingOutput, 0, m234BenchmarkBatch)
		for index := 0; index < m234BenchmarkBatch; index++ {
			pending = append(pending, SinkPendingOutput{
				Frontier:       uint64(index),
				IdempotencyKey: string(append([]byte(nil), key...)),
				Payload:        append([]byte(nil), payload...),
			})
		}
		runtime.KeepAlive(pending)
	}
}

func BenchmarkM234BoundedQueueRetained256(b *testing.B) {
	payload := []byte("payload-0123456789")
	key := "event-key"
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		queue, err := NewSinkPendingOutputQueue(SinkPendingOutputQueueOptions{
			MaxItems: m234BenchmarkBatch,
			MaxBytes: m234BenchmarkBatch * 64,
		})
		if err != nil {
			b.Fatal(err)
		}
		for index := 0; index < m234BenchmarkBatch; index++ {
			if _, err := queue.TryEnqueue(SinkPendingOutput{
				Frontier:       uint64(index),
				IdempotencyKey: key,
				Payload:        payload,
			}); err != nil {
				b.Fatal(err)
			}
		}
		runtime.KeepAlive(queue)
	}
}
