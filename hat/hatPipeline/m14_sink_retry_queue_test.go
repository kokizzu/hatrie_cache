package hatPipeline

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestM14SinkRetryQueuePerSinkRetryAndDeadLetter(t *testing.T) {
	queue, err := NewSinkRetryQueue[string](SinkRetryQueueOptions{
		Capacity:          2,
		VisibilityTimeout: time.Minute,
		MaxAttempts:       2,
		DeadLetterLimit:   4,
		Epoch:             7,
	})
	if err != nil {
		t.Fatalf("NewSinkRetryQueue() error = %v", err)
	}
	now := time.Unix(100, 0)
	if !queue.EnqueueAt("orders", now, "order-1") || !queue.EnqueueAt("payments", now, "payment-1") {
		t.Fatal("EnqueueAt() rejected an item below the per-sink capacity")
	}
	if got := queue.Stats(); got.Pending != 2 || got.SinkCount != 2 {
		t.Fatalf("initial stats = %+v", got)
	}

	lease, ok := queue.Lease("orders", now)
	if !ok {
		t.Fatal("Lease() did not return the ready order")
	}
	if lease.Sink != "orders" || lease.Value != "order-1" || lease.Attempts != 1 || lease.Token.Epoch != 7 {
		t.Fatalf("first lease = %+v", lease)
	}
	crossSink := lease
	crossSink.Sink = "payments"
	crossSink.Token.Sink = "payments"
	if queue.Ack(crossSink) {
		t.Fatal("Ack() accepted a lease retargeted to another sink")
	}
	outcome, deadLetterID, valid := queue.Retry(lease, now.Add(time.Second), "temporary failure")
	if !valid || outcome != SinkRetryOutcomeRequeued || deadLetterID != 0 {
		t.Fatalf("first Retry() = outcome %v, dead-letter %d, valid %v", outcome, deadLetterID, valid)
	}

	lease, ok = queue.Lease("orders", now.Add(time.Second))
	if !ok || lease.Attempts != 2 {
		t.Fatalf("second lease = %+v, ok %v", lease, ok)
	}
	lease.Value = "caller mutation must not change the retained payload"
	outcome, deadLetterID, valid = queue.Retry(lease, now.Add(2*time.Second), "permanent failure")
	if !valid || outcome != SinkRetryOutcomeDeadLettered || deadLetterID == 0 {
		t.Fatalf("terminal Retry() = outcome %v, dead-letter %d, valid %v", outcome, deadLetterID, valid)
	}
	deadLetters := queue.DeadLetters()
	if len(deadLetters) != 1 {
		t.Fatalf("dead letters = %+v", deadLetters)
	}
	deadLetter := deadLetters[0]
	if deadLetter.ID != deadLetterID || deadLetter.Sink != "orders" || deadLetter.Value != "order-1" || deadLetter.Attempts != 2 || deadLetter.Reason != "permanent failure" {
		t.Fatalf("dead letter = %+v", deadLetter)
	}

	if !queue.ReplayDeadLetter(deadLetterID, now) {
		t.Fatal("ReplayDeadLetter() rejected a retained failure")
	}
	if queue.ReplayDeadLetter(deadLetterID, now) {
		t.Fatal("ReplayDeadLetter() replayed the same failure twice")
	}
	lease, ok = queue.Lease("orders", now)
	if !ok || lease.Value != "order-1" || lease.Attempts != 1 {
		t.Fatalf("replayed lease = %+v, ok %v", lease, ok)
	}
	if !queue.Ack(lease) || queue.Ack(lease) {
		t.Fatal("Ack() did not make the lease idempotently one-shot")
	}
	if got := queue.Stats(); got.Pending != 1 || got.Leased != 0 || got.DeadLetters != 0 {
		t.Fatalf("final stats = %+v", got)
	}
}

func TestM14SinkRetryQueueBoundsAndValidation(t *testing.T) {
	if _, err := NewSinkRetryQueue[string](SinkRetryQueueOptions{Capacity: -1}); !errors.Is(err, ErrSinkRetryQueueInvalidCapacity) {
		t.Fatalf("invalid capacity error = %v", err)
	}
	if _, err := NewSinkRetryQueue[string](SinkRetryQueueOptions{DeadLetterLimit: -2}); !errors.Is(err, ErrSinkRetryQueueInvalidDeadLetterLimit) {
		t.Fatalf("invalid dead-letter limit error = %v", err)
	}
	queue, err := NewSinkRetryQueue[int](SinkRetryQueueOptions{Capacity: 1, DeadLetterLimit: 2})
	if err != nil {
		t.Fatalf("NewSinkRetryQueue() error = %v", err)
	}
	now := time.Unix(200, 0)
	if queue.Enqueue("", 1) || queue.Enqueue("   ", 2) {
		t.Fatal("Enqueue() accepted an invalid sink")
	}
	if !queue.EnqueueAt("cache", now, 1) || queue.EnqueueAt("cache", now, 2) {
		t.Fatal("EnqueueAt() did not enforce the per-sink capacity")
	}
	lease, ok := queue.Lease("cache", now)
	if !ok {
		t.Fatal("Lease() did not return the item")
	}
	if queue.Enqueue("cache", 2) {
		t.Fatal("Enqueue() ignored leased items when enforcing capacity")
	}
	if !queue.Ack(lease) || !queue.Enqueue("cache", 2) {
		t.Fatal("capacity was not released after Ack()")
	}
}

func TestM14SinkRetryQueueExpiryAndEpochFence(t *testing.T) {
	queue, err := NewSinkRetryQueue[string](SinkRetryQueueOptions{
		Capacity:          2,
		VisibilityTimeout: time.Second,
		MaxAttempts:       3,
		DeadLetterLimit:   2,
		Epoch:             42,
	})
	if err != nil {
		t.Fatalf("NewSinkRetryQueue() error = %v", err)
	}
	now := time.Unix(300, 0)
	if !queue.EnqueueAt("cache", now, "value") {
		t.Fatal("EnqueueAt() rejected the item")
	}
	lease, ok := queue.Lease("cache", now)
	if !ok {
		t.Fatal("Lease() did not return the item")
	}
	stale := lease
	stale.Token.Epoch--
	if queue.Ack(stale) {
		t.Fatal("Ack() accepted a stale epoch")
	}
	if recovered := queue.RequeueExpired(now.Add(time.Second)); recovered != 1 {
		t.Fatalf("RequeueExpired() = %d, want 1", recovered)
	}
	lease, ok = queue.Lease("cache", now.Add(time.Second))
	if !ok || lease.Attempts != 2 {
		t.Fatalf("recovered lease = %+v, ok %v", lease, ok)
	}
	if !queue.Ack(lease) {
		t.Fatal("Ack() rejected the recovered lease")
	}
}

func TestM14SinkRetryQueueMovesExpiredPoisonToDeadLetter(t *testing.T) {
	queue, err := NewSinkRetryQueue[string](SinkRetryQueueOptions{
		VisibilityTimeout: time.Second,
		MaxAttempts:       1,
		DeadLetterLimit:   2,
	})
	if err != nil {
		t.Fatalf("NewSinkRetryQueue() error = %v", err)
	}
	now := time.Unix(400, 0)
	if !queue.EnqueueAt("sink", now, "poison") {
		t.Fatal("EnqueueAt() rejected the poison item")
	}
	lease, ok := queue.Lease("sink", now)
	if !ok || lease.Attempts != 1 {
		t.Fatalf("first poison lease = %+v, ok %v", lease, ok)
	}
	if got := queue.RequeueExpired(now.Add(time.Second)); got != 1 {
		t.Fatalf("RequeueExpired() = %d, want 1", got)
	}
	if _, ok := queue.Lease("sink", now.Add(time.Second)); ok {
		t.Fatal("expired poison item was leased beyond MaxAttempts")
	}
	deadLetters := queue.DeadLetters()
	if len(deadLetters) != 1 || deadLetters[0].Attempts != 2 || deadLetters[0].Reason != "maximum delivery attempts exceeded" {
		t.Fatalf("expired poison dead letters = %+v", deadLetters)
	}
}

func TestM14SinkRetryQueueConcurrentAccess(t *testing.T) {
	queue, err := NewSinkRetryQueue[int](SinkRetryQueueOptions{Capacity: 64, DeadLetterLimit: 8})
	if err != nil {
		t.Fatalf("NewSinkRetryQueue() error = %v", err)
	}
	const workers = 8
	const perWorker = 16
	var wait sync.WaitGroup
	failures := make(chan string, workers*2)
	now := time.Unix(500, 0)
	for worker := range workers {
		worker := worker
		wait.Add(1)
		go func() {
			defer wait.Done()
			sink := fmt.Sprintf("sink-%d", worker)
			for item := range perWorker {
				if !queue.EnqueueAt(sink, now, worker*perWorker+item) {
					failures <- "enqueue"
				}
			}
			for range perWorker {
				lease, ok := queue.Lease(sink, now)
				if !ok {
					failures <- "lease"
					continue
				}
				if !queue.Ack(lease) {
					failures <- "ack"
				}
			}
		}()
	}
	wait.Wait()
	close(failures)
	for failure := range failures {
		t.Fatalf("concurrent operation failed: %s", failure)
	}
	if got := queue.Stats(); got.Pending != 0 || got.Leased != 0 || got.DeadLetters != 0 {
		t.Fatalf("concurrent final stats = %+v", got)
	}
}
