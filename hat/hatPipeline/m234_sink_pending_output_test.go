package hatPipeline

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestM234PendingOutputQueueBoundsFIFOAndAcknowledgement(t *testing.T) {
	queue, err := NewSinkPendingOutputQueue(SinkPendingOutputQueueOptions{
		MaxItems: 2,
		MaxBytes: 32,
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := queue.TryEnqueue(SinkPendingOutput{Frontier: 1, IdempotencyKey: "one", Payload: []byte("alpha")})
	if err != nil {
		t.Fatal(err)
	}
	second, err := queue.TryEnqueue(SinkPendingOutput{Frontier: 2, IdempotencyKey: "two", Payload: []byte("beta")})
	if err != nil {
		t.Fatal(err)
	}
	if first == 0 || second != first+1 {
		t.Fatalf("sequences = %d, %d", first, second)
	}
	if _, err := queue.TryEnqueue(SinkPendingOutput{Frontier: 3, IdempotencyKey: "three", Payload: []byte("gamma")}); !errors.Is(err, ErrSinkPendingOutputFull) {
		t.Fatalf("full enqueue error = %v", err)
	}
	status := queue.Stats()
	if status.Items != 2 || status.Bytes != 15 {
		t.Fatalf("status = %#v, want two entries and 15 bytes", status)
	}
	peek, found, err := queue.Peek(context.Background())
	if err != nil || !found {
		t.Fatalf("Peek() = %#v, %v", peek, err)
	}
	if peek.Sequence != first || peek.Frontier != 1 || peek.IdempotencyKey != "one" || string(peek.Payload) != "alpha" {
		t.Fatalf("first peek = %#v", peek)
	}
	if err := queue.Acknowledge(second); !errors.Is(err, ErrSinkPendingOutputNotHead) {
		t.Fatalf("out-of-order acknowledgement error = %v", err)
	}
	if err := queue.Acknowledge(first); err != nil {
		t.Fatal(err)
	}
	third, err := queue.TryEnqueue(SinkPendingOutput{Frontier: 3, IdempotencyKey: "three", Payload: []byte("gamma")})
	if err != nil {
		t.Fatal(err)
	}
	if third != second+1 {
		t.Fatalf("third sequence = %d, want %d", third, second+1)
	}
	got := make([]SinkPendingOutput, 0, 2)
	for range 2 {
		value, found, err := queue.Peek(context.Background())
		if err != nil || !found {
			t.Fatalf("Peek() = %#v, %v", value, err)
		}
		got = append(got, value)
		if err := queue.Acknowledge(value.Sequence); err != nil {
			t.Fatal(err)
		}
	}
	if got[0].Frontier != 2 || got[1].Frontier != 3 {
		t.Fatalf("remaining FIFO values = %#v", got)
	}
}

func TestM234PendingOutputQueueBlocksAndWakesOnAcknowledgement(t *testing.T) {
	queue, err := NewSinkPendingOutputQueue(SinkPendingOutputQueueOptions{MaxItems: 1, MaxBytes: 32})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queue.TryEnqueue(SinkPendingOutput{Payload: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := queue.Enqueue(context.Background(), SinkPendingOutput{Payload: []byte("second")})
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("blocked enqueue completed early: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	first, found, err := queue.Peek(context.Background())
	if err != nil || !found {
		t.Fatalf("Peek() = %#v, %v", first, err)
	}
	if err := queue.Acknowledge(first.Sequence); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("woken enqueue error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("enqueue did not wake after acknowledgement")
	}
}

func TestM234PendingOutputQueueCancellationAndClose(t *testing.T) {
	queue, err := NewSinkPendingOutputQueue(SinkPendingOutputQueueOptions{MaxItems: 1, MaxBytes: 32})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queue.TryEnqueue(SinkPendingOutput{Payload: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := queue.Enqueue(ctx, SinkPendingOutput{Payload: []byte("second")})
		done <- err
	}()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled enqueue error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled enqueue did not return")
	}
	if err := queue.Close(); err != nil {
		t.Fatal(err)
	}
	if err := queue.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.TryEnqueue(SinkPendingOutput{Payload: []byte("closed")}); !errors.Is(err, ErrSinkPendingOutputClosed) {
		t.Fatalf("closed enqueue error = %v", err)
	}
	first, found, err := queue.Peek(context.Background())
	if err != nil || !found {
		t.Fatalf("pending peek after close = %#v, %v", first, err)
	}
	if err := queue.Acknowledge(first.Sequence); err != nil {
		t.Fatal(err)
	}
	if _, found, err := queue.Peek(context.Background()); !errors.Is(err, ErrSinkPendingOutputClosed) || found {
		t.Fatalf("empty closed peek = found %v, error %v", found, err)
	}
}

func TestM234PendingOutputQueueCopiesMutableInputAndPeek(t *testing.T) {
	queue, err := NewSinkPendingOutputQueue(SinkPendingOutputQueueOptions{MaxItems: 2, MaxBytes: 32})
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("value")
	sequence, err := queue.TryEnqueue(SinkPendingOutput{IdempotencyKey: "  key ", Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'
	first, found, err := queue.Peek(context.Background())
	if err != nil || !found {
		t.Fatalf("Peek() = %#v, %v", first, err)
	}
	if first.Sequence != sequence || first.IdempotencyKey != "key" || string(first.Payload) != "value" {
		t.Fatalf("copied first value = %#v", first)
	}
	first.Payload[0] = 'Y'
	second, found, err := queue.Peek(context.Background())
	if err != nil || !found || string(second.Payload) != "value" {
		t.Fatalf("peek exposed queue payload = %#v, %v", second, err)
	}
	if !reflect.DeepEqual(first.Payload, []byte("Yalue")) {
		t.Fatalf("returned payload mutation = %q", first.Payload)
	}
}

func TestM234PendingOutputQueueRejectsInvalidOptionsAndOversize(t *testing.T) {
	for _, options := range []SinkPendingOutputQueueOptions{
		{MaxItems: -1},
		{MaxItems: MaxSinkPendingOutputItems + 1},
		{MaxBytes: -1},
		{MaxBytes: MaxSinkPendingOutputBytes + 1},
	} {
		if _, err := NewSinkPendingOutputQueue(options); !errors.Is(err, ErrSinkPendingOutputOptionsInvalid) {
			t.Fatalf("options %#v error = %v", options, err)
		}
	}
	queue, err := NewSinkPendingOutputQueue(SinkPendingOutputQueueOptions{MaxItems: 2, MaxBytes: 4})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queue.TryEnqueue(SinkPendingOutput{Payload: []byte("12345")}); !errors.Is(err, ErrSinkPendingOutputTooLarge) {
		t.Fatalf("oversize payload error = %v", err)
	}
	if _, err := queue.TryEnqueue(SinkPendingOutput{IdempotencyKey: "12345"}); !errors.Is(err, ErrSinkPendingOutputTooLarge) {
		t.Fatalf("oversize key error = %v", err)
	}
	if _, err := queue.TryEnqueue(SinkPendingOutput{Sequence: 1, Payload: []byte("ok")}); !errors.Is(err, ErrSinkPendingOutputInvalid) {
		t.Fatalf("caller sequence error = %v", err)
	}
}
