package hatFiber

import (
	"context"
	"errors"
	"testing"
)

func TestT236MailboxPreservesFIFOAndBatches(t *testing.T) {
	mailbox, err := NewMailbox[int](4)
	if err != nil {
		t.Fatalf("NewMailbox() error = %v", err)
	}
	for value := 1; value <= 4; value++ {
		if err := mailbox.TrySend(value); err != nil {
			t.Fatalf("TrySend(%d) error = %v", value, err)
		}
	}
	if err := mailbox.TrySend(5); !errors.Is(err, ErrMailboxFull) {
		t.Fatalf("TrySend(full) error = %v, want ErrMailboxFull", err)
	}
	values := make([]int, 3)
	count, err := mailbox.ReceiveBatch(values)
	if err != nil || count != 3 {
		t.Fatalf("ReceiveBatch() = %d, error = %v, want three values", count, err)
	}
	if want := []int{1, 2, 3}; !equalInts(values, want) {
		t.Fatalf("ReceiveBatch() values = %v, want %v", values, want)
	}
	if err := mailbox.TrySend(5); err != nil {
		t.Fatalf("TrySend(after drain) error = %v", err)
	}
	for _, want := range []int{4, 5} {
		value, err := mailbox.TryReceive()
		if err != nil || value != want {
			t.Fatalf("TryReceive() = %d, error = %v, want %d", value, err, want)
		}
	}
	if _, err := mailbox.TryReceive(); !errors.Is(err, ErrMailboxEmpty) {
		t.Fatalf("TryReceive(empty) error = %v, want ErrMailboxEmpty", err)
	}
}

func TestT236MailboxSendReceiveAndClose(t *testing.T) {
	mailbox, err := NewMailbox[int](1)
	if err != nil {
		t.Fatalf("NewMailbox() error = %v", err)
	}
	if err := mailbox.Send(context.Background(), 42); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	value, err := mailbox.Receive(context.Background())
	if err != nil || value != 42 {
		t.Fatalf("Receive() = %d, error = %v, want 42", value, err)
	}
	if err := mailbox.TrySend(7); err != nil {
		t.Fatalf("TrySend(buffered) error = %v", err)
	}
	if err := mailbox.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := mailbox.TrySend(7); !errors.Is(err, ErrMailboxClosed) {
		t.Fatalf("TrySend(after close) error = %v, want ErrMailboxClosed", err)
	}
	value, err = mailbox.Receive(context.Background())
	if err != nil || value != 7 {
		t.Fatalf("Receive(buffered after close) = %d, error = %v, want 7", value, err)
	}
	if _, err := mailbox.Receive(context.Background()); !errors.Is(err, ErrMailboxClosed) {
		t.Fatalf("Receive(after close) error = %v, want ErrMailboxClosed", err)
	}
}

func TestT236MailboxContextCancellationUnblocksWaiters(t *testing.T) {
	mailbox, err := NewMailbox[int](1)
	if err != nil {
		t.Fatalf("NewMailbox() error = %v", err)
	}
	if err := mailbox.TrySend(1); err != nil {
		t.Fatalf("TrySend() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := mailbox.Send(ctx, 2); !errors.Is(err, context.Canceled) {
		t.Fatalf("Send(cancelled) error = %v, want context.Canceled", err)
	}
	if _, err := mailbox.Receive(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Receive(cancelled) error = %v, want context.Canceled", err)
	}
}

func equalInts(left, right []int) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
