package hatFiber

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestT236MailboxWakesMultipleReceivers(t *testing.T) {
	mailbox, err := NewMailbox[int](2)
	if err != nil {
		t.Fatalf("NewMailbox() error = %v", err)
	}
	values := make(chan int, 2)
	var receivers sync.WaitGroup
	receivers.Add(2)
	for range 2 {
		go func() {
			defer receivers.Done()
			value, err := mailbox.Receive(context.Background())
			if err == nil {
				values <- value
			}
		}()
	}
	if err := mailbox.Send(context.Background(), 1); err != nil {
		t.Fatalf("Send(1) error = %v", err)
	}
	if err := mailbox.Send(context.Background(), 2); err != nil {
		t.Fatalf("Send(2) error = %v", err)
	}
	done := make(chan struct{})
	go func() {
		receivers.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("receivers did not both wake")
	}
	close(values)
	if len(values) != 2 {
		t.Fatalf("received %d values, want 2", len(values))
	}
}

func TestT236MailboxWakesMultipleSenders(t *testing.T) {
	mailbox, err := NewMailbox[int](1)
	if err != nil {
		t.Fatalf("NewMailbox() error = %v", err)
	}
	if err := mailbox.TrySend(0); err != nil {
		t.Fatalf("TrySend(initial) error = %v", err)
	}
	var senders sync.WaitGroup
	senders.Add(2)
	for value := 1; value <= 2; value++ {
		value := value
		go func() {
			defer senders.Done()
			if err := mailbox.Send(context.Background(), value); err != nil {
				t.Errorf("Send(%d) error = %v", value, err)
			}
		}()
	}
	for range 3 {
		if _, err := mailbox.Receive(context.Background()); err != nil {
			t.Fatalf("Receive() error = %v", err)
		}
	}
	senders.Wait()
}
