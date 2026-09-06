package hatPipeline

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestChannelProvidesTypedBackpressureAndDrainOnClose(t *testing.T) {
	channel, err := NewChannel[string](1)
	if err != nil {
		t.Fatal(err)
	}
	if channel.Capacity() != 1 {
		t.Fatalf("capacity = %d, want 1", channel.Capacity())
	}
	if err := channel.Send(context.Background(), "first"); err != nil {
		t.Fatal(err)
	}
	if channel.Len() != 1 {
		t.Fatalf("len = %d, want 1", channel.Len())
	}

	blocked := make(chan error, 1)
	go func() { blocked <- channel.Send(context.Background(), "second") }()
	select {
	case err := <-blocked:
		t.Fatalf("send completed before capacity was available: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	channel.Close()
	channel.Close()
	if err := <-blocked; !errors.Is(err, ErrChannelClosed) {
		t.Fatalf("blocked send error = %v, want %v", err, ErrChannelClosed)
	}

	value, ok, err := channel.Receive(context.Background())
	if err != nil || !ok || value != "first" {
		t.Fatalf("first receive = (%q, %t, %v), want (first, true, nil)", value, ok, err)
	}
	value, ok, err = channel.Receive(context.Background())
	if err != nil || ok || value != "" {
		t.Fatalf("drained receive = (%q, %t, %v), want (empty, false, nil)", value, ok, err)
	}
	if err := channel.Send(context.Background(), "after-close"); !errors.Is(err, ErrChannelClosed) {
		t.Fatalf("send after close error = %v, want %v", err, ErrChannelClosed)
	}
}

func TestChannelHonorsReceiveCancellationAndReportsCapacity(t *testing.T) {
	channel, err := NewChannel[int](0)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	value, ok, err := channel.Receive(ctx)
	if !errors.Is(err, context.Canceled) || ok || value != 0 {
		t.Fatalf("receive = (%d, %t, %v), want (0, false, context canceled)", value, ok, err)
	}

	if _, err := NewChannel[int](-1); !errors.Is(err, ErrChannelInvalid) {
		t.Fatalf("negative capacity error = %v, want %v", err, ErrChannelInvalid)
	}
	var nilChannel *Channel[int]
	if err := nilChannel.Send(context.Background(), 1); !errors.Is(err, ErrChannelInvalid) {
		t.Fatalf("nil send error = %v, want %v", err, ErrChannelInvalid)
	}
	if _, _, err := nilChannel.Receive(context.Background()); !errors.Is(err, ErrChannelInvalid) {
		t.Fatalf("nil receive error = %v, want %v", err, ErrChannelInvalid)
	}
}
