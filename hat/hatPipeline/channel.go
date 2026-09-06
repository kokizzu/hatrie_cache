package hatPipeline

import (
	"context"
	"errors"
	"sync"
)

var (
	// ErrChannelClosed reports that a value could not be sent because the channel is closed.
	ErrChannelClosed = errors.New("hatPipeline: channel is closed")
	// ErrChannelInvalid reports a nil or otherwise unusable channel.
	ErrChannelInvalid = errors.New("hatPipeline: invalid channel")
)

// Channel is a typed, context-aware producer-consumer channel.
//
// Receive drains values already buffered before reporting that the channel is
// closed. Close is safe to call more than once.
type Channel[T any] struct {
	values chan T
	done   chan struct{}
	once   sync.Once
	mu     sync.Mutex
	closed bool
	sends  sync.WaitGroup
}

// NewChannel creates a channel with the requested buffer capacity.
func NewChannel[T any](capacity int) (*Channel[T], error) {
	if capacity < 0 {
		return nil, ErrChannelInvalid
	}
	return &Channel[T]{
		values: make(chan T, capacity),
		done:   make(chan struct{}),
	}, nil
}

// Send publishes value, waiting for capacity or ctx cancellation.
func (channel *Channel[T]) Send(ctx context.Context, value T) (err error) {
	return channel.sendWithStop(ctx, nil, nil, value)
}

func (channel *Channel[T]) sendWithStop(ctx context.Context, stop <-chan struct{}, stopErr error, value T) (err error) {
	if channel == nil || channel.values == nil {
		return ErrChannelInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	channel.mu.Lock()
	if channel.closed {
		channel.mu.Unlock()
		return ErrChannelClosed
	}
	channel.sends.Add(1)
	channel.mu.Unlock()
	defer channel.sends.Done()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-stop:
		if stopErr != nil {
			return stopErr
		}
		return ErrChannelClosed
	case <-channel.done:
		return ErrChannelClosed
	case channel.values <- value:
		return nil
	}
}

// Receive returns the next value, waiting for a value or ctx cancellation.
// A false ok result with a nil error means the channel has been closed and
// all buffered values have been drained.
func (channel *Channel[T]) Receive(ctx context.Context) (value T, ok bool, err error) {
	if channel == nil || channel.values == nil {
		return value, false, ErrChannelInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return value, false, ctx.Err()
	case value, ok = <-channel.values:
		return value, ok, nil
	}
}

// Close stops new sends while allowing Receive to drain buffered values.
func (channel *Channel[T]) Close() {
	if channel == nil || channel.values == nil {
		return
	}
	channel.once.Do(func() {
		channel.mu.Lock()
		channel.closed = true
		close(channel.done)
		channel.mu.Unlock()
		channel.sends.Wait()
		close(channel.values)
	})
}

// In exposes the read-only channel for select-heavy consumers.
func (channel *Channel[T]) In() <-chan T {
	if channel == nil {
		return nil
	}
	return channel.values
}

// Len reports the number of values currently buffered.
func (channel *Channel[T]) Len() int {
	if channel == nil || channel.values == nil {
		return 0
	}
	return len(channel.values)
}

// Capacity reports the configured buffer capacity.
func (channel *Channel[T]) Capacity() int {
	if channel == nil || channel.values == nil {
		return 0
	}
	return cap(channel.values)
}
