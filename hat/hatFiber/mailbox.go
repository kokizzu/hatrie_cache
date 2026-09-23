package hatFiber

import (
	"context"
	"errors"
	"sync"
)

var (
	// ErrMailboxInvalid reports a nil mailbox or an uninitialized zero value.
	ErrMailboxInvalid = errors.New("hatFiber: mailbox is invalid")
	// ErrMailboxCapacity reports an unsupported mailbox capacity.
	ErrMailboxCapacity = errors.New("hatFiber: mailbox capacity is invalid")
	// ErrMailboxClosed reports an operation after the mailbox was closed.
	ErrMailboxClosed = errors.New("hatFiber: mailbox is closed")
	// ErrMailboxFull reports a nonblocking send with no available slot.
	ErrMailboxFull = errors.New("hatFiber: mailbox is full")
	// ErrMailboxEmpty reports a nonblocking receive with no available value.
	ErrMailboxEmpty = errors.New("hatFiber: mailbox is empty")
)

// MaxMailboxCapacity bounds storage reserved by one mailbox.
const MaxMailboxCapacity = 1 << 20

// Mailbox is a bounded, concurrency-safe FIFO queue for independent workers.
// It retains one fixed ring buffer and two notification channels; values are
// stored in place and operations do not allocate per element. Send and
// Receive may be called concurrently by multiple goroutines.
type Mailbox[T any] struct {
	mu       sync.Mutex
	notEmpty chan struct{}
	notFull  chan struct{}
	values   []T
	head     int
	tail     int
	count    int
	closed   bool
}

// NewMailbox creates a bounded mailbox with capacity slots.
func NewMailbox[T any](capacity int) (*Mailbox[T], error) {
	if capacity < 1 || capacity > MaxMailboxCapacity {
		return nil, ErrMailboxCapacity
	}
	return &Mailbox[T]{
		notEmpty: make(chan struct{}, 1),
		notFull:  make(chan struct{}, 1),
		values:   make([]T, capacity),
	}, nil
}

// Send queues value, waiting for capacity or context cancellation. A nil
// context is treated as context.Background.
func (mailbox *Mailbox[T]) Send(ctx context.Context, value T) error {
	if mailbox == nil || mailbox.notEmpty == nil || mailbox.notFull == nil || len(mailbox.values) == 0 {
		return ErrMailboxInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		mailbox.mu.Lock()
		if mailbox.closed {
			mailbox.mu.Unlock()
			return ErrMailboxClosed
		}
		if mailbox.count < len(mailbox.values) {
			mailbox.push(value)
			mailbox.signal(mailbox.notEmpty)
			if mailbox.count < len(mailbox.values) {
				mailbox.signal(mailbox.notFull)
			}
			mailbox.mu.Unlock()
			return nil
		}
		wake := mailbox.notFull
		mailbox.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// TrySend queues value without waiting for capacity or context cancellation.
func (mailbox *Mailbox[T]) TrySend(value T) error {
	if mailbox == nil || mailbox.notEmpty == nil || mailbox.notFull == nil || len(mailbox.values) == 0 {
		return ErrMailboxInvalid
	}
	mailbox.mu.Lock()
	defer mailbox.mu.Unlock()
	if mailbox.closed {
		return ErrMailboxClosed
	}
	if mailbox.count == len(mailbox.values) {
		return ErrMailboxFull
	}
	mailbox.push(value)
	mailbox.signal(mailbox.notEmpty)
	if mailbox.count < len(mailbox.values) {
		mailbox.signal(mailbox.notFull)
	}
	return nil
}

// Receive removes and returns the oldest value, waiting for a value or
// context cancellation. Buffered values remain readable after Close.
func (mailbox *Mailbox[T]) Receive(ctx context.Context) (T, error) {
	var zero T
	if mailbox == nil || mailbox.notEmpty == nil || mailbox.notFull == nil || len(mailbox.values) == 0 {
		return zero, ErrMailboxInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		mailbox.mu.Lock()
		if mailbox.count > 0 {
			value := mailbox.pop()
			if !mailbox.closed {
				mailbox.signal(mailbox.notFull)
				if mailbox.count > 0 {
					mailbox.signal(mailbox.notEmpty)
				}
			}
			mailbox.mu.Unlock()
			return value, nil
		}
		if mailbox.closed {
			mailbox.mu.Unlock()
			return zero, ErrMailboxClosed
		}
		wake := mailbox.notEmpty
		mailbox.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			return zero, ctx.Err()
		}
	}
}

// TryReceive removes and returns the oldest value without waiting. Buffered
// values remain readable after Close.
func (mailbox *Mailbox[T]) TryReceive() (T, error) {
	var zero T
	if mailbox == nil || mailbox.notEmpty == nil || mailbox.notFull == nil || len(mailbox.values) == 0 {
		return zero, ErrMailboxInvalid
	}
	mailbox.mu.Lock()
	defer mailbox.mu.Unlock()
	if mailbox.count > 0 {
		value := mailbox.pop()
		if !mailbox.closed {
			mailbox.signal(mailbox.notFull)
			if mailbox.count > 0 {
				mailbox.signal(mailbox.notEmpty)
			}
		}
		return value, nil
	}
	if mailbox.closed {
		return zero, ErrMailboxClosed
	}
	return zero, ErrMailboxEmpty
}

// ReceiveBatch removes up to len(dst) oldest values without waiting. It
// returns ErrMailboxEmpty when no value is available and ErrMailboxClosed
// after the mailbox is closed and drained.
func (mailbox *Mailbox[T]) ReceiveBatch(dst []T) (int, error) {
	if mailbox == nil || mailbox.notEmpty == nil || mailbox.notFull == nil || len(mailbox.values) == 0 {
		return 0, ErrMailboxInvalid
	}
	if len(dst) == 0 {
		return 0, ErrMailboxEmpty
	}
	mailbox.mu.Lock()
	defer mailbox.mu.Unlock()
	if mailbox.count == 0 {
		if mailbox.closed {
			return 0, ErrMailboxClosed
		}
		return 0, ErrMailboxEmpty
	}
	count := len(dst)
	if count > mailbox.count {
		count = mailbox.count
	}
	for index := 0; index < count; index++ {
		dst[index] = mailbox.pop()
	}
	if !mailbox.closed {
		mailbox.signal(mailbox.notFull)
		if mailbox.count > 0 {
			mailbox.signal(mailbox.notEmpty)
		}
	}
	return count, nil
}

// Close prevents new sends and wakes blocked senders and receivers. Already
// buffered values can still be received before ErrMailboxClosed is returned.
func (mailbox *Mailbox[T]) Close() error {
	if mailbox == nil || mailbox.notEmpty == nil || mailbox.notFull == nil || len(mailbox.values) == 0 {
		return ErrMailboxInvalid
	}
	mailbox.mu.Lock()
	defer mailbox.mu.Unlock()
	if mailbox.closed {
		return nil
	}
	mailbox.closed = true
	close(mailbox.notEmpty)
	close(mailbox.notFull)
	return nil
}

// Len reports the number of buffered values.
func (mailbox *Mailbox[T]) Len() int {
	if mailbox == nil || mailbox.notEmpty == nil || mailbox.notFull == nil {
		return 0
	}
	mailbox.mu.Lock()
	defer mailbox.mu.Unlock()
	return mailbox.count
}

// Cap reports the fixed mailbox capacity.
func (mailbox *Mailbox[T]) Cap() int {
	if mailbox == nil {
		return 0
	}
	return len(mailbox.values)
}

// Closed reports whether Close has been called.
func (mailbox *Mailbox[T]) Closed() bool {
	if mailbox == nil || mailbox.notEmpty == nil || mailbox.notFull == nil {
		return false
	}
	mailbox.mu.Lock()
	defer mailbox.mu.Unlock()
	return mailbox.closed
}

func (mailbox *Mailbox[T]) push(value T) {
	mailbox.values[mailbox.tail] = value
	mailbox.tail++
	if mailbox.tail == len(mailbox.values) {
		mailbox.tail = 0
	}
	mailbox.count++
}

func (mailbox *Mailbox[T]) pop() T {
	value := mailbox.values[mailbox.head]
	var zero T
	mailbox.values[mailbox.head] = zero
	mailbox.head++
	if mailbox.head == len(mailbox.values) {
		mailbox.head = 0
	}
	mailbox.count--
	return value
}

func (mailbox *Mailbox[T]) signal(channel chan struct{}) {
	select {
	case channel <- struct{}{}:
	default:
	}
}
