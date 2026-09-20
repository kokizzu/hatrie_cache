package hatFiber

// Channel is a bounded, single-owner fiber channel. A zero-capacity channel
// performs rendezvous handoff; a positive capacity stores buffered values.
type Channel[T any] struct {
	scheduler      *Scheduler
	buffer         []T
	head           int
	tail           int
	count          int
	receiveWaiters fiberWaitQueue
	sendWaiters    fiberWaitQueue
	pending        T
	hasPending     bool
	closed         bool
}

// NewChannel creates a channel with capacity values and a waiter bound equal
// to scheduler.Capacity().
func NewChannel[T any](scheduler *Scheduler, capacity int) (*Channel[T], error) {
	if capacity < 0 || capacity > MaxChannelCapacity {
		return nil, ErrChannelCapacity
	}
	receiveWaiters, err := newFiberWaitQueue(scheduler)
	if err != nil {
		return nil, err
	}
	sendWaiters, err := newFiberWaitQueue(scheduler)
	if err != nil {
		return nil, err
	}
	return &Channel[T]{
		scheduler:      scheduler,
		buffer:         make([]T, capacity),
		receiveWaiters: receiveWaiters,
		sendWaiters:    sendWaiters,
	}, nil
}

// TrySend attempts to send without parking. ErrChannelWouldBlock means the
// caller should return WaitSend(value) from its StepFunc and retry the same
// value after resumption; the channel does not retain that value while parked.
func (channel *Channel[T]) TrySend(value T) error {
	if channel == nil {
		return ErrSchedulerRequired
	}
	if channel.closed {
		return ErrChannelClosed
	}
	if channel.deliverToReceiver(value) {
		return nil
	}
	if len(channel.buffer) == channel.count {
		return ErrChannelWouldBlock
	}
	channel.buffer[channel.tail] = value
	channel.tail++
	if channel.tail == len(channel.buffer) {
		channel.tail = 0
	}
	channel.count++
	return nil
}

// WaitSend parks the current fiber until TrySend can be retried. The value is
// deliberately caller-owned, avoiding hidden retained payload memory.
func (channel *Channel[T]) WaitSend(value T) (Step, error) {
	_ = value
	if channel == nil {
		return 0, ErrSchedulerRequired
	}
	if channel.closed {
		return 0, ErrChannelClosed
	}
	if channel.canSend() {
		return StepYield, nil
	}
	return channel.sendWaiters.waitCurrent()
}

// TryReceive attempts to receive without parking. A closed, drained channel
// returns ok=false and nil error; an open empty channel returns
// ErrChannelWouldBlock.
func (channel *Channel[T]) TryReceive() (value T, ok bool, err error) {
	if channel == nil {
		return value, false, ErrSchedulerRequired
	}
	if channel.hasPending {
		value = channel.pending
		var zero T
		channel.pending = zero
		channel.hasPending = false
		return value, true, nil
	}
	if channel.count > 0 {
		value = channel.buffer[channel.head]
		var zero T
		channel.buffer[channel.head] = zero
		channel.head++
		if channel.head == len(channel.buffer) {
			channel.head = 0
		}
		channel.count--
		channel.wakeSender()
		return value, true, nil
	}
	if channel.closed {
		return value, false, nil
	}
	// For an unbuffered channel, waking a sender makes it retry against this
	// receiver's next WaitReceive cycle. This keeps payload ownership with the
	// caller instead of retaining arbitrary values in the wait queue.
	channel.sendWaiters.wakeOne()
	return value, false, ErrChannelWouldBlock
}

// WaitReceive parks the current fiber until TryReceive can be retried.
func (channel *Channel[T]) WaitReceive() (Step, error) {
	if channel == nil {
		return 0, ErrSchedulerRequired
	}
	if channel.hasPending || channel.count > 0 || (channel.closed && channel.count == 0) {
		return StepYield, nil
	}
	return channel.receiveWaiters.waitCurrent()
}

// Close prevents sends and wakes both sender and receiver waiters. Buffered
// values remain readable before the channel reports closed and drained.
func (channel *Channel[T]) Close() error {
	if channel == nil {
		return ErrSchedulerRequired
	}
	if channel.closed {
		return nil
	}
	channel.closed = true
	channel.receiveWaiters.wakeAll()
	channel.sendWaiters.wakeAll()
	return nil
}

// Len reports buffered values, excluding a rendezvous pending value.
func (channel *Channel[T]) Len() int {
	if channel == nil {
		return 0
	}
	return channel.count
}

// Cap reports the configured buffer capacity.
func (channel *Channel[T]) Cap() int {
	if channel == nil {
		return 0
	}
	return len(channel.buffer)
}

// Closed reports whether Close has been called.
func (channel *Channel[T]) Closed() bool {
	return channel != nil && channel.closed
}

func (channel *Channel[T]) canSend() bool {
	return channel != nil && !channel.closed && (channel.receiveWaiters.count > 0 || channel.count < len(channel.buffer))
}

func (channel *Channel[T]) deliverToReceiver(value T) bool {
	for channel.receiveWaiters.count > 0 {
		identifier := channel.receiveWaiters.popWaiting()
		if identifier == 0 {
			return false
		}
		channel.pending = value
		channel.hasPending = true
		if channel.scheduler.resume(identifier) {
			return true
		}
		var zero T
		channel.pending = zero
		channel.hasPending = false
	}
	return false
}

func (channel *Channel[T]) wakeSender() {
	if channel.closed {
		return
	}
	channel.sendWaiters.wakeOne()
}
