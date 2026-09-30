package hatPipeline

import (
	"context"
	"errors"
	"strings"
	"sync"
)

const (
	// DefaultSinkPendingOutputItems bounds queued outputs when no item limit is
	// configured.
	DefaultSinkPendingOutputItems = 1024
	// DefaultSinkPendingOutputBytes bounds retained payload and key bytes when
	// no byte limit is configured.
	DefaultSinkPendingOutputBytes int64 = 64 << 20
	// MaxSinkPendingOutputItems prevents accidental unbounded item retention.
	MaxSinkPendingOutputItems = 65536
	// MaxSinkPendingOutputBytes prevents accidental unbounded byte retention.
	MaxSinkPendingOutputBytes int64 = 1 << 30
	// MaxSinkPendingOutputKeyBytes bounds retained idempotency-key metadata.
	MaxSinkPendingOutputKeyBytes = 1024
	// initialSinkPendingOutputQueueItems avoids repeated ring growth for the
	// common small-batch case without reserving the full configured bound.
	initialSinkPendingOutputQueueItems = 256
)

var (
	// ErrSinkPendingOutputQueueNil reports a method call on a nil queue.
	ErrSinkPendingOutputQueueNil = errors.New("hatPipeline: sink pending-output queue is nil")
	// ErrSinkPendingOutputOptionsInvalid reports an invalid queue bound.
	ErrSinkPendingOutputOptionsInvalid = errors.New("hatPipeline: sink pending-output options are invalid")
	// ErrSinkPendingOutputInvalid reports malformed caller-owned output
	// metadata.
	ErrSinkPendingOutputInvalid = errors.New("hatPipeline: sink pending output is invalid")
	// ErrSinkPendingOutputTooLarge reports one output larger than the byte
	// bound.
	ErrSinkPendingOutputTooLarge = errors.New("hatPipeline: sink pending output is too large")
	// ErrSinkPendingOutputFull reports that TryEnqueue cannot admit an output.
	ErrSinkPendingOutputFull = errors.New("hatPipeline: sink pending-output queue is full")
	// ErrSinkPendingOutputClosed reports an operation after the queue closed and
	// all pending outputs were drained.
	ErrSinkPendingOutputClosed = errors.New("hatPipeline: sink pending-output queue is closed")
	// ErrSinkPendingOutputNotHead reports an acknowledgement for an output that
	// is not the oldest pending output.
	ErrSinkPendingOutputNotHead = errors.New("hatPipeline: sink pending output is not the queue head")
)

// SinkPendingOutput is one payload waiting for successful external delivery.
// Sequence is assigned by the queue; callers must leave it zero when enqueueing.
type SinkPendingOutput struct {
	Sequence       uint64 `json:"sequence"`
	Frontier       uint64 `json:"frontier"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
	Payload        []byte `json:"payload,omitempty"`
}

// SinkPendingOutputQueueOptions bounds retained pending output. Zero values
// select the exported defaults.
type SinkPendingOutputQueueOptions struct {
	MaxItems int   `json:"max_items,omitempty"`
	MaxBytes int64 `json:"max_bytes,omitempty"`
}

// SinkPendingOutputQueueStats is a point-in-time queue occupancy snapshot.
type SinkPendingOutputQueueStats struct {
	Items    int   `json:"items"`
	Bytes    int64 `json:"bytes"`
	MaxItems int   `json:"max_items"`
	MaxBytes int64 `json:"max_bytes"`
	Closed   bool  `json:"closed"`
}

// SinkPendingOutputQueue is a bounded FIFO for output payloads. Enqueue copies
// payload and key data, Peek returns another copy, and Acknowledge removes only
// the current head after the external sink confirms delivery.
type SinkPendingOutputQueue struct {
	mu       sync.Mutex
	entries  []SinkPendingOutput
	head     int
	length   int
	bytes    int64
	maxItems int
	maxBytes int64
	next     uint64
	changed  chan struct{}
	waiters  int
	closed   bool
}

// NewSinkPendingOutputQueue creates an empty bounded output queue.
func NewSinkPendingOutputQueue(options SinkPendingOutputQueueOptions) (*SinkPendingOutputQueue, error) {
	maxItems := options.MaxItems
	if maxItems == 0 {
		maxItems = DefaultSinkPendingOutputItems
	}
	maxBytes := options.MaxBytes
	if maxBytes == 0 {
		maxBytes = DefaultSinkPendingOutputBytes
	}
	if maxItems < 1 || maxItems > MaxSinkPendingOutputItems || maxBytes < 1 || maxBytes > MaxSinkPendingOutputBytes {
		return nil, ErrSinkPendingOutputOptionsInvalid
	}
	initialItems := maxItems
	if initialItems > initialSinkPendingOutputQueueItems {
		initialItems = initialSinkPendingOutputQueueItems
	}
	return &SinkPendingOutputQueue{
		maxItems: maxItems,
		maxBytes: maxBytes,
		next:     1,
		entries:  make([]SinkPendingOutput, initialItems),
		changed:  make(chan struct{}),
	}, nil
}

// Enqueue waits for item and byte capacity, or returns when ctx is canceled.
// The caller's key and payload may be safely reused after this method returns.
func (queue *SinkPendingOutputQueue) Enqueue(ctx context.Context, output SinkPendingOutput) (uint64, error) {
	if queue == nil {
		return 0, ErrSinkPendingOutputQueueNil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	normalized, size, err := normalizeSinkPendingOutput(output, queue.maxBytes)
	if err != nil {
		return 0, err
	}
	for {
		queue.mu.Lock()
		if queue.closed {
			queue.mu.Unlock()
			return 0, ErrSinkPendingOutputClosed
		}
		if queue.canFitLocked(size) {
			sequence, err := queue.appendLocked(normalized, size)
			if err == nil {
				queue.signalLocked()
			}
			queue.mu.Unlock()
			return sequence, err
		}
		changed := queue.changed
		queue.waiters++
		queue.mu.Unlock()
		if err := queue.waitForChange(ctx, changed); err != nil {
			return 0, err
		}
	}
}

// TryEnqueue admits an output immediately or returns ErrSinkPendingOutputFull
// when either the item or byte bound is currently occupied.
func (queue *SinkPendingOutputQueue) TryEnqueue(output SinkPendingOutput) (uint64, error) {
	if queue == nil {
		return 0, ErrSinkPendingOutputQueueNil
	}
	normalized, size, err := normalizeSinkPendingOutput(output, queue.maxBytes)
	if err != nil {
		return 0, err
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if queue.closed {
		return 0, ErrSinkPendingOutputClosed
	}
	if !queue.canFitLocked(size) {
		return 0, ErrSinkPendingOutputFull
	}
	sequence, err := queue.appendLocked(normalized, size)
	if err == nil {
		queue.signalLocked()
	}
	return sequence, err
}

// Peek waits for the oldest pending output. A closed queue still returns all
// outputs already admitted before returning ErrSinkPendingOutputClosed.
func (queue *SinkPendingOutputQueue) Peek(ctx context.Context) (SinkPendingOutput, bool, error) {
	if queue == nil {
		return SinkPendingOutput{}, false, ErrSinkPendingOutputQueueNil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		queue.mu.Lock()
		if queue.length > 0 {
			output := cloneSinkPendingOutput(queue.entries[queue.head])
			queue.mu.Unlock()
			return output, true, nil
		}
		if queue.closed {
			queue.mu.Unlock()
			return SinkPendingOutput{}, false, ErrSinkPendingOutputClosed
		}
		changed := queue.changed
		queue.waiters++
		queue.mu.Unlock()
		if err := queue.waitForChange(ctx, changed); err != nil {
			return SinkPendingOutput{}, false, err
		}
	}
}

// Acknowledge removes the oldest output after the sink confirms delivery.
// Pending outputs remain readable after Close until they are acknowledged.
func (queue *SinkPendingOutputQueue) Acknowledge(sequence uint64) error {
	if queue == nil {
		return ErrSinkPendingOutputQueueNil
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if queue.length == 0 {
		if queue.closed {
			return ErrSinkPendingOutputClosed
		}
		return ErrSinkPendingOutputNotHead
	}
	if sequence == 0 || queue.entries[queue.head].Sequence != sequence {
		return ErrSinkPendingOutputNotHead
	}
	entry := &queue.entries[queue.head]
	queue.bytes -= sinkPendingOutputBytes(*entry)
	*entry = SinkPendingOutput{}
	queue.head = (queue.head + 1) % len(queue.entries)
	queue.length--
	if queue.length == 0 {
		queue.head = 0
	}
	queue.signalLocked()
	return nil
}

// Stats returns current occupancy and configured bounds.
func (queue *SinkPendingOutputQueue) Stats() SinkPendingOutputQueueStats {
	if queue == nil {
		return SinkPendingOutputQueueStats{}
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	return SinkPendingOutputQueueStats{
		Items:    queue.length,
		Bytes:    queue.bytes,
		MaxItems: queue.maxItems,
		MaxBytes: queue.maxBytes,
		Closed:   queue.closed,
	}
}

// Close rejects new outputs, wakes waiters, and preserves already queued
// outputs for draining. It is idempotent.
func (queue *SinkPendingOutputQueue) Close() error {
	if queue == nil {
		return ErrSinkPendingOutputQueueNil
	}
	queue.mu.Lock()
	if !queue.closed {
		queue.closed = true
		queue.signalLocked()
	}
	queue.mu.Unlock()
	return nil
}

func normalizeSinkPendingOutput(output SinkPendingOutput, maxBytes int64) (SinkPendingOutput, int64, error) {
	if output.Sequence != 0 {
		return SinkPendingOutput{}, 0, ErrSinkPendingOutputInvalid
	}
	output.IdempotencyKey = strings.TrimSpace(output.IdempotencyKey)
	if len(output.IdempotencyKey) > MaxSinkPendingOutputKeyBytes {
		return SinkPendingOutput{}, 0, ErrSinkPendingOutputTooLarge
	}
	keyBytes := int64(len(output.IdempotencyKey))
	payloadBytes := int64(len(output.Payload))
	if payloadBytes > maxBytes || keyBytes > maxBytes-payloadBytes {
		return SinkPendingOutput{}, 0, ErrSinkPendingOutputTooLarge
	}
	if output.Payload != nil {
		output.Payload = append([]byte(nil), output.Payload...)
	}
	return output, keyBytes + payloadBytes, nil
}

func (queue *SinkPendingOutputQueue) canFitLocked(size int64) bool {
	return queue.length < queue.maxItems && size <= queue.maxBytes-queue.bytes
}

func (queue *SinkPendingOutputQueue) appendLocked(output SinkPendingOutput, size int64) (uint64, error) {
	if queue.next == 0 {
		return 0, ErrSinkPendingOutputInvalid
	}
	if len(queue.entries) == queue.length {
		capacity := len(queue.entries) * 2
		if capacity == 0 {
			capacity = 16
		}
		if capacity > queue.maxItems {
			capacity = queue.maxItems
		}
		entries := make([]SinkPendingOutput, capacity)
		for index := 0; index < queue.length; index++ {
			entries[index] = queue.entries[(queue.head+index)%len(queue.entries)]
		}
		queue.entries = entries
		queue.head = 0
	}
	index := (queue.head + queue.length) % len(queue.entries)
	output.Sequence = queue.next
	queue.next++
	queue.entries[index] = output
	queue.length++
	queue.bytes += size
	return output.Sequence, nil
}

func (queue *SinkPendingOutputQueue) signalLocked() {
	if queue.waiters == 0 {
		return
	}
	close(queue.changed)
	queue.changed = make(chan struct{})
}

func (queue *SinkPendingOutputQueue) waitForChange(ctx context.Context, changed chan struct{}) error {
	var err error
	select {
	case <-changed:
	case <-ctx.Done():
		err = ctx.Err()
	}
	queue.mu.Lock()
	queue.waiters--
	queue.mu.Unlock()
	return err
}

func cloneSinkPendingOutput(output SinkPendingOutput) SinkPendingOutput {
	if output.Payload != nil {
		output.Payload = append([]byte(nil), output.Payload...)
	}
	return output
}

func sinkPendingOutputBytes(output SinkPendingOutput) int64 {
	return int64(len(output.IdempotencyKey) + len(output.Payload))
}
