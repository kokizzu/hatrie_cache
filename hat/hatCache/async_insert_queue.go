package hatCache

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

const (
	// DefaultAsyncInsertQueueRegistryCapacity bounds the number of named
	// queues in a monitoring registry when a capacity is not supplied.
	DefaultAsyncInsertQueueRegistryCapacity = 16
	// MaxAsyncInsertQueueRegistryCapacity prevents an operator mistake from
	// turning the registry into an unbounded name-to-queue map.
	MaxAsyncInsertQueueRegistryCapacity = 1024
	maxAsyncInsertQueueNameBytes        = 256
)

var (
	ErrAsyncInsertQueueRegistryNil             = errors.New("hatriecache: async insert queue registry is nil")
	ErrAsyncInsertQueueRegistryCapacityInvalid = errors.New("hatriecache: async insert queue registry capacity is invalid")
	ErrAsyncInsertQueueRegistryFull            = errors.New("hatriecache: async insert queue registry is full")
	ErrAsyncInsertQueueInvalidName             = errors.New("hatriecache: async insert queue name is invalid")
	ErrAsyncInsertQueueNilBuffer               = errors.New("hatriecache: async insert queue buffer is nil")
	ErrAsyncInsertQueueExists                  = errors.New("hatriecache: async insert queue already exists")
	ErrAsyncInsertQueueNotFound                = errors.New("hatriecache: async insert queue was not found")
)

// AsyncInsertBufferStats is a point-in-time, value-only view of one async
// insert buffer. Pending includes commands waiting for the worker and commands
// currently being applied; it never includes command payloads.
type AsyncInsertBufferStats struct {
	Capacity          int    `json:"capacity"`
	BatchSize         int    `json:"batch_size"`
	Queued            int    `json:"queued"`
	InFlight          int    `json:"in_flight"`
	Pending           int    `json:"pending"`
	CurrentBatchItems int    `json:"current_batch_items"`
	ReadyBatches      int    `json:"ready_batches"`
	Submitted         uint64 `json:"submitted"`
	FlushedBatches    uint64 `json:"flushed_batches"`
	FlushedItems      uint64 `json:"flushed_items"`
	FailedBatches     uint64 `json:"failed_batches"`
	Closed            bool   `json:"closed"`
}

// Stats returns a consistent queue-depth snapshot without exposing requests.
// A nil receiver returns the zero value.
func (buffer *AsyncInsertBuffer) Stats() AsyncInsertBufferStats {
	if buffer == nil {
		return AsyncInsertBufferStats{}
	}
	buffer.mu.Lock()
	currentItems := 0
	if buffer.current != nil {
		currentItems = len(buffer.current.requests)
	}
	stats := AsyncInsertBufferStats{
		Capacity:          buffer.options.Capacity,
		BatchSize:         buffer.options.BatchSize,
		Queued:            buffer.queued,
		InFlight:          buffer.inFlight,
		Pending:           buffer.queued + buffer.inFlight,
		CurrentBatchItems: currentItems,
		ReadyBatches:      len(buffer.ready),
		Submitted:         buffer.submitted,
		FlushedBatches:    buffer.flushedBatches,
		FlushedItems:      buffer.flushedItems,
		FailedBatches:     buffer.failedBatches,
		Closed:            buffer.closed,
	}
	buffer.mu.Unlock()
	return stats
}

func (buffer *AsyncInsertBuffer) finishBatch(batch *asyncInsertBatch, response CacheCommandResponse, err error) {
	if batch == nil {
		return
	}
	items := len(batch.requests)
	buffer.mu.Lock()
	if items >= buffer.inFlight {
		buffer.inFlight = 0
	} else {
		buffer.inFlight -= items
	}
	if err != nil {
		buffer.failedBatches++
	} else {
		buffer.flushedBatches++
		buffer.flushedItems += uint64(items)
	}
	buffer.mu.Unlock()
	batch.complete(response, err)
}

// AsyncInsertQueueStats names one registered async insert buffer.
type AsyncInsertQueueStats struct {
	Name string `json:"name"`
	AsyncInsertBufferStats
}

// AsyncInsertQueuesResponse is returned by the monitoring queue-status API.
type AsyncInsertQueuesResponse struct {
	Queues []AsyncInsertQueueStats `json:"queues"`
}

// AsyncInsertQueueFlushResponse is returned after a targeted or all-queue
// flush completes.
type AsyncInsertQueueFlushResponse struct {
	Flushed bool                    `json:"flushed"`
	Name    string                  `json:"name,omitempty"`
	Queues  []AsyncInsertQueueStats `json:"queues"`
}

// AsyncInsertQueueRegistry provides bounded names for caller-owned async
// insert buffers. Registration does not start or stop a buffer; lifecycle
// ownership remains with the caller.
type AsyncInsertQueueRegistry struct {
	mu       sync.RWMutex
	capacity int
	queues   map[string]*AsyncInsertBuffer
}

// NewAsyncInsertQueueRegistry creates an empty bounded queue registry.
// Capacity zero selects DefaultAsyncInsertQueueRegistryCapacity.
func NewAsyncInsertQueueRegistry(capacity int) (*AsyncInsertQueueRegistry, error) {
	if capacity == 0 {
		capacity = DefaultAsyncInsertQueueRegistryCapacity
	}
	if capacity < 1 || capacity > MaxAsyncInsertQueueRegistryCapacity {
		return nil, ErrAsyncInsertQueueRegistryCapacityInvalid
	}
	return &AsyncInsertQueueRegistry{
		capacity: capacity,
		queues:   make(map[string]*AsyncInsertBuffer, capacity),
	}, nil
}

// Register adds one named buffer. Names are trimmed and limited to a bounded
// UTF-8 byte length; the buffer pointer is never replaced implicitly.
func (registry *AsyncInsertQueueRegistry) Register(name string, buffer *AsyncInsertBuffer) error {
	if registry == nil {
		return ErrAsyncInsertQueueRegistryNil
	}
	name, err := normalizeAsyncInsertQueueName(name)
	if err != nil {
		return err
	}
	if buffer == nil {
		return ErrAsyncInsertQueueNilBuffer
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, exists := registry.queues[name]; exists {
		return ErrAsyncInsertQueueExists
	}
	if len(registry.queues) >= registry.capacity {
		return ErrAsyncInsertQueueRegistryFull
	}
	registry.queues[name] = buffer
	return nil
}

// Unregister removes a name without closing its buffer. It returns whether a
// registration was removed.
func (registry *AsyncInsertQueueRegistry) Unregister(name string) bool {
	if registry == nil {
		return false
	}
	name, err := normalizeAsyncInsertQueueName(name)
	if err != nil {
		return false
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, exists := registry.queues[name]; !exists {
		return false
	}
	delete(registry.queues, name)
	return true
}

// Names returns registered names in deterministic order.
func (registry *AsyncInsertQueueRegistry) Names() []string {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	names := make([]string, 0, len(registry.queues))
	for name := range registry.queues {
		names = append(names, name)
	}
	registry.mu.RUnlock()
	sort.Strings(names)
	return names
}

// Stats returns all queue snapshots in deterministic name order.
func (registry *AsyncInsertQueueRegistry) Stats() []AsyncInsertQueueStats {
	if registry == nil {
		return nil
	}
	queues := registry.snapshotQueues()
	stats := make([]AsyncInsertQueueStats, 0, len(queues))
	for _, queue := range queues {
		stats = append(stats, AsyncInsertQueueStats{Name: queue.name, AsyncInsertBufferStats: queue.buffer.Stats()})
	}
	return stats
}

// RegisteredStats returns one named queue snapshot.
func (registry *AsyncInsertQueueRegistry) RegisteredStats(name string) (AsyncInsertQueueStats, error) {
	queue, err := registry.lookup(name)
	if err != nil {
		return AsyncInsertQueueStats{}, err
	}
	return AsyncInsertQueueStats{Name: queue.name, AsyncInsertBufferStats: queue.buffer.Stats()}, nil
}

// Flush synchronously drains one named queue. The registry lock is released
// before waiting, so status and registration operations remain responsive.
func (registry *AsyncInsertQueueRegistry) Flush(ctx context.Context, name string) error {
	queue, err := registry.lookup(name)
	if err != nil {
		return err
	}
	return queue.buffer.Flush(ctx)
}

// FlushAll drains every currently registered queue in deterministic order.
// It returns all queue errors while continuing to give each queue a chance to
// drain.
func (registry *AsyncInsertQueueRegistry) FlushAll(ctx context.Context) error {
	if registry == nil {
		return ErrAsyncInsertQueueRegistryNil
	}
	var flushErr error
	for _, queue := range registry.snapshotQueues() {
		flushErr = errors.Join(flushErr, queue.buffer.Flush(ctx))
	}
	return flushErr
}

type asyncInsertQueueRef struct {
	name   string
	buffer *AsyncInsertBuffer
}

func (registry *AsyncInsertQueueRegistry) snapshotQueues() []asyncInsertQueueRef {
	registry.mu.RLock()
	queues := make([]asyncInsertQueueRef, 0, len(registry.queues))
	for name, buffer := range registry.queues {
		queues = append(queues, asyncInsertQueueRef{name: name, buffer: buffer})
	}
	registry.mu.RUnlock()
	sort.Slice(queues, func(left, right int) bool { return queues[left].name < queues[right].name })
	return queues
}

func (registry *AsyncInsertQueueRegistry) lookup(name string) (asyncInsertQueueRef, error) {
	if registry == nil {
		return asyncInsertQueueRef{}, ErrAsyncInsertQueueRegistryNil
	}
	name, err := normalizeAsyncInsertQueueName(name)
	if err != nil {
		return asyncInsertQueueRef{}, err
	}
	registry.mu.RLock()
	buffer, ok := registry.queues[name]
	registry.mu.RUnlock()
	if !ok {
		return asyncInsertQueueRef{}, fmt.Errorf("%w: %s", ErrAsyncInsertQueueNotFound, name)
	}
	return asyncInsertQueueRef{name: name, buffer: buffer}, nil
}

func normalizeAsyncInsertQueueName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" || len(name) > maxAsyncInsertQueueNameBytes {
		return "", ErrAsyncInsertQueueInvalidName
	}
	return name, nil
}
