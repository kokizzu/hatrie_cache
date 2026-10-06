package hatPipeline

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
)

var (
	// ErrAsyncBatcherRegistryNil indicates that a method was called on a nil registry.
	ErrAsyncBatcherRegistryNil = errors.New("hatPipeline: async batcher registry is nil")
	// ErrAsyncBatcherRegistryClosed indicates that the registry has been closed.
	ErrAsyncBatcherRegistryClosed = errors.New("hatPipeline: async batcher registry is closed")
	// ErrAsyncBatcherRegistryNameRequired indicates that a queue name is empty.
	ErrAsyncBatcherRegistryNameRequired = errors.New("hatPipeline: async batcher registry queue name is required")
	// ErrAsyncBatcherRegistryNameTooLong indicates that a queue name exceeds the bounded length.
	ErrAsyncBatcherRegistryNameTooLong = errors.New("hatPipeline: async batcher registry queue name is too long")
	// ErrAsyncBatcherRegistryQueueRequired indicates that a queue control is nil.
	ErrAsyncBatcherRegistryQueueRequired = errors.New("hatPipeline: async batcher registry queue is required")
	// ErrAsyncBatcherRegistryQueueExists indicates that a queue name is already registered.
	ErrAsyncBatcherRegistryQueueExists = errors.New("hatPipeline: async batcher registry queue already exists")
	// ErrAsyncBatcherRegistryQueueMissing indicates that a queue name is not registered.
	ErrAsyncBatcherRegistryQueueMissing = errors.New("hatPipeline: async batcher registry queue is missing")
	// ErrAsyncBatcherRegistryMaxQueuesInvalid indicates an invalid queue limit.
	ErrAsyncBatcherRegistryMaxQueuesInvalid = errors.New("hatPipeline: async batcher registry max queues is invalid")
	// ErrAsyncBatcherRegistryMaxQueuesReached indicates that the queue limit was reached.
	ErrAsyncBatcherRegistryMaxQueuesReached = errors.New("hatPipeline: async batcher registry max queues reached")
)

const (
	// DefaultAsyncBatcherRegistryMaxQueues bounds the number of registered queues.
	DefaultAsyncBatcherRegistryMaxQueues = 1024
	// MaxAsyncBatcherRegistryMaxQueues prevents accidental registry explosions.
	MaxAsyncBatcherRegistryMaxQueues = 1 << 16
	// MaxAsyncBatcherRegistryNameBytes bounds memory retained for one queue name.
	MaxAsyncBatcherRegistryNameBytes = 128
)

// AsyncBatcherControl is the small control surface needed by a queue registry.
// AsyncBatcher implements it, and other batcher implementations can opt in
// without depending on the registry's concrete type.
type AsyncBatcherControl interface {
	Flush(context.Context) error
	Close(context.Context) error
	Stats() AsyncBatcherStats
}

// AsyncBatcherRegistryOptions configures an opt-in queue registry. A zero
// MaxQueues uses DefaultAsyncBatcherRegistryMaxQueues.
type AsyncBatcherRegistryOptions struct {
	MaxQueues int
}

// AsyncBatcherQueueStatus is a stable, point-in-time queue status record.
type AsyncBatcherQueueStatus struct {
	Name  string
	Stats AsyncBatcherStats
}

// AsyncBatcherRegistry provides bounded operator control over named batchers.
// It does not start workers, expose a network endpoint, or own authentication;
// callers decide how to publish this control surface.
type AsyncBatcherRegistry struct {
	mu        sync.RWMutex
	queues    map[string]AsyncBatcherControl
	maxQueues int
	closed    bool
	closeDone chan struct{}
	closeErr  error
}

// NewAsyncBatcherRegistry creates a registry without starting any workers.
func NewAsyncBatcherRegistry(options AsyncBatcherRegistryOptions) (*AsyncBatcherRegistry, error) {
	if options.MaxQueues == 0 {
		options.MaxQueues = DefaultAsyncBatcherRegistryMaxQueues
	}
	if options.MaxQueues < 1 || options.MaxQueues > MaxAsyncBatcherRegistryMaxQueues {
		return nil, ErrAsyncBatcherRegistryMaxQueuesInvalid
	}
	return &AsyncBatcherRegistry{
		queues:    make(map[string]AsyncBatcherControl),
		maxQueues: options.MaxQueues,
		closeDone: make(chan struct{}),
	}, nil
}

// Register adds a named queue. Queue ownership remains with the caller until
// Registry.Close is called; duplicate names are rejected.
func (registry *AsyncBatcherRegistry) Register(name string, queue AsyncBatcherControl) error {
	if registry == nil {
		return ErrAsyncBatcherRegistryNil
	}
	if err := validateAsyncBatcherRegistryName(name); err != nil {
		return err
	}
	if isNilAsyncBatcherControl(queue) {
		return ErrAsyncBatcherRegistryQueueRequired
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrAsyncBatcherRegistryClosed
	}
	if _, exists := registry.queues[name]; exists {
		return ErrAsyncBatcherRegistryQueueExists
	}
	if len(registry.queues) >= registry.maxQueues {
		return ErrAsyncBatcherRegistryMaxQueuesReached
	}
	registry.queues[name] = queue
	return nil
}

// Unregister removes a queue without closing it. The returned control remains
// owned by the caller and must be closed by the caller when it is no longer used.
func (registry *AsyncBatcherRegistry) Unregister(name string) (AsyncBatcherControl, error) {
	if registry == nil {
		return nil, ErrAsyncBatcherRegistryNil
	}
	if err := validateAsyncBatcherRegistryName(name); err != nil {
		return nil, err
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return nil, ErrAsyncBatcherRegistryClosed
	}
	queue, exists := registry.queues[name]
	if !exists {
		return nil, ErrAsyncBatcherRegistryQueueMissing
	}
	delete(registry.queues, name)
	return queue, nil
}

// Flush flushes one registered queue. The registry lock is released before
// invoking user code, so a queue may safely inspect or update the registry.
func (registry *AsyncBatcherRegistry) Flush(ctx context.Context, name string) error {
	if registry == nil {
		return ErrAsyncBatcherRegistryNil
	}
	if err := validateAsyncBatcherRegistryName(name); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}

	registry.mu.RLock()
	if registry.closed {
		registry.mu.RUnlock()
		return ErrAsyncBatcherRegistryClosed
	}
	queue, exists := registry.queues[name]
	registry.mu.RUnlock()
	if !exists {
		return ErrAsyncBatcherRegistryQueueMissing
	}
	return queue.Flush(ctx)
}

// FlushAll flushes all currently registered queues in name order. It attempts
// every queue and joins all errors, including the queue name in each error.
func (registry *AsyncBatcherRegistry) FlushAll(ctx context.Context) error {
	if registry == nil {
		return ErrAsyncBatcherRegistryNil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	queues, err := registry.snapshotControls()
	if err != nil {
		return err
	}
	var errs []error
	for _, queue := range queues {
		if err := queue.control.Flush(ctx); err != nil {
			errs = append(errs, fmt.Errorf("async batcher queue %q: %w", queue.name, err))
		}
	}
	return errors.Join(errs...)
}

// Snapshot returns registered queue statuses sorted by queue name. A nil or
// closed registry returns an empty snapshot.
func (registry *AsyncBatcherRegistry) Snapshot() []AsyncBatcherQueueStatus {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	queues := make([]namedAsyncBatcherControl, 0, len(registry.queues))
	if !registry.closed {
		for name, queue := range registry.queues {
			queues = append(queues, namedAsyncBatcherControl{name: name, control: queue})
		}
	}
	registry.mu.RUnlock()

	sort.Slice(queues, func(i, j int) bool { return queues[i].name < queues[j].name })
	statuses := make([]AsyncBatcherQueueStatus, 0, len(queues))
	for _, queue := range queues {
		statuses = append(statuses, AsyncBatcherQueueStatus{
			Name:  queue.name,
			Stats: queue.control.Stats(),
		})
	}
	return statuses
}

// Close marks the registry closed and closes every queue that remains
// registered. It is safe to call more than once; later calls observe the first
// result. Unregistered queues remain owned by the caller.
func (registry *AsyncBatcherRegistry) Close(ctx context.Context) error {
	if registry == nil {
		return ErrAsyncBatcherRegistryNil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	registry.mu.Lock()
	if registry.closed {
		done := registry.closeDone
		registry.mu.Unlock()
		select {
		case <-done:
			registry.mu.RLock()
			err := registry.closeErr
			registry.mu.RUnlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	registry.closed = true
	queues := make([]namedAsyncBatcherControl, 0, len(registry.queues))
	for name, queue := range registry.queues {
		queues = append(queues, namedAsyncBatcherControl{name: name, control: queue})
	}
	registry.queues = nil
	registry.mu.Unlock()

	sort.Slice(queues, func(i, j int) bool { return queues[i].name < queues[j].name })
	var errs []error
	for _, queue := range queues {
		if err := queue.control.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("async batcher queue %q: %w", queue.name, err))
		}
	}
	closeErr := errors.Join(errs...)

	registry.mu.Lock()
	registry.closeErr = closeErr
	close(registry.closeDone)
	registry.mu.Unlock()
	return closeErr
}

type namedAsyncBatcherControl struct {
	name    string
	control AsyncBatcherControl
}

func (registry *AsyncBatcherRegistry) snapshotControls() ([]namedAsyncBatcherControl, error) {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	if registry.closed {
		return nil, ErrAsyncBatcherRegistryClosed
	}
	queues := make([]namedAsyncBatcherControl, 0, len(registry.queues))
	for name, queue := range registry.queues {
		queues = append(queues, namedAsyncBatcherControl{name: name, control: queue})
	}
	sort.Slice(queues, func(i, j int) bool { return queues[i].name < queues[j].name })
	return queues, nil
}

func validateAsyncBatcherRegistryName(name string) error {
	if name == "" {
		return ErrAsyncBatcherRegistryNameRequired
	}
	if len(name) > MaxAsyncBatcherRegistryNameBytes {
		return ErrAsyncBatcherRegistryNameTooLong
	}
	return nil
}

func isNilAsyncBatcherControl(queue AsyncBatcherControl) bool {
	if queue == nil {
		return true
	}
	value := reflect.ValueOf(queue)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
