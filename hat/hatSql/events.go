package hatSql

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ChangeEvent is one ordered CDC record available to downstream consumers.
type ChangeEvent struct {
	Sequence                  uint64
	Namespace, Operation, Key string
	Value                     interface{}
	At                        time.Time
}
type ChangeLog struct {
	mu     sync.RWMutex
	events []ChangeEvent
}

func NewChangeLog() *ChangeLog { return &ChangeLog{} }
func (log *ChangeLog) Append(event ChangeEvent) error {
	if log == nil {
		return fmt.Errorf("change log is required")
	}
	if event.Namespace == "" || event.Operation == "" {
		return fmt.Errorf("change event namespace and operation are required")
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	event.Sequence = uint64(len(log.events) + 1)
	if event.At.IsZero() {
		event.At = time.Now().UTC()
	}
	log.events = append(log.events, event)
	return nil
}
func (log *ChangeLog) After(sequence uint64) []ChangeEvent {
	if log == nil {
		return nil
	}
	log.mu.RLock()
	defer log.mu.RUnlock()
	start := int(sequence)
	if start > len(log.events) {
		start = len(log.events)
	}
	return append([]ChangeEvent(nil), log.events[start:]...)
}

// EventSink consumes query or mutation events. WebhookSink keeps HTTP policy
// outside the engine by accepting an application-owned delivery callback.
type EventSink interface {
	Deliver(context.Context, ChangeEvent) error
}
type EventSinkFunc func(context.Context, ChangeEvent) error

func (sink EventSinkFunc) Deliver(ctx context.Context, event ChangeEvent) error {
	return sink(ctx, event)
}

type WebhookSink struct {
	Send func(context.Context, ChangeEvent) error
}

func (sink WebhookSink) Deliver(ctx context.Context, event ChangeEvent) error {
	if sink.Send == nil {
		return fmt.Errorf("webhook sender is required")
	}
	return sink.Send(ctx, event)
}

type IdempotencyKeys struct {
	mu     sync.Mutex
	values map[string]interface{}
}

func NewIdempotencyKeys() *IdempotencyKeys {
	return &IdempotencyKeys{values: make(map[string]interface{})}
}
func (keys *IdempotencyKeys) Execute(ctx context.Context, key string, execute func() (interface{}, error)) (interface{}, error) {
	if keys == nil || key == "" || execute == nil {
		return nil, fmt.Errorf("idempotency key and executor are required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	keys.mu.Lock()
	defer keys.mu.Unlock()
	if value, ok := keys.values[key]; ok {
		return value, nil
	}
	value, err := execute()
	if err != nil {
		return nil, err
	}
	keys.values[key] = value
	return value, nil
}
