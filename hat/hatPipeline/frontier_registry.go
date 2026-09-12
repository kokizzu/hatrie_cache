package hatPipeline

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

const (
	// DefaultFrontierMaxObjects bounds the default number of named frontiers.
	DefaultFrontierMaxObjects = 1024
	maxFrontierObjects        = 1 << 20
)

var (
	ErrFrontierClosed            = errors.New("hatPipeline: frontier registry is closed")
	ErrFrontierIDEmpty           = errors.New("hatPipeline: frontier ID is empty")
	ErrFrontierAlreadyRegistered = errors.New("hatPipeline: frontier is already registered")
	ErrFrontierNotFound          = errors.New("hatPipeline: frontier is not registered")
	ErrFrontierObjectLimit       = errors.New("hatPipeline: frontier object limit reached")
	ErrFrontierOrderInvalid      = errors.New("hatPipeline: frontier lower exceeds upper")
	ErrFrontierRegression        = errors.New("hatPipeline: frontier regressed")
	ErrFrontierOptionsInvalid    = errors.New("hatPipeline: frontier registry options are invalid")
)

// FrontierRegistryOptions bounds named frontier state.
type FrontierRegistryOptions struct {
	// MaxObjects is the maximum number of registered frontier IDs. Zero uses
	// DefaultFrontierMaxObjects.
	MaxObjects int
}

// FrontierSnapshot is a point-in-time lower/upper frontier observation.
// Lower and Upper are monotone and Lower is never greater than Upper.
type FrontierSnapshot struct {
	ID         string
	Lower      uint64
	Upper      uint64
	Generation uint64
	UpdatedAt  time.Time
}

type frontierObject struct {
	mu         sync.Mutex
	id         string
	lower      uint64
	upper      uint64
	generation uint64
	updatedAt  time.Time
	notify     chan struct{}
}

// FrontierRegistry exposes named lower/upper frontiers for external dataflow
// consumers. It is a control-plane primitive and does not run a worker.
type FrontierRegistry struct {
	mu         sync.RWMutex
	objects    map[string]*frontierObject
	maxObjects int
	closed     bool
}

// NewFrontierRegistry creates a bounded named-frontier registry.
func NewFrontierRegistry(options FrontierRegistryOptions) (*FrontierRegistry, error) {
	maxObjects := options.MaxObjects
	if maxObjects == 0 {
		maxObjects = DefaultFrontierMaxObjects
	}
	if maxObjects < 1 || maxObjects > maxFrontierObjects {
		return nil, ErrFrontierOptionsInvalid
	}
	return &FrontierRegistry{
		objects:    make(map[string]*frontierObject),
		maxObjects: maxObjects,
	}, nil
}

// Register adds an ID with lower and upper frontiers at zero.
func (registry *FrontierRegistry) Register(id string) error {
	if registry == nil {
		return ErrFrontierClosed
	}
	if id == "" {
		return ErrFrontierIDEmpty
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrFrontierClosed
	}
	if registry.objects == nil {
		registry.objects = make(map[string]*frontierObject)
	}
	if registry.maxObjects == 0 {
		registry.maxObjects = DefaultFrontierMaxObjects
	}
	if _, exists := registry.objects[id]; exists {
		return ErrFrontierAlreadyRegistered
	}
	if len(registry.objects) >= registry.maxObjects {
		return ErrFrontierObjectLimit
	}
	registry.objects[id] = &frontierObject{
		id:        id,
		updatedAt: time.Now().UTC(),
	}
	return nil
}

// Unregister removes a named frontier and wakes consumers waiting on it.
func (registry *FrontierRegistry) Unregister(id string) error {
	if registry == nil {
		return ErrFrontierClosed
	}
	if id == "" {
		return ErrFrontierIDEmpty
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrFrontierClosed
	}
	object := registry.objects[id]
	if object == nil {
		return ErrFrontierNotFound
	}
	delete(registry.objects, id)
	object.mu.Lock()
	if object.notify != nil {
		close(object.notify)
		object.notify = nil
	}
	object.mu.Unlock()
	return nil
}

// Advance publishes a monotone pair of lower and upper frontiers. Equal
// values are idempotent and do not advance Generation.
func (registry *FrontierRegistry) Advance(id string, lower, upper uint64) error {
	if registry == nil {
		return ErrFrontierClosed
	}
	if id == "" {
		return ErrFrontierIDEmpty
	}
	if lower > upper {
		return ErrFrontierOrderInvalid
	}
	object, err := registry.object(id)
	if err != nil {
		return err
	}
	object.mu.Lock()
	defer object.mu.Unlock()
	if lower < object.lower || upper < object.upper {
		return ErrFrontierRegression
	}
	if lower == object.lower && upper == object.upper {
		return nil
	}
	object.lower = lower
	object.upper = upper
	object.generation++
	object.updatedAt = time.Now().UTC()
	if object.notify != nil {
		close(object.notify)
		object.notify = nil
	}
	return nil
}

// Snapshot returns one named frontier without exposing mutable state.
func (registry *FrontierRegistry) Snapshot(id string) (FrontierSnapshot, bool) {
	object := registry.lookup(id)
	if object == nil {
		return FrontierSnapshot{}, false
	}
	return object.snapshot(), true
}

// SnapshotAll returns all named frontiers sorted by ID.
func (registry *FrontierRegistry) SnapshotAll() []FrontierSnapshot {
	if registry == nil {
		return []FrontierSnapshot{}
	}
	registry.mu.RLock()
	objects := make([]*frontierObject, 0, len(registry.objects))
	for _, object := range registry.objects {
		objects = append(objects, object)
	}
	registry.mu.RUnlock()
	sort.Slice(objects, func(i, j int) bool { return objects[i].id < objects[j].id })

	snapshots := make([]FrontierSnapshot, 0, len(objects))
	for _, object := range objects {
		snapshots = append(snapshots, object.snapshot())
	}
	return snapshots
}

// Covers reports whether the named frontier has closed through timestamp.
func (registry *FrontierRegistry) Covers(id string, timestamp uint64) bool {
	snapshot, ok := registry.Snapshot(id)
	return ok && snapshot.Lower >= timestamp
}

// WaitUntil waits until lower reaches target, the frontier is removed/closed,
// or ctx is canceled. Target zero is immediately satisfied.
func (registry *FrontierRegistry) WaitUntil(ctx context.Context, id string, target uint64) error {
	if registry == nil {
		return ErrFrontierClosed
	}
	if id == "" {
		return ErrFrontierIDEmpty
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		registry.mu.RLock()
		closed := registry.closed
		object := registry.objects[id]
		registry.mu.RUnlock()
		if closed {
			return ErrFrontierClosed
		}
		if object == nil {
			return ErrFrontierNotFound
		}
		object.mu.Lock()
		lower := object.lower
		if lower >= target {
			object.mu.Unlock()
			return nil
		}
		if err := ctx.Err(); err != nil {
			object.mu.Unlock()
			return err
		}
		if object.notify == nil {
			object.notify = make(chan struct{})
		}
		notify := object.notify
		object.mu.Unlock()
		select {
		case <-notify:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Close prevents registration and advances, waking all current waiters.
func (registry *FrontierRegistry) Close() error {
	if registry == nil {
		return nil
	}
	registry.mu.Lock()
	if registry.closed {
		registry.mu.Unlock()
		return nil
	}
	registry.closed = true
	objects := make([]*frontierObject, 0, len(registry.objects))
	for _, object := range registry.objects {
		objects = append(objects, object)
	}
	registry.mu.Unlock()
	for _, object := range objects {
		object.mu.Lock()
		if object.notify != nil {
			close(object.notify)
			object.notify = nil
		}
		object.mu.Unlock()
	}
	return nil
}

func (registry *FrontierRegistry) object(id string) (*frontierObject, error) {
	registry.mu.RLock()
	if registry.closed {
		registry.mu.RUnlock()
		return nil, ErrFrontierClosed
	}
	object := registry.objects[id]
	registry.mu.RUnlock()
	if object == nil {
		return nil, ErrFrontierNotFound
	}
	return object, nil
}

func (registry *FrontierRegistry) lookup(id string) *frontierObject {
	if registry == nil || id == "" {
		return nil
	}
	registry.mu.RLock()
	object := registry.objects[id]
	registry.mu.RUnlock()
	return object
}

func (object *frontierObject) snapshot() FrontierSnapshot {
	object.mu.Lock()
	snapshot := FrontierSnapshot{
		ID:         object.id,
		Lower:      object.lower,
		Upper:      object.upper,
		Generation: object.generation,
		UpdatedAt:  object.updatedAt,
	}
	object.mu.Unlock()
	return snapshot
}
