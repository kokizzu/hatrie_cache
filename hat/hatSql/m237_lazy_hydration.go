package hatSql

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

var (
	// ErrSQLLazyHydrationRegistryNil reports a method call on a nil registry.
	ErrSQLLazyHydrationRegistryNil = errors.New("SQL lazy hydration registry is nil")
	// ErrSQLLazyHydrationRequired reports a missing hydration callback.
	ErrSQLLazyHydrationRequired = errors.New("SQL lazy hydration callback is required")
	// ErrSQLLazyHydrationNotFound reports an unknown maintained object.
	ErrSQLLazyHydrationNotFound = errors.New("SQL lazy hydration object is not registered")
	// ErrSQLLazyHydrationAlreadyRegistered reports a duplicate object name/kind.
	ErrSQLLazyHydrationAlreadyRegistered = errors.New("SQL lazy hydration object is already registered")
	// ErrSQLLazyHydrationRunning reports an unregister while hydration is active.
	ErrSQLLazyHydrationRunning = errors.New("SQL lazy hydration object is running")
)

// SQLLazyHydrationFunc hydrates one maintained object for its first reader.
// The object is detached and callback-owned mutation cannot alter registry
// metadata.
type SQLLazyHydrationFunc func(context.Context, SQLMaintainedObject) error

type sqlLazyHydrationEntry struct {
	object      SQLMaintainedObject
	hydrate     SQLLazyHydrationFunc
	ready       bool
	invalidated bool
	running     bool
	runID       uint64
	lastErr     error
	done        chan struct{}
}

// SQLLazyHydrationRegistry runs one hydration callback on demand. Concurrent
// readers of the same object share the active callback; unrelated objects can
// hydrate independently.
type SQLLazyHydrationRegistry struct {
	mu      sync.Mutex
	entries map[sqlDependencyGraphKey]*sqlLazyHydrationEntry
}

// NewSQLLazyHydrationRegistry creates an empty lazy hydration registry.
func NewSQLLazyHydrationRegistry() *SQLLazyHydrationRegistry {
	return &SQLLazyHydrationRegistry{entries: make(map[sqlDependencyGraphKey]*sqlLazyHydrationEntry)}
}

// Register adds one maintained object and its first-reader hydration callback.
// The object kind, name, and dependencies use SQLDependencyGraph validation.
func (registry *SQLLazyHydrationRegistry) Register(object SQLMaintainedObject, hydrate SQLLazyHydrationFunc) error {
	if registry == nil {
		return ErrSQLLazyHydrationRegistryNil
	}
	if hydrate == nil {
		return ErrSQLLazyHydrationRequired
	}
	normalized, err := normalizeSQLLazyHydrationObject(object)
	if err != nil {
		return err
	}
	key := sqlDependencyGraphKey{kind: normalized.Kind, name: normalized.Name}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.entries == nil {
		registry.entries = make(map[sqlDependencyGraphKey]*sqlLazyHydrationEntry)
	}
	if _, exists := registry.entries[key]; exists {
		return ErrSQLLazyHydrationAlreadyRegistered
	}
	registry.entries[key] = &sqlLazyHydrationEntry{object: normalized, hydrate: hydrate}
	return nil
}

// Ensure hydrates one object if it is not already ready. A waiting reader may
// cancel its own wait without canceling the owner callback.
func (registry *SQLLazyHydrationRegistry) Ensure(ctx context.Context, kind SQLMaintainedObjectKind, name string) error {
	if registry == nil {
		return ErrSQLLazyHydrationRegistryNil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	normalizedKind, err := normalizeSQLMaintainedObjectKind(kind)
	if err != nil {
		return err
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return ErrSQLDependencyGraphNameRequired
	}
	key := sqlDependencyGraphKey{kind: normalizedKind, name: name}

	for {
		registry.mu.Lock()
		entry, exists := registry.entries[key]
		if !exists {
			registry.mu.Unlock()
			return ErrSQLLazyHydrationNotFound
		}
		if entry.ready {
			registry.mu.Unlock()
			return nil
		}
		if entry.running {
			done := entry.done
			runID := entry.runID
			registry.mu.Unlock()
			select {
			case <-done:
				registry.mu.Lock()
				if current, stillRegistered := registry.entries[key]; stillRegistered && current == entry && current.runID == runID {
					err := current.lastErr
					ready := current.ready
					registry.mu.Unlock()
					if err != nil {
						return err
					}
					if ready {
						return nil
					}
				} else {
					registry.mu.Unlock()
				}
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		entry.running = true
		entry.runID++
		entry.lastErr = nil
		entry.done = make(chan struct{})
		runID := entry.runID
		hydrate := entry.hydrate
		object := cloneSQLLazyHydrationObject(entry.object)
		registry.mu.Unlock()

		err := hydrate(ctx, object)

		registry.mu.Lock()
		if current, stillRegistered := registry.entries[key]; stillRegistered && current == entry && current.runID == runID {
			entry.running = false
			entry.lastErr = err
			if err == nil && !entry.invalidated {
				entry.ready = true
			}
			entry.invalidated = false
			close(entry.done)
		}
		registry.mu.Unlock()
		if err != nil {
			return fmt.Errorf("hydrate %s %q: %w", object.Kind, object.Name, err)
		}
		return nil
	}
}

// Invalidate marks an object not ready. If hydration is active, its successful
// result is not admitted as ready and the next reader hydrates again.
func (registry *SQLLazyHydrationRegistry) Invalidate(kind SQLMaintainedObjectKind, name string) error {
	if registry == nil {
		return ErrSQLLazyHydrationRegistryNil
	}
	normalizedKind, err := normalizeSQLMaintainedObjectKind(kind)
	if err != nil {
		return err
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return ErrSQLDependencyGraphNameRequired
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	entry, exists := registry.entries[sqlDependencyGraphKey{kind: normalizedKind, name: name}]
	if !exists {
		return ErrSQLLazyHydrationNotFound
	}
	entry.ready = false
	if entry.running {
		entry.invalidated = true
	} else {
		entry.lastErr = nil
	}
	return nil
}

// Unregister removes an object that is not currently hydrating.
func (registry *SQLLazyHydrationRegistry) Unregister(kind SQLMaintainedObjectKind, name string) error {
	if registry == nil {
		return ErrSQLLazyHydrationRegistryNil
	}
	normalizedKind, err := normalizeSQLMaintainedObjectKind(kind)
	if err != nil {
		return err
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return ErrSQLDependencyGraphNameRequired
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	key := sqlDependencyGraphKey{kind: normalizedKind, name: name}
	entry, exists := registry.entries[key]
	if !exists {
		return ErrSQLLazyHydrationNotFound
	}
	if entry.running {
		return ErrSQLLazyHydrationRunning
	}
	delete(registry.entries, key)
	return nil
}

// Snapshot returns detached object metadata in deterministic kind/name order.
func (registry *SQLLazyHydrationRegistry) Snapshot() []SQLMaintainedObject {
	if registry == nil {
		return nil
	}
	registry.mu.Lock()
	objects := make([]SQLMaintainedObject, 0, len(registry.entries))
	for _, entry := range registry.entries {
		objects = append(objects, cloneSQLLazyHydrationObject(entry.object))
	}
	registry.mu.Unlock()
	sort.Sort(sqlMaintainedObjectSlice(objects))
	return objects
}

// Len returns the number of registered objects.
func (registry *SQLLazyHydrationRegistry) Len() int {
	if registry == nil {
		return 0
	}
	registry.mu.Lock()
	length := len(registry.entries)
	registry.mu.Unlock()
	return length
}

func normalizeSQLLazyHydrationObject(object SQLMaintainedObject) (SQLMaintainedObject, error) {
	graph := NewSQLDependencyGraph()
	if err := graph.Register(object.Kind, object.Name, object.Dependencies...); err != nil {
		return SQLMaintainedObject{}, err
	}
	objects := graph.Snapshot()
	return objects[0], nil
}

func cloneSQLLazyHydrationObject(object SQLMaintainedObject) SQLMaintainedObject {
	object.Dependencies = append([]string(nil), object.Dependencies...)
	return object
}
