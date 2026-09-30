package hatSql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	// ErrSQLMaintainedRefreshRegistryNil reports a method call on a nil registry.
	ErrSQLMaintainedRefreshRegistryNil = errors.New("SQL maintained refresh registry is nil")
	// ErrSQLMaintainedRefreshRequired reports a missing refresh callback.
	ErrSQLMaintainedRefreshRequired = errors.New("SQL maintained refresh callback is required")
)

// SQLMaintainedRefreshFunc refreshes one invalidated maintained object. The
// object is detached from the registry and may be safely inspected or copied.
type SQLMaintainedRefreshFunc func(context.Context, SQLMaintainedObject) error

// SQLMaintainedRefreshRegistry couples the bounded M235 dependency graph to
// caller-owned refresh callbacks. RefreshChanged runs only callbacks selected
// by the changed source set; it does not refresh unrelated registrations.
type SQLMaintainedRefreshRegistry struct {
	mu        sync.RWMutex
	graph     *SQLDependencyGraph
	refreshes map[sqlDependencyGraphKey]SQLMaintainedRefreshFunc
}

// NewSQLMaintainedRefreshRegistry creates an empty on-demand refresh registry.
func NewSQLMaintainedRefreshRegistry() *SQLMaintainedRefreshRegistry {
	return &SQLMaintainedRefreshRegistry{
		graph:     NewSQLDependencyGraph(),
		refreshes: make(map[sqlDependencyGraphKey]SQLMaintainedRefreshFunc),
	}
}

// Register adds one maintained object and its refresh callback. Dependencies
// are copied and normalized by the underlying SQLDependencyGraph.
func (registry *SQLMaintainedRefreshRegistry) Register(kind SQLMaintainedObjectKind, name string, dependencies []string, refresh SQLMaintainedRefreshFunc) error {
	if registry == nil {
		return ErrSQLMaintainedRefreshRegistryNil
	}
	if refresh == nil {
		return ErrSQLMaintainedRefreshRequired
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
	registry.ensureLocked()
	if err := registry.graph.Register(normalizedKind, name, dependencies...); err != nil {
		return err
	}
	registry.refreshes[sqlDependencyGraphKey{kind: normalizedKind, name: name}] = refresh
	return nil
}

// Unregister removes one maintained object and its callback.
func (registry *SQLMaintainedRefreshRegistry) Unregister(kind SQLMaintainedObjectKind, name string) error {
	if registry == nil {
		return ErrSQLMaintainedRefreshRegistryNil
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
	registry.ensureLocked()
	if err := registry.graph.Unregister(normalizedKind, name); err != nil {
		return err
	}
	delete(registry.refreshes, sqlDependencyGraphKey{kind: normalizedKind, name: name})
	return nil
}

// RefreshChanged executes affected callbacks in the deterministic graph order.
// It returns callbacks completed before an error; callbacks already completed
// are not rolled back when a later callback fails.
func (registry *SQLMaintainedRefreshRegistry) RefreshChanged(ctx context.Context, changed []string) ([]SQLMaintainedObject, error) {
	return registry.RefreshChangedInto(ctx, nil, changed)
}

// RefreshChangedInto reuses dst's backing array for completed results. The
// registry snapshots callbacks before execution, so registration changes do
// not race with user code and do not affect the current refresh cycle.
func (registry *SQLMaintainedRefreshRegistry) RefreshChangedInto(ctx context.Context, dst []SQLMaintainedObject, changed []string) ([]SQLMaintainedObject, error) {
	if registry == nil {
		return nil, ErrSQLMaintainedRefreshRegistryNil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	registry.mu.RLock()
	if registry.graph == nil || len(registry.refreshes) == 0 {
		registry.mu.RUnlock()
		return dst[:0], nil
	}
	objects := registry.graph.AffectedInto(dst[:0], changed)
	var refreshStorage [16]SQLMaintainedRefreshFunc
	refreshes := refreshStorage[:len(objects)]
	if len(objects) > len(refreshStorage) {
		refreshes = make([]SQLMaintainedRefreshFunc, len(objects))
	}
	eligible := 0
	for _, object := range objects {
		key := sqlDependencyGraphKey{kind: object.Kind, name: object.Name}
		refresh, exists := registry.refreshes[key]
		if exists {
			objects[eligible] = object
			refreshes[eligible] = refresh
			eligible++
		}
	}
	registry.mu.RUnlock()

	objects = objects[:eligible]
	for index, object := range objects {
		if err := refreshes[index](ctx, object); err != nil {
			return objects[:index], fmt.Errorf("refresh %s %q: %w", object.Kind, object.Name, err)
		}
	}
	return objects, nil
}

// Snapshot returns the registered maintained objects in deterministic order.
func (registry *SQLMaintainedRefreshRegistry) Snapshot() []SQLMaintainedObject {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	if registry.graph == nil {
		registry.mu.RUnlock()
		return nil
	}
	objects := registry.graph.Snapshot()
	registry.mu.RUnlock()
	return objects
}

// Len returns the number of registered maintained objects.
func (registry *SQLMaintainedRefreshRegistry) Len() int {
	if registry == nil {
		return 0
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	if registry.graph == nil {
		return 0
	}
	return registry.graph.Len()
}

func (registry *SQLMaintainedRefreshRegistry) ensureLocked() {
	if registry.graph == nil {
		registry.graph = NewSQLDependencyGraph()
	}
	if registry.refreshes == nil {
		registry.refreshes = make(map[sqlDependencyGraphKey]SQLMaintainedRefreshFunc)
	}
}
