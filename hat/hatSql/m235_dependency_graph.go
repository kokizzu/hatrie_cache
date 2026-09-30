package hatSql

import (
	"errors"
	"sort"
	"strings"
	"sync"
)

// SQLMaintainedObjectKind identifies the maintained SQL object affected by a
// source change.
type SQLMaintainedObjectKind string

const (
	// SQLMaintainedObjectView identifies a materialized or incrementally
	// maintained view.
	SQLMaintainedObjectView SQLMaintainedObjectKind = "view"
	// SQLMaintainedObjectIndex identifies a maintained secondary index.
	SQLMaintainedObjectIndex SQLMaintainedObjectKind = "index"
	// MaxSQLDependencyGraphDependencies bounds reverse-index fanout per object.
	MaxSQLDependencyGraphDependencies = 256
)

var (
	ErrSQLDependencyGraphNil                 = errors.New("SQL dependency graph is nil")
	ErrSQLDependencyGraphKindRequired        = errors.New("SQL dependency graph object kind is required")
	ErrSQLDependencyGraphKindUnsupported     = errors.New("SQL dependency graph object kind is unsupported")
	ErrSQLDependencyGraphNameRequired        = errors.New("SQL dependency graph object name is required")
	ErrSQLDependencyGraphDependencyRequired  = errors.New("SQL dependency graph dependency is required")
	ErrSQLDependencyGraphDuplicateDependency = errors.New("SQL dependency graph dependency is duplicated")
	ErrSQLDependencyGraphDependencyLimit     = errors.New("SQL dependency graph dependency limit exceeded")
	ErrSQLDependencyGraphAlreadyRegistered   = errors.New("SQL dependency graph object is already registered")
	ErrSQLDependencyGraphNotFound            = errors.New("SQL dependency graph object is not registered")
)

// SQLMaintainedObject is a detached maintained-object registration. The
// dependency list is canonical and independently owned by the graph result.
type SQLMaintainedObject struct {
	Kind         SQLMaintainedObjectKind `json:"kind"`
	Name         string                  `json:"name"`
	Dependencies []string                `json:"dependencies"`
}

type sqlDependencyGraphKey struct {
	kind SQLMaintainedObjectKind
	name string
}

// SQLDependencyGraph indexes maintained views and indexes by source name.
// Register and Unregister update the reverse index atomically. Affected is
// proportional to changed sources and their dependents, rather than all
// registered objects.
type SQLDependencyGraph struct {
	mu         sync.RWMutex
	objects    map[sqlDependencyGraphKey]SQLMaintainedObject
	dependents map[string]map[sqlDependencyGraphKey]struct{}
}

// NewSQLDependencyGraph creates an empty maintained-object dependency graph.
func NewSQLDependencyGraph() *SQLDependencyGraph {
	return &SQLDependencyGraph{
		objects:    make(map[sqlDependencyGraphKey]SQLMaintainedObject),
		dependents: make(map[string]map[sqlDependencyGraphKey]struct{}),
	}
}

// Register adds one maintained view or index. Dependencies are trimmed,
// validated as unique, and sorted before publication.
func (graph *SQLDependencyGraph) Register(kind SQLMaintainedObjectKind, name string, dependencies ...string) error {
	if graph == nil {
		return ErrSQLDependencyGraphNil
	}
	normalizedKind, err := normalizeSQLMaintainedObjectKind(kind)
	if err != nil {
		return err
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return ErrSQLDependencyGraphNameRequired
	}
	if len(dependencies) == 0 {
		return ErrSQLDependencyGraphDependencyRequired
	}
	normalizedDependencies := make([]string, 0, len(dependencies))
	seen := make(map[string]struct{}, len(dependencies))
	for _, rawDependency := range dependencies {
		dependency := strings.TrimSpace(rawDependency)
		if dependency == "" {
			return ErrSQLDependencyGraphDependencyRequired
		}
		if _, exists := seen[dependency]; exists {
			return ErrSQLDependencyGraphDuplicateDependency
		}
		seen[dependency] = struct{}{}
		normalizedDependencies = append(normalizedDependencies, dependency)
	}
	if len(normalizedDependencies) > MaxSQLDependencyGraphDependencies {
		return ErrSQLDependencyGraphDependencyLimit
	}
	sort.Strings(normalizedDependencies)
	key := sqlDependencyGraphKey{kind: normalizedKind, name: name}
	object := SQLMaintainedObject{Kind: normalizedKind, Name: name, Dependencies: normalizedDependencies}

	graph.mu.Lock()
	defer graph.mu.Unlock()
	graph.ensureMapsLocked()
	if _, exists := graph.objects[key]; exists {
		return ErrSQLDependencyGraphAlreadyRegistered
	}
	graph.objects[key] = object
	for _, dependency := range normalizedDependencies {
		dependents := graph.dependents[dependency]
		if dependents == nil {
			dependents = make(map[sqlDependencyGraphKey]struct{})
			graph.dependents[dependency] = dependents
		}
		dependents[key] = struct{}{}
	}
	return nil
}

// Unregister removes one maintained view or index and its reverse-index
// entries.
func (graph *SQLDependencyGraph) Unregister(kind SQLMaintainedObjectKind, name string) error {
	if graph == nil {
		return ErrSQLDependencyGraphNil
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
	graph.mu.Lock()
	defer graph.mu.Unlock()
	object, exists := graph.objects[key]
	if !exists {
		return ErrSQLDependencyGraphNotFound
	}
	delete(graph.objects, key)
	for _, dependency := range object.Dependencies {
		dependents := graph.dependents[dependency]
		delete(dependents, key)
		if len(dependents) == 0 {
			delete(graph.dependents, dependency)
		}
	}
	return nil
}

// Affected returns maintained objects whose dependencies intersect changed.
// Results are sorted by kind and then name, and all returned slices are
// detached from graph-owned storage.
func (graph *SQLDependencyGraph) Affected(changed []string) []SQLMaintainedObject {
	return graph.AffectedInto(nil, changed)
}

// AffectedInto appends the affected-object result to reusable dst after first
// clearing it. Reusing dst avoids the result slice allocation in polling loops;
// graph-owned dependency slices are still cloned.
func (graph *SQLDependencyGraph) AffectedInto(dst []SQLMaintainedObject, changed []string) []SQLMaintainedObject {
	dst = dst[:0]
	if graph == nil || len(changed) == 0 {
		return dst
	}
	firstDependency := ""
	multipleDependencies := false
	for _, rawDependency := range changed {
		dependency := strings.TrimSpace(rawDependency)
		if dependency == "" {
			continue
		}
		if firstDependency == "" {
			firstDependency = dependency
		} else if firstDependency != dependency {
			multipleDependencies = true
			break
		}
	}
	if firstDependency == "" {
		return dst
	}
	graph.mu.RLock()
	if !multipleDependencies {
		for key := range graph.dependents[firstDependency] {
			object, exists := graph.objects[key]
			if !exists {
				continue
			}
			object.Dependencies = append([]string(nil), object.Dependencies...)
			dst = append(dst, object)
		}
		graph.mu.RUnlock()
		sort.Sort(sqlMaintainedObjectSlice(dst))
		return dst
	}
	candidateCapacity := 0
	for _, rawDependency := range changed {
		dependency := strings.TrimSpace(rawDependency)
		if dependency != "" {
			candidateCapacity += len(graph.dependents[dependency])
		}
	}
	keys := make([]sqlDependencyGraphKey, 0, candidateCapacity)
	for _, rawDependency := range changed {
		dependency := strings.TrimSpace(rawDependency)
		if dependency == "" {
			continue
		}
		for key := range graph.dependents[dependency] {
			keys = append(keys, key)
		}
	}
	sort.Sort(sqlDependencyGraphKeySlice(keys))
	for index, key := range keys {
		if index > 0 && keys[index-1] == key {
			continue
		}
		object, exists := graph.objects[key]
		if !exists {
			continue
		}
		object.Dependencies = append([]string(nil), object.Dependencies...)
		dst = append(dst, object)
	}
	graph.mu.RUnlock()
	sort.Sort(sqlMaintainedObjectSlice(dst))
	return dst
}

// Snapshot returns every registration in deterministic order.
func (graph *SQLDependencyGraph) Snapshot() []SQLMaintainedObject {
	if graph == nil {
		return nil
	}
	graph.mu.RLock()
	objects := make([]SQLMaintainedObject, 0, len(graph.objects))
	for _, object := range graph.objects {
		object.Dependencies = append([]string(nil), object.Dependencies...)
		objects = append(objects, object)
	}
	graph.mu.RUnlock()
	sort.Sort(sqlMaintainedObjectSlice(objects))
	return objects
}

// Len returns the number of registered maintained objects.
func (graph *SQLDependencyGraph) Len() int {
	if graph == nil {
		return 0
	}
	graph.mu.RLock()
	length := len(graph.objects)
	graph.mu.RUnlock()
	return length
}

func (graph *SQLDependencyGraph) ensureMapsLocked() {
	if graph.objects == nil {
		graph.objects = make(map[sqlDependencyGraphKey]SQLMaintainedObject)
	}
	if graph.dependents == nil {
		graph.dependents = make(map[string]map[sqlDependencyGraphKey]struct{})
	}
}

func normalizeSQLMaintainedObjectKind(kind SQLMaintainedObjectKind) (SQLMaintainedObjectKind, error) {
	kind = SQLMaintainedObjectKind(strings.TrimSpace(string(kind)))
	if kind == "" {
		return "", ErrSQLDependencyGraphKindRequired
	}
	if kind != SQLMaintainedObjectView && kind != SQLMaintainedObjectIndex {
		return "", ErrSQLDependencyGraphKindUnsupported
	}
	return kind, nil
}

type sqlMaintainedObjectSlice []SQLMaintainedObject

type sqlDependencyGraphKeySlice []sqlDependencyGraphKey

func (keys sqlDependencyGraphKeySlice) Len() int {
	return len(keys)
}

func (keys sqlDependencyGraphKeySlice) Less(left, right int) bool {
	if keys[left].kind != keys[right].kind {
		return keys[left].kind < keys[right].kind
	}
	return keys[left].name < keys[right].name
}

func (keys sqlDependencyGraphKeySlice) Swap(left, right int) {
	keys[left], keys[right] = keys[right], keys[left]
}

func (objects sqlMaintainedObjectSlice) Len() int {
	return len(objects)
}

func (objects sqlMaintainedObjectSlice) Less(left, right int) bool {
	if objects[left].Kind != objects[right].Kind {
		return objects[left].Kind < objects[right].Kind
	}
	return objects[left].Name < objects[right].Name
}

func (objects sqlMaintainedObjectSlice) Swap(left, right int) {
	objects[left], objects[right] = objects[right], objects[left]
}
