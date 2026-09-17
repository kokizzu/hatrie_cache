package hatSql

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

const (
	// CatalogObjectKindSource identifies a source object in the catalog.
	CatalogObjectKindSource = "source"
	// CatalogObjectKindView identifies a view object in the catalog.
	CatalogObjectKindView = "view"
	// CatalogObjectKindIndex identifies an index object in the catalog.
	CatalogObjectKindIndex = "index"
	// CatalogObjectKindSink identifies a sink object in the catalog.
	CatalogObjectKindSink = "sink"

	// DefaultCatalogDependencyMaxDepth bounds a dependency closure when the
	// caller does not provide a traversal limit.
	DefaultCatalogDependencyMaxDepth = 64
	// DefaultCatalogDependencyMaxRows bounds the number of closure edges when
	// the caller does not provide a traversal limit.
	DefaultCatalogDependencyMaxRows = 1 << 16
)

var (
	// ErrCatalogDependencyInvalid indicates malformed dependency metadata or
	// traversal options.
	ErrCatalogDependencyInvalid = errors.New("catalog dependency is invalid")
	// ErrCatalogDependencyDepthExceeded indicates an incomplete closure caused
	// by the configured maximum depth.
	ErrCatalogDependencyDepthExceeded = errors.New("catalog dependency depth exceeded")
	// ErrCatalogDependencyLimitExceeded indicates an incomplete closure caused
	// by the configured maximum edge count.
	ErrCatalogDependencyLimitExceeded = errors.New("catalog dependency row limit exceeded")
)

// CatalogObject describes one versioned object in a Catalog snapshot.
type CatalogObject struct {
	Namespace string
	Name      string
	Kind      string
	Type      string
	Version   uint64
	State     string
}

// CatalogDependency describes one direct object-to-object dependency edge.
type CatalogDependency struct {
	Namespace          string
	Object             string
	ObjectKind         string
	DependsOnNamespace string
	DependsOn          string
	DependsOnKind      string
}

// CatalogObjectRef identifies an object for dependency traversal.
type CatalogObjectRef struct {
	Namespace string
	Name      string
	Kind      string
}

// CatalogDependencyPath is one edge discovered by DependencyClosure.
type CatalogDependencyPath struct {
	Root      CatalogObjectRef
	Object    CatalogObjectRef
	DependsOn CatalogObjectRef
	Depth     int
}

// CatalogDependencyTraversalOptions bounds dependency closure work. Zero
// values select the conservative package defaults.
type CatalogDependencyTraversalOptions struct {
	MaxDepth int
	MaxRows  int
}

type catalogDependencyEdge struct {
	object    CatalogObjectRef
	dependsOn CatalogObjectRef
}

type catalogDependencyTraversalNode struct {
	ref   CatalogObjectRef
	depth int
}

// DependencyClosure returns each reachable direct dependency edge once in
// deterministic breadth-first order. Cycles are reported as edges but never
// revisited, and a limit error is returned whenever the result would be
// incomplete.
func (catalog Catalog) DependencyClosure(root CatalogObjectRef, options CatalogDependencyTraversalOptions) ([]CatalogDependencyPath, error) {
	root = normalizeCatalogObjectRef(root)
	if root.Name == "" || root.Kind == "" {
		return nil, fmt.Errorf("%w: root object requires name and kind", ErrCatalogDependencyInvalid)
	}
	maxDepth, maxRows, err := normalizeCatalogDependencyTraversalOptions(options)
	if err != nil {
		return nil, err
	}
	edges, err := catalogDependencyEdges(catalog)
	if err != nil {
		return nil, err
	}
	adjacency := make(map[CatalogObjectRef][]CatalogObjectRef, len(edges))
	for _, edge := range edges {
		adjacency[edge.object] = append(adjacency[edge.object], edge.dependsOn)
	}

	paths := make([]CatalogDependencyPath, 0)
	queue := []catalogDependencyTraversalNode{{ref: root}}
	visited := map[CatalogObjectRef]struct{}{root: {}}
	for head := 0; head < len(queue); head++ {
		current := queue[head]
		outgoing := adjacency[current.ref]
		if current.depth >= maxDepth && len(outgoing) != 0 {
			return nil, fmt.Errorf("%w: %s.%s at depth %d", ErrCatalogDependencyDepthExceeded, current.ref.Namespace, current.ref.Name, current.depth)
		}
		for _, dependsOn := range outgoing {
			if len(paths) >= maxRows {
				return nil, fmt.Errorf("%w: maximum %d edges", ErrCatalogDependencyLimitExceeded, maxRows)
			}
			paths = append(paths, CatalogDependencyPath{
				Root:      root,
				Object:    current.ref,
				DependsOn: dependsOn,
				Depth:     current.depth + 1,
			})
			if _, exists := visited[dependsOn]; exists {
				continue
			}
			visited[dependsOn] = struct{}{}
			queue = append(queue, catalogDependencyTraversalNode{ref: dependsOn, depth: current.depth + 1})
		}
	}
	return paths, nil
}

func normalizeCatalogDependencyTraversalOptions(options CatalogDependencyTraversalOptions) (int, int, error) {
	if options.MaxDepth < 0 || options.MaxRows < 0 {
		return 0, 0, fmt.Errorf("%w: traversal limits must not be negative", ErrCatalogDependencyInvalid)
	}
	maxDepth := options.MaxDepth
	if maxDepth == 0 {
		maxDepth = DefaultCatalogDependencyMaxDepth
	}
	maxRows := options.MaxRows
	if maxRows == 0 {
		maxRows = DefaultCatalogDependencyMaxRows
	}
	return maxDepth, maxRows, nil
}

func normalizeCatalogObjectRef(ref CatalogObjectRef) CatalogObjectRef {
	ref.Namespace = strings.TrimSpace(ref.Namespace)
	ref.Name = strings.TrimSpace(ref.Name)
	ref.Kind = strings.ToLower(strings.TrimSpace(ref.Kind))
	return ref
}

func catalogObjects(catalog Catalog) ([]CatalogObject, error) {
	version := catalogVersion(catalog)
	objects := make(map[CatalogObjectRef]CatalogObject, len(catalog.Objects)+len(catalog.Sources)+len(catalog.Indexes))
	for _, object := range catalog.Objects {
		normalized, err := normalizeCatalogObject(object, version)
		if err != nil {
			return nil, err
		}
		ref := CatalogObjectRef{Namespace: normalized.Namespace, Name: normalized.Name, Kind: normalized.Kind}
		if _, exists := objects[ref]; exists {
			return nil, fmt.Errorf("%w: duplicate object %s.%s (%s)", ErrCatalogDependencyInvalid, ref.Namespace, ref.Name, ref.Kind)
		}
		objects[ref] = normalized
	}
	for _, source := range catalog.Sources {
		if strings.TrimSpace(source.Name) == "" {
			continue
		}
		object := CatalogObject{
			Namespace: source.Namespace,
			Name:      source.Name,
			Kind:      CatalogObjectKindSource,
			Type:      source.Kind,
			Version:   version,
			State:     "ready",
		}
		if err := addDerivedCatalogObject(objects, object); err != nil {
			return nil, err
		}
	}
	for _, index := range catalog.Indexes {
		if strings.TrimSpace(index.Name) == "" {
			continue
		}
		object := CatalogObject{
			Namespace: index.Namespace,
			Name:      index.Name,
			Kind:      CatalogObjectKindIndex,
			Type:      index.Kind,
			Version:   version,
			State:     "ready",
		}
		if err := addDerivedCatalogObject(objects, object); err != nil {
			return nil, err
		}
	}
	result := make([]CatalogObject, 0, len(objects))
	for _, object := range objects {
		result = append(result, object)
	}
	sort.Slice(result, func(left, right int) bool {
		return catalogObjectLess(result[left], result[right])
	})
	return result, nil
}

func normalizeCatalogObject(object CatalogObject, catalogVersion uint64) (CatalogObject, error) {
	object.Namespace = strings.TrimSpace(object.Namespace)
	object.Name = strings.TrimSpace(object.Name)
	object.Kind = strings.ToLower(strings.TrimSpace(object.Kind))
	object.Type = strings.TrimSpace(object.Type)
	object.State = strings.TrimSpace(object.State)
	if object.Name == "" || object.Kind == "" {
		return CatalogObject{}, fmt.Errorf("%w: object requires name and kind", ErrCatalogDependencyInvalid)
	}
	if object.Version == 0 {
		object.Version = catalogVersion
	}
	if object.State == "" {
		object.State = "ready"
	}
	return object, nil
}

func addDerivedCatalogObject(objects map[CatalogObjectRef]CatalogObject, object CatalogObject) error {
	normalized, err := normalizeCatalogObject(object, object.Version)
	if err != nil {
		return err
	}
	ref := CatalogObjectRef{Namespace: normalized.Namespace, Name: normalized.Name, Kind: normalized.Kind}
	if _, exists := objects[ref]; !exists {
		objects[ref] = normalized
	}
	return nil
}

func catalogObjectLess(left, right CatalogObject) bool {
	if left.Namespace != right.Namespace {
		return left.Namespace < right.Namespace
	}
	if left.Name != right.Name {
		return left.Name < right.Name
	}
	return left.Kind < right.Kind
}

func catalogDependencyEdges(catalog Catalog) ([]catalogDependencyEdge, error) {
	edges := make([]catalogDependencyEdge, 0, len(catalog.Dependencies)+len(catalog.Indexes))
	seen := make(map[catalogDependencyEdge]struct{}, cap(edges))
	add := func(dependency CatalogDependency) error {
		dependency.Namespace = strings.TrimSpace(dependency.Namespace)
		dependency.Object = strings.TrimSpace(dependency.Object)
		dependency.ObjectKind = strings.ToLower(strings.TrimSpace(dependency.ObjectKind))
		dependency.DependsOnNamespace = strings.TrimSpace(dependency.DependsOnNamespace)
		dependency.DependsOn = strings.TrimSpace(dependency.DependsOn)
		dependency.DependsOnKind = strings.ToLower(strings.TrimSpace(dependency.DependsOnKind))
		if dependency.Object == "" || dependency.ObjectKind == "" || dependency.DependsOn == "" {
			return fmt.Errorf("%w: dependency requires object, object kind, and dependency name", ErrCatalogDependencyInvalid)
		}
		edge := catalogDependencyEdge{
			object:    CatalogObjectRef{Namespace: dependency.Namespace, Name: dependency.Object, Kind: dependency.ObjectKind},
			dependsOn: CatalogObjectRef{Namespace: dependency.DependsOnNamespace, Name: dependency.DependsOn, Kind: dependency.DependsOnKind},
		}
		if _, exists := seen[edge]; exists {
			return nil
		}
		seen[edge] = struct{}{}
		edges = append(edges, edge)
		return nil
	}
	for _, dependency := range catalog.Dependencies {
		if err := add(dependency); err != nil {
			return nil, err
		}
	}
	for _, index := range catalog.Indexes {
		if strings.TrimSpace(index.Name) == "" || strings.TrimSpace(index.Source) == "" {
			continue
		}
		if err := add(CatalogDependency{
			Namespace:          index.Namespace,
			Object:             index.Name,
			ObjectKind:         CatalogObjectKindIndex,
			DependsOnNamespace: index.Namespace,
			DependsOn:          index.Source,
			DependsOnKind:      CatalogObjectKindSource,
		}); err != nil {
			return nil, err
		}
	}
	sort.Slice(edges, func(left, right int) bool {
		if catalogObjectRefLess(edges[left].object, edges[right].object) {
			return true
		}
		if catalogObjectRefLess(edges[right].object, edges[left].object) {
			return false
		}
		return catalogObjectRefLess(edges[left].dependsOn, edges[right].dependsOn)
	})
	return edges, nil
}

func catalogObjectRefLess(left, right CatalogObjectRef) bool {
	if left.Namespace != right.Namespace {
		return left.Namespace < right.Namespace
	}
	if left.Name != right.Name {
		return left.Name < right.Name
	}
	return left.Kind < right.Kind
}

func catalogVersion(catalog Catalog) uint64 {
	if catalog.Version == 0 {
		return 1
	}
	return catalog.Version
}

func catalogObjectRows(catalog Catalog) ([]Row, error) {
	objects, err := catalogObjects(catalog)
	if err != nil {
		return nil, err
	}
	version := catalogVersion(catalog)
	rows := make([]Row, len(objects))
	for index, object := range objects {
		rows[index] = Row{
			"catalog_version": uint64(version),
			"namespace":       object.Namespace,
			"name":            object.Name,
			"kind":            object.Kind,
			"type":            object.Type,
			"object_version":  uint64(object.Version),
			"state":           object.State,
		}
	}
	return rows, nil
}

func catalogDependencyRows(catalog Catalog) ([]Row, error) {
	edges, err := catalogDependencyEdges(catalog)
	if err != nil {
		return nil, err
	}
	version := catalogVersion(catalog)
	positions := make(map[CatalogObjectRef]int)
	rows := make([]Row, len(edges))
	for index, edge := range edges {
		position := positions[edge.object] + 1
		positions[edge.object] = position
		rows[index] = Row{
			"catalog_version":      uint64(version),
			"namespace":            edge.object.Namespace,
			"object":               edge.object.Name,
			"object_kind":          edge.object.Kind,
			"depends_on_namespace": edge.dependsOn.Namespace,
			"depends_on":           edge.dependsOn.Name,
			"depends_on_kind":      edge.dependsOn.Kind,
			"ordinal_position":     int64(position),
		}
	}
	return rows, nil
}
