package hatDataStructure

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

var (
	ErrRTreeSpaceCatalogDefinition = errors.New("invalid r-tree space index definition")
	ErrRTreeSpaceCatalogExists     = errors.New("r-tree space index already exists")
	ErrRTreeSpaceCatalogNotFound   = errors.New("r-tree space index not found")
	ErrRTreeSpaceCatalogRebuilding = errors.New("r-tree space index is rebuilding")
	ErrRTreeSpaceCatalogNil        = errors.New("nil r-tree space catalog")
)

// RTreeSpaceIndexState describes the lifecycle state of a named spatial index.
type RTreeSpaceIndexState uint8

const (
	RTreeSpaceReady RTreeSpaceIndexState = iota + 1
	RTreeSpaceRebuilding
)

// String returns the stable text form used by diagnostics and planners.
func (state RTreeSpaceIndexState) String() string {
	switch state {
	case RTreeSpaceReady:
		return "ready"
	case RTreeSpaceRebuilding:
		return "rebuilding"
	default:
		return "unknown"
	}
}

// RTreeSpaceIndexDefinition describes how a row participates in a named
// spatial index. A false indexed result removes the row from the index.
type RTreeSpaceIndexDefinition[T any] struct {
	Name            string
	BoundsName      string
	MaxEntries      int
	BoundsExtractor func(value T) (bounds RTreeBounds, indexed bool, err error)
}

// RTreeSpaceRow is one row supplied to a space-index rebuild.
type RTreeSpaceRow[T any] struct {
	ID    uint64
	Value T
}

// RTreeSpaceMetadata is planner-visible state for a named spatial index.
type RTreeSpaceMetadata struct {
	Name       string
	BoundsName string
	State      RTreeSpaceIndexState
	Generation uint64
	Entries    int
	MaxEntries int
}

type rTreeSpaceIndex[T any] struct {
	definition RTreeSpaceIndexDefinition[T]
	tree       *RTree
	state      RTreeSpaceIndexState
	generation uint64
}

// RTreeSpaceCatalog owns named R-trees and keeps their membership in sync with
// row writes. The zero value is ready for use.
type RTreeSpaceCatalog[T any] struct {
	mu      sync.RWMutex
	indexes map[string]*rTreeSpaceIndex[T]
}

// NewRTreeSpaceCatalog creates an empty named spatial-index catalog.
func NewRTreeSpaceCatalog[T any]() *RTreeSpaceCatalog[T] {
	return &RTreeSpaceCatalog[T]{
		indexes: make(map[string]*rTreeSpaceIndex[T]),
	}
}

// Create adds a named spatial index. Names are trimmed and must be unique.
func (catalog *RTreeSpaceCatalog[T]) Create(definition RTreeSpaceIndexDefinition[T]) error {
	if catalog == nil {
		return ErrRTreeSpaceCatalogNil
	}
	definition, err := normalizeRTreeSpaceDefinition(definition)
	if err != nil {
		return err
	}
	tree, err := NewRTree(definition.MaxEntries)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrRTreeSpaceCatalogDefinition, err)
	}

	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	if catalog.indexes == nil {
		catalog.indexes = make(map[string]*rTreeSpaceIndex[T])
	}
	if _, exists := catalog.indexes[definition.Name]; exists {
		return fmt.Errorf("%w: %s", ErrRTreeSpaceCatalogExists, definition.Name)
	}
	catalog.indexes[definition.Name] = &rTreeSpaceIndex[T]{
		definition: definition,
		tree:       tree,
		state:      RTreeSpaceReady,
		generation: 1,
	}
	return nil
}

// Upsert inserts or replaces a row's spatial membership. A row whose
// extractor returns false is removed from the index.
func (catalog *RTreeSpaceCatalog[T]) Upsert(name string, id uint64, value T) error {
	if catalog == nil {
		return ErrRTreeSpaceCatalogNil
	}
	name = strings.TrimSpace(name)
	catalog.mu.RLock()
	index, err := catalog.indexLocked(name)
	if err != nil {
		catalog.mu.RUnlock()
		return err
	}
	if index.state != RTreeSpaceReady {
		catalog.mu.RUnlock()
		return ErrRTreeSpaceCatalogRebuilding
	}
	bounds, indexed, err := index.definition.BoundsExtractor(value)
	if err != nil {
		catalog.mu.RUnlock()
		return fmt.Errorf("extract %s bounds: %w", name, err)
	}
	if !indexed {
		index.tree.Delete(id)
		catalog.mu.RUnlock()
		return nil
	}
	err = index.tree.Upsert(id, bounds)
	catalog.mu.RUnlock()
	return err
}

// Delete removes a row from a named spatial index. It reports whether the row
// was present in the index.
func (catalog *RTreeSpaceCatalog[T]) Delete(name string, id uint64) (bool, error) {
	if catalog == nil {
		return false, ErrRTreeSpaceCatalogNil
	}
	name = strings.TrimSpace(name)
	catalog.mu.RLock()
	defer catalog.mu.RUnlock()
	index, err := catalog.indexLocked(name)
	if err != nil {
		return false, err
	}
	if index.state != RTreeSpaceReady {
		return false, ErrRTreeSpaceCatalogRebuilding
	}
	return index.tree.Delete(id), nil
}

// Search returns the sorted IDs whose bounds intersect the query bounds.
func (catalog *RTreeSpaceCatalog[T]) Search(name string, bounds RTreeBounds) ([]uint64, error) {
	if catalog == nil {
		return nil, ErrRTreeSpaceCatalogNil
	}
	name = strings.TrimSpace(name)
	catalog.mu.RLock()
	defer catalog.mu.RUnlock()
	index, err := catalog.indexLocked(name)
	if err != nil {
		return nil, err
	}
	return index.tree.Search(bounds)
}

// Rebuild replaces a named spatial index from a row snapshot. The old tree
// remains readable while the replacement is built; writes are fenced until
// the replacement is atomically installed.
func (catalog *RTreeSpaceCatalog[T]) Rebuild(name string, rows []RTreeSpaceRow[T]) error {
	if catalog == nil {
		return ErrRTreeSpaceCatalogNil
	}
	name = strings.TrimSpace(name)
	rows = append([]RTreeSpaceRow[T](nil), rows...)

	catalog.mu.Lock()
	index, err := catalog.indexLocked(name)
	if err != nil {
		catalog.mu.Unlock()
		return err
	}
	if index.state != RTreeSpaceReady {
		catalog.mu.Unlock()
		return ErrRTreeSpaceCatalogRebuilding
	}
	index.state = RTreeSpaceRebuilding
	definition := index.definition
	catalog.mu.Unlock()

	replacement, err := NewRTree(definition.MaxEntries)
	if err == nil {
		for _, row := range rows {
			var bounds RTreeBounds
			var indexed bool
			bounds, indexed, err = definition.BoundsExtractor(row.Value)
			if err != nil {
				err = fmt.Errorf("extract %s bounds: %w", name, err)
				break
			}
			if !indexed {
				continue
			}
			err = replacement.Upsert(row.ID, bounds)
			if err != nil {
				break
			}
		}
	}

	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	if err != nil {
		if current, exists := catalog.indexes[name]; exists && current == index {
			current.state = RTreeSpaceReady
		}
		return err
	}
	if current, exists := catalog.indexes[name]; !exists || current != index {
		return ErrRTreeSpaceCatalogNotFound
	} else if current.state != RTreeSpaceRebuilding {
		return ErrRTreeSpaceCatalogRebuilding
	} else {
		current.tree = replacement
		current.state = RTreeSpaceReady
		current.generation++
	}
	return nil
}

// Metadata returns the current planner-visible state of a named index.
func (catalog *RTreeSpaceCatalog[T]) Metadata(name string) (RTreeSpaceMetadata, error) {
	if catalog == nil {
		return RTreeSpaceMetadata{}, ErrRTreeSpaceCatalogNil
	}
	name = strings.TrimSpace(name)
	catalog.mu.RLock()
	defer catalog.mu.RUnlock()
	index, err := catalog.indexLocked(name)
	if err != nil {
		return RTreeSpaceMetadata{}, err
	}
	return rTreeSpaceMetadata(index), nil
}

// ListMetadata returns planner-visible metadata sorted by index name.
func (catalog *RTreeSpaceCatalog[T]) ListMetadata() ([]RTreeSpaceMetadata, error) {
	if catalog == nil {
		return nil, ErrRTreeSpaceCatalogNil
	}
	catalog.mu.RLock()
	defer catalog.mu.RUnlock()
	metadata := make([]RTreeSpaceMetadata, 0, len(catalog.indexes))
	for _, index := range catalog.indexes {
		metadata = append(metadata, rTreeSpaceMetadata(index))
	}
	sort.Slice(metadata, func(i, j int) bool {
		return metadata[i].Name < metadata[j].Name
	})
	return metadata, nil
}

// Drop removes a named spatial index.
func (catalog *RTreeSpaceCatalog[T]) Drop(name string) error {
	if catalog == nil {
		return ErrRTreeSpaceCatalogNil
	}
	name = strings.TrimSpace(name)
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	index, err := catalog.indexLocked(name)
	if err != nil {
		return err
	}
	if index.state != RTreeSpaceReady {
		return ErrRTreeSpaceCatalogRebuilding
	}
	delete(catalog.indexes, name)
	return nil
}

func normalizeRTreeSpaceDefinition[T any](definition RTreeSpaceIndexDefinition[T]) (RTreeSpaceIndexDefinition[T], error) {
	definition.Name = strings.TrimSpace(definition.Name)
	definition.BoundsName = strings.TrimSpace(definition.BoundsName)
	if definition.BoundsName == "" {
		definition.BoundsName = "bounds"
	}
	if definition.Name == "" || definition.BoundsExtractor == nil || definition.MaxEntries < 0 {
		return RTreeSpaceIndexDefinition[T]{}, fmt.Errorf("%w: name, extractor, and max entries are required", ErrRTreeSpaceCatalogDefinition)
	}
	return definition, nil
}

func (catalog *RTreeSpaceCatalog[T]) indexLocked(name string) (*rTreeSpaceIndex[T], error) {
	index, exists := catalog.indexes[name]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrRTreeSpaceCatalogNotFound, name)
	}
	return index, nil
}

func rTreeSpaceMetadata[T any](index *rTreeSpaceIndex[T]) RTreeSpaceMetadata {
	return RTreeSpaceMetadata{
		Name:       index.definition.Name,
		BoundsName: index.definition.BoundsName,
		State:      index.state,
		Generation: index.generation,
		Entries:    index.tree.Len(),
		MaxEntries: index.definition.MaxEntries,
	}
}
