package hatDataStructure

import (
	"errors"
	"sort"
	"strings"
	"sync"
)

var (
	// ErrConditionalIndexCatalogNil reports an operation on a nil catalog.
	ErrConditionalIndexCatalogNil = errors.New("conditional index catalog is nil")
	// ErrConditionalIndexCatalogDefinition reports invalid schema metadata.
	ErrConditionalIndexCatalogDefinition = errors.New("conditional index catalog definition is invalid")
	// ErrConditionalIndexCatalogExists reports a duplicate index name.
	ErrConditionalIndexCatalogExists = errors.New("conditional index catalog entry already exists")
	// ErrConditionalIndexCatalogNotFound reports an unknown index name.
	ErrConditionalIndexCatalogNotFound = errors.New("conditional index catalog entry was not found")
	// ErrConditionalIndexCatalogRebuilding reports a write during rebuild.
	ErrConditionalIndexCatalogRebuilding = errors.New("conditional index catalog entry is rebuilding")
)

// ConditionalIndexState describes a catalog entry's lifecycle.
type ConditionalIndexState uint8

const (
	ConditionalIndexReady ConditionalIndexState = iota + 1
	ConditionalIndexRebuilding
)

// String returns the stable planner-facing state name.
func (state ConditionalIndexState) String() string {
	switch state {
	case ConditionalIndexReady:
		return "ready"
	case ConditionalIndexRebuilding:
		return "rebuilding"
	default:
		return "unknown"
	}
}

// ConditionalIndexDefinition describes a named conditional functional index.
// ExtractorName and PredicateName are stable planner metadata supplied by the
// schema layer; the functions implement those expressions.
type ConditionalIndexDefinition[T any, K comparable] struct {
	Name          string
	ExtractorName string
	PredicateName string
	Capacity      int
	Extractor     func(T) K
	Predicate     func(T) bool
}

// ConditionalIndexRow is one row used to build or rebuild an index.
type ConditionalIndexRow[T any] struct {
	ID    uint64
	Value T
}

// ConditionalIndexMetadata is planner-readable lifecycle and cardinality
// metadata. It is returned by value and safe for callers to modify.
type ConditionalIndexMetadata struct {
	Name          string
	ExtractorName string
	PredicateName string
	State         ConditionalIndexState
	Generation    uint64
	Entries       int
	DistinctKeys  int
}

type conditionalIndexCatalogEntry[T any, K comparable] struct {
	definition ConditionalIndexDefinition[T, K]
	index      *ConditionalFunctionalIndex[T, K]
	state      ConditionalIndexState
	generation uint64
}

// ConditionalIndexCatalog owns schema-level lifecycle for conditional indexes
// sharing one row and key type. Rebuild fences writes, but reads continue from
// the previous immutable index until the replacement is ready.
type ConditionalIndexCatalog[T any, K comparable] struct {
	mu      sync.RWMutex
	indexes map[string]*conditionalIndexCatalogEntry[T, K]
}

// NewConditionalIndexCatalog creates an empty conditional-index catalog.
func NewConditionalIndexCatalog[T any, K comparable]() *ConditionalIndexCatalog[T, K] {
	return &ConditionalIndexCatalog[T, K]{
		indexes: make(map[string]*conditionalIndexCatalogEntry[T, K]),
	}
}

// Create registers an empty ready index with generation one.
func (catalog *ConditionalIndexCatalog[T, K]) Create(definition ConditionalIndexDefinition[T, K]) error {
	if catalog == nil {
		return ErrConditionalIndexCatalogNil
	}
	if err := validateConditionalIndexDefinition(definition); err != nil {
		return err
	}
	index, err := NewConditionalFunctionalIndex(definition.Extractor, definition.Predicate, definition.Capacity)
	if err != nil {
		return err
	}
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	catalog.ensureMapLocked()
	if _, exists := catalog.indexes[definition.Name]; exists {
		return ErrConditionalIndexCatalogExists
	}
	catalog.indexes[definition.Name] = &conditionalIndexCatalogEntry[T, K]{
		definition: definition,
		index:      index,
		state:      ConditionalIndexReady,
		generation: 1,
	}
	return nil
}

// Rebuild atomically replaces name with rows satisfying its immutable
// definition. Writers are rejected during the off-lock build; reads continue
// against the old index until the swap.
func (catalog *ConditionalIndexCatalog[T, K]) Rebuild(name string, rows []ConditionalIndexRow[T]) error {
	if catalog == nil {
		return ErrConditionalIndexCatalogNil
	}
	rows = append([]ConditionalIndexRow[T](nil), rows...)
	catalog.mu.Lock()
	catalog.ensureMapLocked()
	entry, ok := catalog.indexes[name]
	if !ok {
		catalog.mu.Unlock()
		return ErrConditionalIndexCatalogNotFound
	}
	if entry.state == ConditionalIndexRebuilding {
		catalog.mu.Unlock()
		return ErrConditionalIndexCatalogRebuilding
	}
	entry.state = ConditionalIndexRebuilding
	definition := entry.definition
	old := entry.index
	catalog.mu.Unlock()

	replacement, err := NewConditionalFunctionalIndex(definition.Extractor, definition.Predicate, definition.Capacity)
	if err == nil {
		for _, row := range rows {
			if err = replacement.Upsert(row.ID, row.Value); err != nil {
				break
			}
		}
	}

	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	current, stillPresent := catalog.indexes[name]
	if !stillPresent || current.index != old || current.state != ConditionalIndexRebuilding {
		if err != nil {
			return err
		}
		return ErrConditionalIndexCatalogNotFound
	}
	if err != nil {
		current.state = ConditionalIndexReady
		return err
	}
	current.index = replacement
	current.state = ConditionalIndexReady
	current.generation++
	return nil
}

// Upsert updates a ready index. It is rejected while a replacement is being
// built so the atomic swap cannot lose a concurrent write.
func (catalog *ConditionalIndexCatalog[T, K]) Upsert(name string, id uint64, value T) error {
	if catalog == nil {
		return ErrConditionalIndexCatalogNil
	}
	catalog.mu.RLock()
	defer catalog.mu.RUnlock()
	entry, err := catalog.entryLocked(name)
	if err != nil {
		return err
	}
	if entry.state == ConditionalIndexRebuilding {
		return ErrConditionalIndexCatalogRebuilding
	}
	return entry.index.Upsert(id, value)
}

// Delete removes an ID from a ready index.
func (catalog *ConditionalIndexCatalog[T, K]) Delete(name string, id uint64) (bool, error) {
	if catalog == nil {
		return false, ErrConditionalIndexCatalogNil
	}
	catalog.mu.RLock()
	defer catalog.mu.RUnlock()
	entry, err := catalog.entryLocked(name)
	if err != nil {
		return false, err
	}
	if entry.state == ConditionalIndexRebuilding {
		return false, ErrConditionalIndexCatalogRebuilding
	}
	return entry.index.Delete(id), nil
}

// LookupIDs returns IDs for key. During rebuild it reads the previous ready
// index, which is consistent until the atomic replacement.
func (catalog *ConditionalIndexCatalog[T, K]) LookupIDs(name string, key K) ([]uint64, error) {
	if catalog == nil {
		return nil, ErrConditionalIndexCatalogNil
	}
	catalog.mu.RLock()
	defer catalog.mu.RUnlock()
	entry, err := catalog.entryLocked(name)
	if err != nil {
		return nil, err
	}
	return entry.index.LookupIDs(key), nil
}

// Metadata returns planner-readable metadata for name.
func (catalog *ConditionalIndexCatalog[T, K]) Metadata(name string) (ConditionalIndexMetadata, error) {
	if catalog == nil {
		return ConditionalIndexMetadata{}, ErrConditionalIndexCatalogNil
	}
	catalog.mu.RLock()
	defer catalog.mu.RUnlock()
	entry, err := catalog.entryLocked(name)
	if err != nil {
		return ConditionalIndexMetadata{}, err
	}
	return conditionalIndexMetadata(entry), nil
}

// ListMetadata returns metadata sorted by index name for deterministic planner
// inspection and explain output.
func (catalog *ConditionalIndexCatalog[T, K]) ListMetadata() []ConditionalIndexMetadata {
	if catalog == nil {
		return nil
	}
	catalog.mu.RLock()
	defer catalog.mu.RUnlock()
	metadata := make([]ConditionalIndexMetadata, 0, len(catalog.indexes))
	for _, entry := range catalog.indexes {
		metadata = append(metadata, conditionalIndexMetadata(entry))
	}
	sort.Slice(metadata, func(i, j int) bool { return metadata[i].Name < metadata[j].Name })
	return metadata
}

// Drop removes a ready index and reports whether it existed. Rebuilding
// indexes are retained until their replacement has safely swapped in.
func (catalog *ConditionalIndexCatalog[T, K]) Drop(name string) bool {
	if catalog == nil {
		return false
	}
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	entry, ok := catalog.indexes[name]
	if !ok || entry.state == ConditionalIndexRebuilding {
		return false
	}
	delete(catalog.indexes, name)
	return true
}

func (catalog *ConditionalIndexCatalog[T, K]) ensureMapLocked() {
	if catalog.indexes == nil {
		catalog.indexes = make(map[string]*conditionalIndexCatalogEntry[T, K])
	}
}

func (catalog *ConditionalIndexCatalog[T, K]) entryLocked(name string) (*conditionalIndexCatalogEntry[T, K], error) {
	entry, ok := catalog.indexes[name]
	if !ok {
		return nil, ErrConditionalIndexCatalogNotFound
	}
	return entry, nil
}

func conditionalIndexMetadata[T any, K comparable](entry *conditionalIndexCatalogEntry[T, K]) ConditionalIndexMetadata {
	return ConditionalIndexMetadata{
		Name:          entry.definition.Name,
		ExtractorName: entry.definition.ExtractorName,
		PredicateName: entry.definition.PredicateName,
		State:         entry.state,
		Generation:    entry.generation,
		Entries:       entry.index.Len(),
		DistinctKeys:  entry.index.DistinctKeys(),
	}
}

func validateConditionalIndexDefinition[T any, K comparable](definition ConditionalIndexDefinition[T, K]) error {
	if strings.TrimSpace(definition.Name) == "" || strings.TrimSpace(definition.ExtractorName) == "" || strings.TrimSpace(definition.PredicateName) == "" || definition.Capacity < 0 || definition.Extractor == nil || definition.Predicate == nil {
		return ErrConditionalIndexCatalogDefinition
	}
	return nil
}
