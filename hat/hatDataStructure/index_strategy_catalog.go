package hatDataStructure

import (
	"errors"
	"sort"
	"strings"
	"sync"
)

const (
	// DefaultIndexStrategyCatalogCapacity bounds the default planner metadata.
	DefaultIndexStrategyCatalogCapacity = 64
	// MaxIndexStrategyCatalogCapacity prevents accidental unbounded metadata.
	MaxIndexStrategyCatalogCapacity = 4096
	// MaxIndexStrategyNameBytes bounds names used in diagnostics and hints.
	MaxIndexStrategyNameBytes = 256
	// MaxIndexStrategyFields bounds fields retained for one strategy.
	MaxIndexStrategyFields = 32
	// MaxIndexStrategyFieldBytes bounds one field name retained by a strategy.
	MaxIndexStrategyFieldBytes = 256
)

var (
	// ErrIndexStrategyCatalogNil indicates an operation on a nil catalog.
	ErrIndexStrategyCatalogNil = errors.New("hatDataStructure: index strategy catalog is nil")
	// ErrIndexStrategyCapacityInvalid indicates an unsupported catalog capacity.
	ErrIndexStrategyCapacityInvalid = errors.New("hatDataStructure: index strategy catalog capacity is invalid")
	// ErrIndexStrategyCapacity indicates that the catalog has no free entry.
	ErrIndexStrategyCapacity = errors.New("hatDataStructure: index strategy catalog capacity exceeded")
	// ErrIndexStrategyNameExists indicates duplicate strategy registration.
	ErrIndexStrategyNameExists = errors.New("hatDataStructure: index strategy name already exists")
	// ErrIndexStrategyNotFound indicates an unknown named strategy.
	ErrIndexStrategyNotFound = errors.New("hatDataStructure: index strategy not found")
	// ErrIndexStrategyCapabilityMismatch indicates that a hint cannot use a strategy.
	ErrIndexStrategyCapabilityMismatch = errors.New("hatDataStructure: index strategy capability mismatch")
	// ErrIndexStrategyDescriptorInvalid indicates malformed strategy metadata.
	ErrIndexStrategyDescriptorInvalid = errors.New("hatDataStructure: index strategy descriptor is invalid")
	// ErrIndexStrategyHintInvalid indicates malformed strategy requirements.
	ErrIndexStrategyHintInvalid = errors.New("hatDataStructure: index strategy hint is invalid")
	// ErrIndexStrategyDestinationNil indicates a nil ResolveInto destination.
	ErrIndexStrategyDestinationNil = errors.New("hatDataStructure: index strategy destination is nil")
)

// IndexStrategyKind identifies the physical access strategy a caller exposes.
type IndexStrategyKind string

const (
	IndexStrategyHash       IndexStrategyKind = "hash"
	IndexStrategyOrdered    IndexStrategyKind = "ordered"
	IndexStrategyFunctional IndexStrategyKind = "functional"
	IndexStrategyMultikey   IndexStrategyKind = "multikey"
	IndexStrategyBitset     IndexStrategyKind = "bitset"
	IndexStrategyVector     IndexStrategyKind = "vector"
)

// IndexStrategyOperation identifies the predicate or ordering requirement.
type IndexStrategyOperation string

const (
	IndexOperationEquality IndexStrategyOperation = "equality"
	IndexOperationRange    IndexStrategyOperation = "range"
	IndexOperationPrefix   IndexStrategyOperation = "prefix"
	IndexOperationOrder    IndexStrategyOperation = "order"
)

// IndexStrategyCapabilities describes operations that a strategy can answer.
type IndexStrategyCapabilities struct {
	Equality bool
	Range    bool
	Prefix   bool
	Ordered  bool
}

// IndexStrategyDescriptor is bounded metadata for one named index strategy.
// Estimated fields are advisory and never affect correctness.
type IndexStrategyDescriptor struct {
	Name                 string
	Kind                 IndexStrategyKind
	Fields               []string
	Unique               bool
	Capabilities         IndexStrategyCapabilities
	EstimatedCardinality uint64
	EstimatedBytes       uint64
}

// IndexStrategyHint identifies one named strategy or a capability query.
// Name may be empty for Suggest, but Resolve requires it.
type IndexStrategyHint struct {
	Name          string
	Field         string
	Operation     IndexStrategyOperation
	RequireUnique bool
}

// IndexStrategyCatalogOptions configures the bounded metadata catalog.
type IndexStrategyCatalogOptions struct {
	Capacity int
}

// IndexStrategyCatalog stores opt-in index metadata and resolves hints.
// It is separate from index hot paths: callers decide when to register and
// when to consult it during planning or operator inspection.
type IndexStrategyCatalog struct {
	mu          sync.RWMutex
	capacity    int
	descriptors map[string]IndexStrategyDescriptor
}

// NewIndexStrategyCatalog creates a bounded strategy catalog.
func NewIndexStrategyCatalog(options IndexStrategyCatalogOptions) (*IndexStrategyCatalog, error) {
	capacity := options.Capacity
	if capacity == 0 {
		capacity = DefaultIndexStrategyCatalogCapacity
	}
	if capacity < 1 || capacity > MaxIndexStrategyCatalogCapacity {
		return nil, ErrIndexStrategyCapacityInvalid
	}
	return &IndexStrategyCatalog{
		capacity:    capacity,
		descriptors: make(map[string]IndexStrategyDescriptor, capacity),
	}, nil
}

// NewDefaultIndexStrategyCatalog creates a catalog with compact defaults.
func NewDefaultIndexStrategyCatalog() *IndexStrategyCatalog {
	catalog, err := NewIndexStrategyCatalog(IndexStrategyCatalogOptions{})
	if err != nil {
		return nil
	}
	return catalog
}

// Register adds a descriptor atomically. Names cannot be registered twice.
func (catalog *IndexStrategyCatalog) Register(descriptor IndexStrategyDescriptor) error {
	if catalog == nil {
		return ErrIndexStrategyCatalogNil
	}
	if err := validateIndexStrategyDescriptor(descriptor); err != nil {
		return err
	}
	descriptor = normalizeIndexStrategyDescriptor(descriptor)
	owned := descriptor
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	if _, exists := catalog.descriptors[owned.Name]; exists {
		return ErrIndexStrategyNameExists
	}
	if len(catalog.descriptors) >= catalog.capacity {
		return ErrIndexStrategyCapacity
	}
	catalog.descriptors[owned.Name] = owned
	return nil
}

// Unregister removes a descriptor by name. It is safe to call for a missing name.
func (catalog *IndexStrategyCatalog) Unregister(name string) error {
	if catalog == nil {
		return ErrIndexStrategyCatalogNil
	}
	name = strings.TrimSpace(name)
	if name == "" || len(name) > MaxIndexStrategyNameBytes {
		return ErrIndexStrategyDescriptorInvalid
	}
	catalog.mu.Lock()
	delete(catalog.descriptors, name)
	catalog.mu.Unlock()
	return nil
}

// Len returns the number of registered strategies.
func (catalog *IndexStrategyCatalog) Len() int {
	if catalog == nil {
		return 0
	}
	catalog.mu.RLock()
	length := len(catalog.descriptors)
	catalog.mu.RUnlock()
	return length
}

// Resolve resolves a named hint and returns an owned descriptor copy.
func (catalog *IndexStrategyCatalog) Resolve(hint IndexStrategyHint) (IndexStrategyDescriptor, error) {
	var descriptor IndexStrategyDescriptor
	if err := catalog.ResolveInto(&descriptor, hint); err != nil {
		return IndexStrategyDescriptor{}, err
	}
	return descriptor, nil
}

// ResolveInto resolves a named hint into dst, reusing its Fields capacity.
func (catalog *IndexStrategyCatalog) ResolveInto(dst *IndexStrategyDescriptor, hint IndexStrategyHint) error {
	if catalog == nil {
		return ErrIndexStrategyCatalogNil
	}
	if dst == nil {
		return ErrIndexStrategyDestinationNil
	}
	hint = normalizeIndexStrategyHint(hint)
	if err := validateIndexStrategyHint(hint, true); err != nil {
		return err
	}
	catalog.mu.RLock()
	descriptor, found := catalog.descriptors[hint.Name]
	if found {
		if !indexStrategyMatchesHint(descriptor, hint) {
			found = false
		}
	}
	if found {
		*dst = cloneIndexStrategyDescriptorInto(*dst, descriptor)
	}
	catalog.mu.RUnlock()
	if !found {
		if _, exists := catalog.descriptor(hint.Name); !exists {
			return ErrIndexStrategyNotFound
		}
		return ErrIndexStrategyCapabilityMismatch
	}
	return nil
}

// Suggest returns deterministic candidates matching a capability hint.
// Candidates prefer known smaller byte estimates, then higher cardinality,
// then lexical names. An empty result is not an error.
func (catalog *IndexStrategyCatalog) Suggest(hint IndexStrategyHint) ([]IndexStrategyDescriptor, error) {
	return catalog.SuggestInto(nil, hint)
}

// SuggestInto returns deterministic candidates into dst, reusing descriptor
// and field backing storage when the caller retains the returned slice.
func (catalog *IndexStrategyCatalog) SuggestInto(dst []IndexStrategyDescriptor, hint IndexStrategyHint) ([]IndexStrategyDescriptor, error) {
	if catalog == nil {
		return nil, ErrIndexStrategyCatalogNil
	}
	hint = normalizeIndexStrategyHint(hint)
	if err := validateIndexStrategyHint(hint, false); err != nil {
		return nil, err
	}
	dst = dst[:0]
	catalog.mu.RLock()
	for _, descriptor := range catalog.descriptors {
		if indexStrategyMatchesHint(descriptor, hint) {
			var candidate IndexStrategyDescriptor
			if len(dst) < cap(dst) {
				dst = dst[:len(dst)+1]
				candidate = dst[len(dst)-1]
			} else {
				dst = append(dst, candidate)
			}
			dst[len(dst)-1] = cloneIndexStrategyDescriptorInto(candidate, descriptor)
		}
	}
	catalog.mu.RUnlock()
	sortIndexStrategyDescriptors(dst)
	return dst, nil
}

// Snapshot returns all descriptors sorted by name and owned by the caller.
func (catalog *IndexStrategyCatalog) Snapshot() []IndexStrategyDescriptor {
	if catalog == nil {
		return nil
	}
	catalog.mu.RLock()
	snapshot := make([]IndexStrategyDescriptor, 0, len(catalog.descriptors))
	for _, descriptor := range catalog.descriptors {
		snapshot = append(snapshot, cloneIndexStrategyDescriptor(descriptor))
	}
	catalog.mu.RUnlock()
	sort.Slice(snapshot, func(left, right int) bool { return snapshot[left].Name < snapshot[right].Name })
	return snapshot
}

func (catalog *IndexStrategyCatalog) descriptor(name string) (IndexStrategyDescriptor, bool) {
	catalog.mu.RLock()
	descriptor, found := catalog.descriptors[name]
	catalog.mu.RUnlock()
	return descriptor, found
}

func validateIndexStrategyDescriptor(descriptor IndexStrategyDescriptor) error {
	descriptor.Name = strings.TrimSpace(descriptor.Name)
	if descriptor.Name == "" || len(descriptor.Name) > MaxIndexStrategyNameBytes || descriptor.Kind == "" || len(descriptor.Fields) == 0 || len(descriptor.Fields) > MaxIndexStrategyFields {
		return ErrIndexStrategyDescriptorInvalid
	}
	if !descriptor.Capabilities.Equality && !descriptor.Capabilities.Range && !descriptor.Capabilities.Prefix && !descriptor.Capabilities.Ordered {
		return ErrIndexStrategyDescriptorInvalid
	}
	for _, field := range descriptor.Fields {
		field = strings.TrimSpace(field)
		if field == "" || len(field) > MaxIndexStrategyFieldBytes {
			return ErrIndexStrategyDescriptorInvalid
		}
	}
	return nil
}

func normalizeIndexStrategyDescriptor(descriptor IndexStrategyDescriptor) IndexStrategyDescriptor {
	descriptor.Name = strings.TrimSpace(descriptor.Name)
	if len(descriptor.Fields) == 0 {
		return descriptor
	}
	fields := make([]string, len(descriptor.Fields))
	for index, field := range descriptor.Fields {
		fields[index] = strings.TrimSpace(field)
	}
	descriptor.Fields = fields
	return descriptor
}

func normalizeIndexStrategyHint(hint IndexStrategyHint) IndexStrategyHint {
	hint.Name = strings.TrimSpace(hint.Name)
	hint.Field = strings.TrimSpace(hint.Field)
	return hint
}

func validateIndexStrategyHint(hint IndexStrategyHint, requireName bool) error {
	if requireName && strings.TrimSpace(hint.Name) == "" {
		return ErrIndexStrategyHintInvalid
	}
	if len(hint.Name) > MaxIndexStrategyNameBytes || len(hint.Field) > MaxIndexStrategyFieldBytes {
		return ErrIndexStrategyHintInvalid
	}
	switch hint.Operation {
	case IndexOperationEquality, IndexOperationRange, IndexOperationPrefix, IndexOperationOrder:
		return nil
	default:
		return ErrIndexStrategyHintInvalid
	}
}

func indexStrategyMatchesHint(descriptor IndexStrategyDescriptor, hint IndexStrategyHint) bool {
	if hint.Name != "" && descriptor.Name != hint.Name {
		return false
	}
	if hint.Field != "" {
		fieldFound := false
		for _, field := range descriptor.Fields {
			if field == hint.Field {
				fieldFound = true
				break
			}
		}
		if !fieldFound {
			return false
		}
	}
	if hint.RequireUnique && !descriptor.Unique {
		return false
	}
	switch hint.Operation {
	case IndexOperationEquality:
		return descriptor.Capabilities.Equality
	case IndexOperationRange:
		return descriptor.Capabilities.Range
	case IndexOperationPrefix:
		return descriptor.Capabilities.Prefix
	case IndexOperationOrder:
		return descriptor.Capabilities.Ordered
	default:
		return false
	}
}

func cloneIndexStrategyDescriptor(descriptor IndexStrategyDescriptor) IndexStrategyDescriptor {
	return cloneIndexStrategyDescriptorInto(IndexStrategyDescriptor{}, descriptor)
}

func cloneIndexStrategyDescriptorInto(dst, descriptor IndexStrategyDescriptor) IndexStrategyDescriptor {
	dst.Name = descriptor.Name
	dst.Kind = descriptor.Kind
	dst.Unique = descriptor.Unique
	dst.Capabilities = descriptor.Capabilities
	dst.EstimatedCardinality = descriptor.EstimatedCardinality
	dst.EstimatedBytes = descriptor.EstimatedBytes
	dst.Fields = append(dst.Fields[:0], descriptor.Fields...)
	return dst
}

func sortIndexStrategyDescriptors(descriptors []IndexStrategyDescriptor) {
	for index := 1; index < len(descriptors); index++ {
		candidate := descriptors[index]
		position := index
		for position > 0 && indexStrategyDescriptorBefore(candidate, descriptors[position-1]) {
			descriptors[position] = descriptors[position-1]
			position--
		}
		descriptors[position] = candidate
	}
}

func indexStrategyDescriptorBefore(left, right IndexStrategyDescriptor) bool {
	leftBytes, rightBytes := left.EstimatedBytes, right.EstimatedBytes
	if leftBytes == 0 {
		return rightBytes != 0
	}
	if rightBytes == 0 || leftBytes != rightBytes {
		return leftBytes < rightBytes
	}
	if left.EstimatedCardinality != right.EstimatedCardinality {
		return left.EstimatedCardinality > right.EstimatedCardinality
	}
	return left.Name < right.Name
}
