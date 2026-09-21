package hatMerkle

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

const (
	// DefaultSharedPartRegistryMaxEntries bounds registered immutable part
	// descriptors when callers do not provide a limit.
	DefaultSharedPartRegistryMaxEntries = 1024
	// DefaultSharedPartRegistryMaxLeases bounds concurrent readers.
	DefaultSharedPartRegistryMaxLeases = 4096
	// MaxSharedPartRegistryMaxEntries prevents an accidentally unbounded map.
	MaxSharedPartRegistryMaxEntries = 1 << 20
	// MaxSharedPartRegistryMaxLeases prevents an accidentally unbounded lease
	// table.
	MaxSharedPartRegistryMaxLeases = 1 << 20
	// MaxSharedPartRegistryIdentifierBytes bounds IDs and shared locations.
	MaxSharedPartRegistryIdentifierBytes = 256
)

var (
	// ErrSharedPartRegistryNil indicates a method call on a nil registry.
	ErrSharedPartRegistryNil = errors.New("hatriecache: shared part registry is nil")
	// ErrSharedPartRegistryOptionsInvalid indicates an invalid registry bound.
	ErrSharedPartRegistryOptionsInvalid = errors.New("hatriecache: shared part registry options are invalid")
	// ErrSharedPartRegistryInvalid indicates malformed share metadata.
	ErrSharedPartRegistryInvalid = errors.New("hatriecache: shared part descriptor is invalid")
	// ErrSharedPartRegistryVerifierRequired indicates unverified registration.
	ErrSharedPartRegistryVerifierRequired = errors.New("hatriecache: shared part verifier is required")
	// ErrSharedPartRegistryNotFound indicates an unknown share or lease.
	ErrSharedPartRegistryNotFound = errors.New("hatriecache: shared part was not found")
	// ErrSharedPartRegistryCapacity indicates that the descriptor bound is full.
	ErrSharedPartRegistryCapacity = errors.New("hatriecache: shared part registry capacity reached")
	// ErrSharedPartRegistryLeaseCapacity indicates that the lease bound is full.
	ErrSharedPartRegistryLeaseCapacity = errors.New("hatriecache: shared part lease capacity reached")
	// ErrSharedPartRegistryRetired indicates that no new readers may acquire a
	// retired share.
	ErrSharedPartRegistryRetired = errors.New("hatriecache: shared part is retired")
	// ErrSharedPartRegistryLeased indicates that a removal would invalidate a
	// live reader.
	ErrSharedPartRegistryLeased = errors.New("hatriecache: shared part still has active leases")
	// ErrSharedPartRegistryLeaseNotFound indicates a duplicate or unknown
	// release.
	ErrSharedPartRegistryLeaseNotFound = errors.New("hatriecache: shared part lease was not found")
	// ErrSharedPartRegistryConflict indicates a reused share ID with different
	// immutable metadata.
	ErrSharedPartRegistryConflict = errors.New("hatriecache: shared part metadata conflicts")
	// ErrSharedPartRegistryActive indicates that active shares must be retired
	// before removal.
	ErrSharedPartRegistryActive = errors.New("hatriecache: shared part must be retired before removal")
)

// SharedPartRegistryOptions bounds metadata and lease state. The registry
// stores descriptors only; it never owns the bytes at Entry.Location.
type SharedPartRegistryOptions struct {
	MaxEntries int
	MaxLeases  int
}

// SharedPartDescriptor identifies an immutable part that another replica may
// open at the same location. The caller must verify the manifest and sharing
// authorization before registration through SharedPartVerifier.
type SharedPartDescriptor struct {
	ShareID       string
	SourceReplica string
	Entry         PartCatalogEntry
}

// SharedPartReference is a detached, immutable metadata snapshot returned to
// callers. RegistrationGeneration changes when registry membership changes,
// not when a reader acquires or releases a lease.
type SharedPartReference struct {
	SharedPartDescriptor
	RegistrationGeneration uint64
}

// SharedPartVerifier authenticates or otherwise validates a descriptor before
// it becomes visible. It must not be assumed that a remote Location is local
// or trusted merely because its manifest is present.
type SharedPartVerifier func(SharedPartDescriptor) error

// SharedPartLease keeps one shared reference live. Callers must Release the
// ID when the reader closes its handle.
type SharedPartLease struct {
	ID        uint64
	Reference SharedPartReference
}

// SharedPartRegistryEntry describes one registered reference and its current
// retirement/lease state.
type SharedPartRegistryEntry struct {
	Reference SharedPartReference
	Leases    int
	Retired   bool
}

// SharedPartRegistrySnapshot is a detached deterministic registry snapshot.
type SharedPartRegistrySnapshot struct {
	Generation   uint64
	ActiveLeases int
	Entries      []SharedPartRegistryEntry
}

type sharedPartRegistryRecord struct {
	reference SharedPartReference
	leases    int
	retired   bool
}

// SharedPartRegistry tracks verified immutable references and reader leases.
// It is a metadata-only primitive: registering or acquiring a share never
// copies, opens, reads, or deletes the underlying part bytes.
type SharedPartRegistry struct {
	mu         sync.RWMutex
	maxEntries int
	maxLeases  int
	generation uint64
	nextLease  uint64
	parts      map[string]sharedPartRegistryRecord
	leases     map[uint64]string
}

// NewSharedPartRegistry creates a bounded metadata-only share registry. Zero
// bounds select the documented defaults.
func NewSharedPartRegistry(options SharedPartRegistryOptions) (*SharedPartRegistry, error) {
	if options.MaxEntries < 0 || options.MaxLeases < 0 || options.MaxEntries > MaxSharedPartRegistryMaxEntries || options.MaxLeases > MaxSharedPartRegistryMaxLeases {
		return nil, ErrSharedPartRegistryOptionsInvalid
	}
	if options.MaxEntries == 0 {
		options.MaxEntries = DefaultSharedPartRegistryMaxEntries
	}
	if options.MaxLeases == 0 {
		options.MaxLeases = DefaultSharedPartRegistryMaxLeases
	}
	return &SharedPartRegistry{
		maxEntries: options.MaxEntries,
		maxLeases:  options.MaxLeases,
		parts:      make(map[string]sharedPartRegistryRecord),
		leases:     make(map[uint64]string),
	}, nil
}

// Register verifies and publishes one immutable reference. Registering the
// exact same descriptor again is idempotent; reusing a share ID with different
// metadata is rejected. A retired share must be removed before that ID can be
// registered again.
func (registry *SharedPartRegistry) Register(descriptor SharedPartDescriptor, verify SharedPartVerifier) (SharedPartReference, error) {
	if registry == nil {
		return SharedPartReference{}, ErrSharedPartRegistryNil
	}
	if verify == nil {
		return SharedPartReference{}, ErrSharedPartRegistryVerifierRequired
	}
	normalized, err := normalizeSharedPartDescriptor(descriptor)
	if err != nil {
		return SharedPartReference{}, err
	}
	if err := verify(cloneSharedPartDescriptor(normalized)); err != nil {
		return SharedPartReference{}, err
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	if existing, ok := registry.parts[normalized.ShareID]; ok {
		if existing.retired {
			return SharedPartReference{}, ErrSharedPartRegistryRetired
		}
		if !sharedPartDescriptorEqual(existing.reference.SharedPartDescriptor, normalized) {
			return SharedPartReference{}, ErrSharedPartRegistryConflict
		}
		return cloneSharedPartReference(existing.reference), nil
	}
	if len(registry.parts) >= registry.maxEntries {
		return SharedPartReference{}, ErrSharedPartRegistryCapacity
	}
	registry.generation++
	reference := SharedPartReference{
		SharedPartDescriptor:   cloneSharedPartDescriptor(normalized),
		RegistrationGeneration: registry.generation,
	}
	registry.parts[normalized.ShareID] = sharedPartRegistryRecord{reference: cloneSharedPartReference(reference)}
	return reference, nil
}

// Acquire grants a reader lease for an active share. The returned descriptor
// is a copy of metadata and remains valid until the caller releases the lease.
func (registry *SharedPartRegistry) Acquire(shareID string) (SharedPartLease, error) {
	if registry == nil {
		return SharedPartLease{}, ErrSharedPartRegistryNil
	}
	shareID, err := normalizeSharedPartIdentifier(shareID, "share ID")
	if err != nil {
		return SharedPartLease{}, err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	record, ok := registry.parts[shareID]
	if !ok {
		return SharedPartLease{}, ErrSharedPartRegistryNotFound
	}
	if record.retired {
		return SharedPartLease{}, ErrSharedPartRegistryRetired
	}
	if len(registry.leases) >= registry.maxLeases {
		return SharedPartLease{}, ErrSharedPartRegistryLeaseCapacity
	}
	id := registry.nextLeaseIDLocked()
	record.leases++
	registry.parts[shareID] = record
	registry.leases[id] = shareID
	return SharedPartLease{ID: id, Reference: cloneSharedPartReference(record.reference)}, nil
}

// Release drops one reader lease. Releasing the same lease twice is an error,
// which makes lifecycle leaks visible to callers.
func (registry *SharedPartRegistry) Release(leaseID uint64) error {
	if registry == nil {
		return ErrSharedPartRegistryNil
	}
	if leaseID == 0 {
		return ErrSharedPartRegistryLeaseNotFound
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	shareID, ok := registry.leases[leaseID]
	if !ok {
		return ErrSharedPartRegistryLeaseNotFound
	}
	record, ok := registry.parts[shareID]
	if !ok || record.leases <= 0 {
		return ErrSharedPartRegistryLeaseNotFound
	}
	delete(registry.leases, leaseID)
	record.leases--
	registry.parts[shareID] = record
	return nil
}

// Retire prevents new acquisitions while allowing existing readers to finish.
// It is idempotent and does not delete or modify the shared bytes.
func (registry *SharedPartRegistry) Retire(shareID string) (SharedPartReference, error) {
	if registry == nil {
		return SharedPartReference{}, ErrSharedPartRegistryNil
	}
	shareID, err := normalizeSharedPartIdentifier(shareID, "share ID")
	if err != nil {
		return SharedPartReference{}, err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	record, ok := registry.parts[shareID]
	if !ok {
		return SharedPartReference{}, ErrSharedPartRegistryNotFound
	}
	if !record.retired {
		record.retired = true
		registry.generation++
		registry.parts[shareID] = record
	}
	return cloneSharedPartReference(record.reference), nil
}

// Remove permanently forgets a retired, unleased descriptor. The underlying
// bytes remain caller-owned and are never deleted by this method.
func (registry *SharedPartRegistry) Remove(shareID string) error {
	if registry == nil {
		return ErrSharedPartRegistryNil
	}
	shareID, err := normalizeSharedPartIdentifier(shareID, "share ID")
	if err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	record, ok := registry.parts[shareID]
	if !ok {
		return ErrSharedPartRegistryNotFound
	}
	if !record.retired {
		return ErrSharedPartRegistryActive
	}
	if record.leases != 0 {
		return ErrSharedPartRegistryLeased
	}
	delete(registry.parts, shareID)
	registry.generation++
	return nil
}

// Snapshot returns deterministic copies of all metadata and current lease
// counts. It never includes or reads part payload bytes.
func (registry *SharedPartRegistry) Snapshot() SharedPartRegistrySnapshot {
	if registry == nil {
		return SharedPartRegistrySnapshot{}
	}
	registry.mu.RLock()
	snapshot := SharedPartRegistrySnapshot{
		Generation: registry.generation,
		Entries:    make([]SharedPartRegistryEntry, 0, len(registry.parts)),
	}
	for _, record := range registry.parts {
		snapshot.Entries = append(snapshot.Entries, SharedPartRegistryEntry{
			Reference: cloneSharedPartReference(record.reference),
			Leases:    record.leases,
			Retired:   record.retired,
		})
		snapshot.ActiveLeases += record.leases
	}
	registry.mu.RUnlock()
	sort.Slice(snapshot.Entries, func(left, right int) bool {
		return snapshot.Entries[left].Reference.ShareID < snapshot.Entries[right].Reference.ShareID
	})
	return snapshot
}

func (registry *SharedPartRegistry) nextLeaseIDLocked() uint64 {
	for {
		registry.nextLease++
		if registry.nextLease == 0 {
			registry.nextLease = 1
		}
		if _, exists := registry.leases[registry.nextLease]; !exists {
			return registry.nextLease
		}
	}
}

func normalizeSharedPartDescriptor(descriptor SharedPartDescriptor) (SharedPartDescriptor, error) {
	shareID, err := normalizeSharedPartIdentifier(descriptor.ShareID, "share ID")
	if err != nil {
		return SharedPartDescriptor{}, err
	}
	sourceReplica, err := normalizeSharedPartIdentifier(descriptor.SourceReplica, "source replica")
	if err != nil {
		return SharedPartDescriptor{}, err
	}
	entry, err := normalizePartCatalogEntry(descriptor.Entry)
	if err != nil {
		return SharedPartDescriptor{}, fmt.Errorf("%w: %v", ErrSharedPartRegistryInvalid, err)
	}
	location, err := normalizeSharedPartIdentifier(entry.Location, "part location")
	if err != nil {
		return SharedPartDescriptor{}, err
	}
	entry.Location = location
	return SharedPartDescriptor{
		ShareID:       shareID,
		SourceReplica: sourceReplica,
		Entry:         clonePartCatalogEntry(entry),
	}, nil
}

func normalizeSharedPartIdentifier(value, label string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > MaxSharedPartRegistryIdentifierBytes || strings.IndexByte(value, 0) >= 0 {
		return "", fmt.Errorf("%w: %s is empty, too long, or contains a NUL byte", ErrSharedPartRegistryInvalid, label)
	}
	return value, nil
}

func sharedPartDescriptorEqual(left, right SharedPartDescriptor) bool {
	return left.ShareID == right.ShareID &&
		left.SourceReplica == right.SourceReplica &&
		left.Entry.Name == right.Entry.Name &&
		left.Entry.Location == right.Entry.Location &&
		left.Entry.Generation == right.Entry.Generation &&
		left.Entry.Manifest.Equal(right.Entry.Manifest)
}

func cloneSharedPartDescriptor(descriptor SharedPartDescriptor) SharedPartDescriptor {
	descriptor.Entry = clonePartCatalogEntry(descriptor.Entry)
	return descriptor
}

func cloneSharedPartReference(reference SharedPartReference) SharedPartReference {
	reference.SharedPartDescriptor = cloneSharedPartDescriptor(reference.SharedPartDescriptor)
	return reference
}
