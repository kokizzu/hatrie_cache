package hatStorage

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

const (
	// DefaultRemotePartRegistryMaxEntries bounds the opt-in metadata registry
	// when callers leave MaxEntries at zero.
	DefaultRemotePartRegistryMaxEntries = 4096
	// MaxRemotePartRegistryEntries prevents accidental unbounded map sizing.
	MaxRemotePartRegistryEntries = 1 << 20
)

var (
	ErrRemotePartRegistryInvalid  = errors.New("hatriecache: remote-part registry input is invalid")
	ErrRemotePartRegistryFull     = errors.New("hatriecache: remote-part registry is full")
	ErrRemotePartRegistryStale    = errors.New("hatriecache: remote-part registry update is stale")
	ErrRemotePartRegistryConflict = errors.New("hatriecache: remote-part registry update conflicts")
	ErrRemotePartRegistryNil      = errors.New("hatriecache: remote-part registry is nil")
)

// RemotePartRegistryOptions bounds an in-memory immutable-part registration
// table. The registry stores references and metadata only; it never stores
// part bytes.
type RemotePartRegistryOptions struct {
	MaxEntries int
}

// RemotePartRegistration binds a logical part key to one immutable remote
// object. Generation is a caller-owned fencing value and must be positive.
type RemotePartRegistration struct {
	Key        string
	Reference  RemotePartReference
	Generation uint64
}

// RemotePartRegistry is a concurrency-safe, bounded metadata registry for
// zero-copy part sharing. Register is idempotent for the same key, reference,
// and generation; newer generations replace older registrations.
type RemotePartRegistry struct {
	mu         sync.RWMutex
	maxEntries int
	entries    map[string]RemotePartRegistration
}

// NewRemotePartRegistry validates an opt-in bounded registry configuration.
func NewRemotePartRegistry(options RemotePartRegistryOptions) (*RemotePartRegistry, error) {
	maxEntries := options.MaxEntries
	if maxEntries == 0 {
		maxEntries = DefaultRemotePartRegistryMaxEntries
	}
	if maxEntries < 1 || maxEntries > MaxRemotePartRegistryEntries {
		return nil, fmt.Errorf("%w: max entries must be from 1 through %d", ErrRemotePartRegistryInvalid, MaxRemotePartRegistryEntries)
	}
	return &RemotePartRegistry{
		maxEntries: maxEntries,
		entries:    make(map[string]RemotePartRegistration, maxEntries),
	}, nil
}

// Register publishes a metadata-only reference. The returned bool is true
// when an existing registration was replaced by a newer generation.
func (registry *RemotePartRegistry) Register(registration RemotePartRegistration) (bool, error) {
	if registry == nil {
		return false, ErrRemotePartRegistryNil
	}
	normalized, err := validateRemotePartRegistration(registration)
	if err != nil {
		return false, err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	existing, exists := registry.entries[normalized.Key]
	if !exists {
		if len(registry.entries) >= registry.maxEntries {
			return false, ErrRemotePartRegistryFull
		}
		registry.entries[normalized.Key] = normalized
		return false, nil
	}
	if normalized.Generation < existing.Generation {
		return false, fmt.Errorf("%w: key %q generation %d is older than %d", ErrRemotePartRegistryStale, normalized.Key, normalized.Generation, existing.Generation)
	}
	if normalized.Generation == existing.Generation {
		if normalized.Reference == existing.Reference {
			return false, nil
		}
		return false, fmt.Errorf("%w: key %q has two references at generation %d", ErrRemotePartRegistryConflict, normalized.Key, normalized.Generation)
	}
	registry.entries[normalized.Key] = normalized
	return true, nil
}

// Lookup returns a registration without copying any part bytes.
func (registry *RemotePartRegistry) Lookup(key string) (RemotePartRegistration, bool) {
	if registry == nil || !validRemotePartRegistryKey(key) {
		return RemotePartRegistration{}, false
	}
	registry.mu.RLock()
	registration, ok := registry.entries[key]
	registry.mu.RUnlock()
	return registration, ok
}

// Unregister removes a registration only when its generation still matches.
// A missing key is an idempotent no-op; a mismatched generation is rejected.
func (registry *RemotePartRegistry) Unregister(key string, generation uint64) (bool, error) {
	if registry == nil {
		return false, ErrRemotePartRegistryNil
	}
	if !validRemotePartRegistryKey(key) || generation == 0 {
		return false, fmt.Errorf("%w: key and positive generation are required", ErrRemotePartRegistryInvalid)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	existing, ok := registry.entries[key]
	if !ok {
		return false, nil
	}
	if existing.Generation != generation {
		return false, fmt.Errorf("%w: key %q generation %d does not match %d", ErrRemotePartRegistryStale, key, generation, existing.Generation)
	}
	delete(registry.entries, key)
	return true, nil
}

// Snapshot returns deterministic metadata-only registrations sorted by key.
func (registry *RemotePartRegistry) Snapshot() []RemotePartRegistration {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	snapshot := make([]RemotePartRegistration, 0, len(registry.entries))
	for _, registration := range registry.entries {
		snapshot = append(snapshot, registration)
	}
	registry.mu.RUnlock()
	sort.Slice(snapshot, func(left, right int) bool {
		return snapshot[left].Key < snapshot[right].Key
	})
	return snapshot
}

// Len returns the number of registered logical parts.
func (registry *RemotePartRegistry) Len() int {
	if registry == nil {
		return 0
	}
	registry.mu.RLock()
	length := len(registry.entries)
	registry.mu.RUnlock()
	return length
}

func validateRemotePartRegistration(registration RemotePartRegistration) (RemotePartRegistration, error) {
	if !validRemotePartRegistryKey(registration.Key) || registration.Generation == 0 {
		return RemotePartRegistration{}, fmt.Errorf("%w: key must be non-empty and generation must be positive", ErrRemotePartRegistryInvalid)
	}
	metadata := registration.Reference.metadata
	if metadata.ObjectURI == "" || metadata.LocalMetadataPath == "" || metadata.Checksum == "" ||
		strings.IndexByte(metadata.ObjectURI, 0) >= 0 || strings.IndexByte(metadata.LocalMetadataPath, 0) >= 0 ||
		strings.IndexByte(metadata.Checksum, 0) >= 0 || filepath.IsAbs(metadata.LocalMetadataPath) ||
		metadata.LocalMetadataPath == "." || metadata.LocalMetadataPath == ".." ||
		strings.HasPrefix(metadata.LocalMetadataPath, ".."+string(filepath.Separator)) {
		return RemotePartRegistration{}, fmt.Errorf("%w: reference is not a valid constructed reference", ErrRemotePartRegistryInvalid)
	}
	return registration, nil
}

func validRemotePartRegistryKey(key string) bool {
	return key != "" && strings.TrimSpace(key) == key && strings.IndexByte(key, 0) < 0
}
