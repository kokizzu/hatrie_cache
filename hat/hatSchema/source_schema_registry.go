package hatSchema

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

const (
	// DefaultSourceSchemaRegistryMaxSources bounds the number of named sources
	// retained by a zero-config registry.
	DefaultSourceSchemaRegistryMaxSources = 256
	// DefaultSourceSchemaRegistryMaxVersionsPerSource bounds retained history
	// for each named source.
	DefaultSourceSchemaRegistryMaxVersionsPerSource = 64
	maxSourceSchemaRegistrySources                  = 1 << 16
	maxSourceSchemaRegistryVersionsPerSource        = 1 << 16
)

// SourceSchemaCompatibilityPolicy controls checks between successive source
// versions. The registry is opt-in; it does not alter existing schema paths.
type SourceSchemaCompatibilityPolicy uint8

const (
	// SourceSchemaCompatibilityRolling allows only changes accepted by
	// CheckRollingCompatibility.
	SourceSchemaCompatibilityRolling SourceSchemaCompatibilityPolicy = iota + 1
	// SourceSchemaCompatibilityAny records versions without a compatibility
	// check, while still rejecting duplicate version/fingerprint conflicts.
	SourceSchemaCompatibilityAny
)

// SourceSchemaRegistryOptions bounds history and selects its compatibility
// policy. Zero limits select the documented defaults.
type SourceSchemaRegistryOptions struct {
	MaxSources           int
	MaxVersionsPerSource int
	Compatibility        SourceSchemaCompatibilityPolicy
}

// SourceSchemaVersion is the immutable public description of one registered
// source version. Definition is copied on registration and on every read.
type SourceSchemaVersion struct {
	Source      string `json:"source"`
	Version     uint64 `json:"version"`
	Fingerprint string `json:"fingerprint"`
	Definition  Source `json:"definition"`
}

var (
	// ErrSourceSchemaRegistryNil reports a nil registry receiver.
	ErrSourceSchemaRegistryNil = errors.New("source schema registry is nil")
	// ErrSourceSchemaRegistryInvalid reports invalid options or input.
	ErrSourceSchemaRegistryInvalid = errors.New("source schema registry is invalid")
	// ErrSourceSchemaRegistryLimit reports a configured history bound.
	ErrSourceSchemaRegistryLimit = errors.New("source schema registry limit exceeded")
	// ErrSourceSchemaUnknownSource reports an unregistered source name.
	ErrSourceSchemaUnknownSource = errors.New("source schema source is unknown")
	// ErrSourceSchemaUnknownVersion reports an unregistered source version.
	ErrSourceSchemaUnknownVersion = errors.New("source schema version is unknown")
	// ErrSourceSchemaFingerprintMismatch reports a known version with a
	// different schema fingerprint.
	ErrSourceSchemaFingerprintMismatch = errors.New("source schema fingerprint mismatch")
	// ErrSourceSchemaVersionConflict reports a duplicate version with a
	// different definition.
	ErrSourceSchemaVersionConflict = errors.New("source schema version conflicts")
	// ErrSourceSchemaVersionOrder reports a version older than the latest
	// registered version.
	ErrSourceSchemaVersionOrder = errors.New("source schema version is out of order")
	// ErrSourceSchemaIncompatible reports a rejected rolling change.
	ErrSourceSchemaIncompatible = errors.New("source schema is incompatible")
)

// SourceSchemaRegistry retains bounded, versioned source definitions for CDC
// or other producers that attach a schema version to each event. Registration
// is serialized; Validate and Lookup use read locks and a sorted history slice
// without allocating on the successful hot path.
type SourceSchemaRegistry struct {
	mu                   sync.RWMutex
	maxSources           int
	maxVersionsPerSource int
	compatibility        SourceSchemaCompatibilityPolicy
	sources              map[string][]SourceSchemaVersion
}

// NewSourceSchemaRegistry creates an independent bounded source registry.
func NewSourceSchemaRegistry(options SourceSchemaRegistryOptions) (*SourceSchemaRegistry, error) {
	maxSources := options.MaxSources
	if maxSources == 0 {
		maxSources = DefaultSourceSchemaRegistryMaxSources
	}
	maxVersions := options.MaxVersionsPerSource
	if maxVersions == 0 {
		maxVersions = DefaultSourceSchemaRegistryMaxVersionsPerSource
	}
	compatibility := options.Compatibility
	if compatibility == 0 {
		compatibility = SourceSchemaCompatibilityRolling
	}
	if maxSources < 1 || maxSources > maxSourceSchemaRegistrySources || maxVersions < 1 || maxVersions > maxSourceSchemaRegistryVersionsPerSource {
		return nil, fmt.Errorf("%w: max sources=%d max versions=%d", ErrSourceSchemaRegistryInvalid, maxSources, maxVersions)
	}
	if compatibility != SourceSchemaCompatibilityRolling && compatibility != SourceSchemaCompatibilityAny {
		return nil, fmt.Errorf("%w: compatibility policy=%d", ErrSourceSchemaRegistryInvalid, compatibility)
	}
	return &SourceSchemaRegistry{
		maxSources:           maxSources,
		maxVersionsPerSource: maxVersions,
		compatibility:        compatibility,
		sources:              make(map[string][]SourceSchemaVersion),
	}, nil
}

// Register validates and records one source version. Re-registering the same
// version and fingerprint is idempotent; all other duplicate or out-of-order
// versions are rejected without changing the registry.
func (registry *SourceSchemaRegistry) Register(source Source, version uint64) (SourceSchemaVersion, error) {
	if registry == nil {
		return SourceSchemaVersion{}, ErrSourceSchemaRegistryNil
	}
	if version == 0 || strings.TrimSpace(source.Name) == "" || strings.TrimSpace(source.Name) != source.Name {
		return SourceSchemaVersion{}, fmt.Errorf("%w: source name and positive version are required", ErrSourceSchemaRegistryInvalid)
	}
	definition := cloneSource(source)
	if err := validateRegisteredSource(definition, version); err != nil {
		return SourceSchemaVersion{}, err
	}
	fingerprint := sourceSchemaFingerprint(definition)

	registry.mu.Lock()
	defer registry.mu.Unlock()
	history, exists := registry.sources[definition.Name]
	if !exists {
		if len(registry.sources) >= registry.maxSources {
			return SourceSchemaVersion{}, fmt.Errorf("%w: sources=%d", ErrSourceSchemaRegistryLimit, registry.maxSources)
		}
		entry := SourceSchemaVersion{Source: definition.Name, Version: version, Fingerprint: fingerprint, Definition: definition}
		registry.sources[definition.Name] = []SourceSchemaVersion{entry}
		return cloneSourceSchemaVersion(entry), nil
	}

	index := sort.Search(len(history), func(index int) bool {
		return history[index].Version >= version
	})
	if index < len(history) && history[index].Version == version {
		if history[index].Fingerprint != fingerprint {
			return SourceSchemaVersion{}, fmt.Errorf("%w: source=%q version=%d", ErrSourceSchemaVersionConflict, definition.Name, version)
		}
		return cloneSourceSchemaVersion(history[index]), nil
	}
	if index != len(history) {
		return SourceSchemaVersion{}, fmt.Errorf("%w: source=%q version=%d latest=%d", ErrSourceSchemaVersionOrder, definition.Name, version, history[len(history)-1].Version)
	}
	if len(history) >= registry.maxVersionsPerSource {
		return SourceSchemaVersion{}, fmt.Errorf("%w: source=%q versions=%d", ErrSourceSchemaRegistryLimit, definition.Name, registry.maxVersionsPerSource)
	}
	if registry.compatibility == SourceSchemaCompatibilityRolling {
		previous := Schema{Version: history[len(history)-1].Version, Sources: map[string]Source{definition.Name: history[len(history)-1].Definition}}
		next := Schema{Version: version, Sources: map[string]Source{definition.Name: definition}}
		report, err := CheckRollingCompatibility(previous, next)
		if err != nil {
			return SourceSchemaVersion{}, fmt.Errorf("%w: source=%q version=%d: %v", ErrSourceSchemaIncompatible, definition.Name, version, err)
		}
		if !report.Compatible {
			return SourceSchemaVersion{}, fmt.Errorf("%w: source=%q version=%d", ErrSourceSchemaIncompatible, definition.Name, version)
		}
	}
	entry := SourceSchemaVersion{Source: definition.Name, Version: version, Fingerprint: fingerprint, Definition: definition}
	registry.sources[definition.Name] = append(history, entry)
	return cloneSourceSchemaVersion(entry), nil
}

func validateRegisteredSource(source Source, version uint64) error {
	schema := Schema{Version: version, Sources: map[string]Source{source.Name: source}}
	if err := schema.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrSourceSchemaRegistryInvalid, err)
	}
	return nil
}

// Validate checks an event's source/version/fingerprint against the registry.
// A successful call does not allocate and does not expose retained schema
// memory.
func (registry *SourceSchemaRegistry) Validate(source string, version uint64, fingerprint string) error {
	if registry == nil {
		return ErrSourceSchemaRegistryNil
	}
	if source == "" || version == 0 || fingerprint == "" {
		return ErrSourceSchemaRegistryInvalid
	}
	registry.mu.RLock()
	history, exists := registry.sources[source]
	if !exists {
		registry.mu.RUnlock()
		return fmt.Errorf("%w: %q", ErrSourceSchemaUnknownSource, source)
	}
	index := sort.Search(len(history), func(index int) bool {
		return history[index].Version >= version
	})
	if index == len(history) || history[index].Version != version {
		registry.mu.RUnlock()
		return fmt.Errorf("%w: source=%q version=%d", ErrSourceSchemaUnknownVersion, source, version)
	}
	if history[index].Fingerprint != fingerprint {
		registry.mu.RUnlock()
		return fmt.Errorf("%w: source=%q version=%d", ErrSourceSchemaFingerprintMismatch, source, version)
	}
	registry.mu.RUnlock()
	return nil
}

// Lookup returns an independent copy of a registered version.
func (registry *SourceSchemaRegistry) Lookup(source string, version uint64) (SourceSchemaVersion, bool) {
	if registry == nil || source == "" || version == 0 {
		return SourceSchemaVersion{}, false
	}
	registry.mu.RLock()
	history := registry.sources[source]
	index := sort.Search(len(history), func(index int) bool {
		return history[index].Version >= version
	})
	if index == len(history) || history[index].Version != version {
		registry.mu.RUnlock()
		return SourceSchemaVersion{}, false
	}
	entry := cloneSourceSchemaVersion(history[index])
	registry.mu.RUnlock()
	return entry, true
}

// Snapshot returns all registered versions in deterministic source/version
// order. Returned definitions are independent of the registry.
func (registry *SourceSchemaRegistry) Snapshot() []SourceSchemaVersion {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	names := make([]string, 0, len(registry.sources))
	for name := range registry.sources {
		names = append(names, name)
	}
	sort.Strings(names)
	versions := 0
	for _, name := range names {
		versions += len(registry.sources[name])
	}
	out := make([]SourceSchemaVersion, 0, versions)
	for _, name := range names {
		for _, entry := range registry.sources[name] {
			out = append(out, cloneSourceSchemaVersion(entry))
		}
	}
	registry.mu.RUnlock()
	return out
}

func cloneSourceSchemaVersion(entry SourceSchemaVersion) SourceSchemaVersion {
	entry.Definition = cloneSource(entry.Definition)
	return entry
}

func sourceSchemaFingerprint(source Source) string {
	return (Schema{Sources: map[string]Source{source.Name: source}}).Fingerprint()
}
