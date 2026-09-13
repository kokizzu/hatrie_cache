package hatSql

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	// DefaultSQLNamedSettingsMaxCollections bounds collections in a default
	// registry.
	DefaultSQLNamedSettingsMaxCollections = 256
	// DefaultSQLNamedSettingsMaxSettingsPerCollection bounds settings in one
	// default collection.
	DefaultSQLNamedSettingsMaxSettingsPerCollection = 128
	// DefaultSQLNamedSettingsMaxSettingValueBytes bounds one setting value in a
	// default registry.
	DefaultSQLNamedSettingsMaxSettingValueBytes = 16 << 10
	// DefaultSQLNamedSettingsMaxInheritanceDepth bounds parent links followed
	// while resolving a profile.
	DefaultSQLNamedSettingsMaxInheritanceDepth = 8
	maxSQLNamedSettingsCollections             = 4096
	maxSQLNamedSettingsPerCollection           = 1024
	maxSQLNamedSettingsValueBytes              = 1 << 20
	maxSQLNamedSettingsTotalEntries            = 1 << 20
	maxSQLNamedSettingsNameBytes               = 256
	maxSQLNamedSettingsKeyBytes                = 256
	maxSQLNamedSettingsInheritanceDepth        = 64
)

var (
	ErrSQLNamedSettingsNameRequired       = errors.New("hatSql: named settings collection name is required")
	ErrSQLNamedSettingsSettingInvalid     = errors.New("hatSql: named setting is invalid")
	ErrSQLNamedSettingsLimitInvalid       = errors.New("hatSql: named settings limit is invalid")
	ErrSQLNamedSettingsLimitExceeded      = errors.New("hatSql: named settings limit exceeded")
	ErrSQLNamedSettingsConflict           = errors.New("hatSql: named settings revision conflict")
	ErrSQLNamedSettingsCollectionNotFound = errors.New("hatSql: named settings collection not found")
	ErrSQLNamedSettingsParentInvalid      = errors.New("hatSql: named settings parent is invalid")
	ErrSQLNamedSettingsParentInUse        = errors.New("hatSql: named settings parent is in use")
)

// SQLNamedSettingValidator validates a setting before it is published or
// applied as a caller override.
type SQLNamedSettingValidator func(key, value string) error

// SQLNamedSettingsRegistryOptions bounds a named settings registry. Values
// are strings so callers can parse them according to their query or storage
// option schema without reflection in the registry.
type SQLNamedSettingsRegistryOptions struct {
	MaxCollections           int
	MaxSettingsPerCollection int
	MaxSettingValueBytes     int
	MaxInheritanceDepth      int
	ValidateSetting          SQLNamedSettingValidator
}

// SQLNamedSettingsProfile describes a profile publication. Parent is resolved
// first, then Values override inherited settings.
type SQLNamedSettingsProfile struct {
	Parent string            `json:"parent,omitempty"`
	Values map[string]string `json:"values"`
}

// SQLNamedSettingsCollection is an isolated versioned settings profile.
// Values returned by the registry are caller-owned copies.
type SQLNamedSettingsCollection struct {
	Name     string            `json:"name"`
	Revision uint64            `json:"revision"`
	Parent   string            `json:"parent,omitempty"`
	Values   map[string]string `json:"values"`
}

// SQLNamedSettingsRegistrySnapshot is a consistent immutable-view copy of all
// collections at one registry revision.
type SQLNamedSettingsRegistrySnapshot struct {
	Revision    uint64                       `json:"revision"`
	Collections []SQLNamedSettingsCollection `json:"collections"`
}

// SQLNamedSettingsRegistryStats contains bounded aggregate registry metrics.
type SQLNamedSettingsRegistryStats struct {
	Revision        uint64 `json:"revision"`
	CollectionCount int    `json:"collection_count"`
	SettingCount    int    `json:"setting_count"`
}

type sqlNamedSettingsCollection struct {
	name     string
	revision uint64
	parent   string
	values   map[string]string
}

type sqlNamedSettingsSnapshot struct {
	revision    uint64
	collections map[string]*sqlNamedSettingsCollection
}

// SQLNamedSettingsRegistry stores versioned named profiles with atomic
// immutable publication. Lookup, Resolve, Snapshot, and Stats never mutate
// data visible to another caller. The zero value is usable.
type SQLNamedSettingsRegistry struct {
	mu                       sync.Mutex
	maxCollections           int
	maxSettingsPerCollection int
	maxSettingValueBytes     int
	maxInheritanceDepth      int
	validateSetting          SQLNamedSettingValidator
	snapshot                 atomic.Pointer[sqlNamedSettingsSnapshot]
}

// NewSQLNamedSettingsRegistry creates a bounded named settings registry. Zero
// limits select sane defaults; excessive configured capacity is rejected.
func NewSQLNamedSettingsRegistry(options SQLNamedSettingsRegistryOptions) (*SQLNamedSettingsRegistry, error) {
	maxCollections := options.MaxCollections
	if maxCollections == 0 {
		maxCollections = DefaultSQLNamedSettingsMaxCollections
	}
	if maxCollections < 0 || maxCollections > maxSQLNamedSettingsCollections {
		return nil, fmt.Errorf("%w: collections", ErrSQLNamedSettingsLimitInvalid)
	}
	maxSettings := options.MaxSettingsPerCollection
	if maxSettings == 0 {
		maxSettings = DefaultSQLNamedSettingsMaxSettingsPerCollection
	}
	if maxSettings < 0 || maxSettings > maxSQLNamedSettingsPerCollection {
		return nil, fmt.Errorf("%w: settings per collection", ErrSQLNamedSettingsLimitInvalid)
	}
	maxValueBytes := options.MaxSettingValueBytes
	if maxValueBytes == 0 {
		maxValueBytes = DefaultSQLNamedSettingsMaxSettingValueBytes
	}
	if maxValueBytes < 0 || maxValueBytes > maxSQLNamedSettingsValueBytes {
		return nil, fmt.Errorf("%w: setting value bytes", ErrSQLNamedSettingsLimitInvalid)
	}
	maxInheritanceDepth := options.MaxInheritanceDepth
	if maxInheritanceDepth == 0 {
		maxInheritanceDepth = DefaultSQLNamedSettingsMaxInheritanceDepth
	}
	if maxInheritanceDepth < 0 || maxInheritanceDepth > maxSQLNamedSettingsInheritanceDepth {
		return nil, fmt.Errorf("%w: inheritance depth", ErrSQLNamedSettingsLimitInvalid)
	}
	if maxCollections > maxSQLNamedSettingsTotalEntries/maxSettings {
		return nil, fmt.Errorf("%w: total settings entries", ErrSQLNamedSettingsLimitInvalid)
	}
	registry := &SQLNamedSettingsRegistry{
		maxCollections:           maxCollections,
		maxSettingsPerCollection: maxSettings,
		maxSettingValueBytes:     maxValueBytes,
		maxInheritanceDepth:      maxInheritanceDepth,
		validateSetting:          options.ValidateSetting,
	}
	registry.snapshot.Store(&sqlNamedSettingsSnapshot{collections: make(map[string]*sqlNamedSettingsCollection, maxCollections)})
	return registry, nil
}

// Put atomically replaces or creates a named settings collection.
func (registry *SQLNamedSettingsRegistry) Put(name string, values map[string]string) (SQLNamedSettingsCollection, error) {
	return registry.put(name, 0, "", values, false)
}

// PutIfRevision atomically replaces a collection only when the registry still
// has expectedRevision. This prevents stale configuration writers from
// silently overwriting a newer profile.
func (registry *SQLNamedSettingsRegistry) PutIfRevision(name string, expectedRevision uint64, values map[string]string) (SQLNamedSettingsCollection, error) {
	return registry.put(name, expectedRevision, "", values, true)
}

// PutProfile atomically replaces or creates a profile with an optional
// inherited parent.
func (registry *SQLNamedSettingsRegistry) PutProfile(name string, profile SQLNamedSettingsProfile) (SQLNamedSettingsCollection, error) {
	return registry.put(name, 0, profile.Parent, profile.Values, false)
}

// PutProfileIfRevision atomically replaces a profile only when the registry
// still has expectedRevision.
func (registry *SQLNamedSettingsRegistry) PutProfileIfRevision(name string, expectedRevision uint64, profile SQLNamedSettingsProfile) (SQLNamedSettingsCollection, error) {
	return registry.put(name, expectedRevision, profile.Parent, profile.Values, true)
}

// DeleteIfRevision removes a collection only when the registry still has
// expectedRevision.
func (registry *SQLNamedSettingsRegistry) DeleteIfRevision(name string, expectedRevision uint64) error {
	if registry == nil {
		return ErrSQLNamedSettingsCollectionNotFound
	}
	if err := validateSQLNamedSettingsName(name); err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	current := registry.currentSnapshotLocked()
	if current.revision != expectedRevision {
		return fmt.Errorf("%w: expected %d, current %d", ErrSQLNamedSettingsConflict, expectedRevision, current.revision)
	}
	if _, ok := current.collections[name]; !ok {
		return ErrSQLNamedSettingsCollectionNotFound
	}
	for _, collection := range current.collections {
		if collection.parent == name {
			return fmt.Errorf("%w: %s", ErrSQLNamedSettingsParentInUse, collection.name)
		}
	}
	collections := cloneSQLNamedSettingsCollections(current.collections)
	delete(collections, name)
	registry.snapshot.Store(&sqlNamedSettingsSnapshot{revision: current.revision + 1, collections: collections})
	return nil
}

// Lookup returns an isolated copy of one named settings collection.
func (registry *SQLNamedSettingsRegistry) Lookup(name string) (SQLNamedSettingsCollection, bool) {
	if registry == nil || validateSQLNamedSettingsName(name) != nil {
		return SQLNamedSettingsCollection{}, false
	}
	snapshot := registry.snapshot.Load()
	if snapshot == nil {
		return SQLNamedSettingsCollection{}, false
	}
	collection, ok := snapshot.collections[name]
	if !ok {
		return SQLNamedSettingsCollection{}, false
	}
	return cloneSQLNamedSettingsCollection(collection), true
}

// LookupValue returns one setting without cloning the rest of its collection.
// The returned string is immutable and caller-owned by Go's string value
// semantics, so this path performs no allocation.
func (registry *SQLNamedSettingsRegistry) LookupValue(name, key string) (string, bool) {
	if registry == nil || validateSQLNamedSettingsName(name) != nil || validateSQLNamedSettingsKey(key) != nil {
		return "", false
	}
	snapshot := registry.snapshot.Load()
	if snapshot == nil {
		return "", false
	}
	collection, ok := snapshot.collections[name]
	if !ok {
		return "", false
	}
	value, ok := collection.values[key]
	return value, ok
}

// Resolve returns one consistent profile with caller-owned overrides applied.
// For inherited profiles, the revision is the highest publication revision in
// the effective parent chain.
func (registry *SQLNamedSettingsRegistry) Resolve(name string, overrides map[string]string) (SQLNamedSettingsCollection, error) {
	if registry == nil {
		return SQLNamedSettingsCollection{}, ErrSQLNamedSettingsCollectionNotFound
	}
	if err := validateSQLNamedSettingsName(name); err != nil {
		return SQLNamedSettingsCollection{}, err
	}
	if err := registry.validateSQLNamedSettingsValues(overrides); err != nil {
		return SQLNamedSettingsCollection{}, err
	}
	snapshot := registry.snapshot.Load()
	if snapshot == nil {
		return SQLNamedSettingsCollection{}, ErrSQLNamedSettingsCollectionNotFound
	}
	base, ok := snapshot.collections[name]
	if !ok {
		return SQLNamedSettingsCollection{}, ErrSQLNamedSettingsCollectionNotFound
	}
	var values map[string]string
	revision := base.revision
	if base.parent == "" {
		values = cloneSQLNamedSettingsValues(base.values)
	} else {
		inheritedValues, inheritedRevision, err := registry.resolveSQLNamedSettingsValues(snapshot, name)
		if err != nil {
			return SQLNamedSettingsCollection{}, err
		}
		values, revision = inheritedValues, inheritedRevision
	}
	for key, value := range overrides {
		if _, exists := values[key]; !exists && len(values) >= registry.configuredMaxSettings() {
			return SQLNamedSettingsCollection{}, fmt.Errorf("%w: settings per collection", ErrSQLNamedSettingsLimitExceeded)
		}
		values[key] = value
	}
	return SQLNamedSettingsCollection{Name: name, Revision: revision, Parent: base.parent, Values: values}, nil
}

// Snapshot returns an isolated, deterministic copy of the registry at one
// atomic revision.
func (registry *SQLNamedSettingsRegistry) Snapshot() SQLNamedSettingsRegistrySnapshot {
	if registry == nil {
		return SQLNamedSettingsRegistrySnapshot{}
	}
	snapshot := registry.snapshot.Load()
	if snapshot == nil {
		return SQLNamedSettingsRegistrySnapshot{}
	}
	collections := make([]SQLNamedSettingsCollection, 0, len(snapshot.collections))
	for _, collection := range snapshot.collections {
		collections = append(collections, cloneSQLNamedSettingsCollection(collection))
	}
	sort.Slice(collections, func(left, right int) bool {
		return collections[left].Name < collections[right].Name
	})
	return SQLNamedSettingsRegistrySnapshot{Revision: snapshot.revision, Collections: collections}
}

// Stats returns aggregate counts without exposing setting names or values.
func (registry *SQLNamedSettingsRegistry) Stats() SQLNamedSettingsRegistryStats {
	if registry == nil {
		return SQLNamedSettingsRegistryStats{}
	}
	snapshot := registry.snapshot.Load()
	if snapshot == nil {
		return SQLNamedSettingsRegistryStats{}
	}
	stats := SQLNamedSettingsRegistryStats{Revision: snapshot.revision, CollectionCount: len(snapshot.collections)}
	for _, collection := range snapshot.collections {
		stats.SettingCount += len(collection.values)
	}
	return stats
}

func (registry *SQLNamedSettingsRegistry) put(name string, expectedRevision uint64, parent string, values map[string]string, compareRevision bool) (SQLNamedSettingsCollection, error) {
	if registry == nil {
		return SQLNamedSettingsCollection{}, ErrSQLNamedSettingsCollectionNotFound
	}
	if err := validateSQLNamedSettingsName(name); err != nil {
		return SQLNamedSettingsCollection{}, err
	}
	if parent != "" {
		if err := validateSQLNamedSettingsName(parent); err != nil {
			return SQLNamedSettingsCollection{}, fmt.Errorf("%w: %v", ErrSQLNamedSettingsParentInvalid, err)
		}
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	current := registry.currentSnapshotLocked()
	if compareRevision && current.revision != expectedRevision {
		return SQLNamedSettingsCollection{}, fmt.Errorf("%w: expected %d, current %d", ErrSQLNamedSettingsConflict, expectedRevision, current.revision)
	}
	clonedValues, err := cloneAndValidateSQLNamedSettingsValues(values, registry.configuredMaxSettings(), registry.configuredMaxSettingValueBytes(), registry.validateSetting)
	if err != nil {
		return SQLNamedSettingsCollection{}, err
	}
	if _, exists := current.collections[name]; !exists && len(current.collections) >= registry.configuredMaxCollections() {
		return SQLNamedSettingsCollection{}, fmt.Errorf("%w: collections", ErrSQLNamedSettingsLimitExceeded)
	}
	if parent != "" {
		if _, exists := current.collections[parent]; !exists {
			return SQLNamedSettingsCollection{}, fmt.Errorf("%w: parent %q does not exist", ErrSQLNamedSettingsParentInvalid, parent)
		}
	}
	collections := cloneSQLNamedSettingsCollections(current.collections)
	revision := current.revision + 1
	collection := &sqlNamedSettingsCollection{name: name, revision: revision, parent: parent, values: clonedValues}
	collections[name] = collection
	updated := &sqlNamedSettingsSnapshot{revision: revision, collections: collections}
	if err := registry.validateSQLNamedSettingsSnapshot(updated); err != nil {
		return SQLNamedSettingsCollection{}, err
	}
	registry.snapshot.Store(updated)
	return cloneSQLNamedSettingsCollection(collection), nil
}

func (registry *SQLNamedSettingsRegistry) currentSnapshotLocked() *sqlNamedSettingsSnapshot {
	snapshot := registry.snapshot.Load()
	if snapshot != nil {
		return snapshot
	}
	snapshot = &sqlNamedSettingsSnapshot{collections: make(map[string]*sqlNamedSettingsCollection)}
	registry.snapshot.Store(snapshot)
	return snapshot
}

func (registry *SQLNamedSettingsRegistry) configuredMaxCollections() int {
	if registry.maxCollections > 0 {
		return registry.maxCollections
	}
	return DefaultSQLNamedSettingsMaxCollections
}

func (registry *SQLNamedSettingsRegistry) configuredMaxSettings() int {
	if registry.maxSettingsPerCollection > 0 {
		return registry.maxSettingsPerCollection
	}
	return DefaultSQLNamedSettingsMaxSettingsPerCollection
}

func (registry *SQLNamedSettingsRegistry) configuredMaxSettingValueBytes() int {
	if registry.maxSettingValueBytes > 0 {
		return registry.maxSettingValueBytes
	}
	return DefaultSQLNamedSettingsMaxSettingValueBytes
}

func (registry *SQLNamedSettingsRegistry) configuredMaxInheritanceDepth() int {
	if registry.maxInheritanceDepth > 0 {
		return registry.maxInheritanceDepth
	}
	return DefaultSQLNamedSettingsMaxInheritanceDepth
}

func (registry *SQLNamedSettingsRegistry) validateSQLNamedSettingsValues(values map[string]string) error {
	return validateSQLNamedSettingsValues(values, registry.configuredMaxSettings(), registry.configuredMaxSettingValueBytes(), registry.validateSetting)
}

func (registry *SQLNamedSettingsRegistry) validateSQLNamedSettingsSnapshot(snapshot *sqlNamedSettingsSnapshot) error {
	for name, collection := range snapshot.collections {
		if collection.parent == "" {
			continue
		}
		if _, _, err := registry.resolveSQLNamedSettingsValues(snapshot, name); err != nil {
			return fmt.Errorf("profile %q: %w", name, err)
		}
	}
	return nil
}

func (registry *SQLNamedSettingsRegistry) resolveSQLNamedSettingsValues(snapshot *sqlNamedSettingsSnapshot, name string) (map[string]string, uint64, error) {
	base, ok := snapshot.collections[name]
	if !ok {
		return nil, 0, ErrSQLNamedSettingsCollectionNotFound
	}
	if base.parent == "" {
		return cloneSQLNamedSettingsValues(base.values), base.revision, nil
	}

	var chainStorage [maxSQLNamedSettingsInheritanceDepth + 1]*sqlNamedSettingsCollection
	chain := chainStorage[:0]
	current := base
	parentDepth := 0
	var revision uint64
	for {
		for _, seen := range chain {
			if seen.name == current.name {
				return nil, 0, fmt.Errorf("%w: cycle at %q", ErrSQLNamedSettingsParentInvalid, current.name)
			}
		}
		chain = append(chain, current)
		if current.revision > revision {
			revision = current.revision
		}
		if current.parent == "" {
			break
		}
		if parentDepth >= registry.configuredMaxInheritanceDepth() {
			return nil, 0, fmt.Errorf("%w: depth exceeds %d", ErrSQLNamedSettingsParentInvalid, registry.configuredMaxInheritanceDepth())
		}
		parentDepth++
		parent, exists := snapshot.collections[current.parent]
		if !exists {
			return nil, 0, fmt.Errorf("%w: parent %q does not exist", ErrSQLNamedSettingsParentInvalid, current.parent)
		}
		current = parent
	}

	capacity := 0
	for _, collection := range chain {
		capacity += len(collection.values)
		if capacity >= registry.configuredMaxSettings() {
			capacity = registry.configuredMaxSettings()
			break
		}
	}
	values := make(map[string]string, capacity)
	for index := len(chain) - 1; index >= 0; index-- {
		for key, value := range chain[index].values {
			values[key] = value
		}
	}
	if len(values) > registry.configuredMaxSettings() {
		return nil, 0, fmt.Errorf("%w: effective inherited settings", ErrSQLNamedSettingsLimitExceeded)
	}
	return values, revision, nil
}

func validateSQLNamedSettingsName(name string) error {
	if strings.TrimSpace(name) == "" {
		return ErrSQLNamedSettingsNameRequired
	}
	if len(name) > maxSQLNamedSettingsNameBytes {
		return fmt.Errorf("%w: name exceeds %d bytes", ErrSQLNamedSettingsNameRequired, maxSQLNamedSettingsNameBytes)
	}
	return nil
}

func validateSQLNamedSettingsValues(values map[string]string, maxSettings, maxValueBytes int, validator SQLNamedSettingValidator) error {
	if len(values) > maxSettings {
		return fmt.Errorf("%w: settings per collection", ErrSQLNamedSettingsLimitExceeded)
	}
	for key, value := range values {
		if validateSQLNamedSettingsKey(key) != nil || len(value) > maxValueBytes {
			return fmt.Errorf("%w: key or value", ErrSQLNamedSettingsSettingInvalid)
		}
		if validator != nil {
			if err := validator(key, value); err != nil {
				return fmt.Errorf("%w: %s: %v", ErrSQLNamedSettingsSettingInvalid, key, err)
			}
		}
	}
	return nil
}

func validateSQLNamedSettingsKey(key string) error {
	if strings.TrimSpace(key) == "" || len(key) > maxSQLNamedSettingsKeyBytes {
		return ErrSQLNamedSettingsSettingInvalid
	}
	return nil
}

func cloneAndValidateSQLNamedSettingsValues(values map[string]string, maxSettings, maxValueBytes int, validator SQLNamedSettingValidator) (map[string]string, error) {
	if err := validateSQLNamedSettingsValues(values, maxSettings, maxValueBytes, validator); err != nil {
		return nil, err
	}
	return cloneSQLNamedSettingsValues(values), nil
}

func cloneSQLNamedSettingsValues(values map[string]string) map[string]string {
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func cloneSQLNamedSettingsCollection(collection *sqlNamedSettingsCollection) SQLNamedSettingsCollection {
	return SQLNamedSettingsCollection{Name: collection.name, Revision: collection.revision, Parent: collection.parent, Values: cloneSQLNamedSettingsValues(collection.values)}
}

func cloneSQLNamedSettingsCollections(collections map[string]*sqlNamedSettingsCollection) map[string]*sqlNamedSettingsCollection {
	cloned := make(map[string]*sqlNamedSettingsCollection, len(collections))
	for name, collection := range collections {
		cloned[name] = &sqlNamedSettingsCollection{name: collection.name, revision: collection.revision, parent: collection.parent, values: collection.values}
	}
	return cloned
}
