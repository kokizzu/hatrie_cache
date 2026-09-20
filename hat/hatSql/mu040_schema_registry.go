package hatSql

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"
)

var (
	// ErrSQLSchemaRegistryNil reports a nil schema registry.
	ErrSQLSchemaRegistryNil = errors.New("SQL schema registry is nil")
	// ErrSQLSchemaDefinitionInvalid reports malformed source schema metadata.
	ErrSQLSchemaDefinitionInvalid = errors.New("SQL schema definition is invalid")
	// ErrSQLSchemaVersionUnknown reports a source/version pair not registered.
	ErrSQLSchemaVersionUnknown = errors.New("SQL schema version is unknown")
	// ErrSQLSchemaIncompatible reports a compatibility-policy violation.
	ErrSQLSchemaIncompatible = errors.New("SQL schema is incompatible")
	// ErrSQLSchemaVersionExists reports an immutable version conflict.
	ErrSQLSchemaVersionExists = errors.New("SQL schema version already exists")
)

const (
	defaultSQLSchemaRegistryMaxSources  = 1024
	defaultSQLSchemaRegistryMaxVersions = 256
	defaultSQLSchemaRegistryMaxColumns  = 256
	maxSQLSchemaRegistrySources         = 1 << 16
	maxSQLSchemaRegistryVersions        = 1 << 16
	maxSQLSchemaRegistryColumns         = 1 << 12
	maxSQLSchemaRegistryIdentifierBytes = 256
	defaultSQLSchemaCompatibility       = SQLSchemaCompatibilityBackward
)

// SQLSchemaCompatibility controls validation between successive versions of a
// source schema. The policy is opt-in through SQLSchemaRegistry construction.
type SQLSchemaCompatibility string

const (
	// SQLSchemaCompatibilityNone records versions without comparing them.
	SQLSchemaCompatibilityNone SQLSchemaCompatibility = "none"
	// SQLSchemaCompatibilityBackward lets a new reader consume old data:
	// existing fields remain stable and new fields must be nullable.
	SQLSchemaCompatibilityBackward SQLSchemaCompatibility = "backward"
	// SQLSchemaCompatibilityForward lets an old reader consume new data:
	// existing fields remain stable and removed fields must be nullable.
	SQLSchemaCompatibilityForward SQLSchemaCompatibility = "forward"
	// SQLSchemaCompatibilityFull applies both backward and forward checks.
	SQLSchemaCompatibilityFull SQLSchemaCompatibility = "full"
)

// SQLSchemaDefinition identifies one immutable schema version for a source.
// Columns use the same named, typed representation as the RowBinary codec.
type SQLSchemaDefinition struct {
	Source  string               `json:"source"`
	Version string               `json:"version"`
	Columns []SQLRowBinaryColumn `json:"columns"`
}

// SQLSchemaRegistryOptions bounds retained schema metadata. Zero values select
// bounded defaults. Compatibility defaults to backward compatibility.
type SQLSchemaRegistryOptions struct {
	Compatibility        SQLSchemaCompatibility
	MaxSources           int
	MaxVersionsPerSource int
	MaxColumns           int
}

// SQLSchemaRegistry stores immutable source schema versions. It retains only
// schema metadata, not source rows or payloads, and is safe for concurrent
// registration, validation, lookup, and snapshot operations.
type SQLSchemaRegistry struct {
	mu      sync.RWMutex
	options SQLSchemaRegistryOptions
	sources map[string]*sqlSchemaHistory
}

type sqlSchemaHistory struct {
	versions    []string
	definitions map[string]SQLSchemaDefinition
}

// NewSQLSchemaRegistry creates a bounded, in-memory schema registry.
func NewSQLSchemaRegistry(options SQLSchemaRegistryOptions) (*SQLSchemaRegistry, error) {
	normalized, err := normalizeSQLSchemaRegistryOptions(options)
	if err != nil {
		return nil, err
	}
	return &SQLSchemaRegistry{
		options: normalized,
		sources: make(map[string]*sqlSchemaHistory),
	}, nil
}

// Register adds one immutable source schema version. Re-registering identical
// metadata is idempotent; reusing a version with different metadata fails.
func (registry *SQLSchemaRegistry) Register(definition SQLSchemaDefinition) error {
	if registry == nil {
		return ErrSQLSchemaRegistryNil
	}
	normalized, err := normalizeSQLSchemaDefinition(definition, registry.options.MaxColumns)
	if err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	history, found := registry.sources[normalized.Source]
	if !found {
		if len(registry.sources) >= registry.options.MaxSources {
			return fmt.Errorf("%w: source limit %d", ErrSQLSchemaDefinitionInvalid, registry.options.MaxSources)
		}
		history = &sqlSchemaHistory{definitions: make(map[string]SQLSchemaDefinition)}
		registry.sources[normalized.Source] = history
	}
	if existing, exists := history.definitions[normalized.Version]; exists {
		if equalSQLSchemaDefinitions(existing, normalized) {
			return nil
		}
		return fmt.Errorf("%w: source %q version %q", ErrSQLSchemaVersionExists, normalized.Source, normalized.Version)
	}
	if len(history.versions) >= registry.options.MaxVersionsPerSource {
		return fmt.Errorf("%w: source %q version limit %d", ErrSQLSchemaDefinitionInvalid, normalized.Source, registry.options.MaxVersionsPerSource)
	}
	if len(history.versions) != 0 {
		previous := history.definitions[history.versions[len(history.versions)-1]]
		if err := validateSQLSchemaCompatibility(previous, normalized, registry.options.Compatibility); err != nil {
			return err
		}
	}
	history.versions = append(history.versions, normalized.Version)
	history.definitions[normalized.Version] = cloneSQLSchemaDefinition(normalized)
	return nil
}

// Validate checks that a source transaction names a registered schema version.
func (registry *SQLSchemaRegistry) Validate(source, version string) error {
	if registry == nil {
		return ErrSQLSchemaRegistryNil
	}
	source, version, err := normalizeSQLSchemaIdentity(source, version)
	if err != nil {
		return err
	}
	return registry.validateNormalized(source, version)
}

func (registry *SQLSchemaRegistry) validateNormalized(source, version string) error {
	registry.mu.RLock()
	history := registry.sources[source]
	_, found := false, false
	if history != nil {
		_, found = history.definitions[version]
	}
	registry.mu.RUnlock()
	if !found {
		return fmt.Errorf("%w: source %q version %q", ErrSQLSchemaVersionUnknown, source, version)
	}
	return nil
}

// Lookup returns an independently owned definition for a registered version.
func (registry *SQLSchemaRegistry) Lookup(source, version string) (SQLSchemaDefinition, bool) {
	if registry == nil {
		return SQLSchemaDefinition{}, false
	}
	source, version, err := normalizeSQLSchemaIdentity(source, version)
	if err != nil {
		return SQLSchemaDefinition{}, false
	}
	registry.mu.RLock()
	history := registry.sources[source]
	definition, found := SQLSchemaDefinition{}, false
	if history != nil {
		definition, found = history.definitions[version]
	}
	registry.mu.RUnlock()
	if !found {
		return SQLSchemaDefinition{}, false
	}
	return cloneSQLSchemaDefinition(definition), true
}

// Versions returns registered versions in registration order.
func (registry *SQLSchemaRegistry) Versions(source string) []string {
	if registry == nil {
		return nil
	}
	source = strings.TrimSpace(source)
	registry.mu.RLock()
	history := registry.sources[source]
	var versions []string
	if history != nil {
		versions = append([]string(nil), history.versions...)
	}
	registry.mu.RUnlock()
	return versions
}

// Snapshot returns definitions in deterministic source/version order. The
// version order within each source is preserved so Restore replays policy
// checks in the same order.
func (registry *SQLSchemaRegistry) Snapshot() []SQLSchemaDefinition {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	sources := make([]string, 0, len(registry.sources))
	for source := range registry.sources {
		sources = append(sources, source)
	}
	sort.Strings(sources)
	snapshot := make([]SQLSchemaDefinition, 0)
	for _, source := range sources {
		history := registry.sources[source]
		for _, version := range history.versions {
			snapshot = append(snapshot, cloneSQLSchemaDefinition(history.definitions[version]))
		}
	}
	registry.mu.RUnlock()
	return snapshot
}

// Restore replaces all registry metadata after validating the complete
// snapshot. A failed restore leaves the current registry unchanged.
func (registry *SQLSchemaRegistry) Restore(snapshot []SQLSchemaDefinition) error {
	if registry == nil {
		return ErrSQLSchemaRegistryNil
	}
	replacement, err := newSQLSchemaRegistryFromSnapshot(snapshot, registry.options)
	if err != nil {
		return err
	}
	registry.mu.Lock()
	registry.sources = replacement.sources
	registry.mu.Unlock()
	return nil
}

func normalizeSQLSchemaRegistryOptions(options SQLSchemaRegistryOptions) (SQLSchemaRegistryOptions, error) {
	if options.Compatibility == "" {
		options.Compatibility = defaultSQLSchemaCompatibility
	}
	switch options.Compatibility {
	case SQLSchemaCompatibilityNone, SQLSchemaCompatibilityBackward, SQLSchemaCompatibilityForward, SQLSchemaCompatibilityFull:
	default:
		return SQLSchemaRegistryOptions{}, fmt.Errorf("%w: unknown compatibility policy %q", ErrSQLSchemaDefinitionInvalid, options.Compatibility)
	}
	if options.MaxSources < 0 || options.MaxSources > maxSQLSchemaRegistrySources {
		return SQLSchemaRegistryOptions{}, fmt.Errorf("%w: MaxSources must be between 0 and %d", ErrSQLSchemaDefinitionInvalid, maxSQLSchemaRegistrySources)
	}
	if options.MaxVersionsPerSource < 0 || options.MaxVersionsPerSource > maxSQLSchemaRegistryVersions {
		return SQLSchemaRegistryOptions{}, fmt.Errorf("%w: MaxVersionsPerSource must be between 0 and %d", ErrSQLSchemaDefinitionInvalid, maxSQLSchemaRegistryVersions)
	}
	if options.MaxColumns < 0 || options.MaxColumns > maxSQLSchemaRegistryColumns {
		return SQLSchemaRegistryOptions{}, fmt.Errorf("%w: MaxColumns must be between 0 and %d", ErrSQLSchemaDefinitionInvalid, maxSQLSchemaRegistryColumns)
	}
	if options.MaxSources == 0 {
		options.MaxSources = defaultSQLSchemaRegistryMaxSources
	}
	if options.MaxVersionsPerSource == 0 {
		options.MaxVersionsPerSource = defaultSQLSchemaRegistryMaxVersions
	}
	if options.MaxColumns == 0 {
		options.MaxColumns = defaultSQLSchemaRegistryMaxColumns
	}
	return options, nil
}

func normalizeSQLSchemaDefinition(definition SQLSchemaDefinition, maxColumns int) (SQLSchemaDefinition, error) {
	source, version, err := normalizeSQLSchemaIdentity(definition.Source, definition.Version)
	if err != nil {
		return SQLSchemaDefinition{}, err
	}
	if len(definition.Columns) == 0 || len(definition.Columns) > maxColumns {
		return SQLSchemaDefinition{}, fmt.Errorf("%w: source %q has %d columns, limit %d", ErrSQLSchemaDefinitionInvalid, source, len(definition.Columns), maxColumns)
	}
	columns := cloneSQLRowBinaryColumns(definition.Columns)
	if err := validateSQLRowBinaryColumns(columns); err != nil {
		return SQLSchemaDefinition{}, fmt.Errorf("%w: source %q: %v", ErrSQLSchemaDefinitionInvalid, source, err)
	}
	return SQLSchemaDefinition{Source: source, Version: version, Columns: columns}, nil
}

func normalizeSQLSchemaIdentity(source, version string) (string, string, error) {
	source = strings.TrimSpace(source)
	version = strings.TrimSpace(version)
	if source == "" || version == "" || len(source) > maxSQLSchemaRegistryIdentifierBytes || len(version) > maxSQLSchemaRegistryIdentifierBytes || !utf8.ValidString(source) || !utf8.ValidString(version) || strings.IndexFunc(source, func(r rune) bool { return r < 0x20 || r == 0x7f }) >= 0 || strings.IndexFunc(version, func(r rune) bool { return r < 0x20 || r == 0x7f }) >= 0 {
		return "", "", ErrSQLSchemaDefinitionInvalid
	}
	return source, version, nil
}

func normalizeSQLSchemaVersion(version string) (string, error) {
	version = strings.TrimSpace(version)
	if version == "" {
		return "", nil
	}
	_, normalized, err := normalizeSQLSchemaIdentity("source", version)
	return normalized, err
}

func newSQLSchemaRegistryFromSnapshot(snapshot []SQLSchemaDefinition, options SQLSchemaRegistryOptions) (*SQLSchemaRegistry, error) {
	registry := &SQLSchemaRegistry{options: options, sources: make(map[string]*sqlSchemaHistory)}
	for _, definition := range snapshot {
		if err := registry.Register(definition); err != nil {
			return nil, err
		}
	}
	return registry, nil
}

func cloneSQLSchemaDefinition(definition SQLSchemaDefinition) SQLSchemaDefinition {
	definition.Columns = cloneSQLRowBinaryColumns(definition.Columns)
	return definition
}

func cloneSQLRowBinaryColumns(columns []SQLRowBinaryColumn) []SQLRowBinaryColumn {
	if len(columns) == 0 {
		return nil
	}
	clone := make([]SQLRowBinaryColumn, len(columns))
	for index, column := range columns {
		clone[index] = column
		clone[index].EnumValues = append([]string(nil), column.EnumValues...)
	}
	return clone
}

func equalSQLSchemaDefinitions(left, right SQLSchemaDefinition) bool {
	if left.Source != right.Source || left.Version != right.Version || len(left.Columns) != len(right.Columns) {
		return false
	}
	for index := range left.Columns {
		if !equalSQLSchemaColumns(left.Columns[index], right.Columns[index]) {
			return false
		}
	}
	return true
}

func equalSQLSchemaColumns(left, right SQLRowBinaryColumn) bool {
	if left.Name != right.Name || left.Type != right.Type || left.Nullable != right.Nullable || left.DecimalScale != right.DecimalScale || left.DecimalPrecision != right.DecimalPrecision || len(left.EnumValues) != len(right.EnumValues) {
		return false
	}
	for index := range left.EnumValues {
		if left.EnumValues[index] != right.EnumValues[index] {
			return false
		}
	}
	return true
}

func validateSQLSchemaCompatibility(previous, next SQLSchemaDefinition, policy SQLSchemaCompatibility) error {
	if policy == SQLSchemaCompatibilityNone {
		return nil
	}
	previousColumns := make(map[string]SQLRowBinaryColumn, len(previous.Columns))
	nextColumns := make(map[string]SQLRowBinaryColumn, len(next.Columns))
	for _, column := range previous.Columns {
		previousColumns[column.Name] = column
	}
	for _, column := range next.Columns {
		nextColumns[column.Name] = column
	}
	for name, previousColumn := range previousColumns {
		nextColumn, exists := nextColumns[name]
		if exists && !equalSQLSchemaColumns(previousColumn, nextColumn) {
			return fmt.Errorf("%w: column %q changed between %q and %q", ErrSQLSchemaIncompatible, name, previous.Version, next.Version)
		}
		if !exists && (policy == SQLSchemaCompatibilityBackward || policy == SQLSchemaCompatibilityForward || policy == SQLSchemaCompatibilityFull) && !previousColumn.Nullable {
			return fmt.Errorf("%w: required column %q was removed in %q", ErrSQLSchemaIncompatible, name, next.Version)
		}
	}
	for name, nextColumn := range nextColumns {
		if _, exists := previousColumns[name]; !exists && (policy == SQLSchemaCompatibilityBackward || policy == SQLSchemaCompatibilityFull) && !nextColumn.Nullable {
			return fmt.Errorf("%w: required column %q was added in %q", ErrSQLSchemaIncompatible, name, next.Version)
		}
	}
	return nil
}
