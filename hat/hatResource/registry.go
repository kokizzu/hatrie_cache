// Package hatResource provides bounded named connection and secret resources.
package hatResource

import (
	"crypto/subtle"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	// DefaultRegistryMaxSecrets bounds retained named secrets by default.
	DefaultRegistryMaxSecrets = 1024
	// DefaultRegistryMaxConnections bounds retained named connections by default.
	DefaultRegistryMaxConnections = 1024
	// DefaultRegistryMaxSecretValueBytes bounds one secret value by default.
	DefaultRegistryMaxSecretValueBytes = 64 << 10
	// DefaultRegistryMaxNameBytes bounds namespaces, names, and owners by default.
	DefaultRegistryMaxNameBytes = 128
	// MaxRegistryEntries prevents accidentally unbounded resource registries.
	MaxRegistryEntries = 1 << 20
	// MaxRegistrySecretValueBytes prevents one resource from consuming excessive memory.
	MaxRegistrySecretValueBytes = 1 << 20
	// MaxRegistryNameBytes prevents unbounded identity retention.
	MaxRegistryNameBytes = 4096
	// MaxConnectionEndpointBytes bounds a connection endpoint.
	MaxConnectionEndpointBytes = 4096
	// MaxConnectionOptions bounds non-secret connection attributes.
	MaxConnectionOptions = 64
	// MaxConnectionOptionBytes bounds one connection attribute.
	MaxConnectionOptionBytes = 1024
	// MaxSecretRotationGrace bounds dual-secret overlap.
	MaxSecretRotationGrace = 365 * 24 * time.Hour
)

var (
	// ErrRegistryNil indicates a method called on a nil Registry.
	ErrRegistryNil = errors.New("hatResource: registry is nil")
	// ErrInvalidOptions indicates invalid or unsafe registry limits.
	ErrInvalidOptions = errors.New("hatResource: invalid registry options")
	// ErrResourceInvalid indicates a malformed resource identity or field.
	ErrResourceInvalid = errors.New("hatResource: invalid resource")
	// ErrResourceExists indicates a duplicate named resource.
	ErrResourceExists = errors.New("hatResource: resource already exists")
	// ErrResourceNotFound indicates an unknown resource.
	ErrResourceNotFound = errors.New("hatResource: resource not found")
	// ErrResourceForbidden indicates an owner or namespace authorization failure.
	ErrResourceForbidden = errors.New("hatResource: resource access forbidden")
	// ErrResourceLimit indicates a configured registry capacity was reached.
	ErrResourceLimit = errors.New("hatResource: resource limit exceeded")
	// ErrSecretValueTooLarge indicates a secret exceeds the configured bound.
	ErrSecretValueTooLarge = errors.New("hatResource: secret value is too large")
	// ErrRotationGraceInvalid indicates an invalid previous-secret overlap.
	ErrRotationGraceInvalid = errors.New("hatResource: secret rotation grace is invalid")
	// ErrSecretRequired indicates that a connection has no secret reference.
	ErrSecretRequired = errors.New("hatResource: connection secret is required")
	// ErrVersionOverflow indicates that a secret cannot be rotated further.
	ErrVersionOverflow = errors.New("hatResource: secret version exhausted")
)

// RegistryOptions bounds retained resource state. Zero values use sane
// defaults; negative values and values above the package maxima are rejected.
type RegistryOptions struct {
	MaxSecrets          int
	MaxConnections      int
	MaxSecretValueBytes int
	MaxNameBytes        int
}

// ResourceRef identifies a resource within a namespace.
type ResourceRef struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

// SecretSpec declares a new named secret. The value is retained only in the
// in-memory registry and is excluded from all metadata snapshots.
type SecretSpec struct {
	Ref   ResourceRef `json:"ref"`
	Owner string      `json:"owner"`
	Value string      `json:"-"`
}

// SecretMetadata describes a secret without exposing its value.
type SecretMetadata struct {
	Ref               ResourceRef `json:"ref"`
	Owner             string      `json:"owner"`
	Version           uint64      `json:"version"`
	PreviousVersion   uint64      `json:"previous_version,omitempty"`
	CreatedAt         time.Time   `json:"created_at"`
	RotatedAt         time.Time   `json:"rotated_at,omitempty"`
	PreviousExpiresAt time.Time   `json:"previous_expires_at,omitempty"`
}

// ResolvedSecret is the current value returned after owner authorization.
// Callers must not log or persist Value.
type ResolvedSecret struct {
	Value   string `json:"-"`
	Version uint64 `json:"version"`
}

// ConnectionSpec declares a named connection and an optional same-owner
// secret reference. Options are non-secret connection attributes.
type ConnectionSpec struct {
	Ref      ResourceRef
	Owner    string
	Driver   string
	Endpoint string
	Secret   ResourceRef
	Options  map[string]string
}

// ConnectionMetadata describes a connection without exposing its attributes.
type ConnectionMetadata struct {
	Ref           ResourceRef `json:"ref"`
	Owner         string      `json:"owner"`
	Driver        string      `json:"driver"`
	Endpoint      string      `json:"endpoint"`
	Secret        ResourceRef `json:"secret,omitempty"`
	SecretVersion uint64      `json:"secret_version,omitempty"`
}

// Connection is an authorized connection definition. It contains no secret
// value; use ResolveConnectionSecret when a connector must obtain one.
type Connection struct {
	ConnectionMetadata
	Options map[string]string
}

// RegistrySnapshot is a deterministic metadata-only view of a Registry.
type RegistrySnapshot struct {
	Secrets     []SecretMetadata     `json:"secrets"`
	Connections []ConnectionMetadata `json:"connections"`
}

type secretEntry struct {
	metadata SecretMetadata
	current  string
	previous string
}

type connectionEntry struct {
	metadata ConnectionMetadata
	options  map[string]string
}

// Registry stores named secrets and connection definitions with bounded
// cardinality and owner-scoped access. It is safe for concurrent use.
type Registry struct {
	mu          sync.RWMutex
	options     RegistryOptions
	secrets     map[ResourceRef]*secretEntry
	connections map[ResourceRef]*connectionEntry
}

// NewRegistry creates an empty bounded resource registry.
func NewRegistry(options RegistryOptions) (*Registry, error) {
	defaults := RegistryOptions{
		MaxSecrets:          DefaultRegistryMaxSecrets,
		MaxConnections:      DefaultRegistryMaxConnections,
		MaxSecretValueBytes: DefaultRegistryMaxSecretValueBytes,
		MaxNameBytes:        DefaultRegistryMaxNameBytes,
	}
	if options.MaxSecrets == 0 {
		options.MaxSecrets = defaults.MaxSecrets
	}
	if options.MaxConnections == 0 {
		options.MaxConnections = defaults.MaxConnections
	}
	if options.MaxSecretValueBytes == 0 {
		options.MaxSecretValueBytes = defaults.MaxSecretValueBytes
	}
	if options.MaxNameBytes == 0 {
		options.MaxNameBytes = defaults.MaxNameBytes
	}
	if options.MaxSecrets < 1 || options.MaxSecrets > MaxRegistryEntries ||
		options.MaxConnections < 1 || options.MaxConnections > MaxRegistryEntries ||
		options.MaxSecretValueBytes < 1 || options.MaxSecretValueBytes > MaxRegistrySecretValueBytes ||
		options.MaxNameBytes < 1 || options.MaxNameBytes > MaxRegistryNameBytes {
		return nil, ErrInvalidOptions
	}
	return &Registry{
		options:     options,
		secrets:     make(map[ResourceRef]*secretEntry),
		connections: make(map[ResourceRef]*connectionEntry),
	}, nil
}

// PutSecret creates a named secret at version one.
func (registry *Registry) PutSecret(spec SecretSpec) (SecretMetadata, error) {
	if registry == nil {
		return SecretMetadata{}, ErrRegistryNil
	}
	ref, owner, err := registry.normalizeIdentity(spec.Ref, spec.Owner)
	if err != nil {
		return SecretMetadata{}, err
	}
	if err := registry.validateSecretValue(spec.Value); err != nil {
		return SecretMetadata{}, err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, exists := registry.secrets[ref]; exists {
		return SecretMetadata{}, ErrResourceExists
	}
	if len(registry.secrets) >= registry.options.MaxSecrets {
		return SecretMetadata{}, ErrResourceLimit
	}
	metadata := SecretMetadata{
		Ref:       ref,
		Owner:     owner,
		Version:   1,
		CreatedAt: time.Now().UTC(),
	}
	registry.secrets[ref] = &secretEntry{metadata: metadata, current: spec.Value}
	return metadata, nil
}

// RotateSecret replaces a secret and optionally accepts its old value during
// grace. Only the owning principal can rotate the resource.
func (registry *Registry) RotateSecret(ref ResourceRef, principal, value string, now time.Time, grace time.Duration) (SecretMetadata, error) {
	if registry == nil {
		return SecretMetadata{}, ErrRegistryNil
	}
	ref, err := registry.normalizeRef(ref)
	if err != nil {
		return SecretMetadata{}, err
	}
	principal, err = registry.normalizeOwner(principal)
	if err != nil {
		return SecretMetadata{}, ErrResourceForbidden
	}
	if err := registry.validateSecretValue(value); err != nil {
		return SecretMetadata{}, err
	}
	if grace < 0 || grace > MaxSecretRotationGrace {
		return SecretMetadata{}, ErrRotationGraceInvalid
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	entry, exists := registry.secrets[ref]
	if !exists {
		return SecretMetadata{}, ErrResourceNotFound
	}
	if entry.metadata.Owner != principal {
		return SecretMetadata{}, ErrResourceForbidden
	}
	if entry.metadata.Version == ^uint64(0) {
		return SecretMetadata{}, ErrVersionOverflow
	}
	entry.previous = ""
	entry.metadata.PreviousVersion = 0
	entry.metadata.PreviousExpiresAt = time.Time{}
	if grace > 0 {
		entry.previous = entry.current
		entry.metadata.PreviousVersion = entry.metadata.Version
		entry.metadata.PreviousExpiresAt = now.Add(grace)
	}
	entry.current = value
	entry.metadata.Version++
	entry.metadata.RotatedAt = now
	return entry.metadata, nil
}

// ResolveSecret returns the current secret after owner authorization.
func (registry *Registry) ResolveSecret(ref ResourceRef, principal string) (ResolvedSecret, error) {
	if registry == nil {
		return ResolvedSecret{}, ErrRegistryNil
	}
	ref, err := registry.normalizeRef(ref)
	if err != nil {
		return ResolvedSecret{}, err
	}
	principal, err = registry.normalizeOwner(principal)
	if err != nil {
		return ResolvedSecret{}, ErrResourceForbidden
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	entry, exists := registry.secrets[ref]
	if !exists {
		return ResolvedSecret{}, ErrResourceNotFound
	}
	if entry.metadata.Owner != principal {
		return ResolvedSecret{}, ErrResourceForbidden
	}
	return ResolvedSecret{Value: entry.current, Version: entry.metadata.Version}, nil
}

// VerifySecret compares a candidate against the current secret or an
// unexpired previous value after owner authorization.
func (registry *Registry) VerifySecret(ref ResourceRef, principal, candidate string, now time.Time) (bool, error) {
	if registry == nil {
		return false, ErrRegistryNil
	}
	ref, err := registry.normalizeRef(ref)
	if err != nil {
		return false, err
	}
	principal, err = registry.normalizeOwner(principal)
	if err != nil {
		return false, ErrResourceForbidden
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	entry, exists := registry.secrets[ref]
	if !exists {
		return false, ErrResourceNotFound
	}
	if entry.metadata.Owner != principal {
		return false, ErrResourceForbidden
	}
	if subtle.ConstantTimeCompare([]byte(candidate), []byte(entry.current)) == 1 {
		return true, nil
	}
	if entry.previous != "" && now.Before(entry.metadata.PreviousExpiresAt) && subtle.ConstantTimeCompare([]byte(candidate), []byte(entry.previous)) == 1 {
		return true, nil
	}
	return false, nil
}

// PutConnection creates a named connection. A referenced secret must already
// exist and have the same owner as the connection.
func (registry *Registry) PutConnection(spec ConnectionSpec) (ConnectionMetadata, error) {
	if registry == nil {
		return ConnectionMetadata{}, ErrRegistryNil
	}
	ref, owner, err := registry.normalizeIdentity(spec.Ref, spec.Owner)
	if err != nil {
		return ConnectionMetadata{}, err
	}
	driver, err := normalizeBoundedText(spec.Driver, registry.options.MaxNameBytes, false)
	if err != nil {
		return ConnectionMetadata{}, err
	}
	endpoint, err := normalizeBoundedText(spec.Endpoint, MaxConnectionEndpointBytes, false)
	if err != nil {
		return ConnectionMetadata{}, err
	}
	secretRef := ResourceRef{}
	if spec.Secret != (ResourceRef{}) {
		secretRef, err = registry.normalizeRef(spec.Secret)
		if err != nil {
			return ConnectionMetadata{}, err
		}
	}
	options, err := normalizeConnectionOptions(spec.Options)
	if err != nil {
		return ConnectionMetadata{}, err
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, exists := registry.connections[ref]; exists {
		return ConnectionMetadata{}, ErrResourceExists
	}
	if len(registry.connections) >= registry.options.MaxConnections {
		return ConnectionMetadata{}, ErrResourceLimit
	}
	secretVersion := uint64(0)
	if secretRef != (ResourceRef{}) {
		secret, exists := registry.secrets[secretRef]
		if !exists {
			return ConnectionMetadata{}, ErrResourceNotFound
		}
		if secret.metadata.Owner != owner {
			return ConnectionMetadata{}, ErrResourceForbidden
		}
		secretVersion = secret.metadata.Version
	}
	metadata := ConnectionMetadata{
		Ref:           ref,
		Owner:         owner,
		Driver:        driver,
		Endpoint:      endpoint,
		Secret:        secretRef,
		SecretVersion: secretVersion,
	}
	registry.connections[ref] = &connectionEntry{metadata: metadata, options: options}
	return metadata, nil
}

// ResolveConnection returns an owner-authorized connection with copied
// options and the current referenced-secret version.
func (registry *Registry) ResolveConnection(ref ResourceRef, principal string) (Connection, error) {
	if registry == nil {
		return Connection{}, ErrRegistryNil
	}
	ref, err := registry.normalizeRef(ref)
	if err != nil {
		return Connection{}, err
	}
	principal, err = registry.normalizeOwner(principal)
	if err != nil {
		return Connection{}, ErrResourceForbidden
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	entry, exists := registry.connections[ref]
	if !exists {
		return Connection{}, ErrResourceNotFound
	}
	if entry.metadata.Owner != principal {
		return Connection{}, ErrResourceForbidden
	}
	connection := Connection{ConnectionMetadata: entry.metadata, Options: cloneOptions(entry.options)}
	if connection.Secret != (ResourceRef{}) {
		secret, exists := registry.secrets[connection.Secret]
		if !exists {
			return Connection{}, ErrResourceNotFound
		}
		connection.SecretVersion = secret.metadata.Version
	}
	return connection, nil
}

// ResolveConnectionSecret resolves the current secret referenced by an
// owner-authorized connection.
func (registry *Registry) ResolveConnectionSecret(ref ResourceRef, principal string) (ResolvedSecret, error) {
	connection, err := registry.ResolveConnection(ref, principal)
	if err != nil {
		return ResolvedSecret{}, err
	}
	if connection.Secret == (ResourceRef{}) {
		return ResolvedSecret{}, ErrSecretRequired
	}
	return registry.ResolveSecret(connection.Secret, principal)
}

// Snapshot returns sorted metadata and never includes secret values or
// connection options.
func (registry *Registry) Snapshot() RegistrySnapshot {
	if registry == nil {
		return RegistrySnapshot{}
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	snapshot := RegistrySnapshot{
		Secrets:     make([]SecretMetadata, 0, len(registry.secrets)),
		Connections: make([]ConnectionMetadata, 0, len(registry.connections)),
	}
	for _, entry := range registry.secrets {
		snapshot.Secrets = append(snapshot.Secrets, entry.metadata)
	}
	for _, entry := range registry.connections {
		metadata := entry.metadata
		if metadata.Secret != (ResourceRef{}) {
			if secret, exists := registry.secrets[metadata.Secret]; exists {
				metadata.SecretVersion = secret.metadata.Version
			}
		}
		snapshot.Connections = append(snapshot.Connections, metadata)
	}
	sort.Slice(snapshot.Secrets, func(left, right int) bool {
		return resourceRefLess(snapshot.Secrets[left].Ref, snapshot.Secrets[right].Ref)
	})
	sort.Slice(snapshot.Connections, func(left, right int) bool {
		return resourceRefLess(snapshot.Connections[left].Ref, snapshot.Connections[right].Ref)
	})
	return snapshot
}

func (registry *Registry) normalizeIdentity(ref ResourceRef, owner string) (ResourceRef, string, error) {
	ref, err := registry.normalizeRef(ref)
	if err != nil {
		return ResourceRef{}, "", err
	}
	owner, err = registry.normalizeOwner(owner)
	if err != nil {
		return ResourceRef{}, "", err
	}
	return ref, owner, nil
}

func (registry *Registry) normalizeRef(ref ResourceRef) (ResourceRef, error) {
	if registry == nil {
		return ResourceRef{}, ErrRegistryNil
	}
	namespace, err := normalizeBoundedText(ref.Namespace, registry.options.MaxNameBytes, true)
	if err != nil {
		return ResourceRef{}, err
	}
	name, err := normalizeBoundedText(ref.Name, registry.options.MaxNameBytes, true)
	if err != nil {
		return ResourceRef{}, err
	}
	return ResourceRef{Namespace: namespace, Name: name}, nil
}

func (registry *Registry) normalizeOwner(owner string) (string, error) {
	if registry == nil {
		return "", ErrRegistryNil
	}
	return normalizeBoundedText(owner, registry.options.MaxNameBytes, true)
}

func (registry *Registry) validateSecretValue(value string) error {
	if value == "" {
		return ErrResourceInvalid
	}
	if len(value) > registry.options.MaxSecretValueBytes {
		return ErrSecretValueTooLarge
	}
	if !utf8.ValidString(value) {
		return ErrResourceInvalid
	}
	return nil
}

func normalizeBoundedText(value string, limit int, required bool) (string, error) {
	value = strings.TrimSpace(value)
	if (!required && value == "") || (required && value == "") || len(value) > limit || !utf8.ValidString(value) {
		return "", ErrResourceInvalid
	}
	return value, nil
}

func normalizeConnectionOptions(options map[string]string) (map[string]string, error) {
	if len(options) == 0 {
		return nil, nil
	}
	if len(options) > MaxConnectionOptions {
		return nil, ErrResourceLimit
	}
	copyOptions := make(map[string]string, len(options))
	for key, value := range options {
		key, err := normalizeBoundedText(key, MaxConnectionOptionBytes, true)
		if err != nil {
			return nil, err
		}
		value, err = normalizeBoundedText(value, MaxConnectionOptionBytes, false)
		if err != nil {
			return nil, err
		}
		copyOptions[key] = value
	}
	return copyOptions, nil
}

func cloneOptions(options map[string]string) map[string]string {
	if len(options) == 0 {
		return nil
	}
	copyOptions := make(map[string]string, len(options))
	for key, value := range options {
		copyOptions[key] = value
	}
	return copyOptions
}

func resourceRefLess(left, right ResourceRef) bool {
	if left.Namespace != right.Namespace {
		return left.Namespace < right.Namespace
	}
	return left.Name < right.Name
}
