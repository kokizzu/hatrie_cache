package hatAuth

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strings"
	"sync"
)

const (
	// DefaultResourceRegistryMaxSecrets is the default number of named secrets.
	DefaultResourceRegistryMaxSecrets = 1024
	// DefaultResourceRegistryMaxConnections is the default number of named connections.
	DefaultResourceRegistryMaxConnections = 1024
	// DefaultResourceRegistryMaxSecretBytes bounds one secret value by default.
	DefaultResourceRegistryMaxSecretBytes = 1 << 20
	// DefaultResourceRegistryMaxNameBytes bounds names, drivers, and endpoints.
	DefaultResourceRegistryMaxNameBytes = 256
	// DefaultResourceRegistryMaxPrincipalBytes bounds owners and readers.
	DefaultResourceRegistryMaxPrincipalBytes = 256
	// DefaultResourceRegistryMaxReaders bounds one resource's reader scope.
	DefaultResourceRegistryMaxReaders = 64
)

var (
	// ErrResourceRegistryNil reports a nil resource registry receiver.
	ErrResourceRegistryNil = errors.New("resource registry is nil")
	// ErrResourceInvalid reports malformed resource metadata or secret values.
	ErrResourceInvalid = errors.New("resource is invalid")
	// ErrResourceAlreadyExists reports a duplicate resource name.
	ErrResourceAlreadyExists = errors.New("resource already exists")
	// ErrResourceNotFound reports a missing resource.
	ErrResourceNotFound = errors.New("resource not found")
	// ErrResourceAccessDenied reports a principal outside a resource scope.
	ErrResourceAccessDenied = errors.New("resource access denied")
	// ErrResourceVersionConflict reports a stale or missing compare-and-swap version.
	ErrResourceVersionConflict = errors.New("resource version conflicts")
	// ErrResourceLimit reports a configured resource or version limit.
	ErrResourceLimit = errors.New("resource limit exceeded")
	// ErrResourceInUse reports deletion of a secret referenced by a connection.
	ErrResourceInUse = errors.New("resource is in use")
)

// ResourceRegistryOptions bounds the in-memory resource catalog. Zero values
// select the documented defaults; negative values are rejected.
type ResourceRegistryOptions struct {
	MaxSecrets        int
	MaxConnections    int
	MaxSecretBytes    int
	MaxNameBytes      int
	MaxPrincipalBytes int
	MaxReaders        int
}

type resourceRegistryOptions struct {
	maxSecrets        int
	maxConnections    int
	maxSecretBytes    int
	maxNameBytes      int
	maxPrincipalBytes int
	maxReaders        int
}

// ResourceRegistry stores named secrets and redacted connection definitions.
// It is safe for concurrent use. Secret values are held only in memory and
// are intentionally excluded from metadata snapshots.
type ResourceRegistry struct {
	mu          sync.RWMutex
	options     resourceRegistryOptions
	secrets     map[string]*secretResource
	connections map[string]*connectionResource
}

// SecretSpec declares a secret and its read scope. Owner may always read and
// rotate the secret; readers may only resolve it. Value is copied on create.
type SecretSpec struct {
	Name    string   `json:"name"`
	Owner   string   `json:"owner"`
	Readers []string `json:"readers,omitempty"`
	Value   []byte   `json:"-"`
}

// String intentionally omits the secret value.
func (spec SecretSpec) String() string {
	return fmt.Sprintf("SecretSpec{Name:%q, Owner:%q, Readers:%v, Value:<redacted>}", spec.Name, spec.Owner, spec.Readers)
}

// GoString keeps %#v formatting redacted as well.
func (spec SecretSpec) GoString() string {
	return spec.String()
}

// SecretMetadata is the safe, value-free representation of a secret.
type SecretMetadata struct {
	Name    string   `json:"name"`
	Owner   string   `json:"owner"`
	Readers []string `json:"readers,omitempty"`
	Version uint64   `json:"version"`
}

// ConnectionSpec declares a named SQL-style connection that references a
// secret by name. The secret value is never part of this definition.
type ConnectionSpec struct {
	Name       string   `json:"name"`
	Owner      string   `json:"owner"`
	Readers    []string `json:"readers,omitempty"`
	Driver     string   `json:"driver"`
	Endpoint   string   `json:"endpoint"`
	Database   string   `json:"database,omitempty"`
	SecretName string   `json:"secret_name"`
	TLS        bool     `json:"tls"`
}

// ConnectionMetadata is the redacted representation of a connection. The
// current SecretVersion makes rotation observable without exposing a value.
type ConnectionMetadata struct {
	Name          string   `json:"name"`
	Owner         string   `json:"owner"`
	Readers       []string `json:"readers,omitempty"`
	Driver        string   `json:"driver"`
	Endpoint      string   `json:"endpoint"`
	Database      string   `json:"database,omitempty"`
	SecretName    string   `json:"secret_name"`
	SecretVersion uint64   `json:"secret_version"`
	TLS           bool     `json:"tls"`
	Version       uint64   `json:"version"`
}

// ResourceSnapshot contains only safe metadata. Secret values must be
// reloaded from an external secret manager after restart.
type ResourceSnapshot struct {
	Secrets     []SecretMetadata     `json:"secrets,omitempty"`
	Connections []ConnectionMetadata `json:"connections,omitempty"`
}

// ResolvedConnection is an explicitly resolved connection. Its secret is
// private so normal formatting and JSON serialization cannot expose it.
type ResolvedConnection struct {
	Metadata ConnectionMetadata `json:"metadata"`
	secret   []byte
}

type secretResource struct {
	metadata SecretMetadata
	value    []byte
}

type connectionResource struct {
	metadata ConnectionMetadata
}

// NewResourceRegistry creates a bounded resource registry.
func NewResourceRegistry(options ResourceRegistryOptions) (*ResourceRegistry, error) {
	normalized, err := normalizeResourceRegistryOptions(options)
	if err != nil {
		return nil, err
	}
	return &ResourceRegistry{
		options:     normalized,
		secrets:     make(map[string]*secretResource),
		connections: make(map[string]*connectionResource),
	}, nil
}

// String reports only resource counts and never formats secret values.
func (registry *ResourceRegistry) String() string {
	if registry == nil {
		return "ResourceRegistry<nil>"
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	return fmt.Sprintf("ResourceRegistry{secrets:%d, connections:%d}", len(registry.secrets), len(registry.connections))
}

// GoString keeps %#v formatting redacted as well.
func (registry *ResourceRegistry) GoString() string {
	return registry.String()
}

// CreateSecret creates version one of a named secret.
func (registry *ResourceRegistry) CreateSecret(spec SecretSpec) (SecretMetadata, error) {
	if registry == nil {
		return SecretMetadata{}, ErrResourceRegistryNil
	}
	normalized, err := normalizeSecretSpec(spec, registry.options)
	if err != nil {
		return SecretMetadata{}, err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, found := registry.secrets[normalized.Name]; found {
		return SecretMetadata{}, ErrResourceAlreadyExists
	}
	if len(registry.secrets) >= registry.options.maxSecrets {
		return SecretMetadata{}, ErrResourceLimit
	}
	metadata := SecretMetadata{
		Name:    normalized.Name,
		Owner:   normalized.Owner,
		Readers: append([]string(nil), normalized.Readers...),
		Version: 1,
	}
	registry.secrets[metadata.Name] = &secretResource{
		metadata: metadata,
		value:    append([]byte(nil), normalized.Value...),
	}
	return cloneSecretMetadata(metadata), nil
}

// ReadSecret returns a copy of the current secret value. Use SecretMetadata
// when the caller also needs the safe version marker.
func (registry *ResourceRegistry) ReadSecret(name, principal string) ([]byte, error) {
	if registry == nil {
		return nil, ErrResourceRegistryNil
	}
	name, principal, err := normalizeLookup(name, principal, registry.options)
	if err != nil {
		return nil, err
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	resource, found := registry.secrets[name]
	if !found {
		return nil, ErrResourceNotFound
	}
	if !resourcePrincipalAllowed(resource.metadata.Owner, resource.metadata.Readers, principal) {
		return nil, ErrResourceAccessDenied
	}
	return append([]byte(nil), resource.value...), nil
}

// SecretMetadata returns value-free metadata when principal is in the secret
// scope. The owner and every reader may inspect it.
func (registry *ResourceRegistry) SecretMetadata(name, principal string) (SecretMetadata, error) {
	if registry == nil {
		return SecretMetadata{}, ErrResourceRegistryNil
	}
	name, principal, err := normalizeLookup(name, principal, registry.options)
	if err != nil {
		return SecretMetadata{}, err
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	resource, found := registry.secrets[name]
	if !found {
		return SecretMetadata{}, ErrResourceNotFound
	}
	if !resourcePrincipalAllowed(resource.metadata.Owner, resource.metadata.Readers, principal) {
		return SecretMetadata{}, ErrResourceAccessDenied
	}
	return cloneSecretMetadata(resource.metadata), nil
}

// RotateSecret replaces a value only when principal is the owner and
// expectedVersion matches the current version. Version zero never means
// unconditional rotation; callers must perform an explicit read first.
func (registry *ResourceRegistry) RotateSecret(name, principal string, expectedVersion uint64, value []byte) (SecretMetadata, error) {
	if registry == nil {
		return SecretMetadata{}, ErrResourceRegistryNil
	}
	name, principal, err := normalizeLookup(name, principal, registry.options)
	if err != nil {
		return SecretMetadata{}, err
	}
	if err := validateSecretValue(value, registry.options.maxSecretBytes); err != nil {
		return SecretMetadata{}, err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	resource, found := registry.secrets[name]
	if !found {
		return SecretMetadata{}, ErrResourceNotFound
	}
	if resource.metadata.Owner != principal {
		return SecretMetadata{}, ErrResourceAccessDenied
	}
	if expectedVersion == 0 || resource.metadata.Version != expectedVersion {
		return SecretMetadata{}, ErrResourceVersionConflict
	}
	if resource.metadata.Version == math.MaxUint64 {
		return SecretMetadata{}, ErrResourceLimit
	}
	clear(resource.value)
	resource.value = append([]byte(nil), value...)
	resource.metadata.Version++
	return cloneSecretMetadata(resource.metadata), nil
}

// DeleteSecret removes an owned secret when it is not referenced by a
// connection and the caller supplies its current version.
func (registry *ResourceRegistry) DeleteSecret(name, principal string, expectedVersion uint64) error {
	if registry == nil {
		return ErrResourceRegistryNil
	}
	name, principal, err := normalizeLookup(name, principal, registry.options)
	if err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	resource, found := registry.secrets[name]
	if !found {
		return ErrResourceNotFound
	}
	if resource.metadata.Owner != principal {
		return ErrResourceAccessDenied
	}
	if expectedVersion == 0 || resource.metadata.Version != expectedVersion {
		return ErrResourceVersionConflict
	}
	for _, connection := range registry.connections {
		if connection.metadata.SecretName == name {
			return ErrResourceInUse
		}
	}
	clear(resource.value)
	delete(registry.secrets, name)
	return nil
}

// CreateConnection creates a redacted connection definition at version one.
// The connection owner must already be allowed to read the referenced secret.
func (registry *ResourceRegistry) CreateConnection(spec ConnectionSpec) (ConnectionMetadata, error) {
	if registry == nil {
		return ConnectionMetadata{}, ErrResourceRegistryNil
	}
	normalized, err := normalizeConnectionSpec(spec, registry.options)
	if err != nil {
		return ConnectionMetadata{}, err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, found := registry.connections[normalized.Name]; found {
		return ConnectionMetadata{}, ErrResourceAlreadyExists
	}
	if len(registry.connections) >= registry.options.maxConnections {
		return ConnectionMetadata{}, ErrResourceLimit
	}
	secret, found := registry.secrets[normalized.SecretName]
	if !found {
		return ConnectionMetadata{}, ErrResourceNotFound
	}
	if !resourcePrincipalAllowed(secret.metadata.Owner, secret.metadata.Readers, normalized.Owner) {
		return ConnectionMetadata{}, ErrResourceAccessDenied
	}
	for _, reader := range normalized.Readers {
		if !resourcePrincipalAllowed(secret.metadata.Owner, secret.metadata.Readers, reader) {
			return ConnectionMetadata{}, ErrResourceAccessDenied
		}
	}
	metadata := ConnectionMetadata{
		Name:          normalized.Name,
		Owner:         normalized.Owner,
		Readers:       append([]string(nil), normalized.Readers...),
		Driver:        normalized.Driver,
		Endpoint:      normalized.Endpoint,
		Database:      normalized.Database,
		SecretName:    normalized.SecretName,
		SecretVersion: secret.metadata.Version,
		TLS:           normalized.TLS,
		Version:       1,
	}
	registry.connections[metadata.Name] = &connectionResource{metadata: metadata}
	return cloneConnectionMetadata(metadata), nil
}

// Connection returns redacted connection metadata for an allowed principal.
func (registry *ResourceRegistry) Connection(name, principal string) (ConnectionMetadata, error) {
	if registry == nil {
		return ConnectionMetadata{}, ErrResourceRegistryNil
	}
	name, principal, err := normalizeLookup(name, principal, registry.options)
	if err != nil {
		return ConnectionMetadata{}, err
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	resource, found := registry.connections[name]
	if !found {
		return ConnectionMetadata{}, ErrResourceNotFound
	}
	if !resourcePrincipalAllowed(resource.metadata.Owner, resource.metadata.Readers, principal) {
		return ConnectionMetadata{}, ErrResourceAccessDenied
	}
	metadata := cloneConnectionMetadata(resource.metadata)
	secret, found := registry.secrets[metadata.SecretName]
	if !found {
		return ConnectionMetadata{}, ErrResourceNotFound
	}
	if !resourcePrincipalAllowed(secret.metadata.Owner, secret.metadata.Readers, principal) {
		return ConnectionMetadata{}, ErrResourceAccessDenied
	}
	metadata.SecretVersion = secret.metadata.Version
	return metadata, nil
}

// ResolveConnection returns redacted metadata plus an explicit copy of the
// current secret. Both connection and secret scopes must allow principal.
func (registry *ResourceRegistry) ResolveConnection(name, principal string) (ResolvedConnection, error) {
	if registry == nil {
		return ResolvedConnection{}, ErrResourceRegistryNil
	}
	name, principal, err := normalizeLookup(name, principal, registry.options)
	if err != nil {
		return ResolvedConnection{}, err
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	connection, found := registry.connections[name]
	if !found {
		return ResolvedConnection{}, ErrResourceNotFound
	}
	if !resourcePrincipalAllowed(connection.metadata.Owner, connection.metadata.Readers, principal) {
		return ResolvedConnection{}, ErrResourceAccessDenied
	}
	secret, found := registry.secrets[connection.metadata.SecretName]
	if !found {
		return ResolvedConnection{}, ErrResourceNotFound
	}
	if !resourcePrincipalAllowed(secret.metadata.Owner, secret.metadata.Readers, principal) {
		return ResolvedConnection{}, ErrResourceAccessDenied
	}
	metadata := cloneConnectionMetadata(connection.metadata)
	metadata.SecretVersion = secret.metadata.Version
	return ResolvedConnection{
		Metadata: metadata,
		secret:   append([]byte(nil), secret.value...),
	}, nil
}

// SecretValue returns a copy of the resolved credential. Callers should keep
// its lifetime short and avoid logging it.
func (resolved ResolvedConnection) SecretValue() []byte {
	return append([]byte(nil), resolved.secret...)
}

// String intentionally omits the secret value.
func (resolved ResolvedConnection) String() string {
	return fmt.Sprintf("ResolvedConnection{Name:%q, Secret:<redacted>, SecretVersion:%d}", resolved.Metadata.Name, resolved.Metadata.SecretVersion)
}

// GoString keeps %#v formatting redacted as well.
func (resolved ResolvedConnection) GoString() string {
	return resolved.String()
}

// DeleteConnection removes an owned connection at its current version.
func (registry *ResourceRegistry) DeleteConnection(name, principal string, expectedVersion uint64) error {
	if registry == nil {
		return ErrResourceRegistryNil
	}
	name, principal, err := normalizeLookup(name, principal, registry.options)
	if err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	resource, found := registry.connections[name]
	if !found {
		return ErrResourceNotFound
	}
	if resource.metadata.Owner != principal {
		return ErrResourceAccessDenied
	}
	if expectedVersion == 0 || resource.metadata.Version != expectedVersion {
		return ErrResourceVersionConflict
	}
	delete(registry.connections, name)
	return nil
}

// Snapshot returns deterministic metadata and never includes secret bytes.
func (registry *ResourceRegistry) Snapshot() ResourceSnapshot {
	if registry == nil {
		return ResourceSnapshot{}
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	snapshot := ResourceSnapshot{
		Secrets:     make([]SecretMetadata, 0, len(registry.secrets)),
		Connections: make([]ConnectionMetadata, 0, len(registry.connections)),
	}
	for _, secret := range registry.secrets {
		snapshot.Secrets = append(snapshot.Secrets, cloneSecretMetadata(secret.metadata))
	}
	for _, connection := range registry.connections {
		metadata := cloneConnectionMetadata(connection.metadata)
		if secret, found := registry.secrets[metadata.SecretName]; found {
			metadata.SecretVersion = secret.metadata.Version
		}
		snapshot.Connections = append(snapshot.Connections, metadata)
	}
	sort.Slice(snapshot.Secrets, func(left, right int) bool {
		return snapshot.Secrets[left].Name < snapshot.Secrets[right].Name
	})
	sort.Slice(snapshot.Connections, func(left, right int) bool {
		return snapshot.Connections[left].Name < snapshot.Connections[right].Name
	})
	return snapshot
}

func normalizeResourceRegistryOptions(options ResourceRegistryOptions) (resourceRegistryOptions, error) {
	for _, value := range []int{
		options.MaxSecrets,
		options.MaxConnections,
		options.MaxSecretBytes,
		options.MaxNameBytes,
		options.MaxPrincipalBytes,
		options.MaxReaders,
	} {
		if value < 0 {
			return resourceRegistryOptions{}, ErrResourceInvalid
		}
	}
	normalized := resourceRegistryOptions{
		maxSecrets:        options.MaxSecrets,
		maxConnections:    options.MaxConnections,
		maxSecretBytes:    options.MaxSecretBytes,
		maxNameBytes:      options.MaxNameBytes,
		maxPrincipalBytes: options.MaxPrincipalBytes,
		maxReaders:        options.MaxReaders,
	}
	if normalized.maxSecrets == 0 {
		normalized.maxSecrets = DefaultResourceRegistryMaxSecrets
	}
	if normalized.maxConnections == 0 {
		normalized.maxConnections = DefaultResourceRegistryMaxConnections
	}
	if normalized.maxSecretBytes == 0 {
		normalized.maxSecretBytes = DefaultResourceRegistryMaxSecretBytes
	}
	if normalized.maxNameBytes == 0 {
		normalized.maxNameBytes = DefaultResourceRegistryMaxNameBytes
	}
	if normalized.maxPrincipalBytes == 0 {
		normalized.maxPrincipalBytes = DefaultResourceRegistryMaxPrincipalBytes
	}
	if normalized.maxReaders == 0 {
		normalized.maxReaders = DefaultResourceRegistryMaxReaders
	}
	return normalized, nil
}

func normalizeSecretSpec(spec SecretSpec, options resourceRegistryOptions) (SecretSpec, error) {
	name, err := normalizeResourceName(spec.Name, options.maxNameBytes)
	if err != nil {
		return SecretSpec{}, err
	}
	owner, err := normalizePrincipal(spec.Owner, options)
	if err != nil {
		return SecretSpec{}, err
	}
	readers, err := normalizeReaders(spec.Readers, options)
	if err != nil {
		return SecretSpec{}, err
	}
	if err := validateSecretValue(spec.Value, options.maxSecretBytes); err != nil {
		return SecretSpec{}, err
	}
	return SecretSpec{Name: name, Owner: owner, Readers: readers, Value: spec.Value}, nil
}

func normalizeConnectionSpec(spec ConnectionSpec, options resourceRegistryOptions) (ConnectionSpec, error) {
	name, err := normalizeResourceName(spec.Name, options.maxNameBytes)
	if err != nil {
		return ConnectionSpec{}, err
	}
	owner, err := normalizePrincipal(spec.Owner, options)
	if err != nil {
		return ConnectionSpec{}, err
	}
	readers, err := normalizeReaders(spec.Readers, options)
	if err != nil {
		return ConnectionSpec{}, err
	}
	driver, err := normalizeResourceName(spec.Driver, options.maxNameBytes)
	if err != nil {
		return ConnectionSpec{}, err
	}
	endpoint, err := normalizeResourceName(spec.Endpoint, options.maxNameBytes)
	if err != nil {
		return ConnectionSpec{}, err
	}
	if err := validateConnectionEndpoint(endpoint); err != nil {
		return ConnectionSpec{}, err
	}
	database := strings.TrimSpace(spec.Database)
	if database != "" {
		if len(database) > options.maxNameBytes || containsResourceControl(database) {
			return ConnectionSpec{}, ErrResourceInvalid
		}
	}
	secretName, err := normalizeResourceName(spec.SecretName, options.maxNameBytes)
	if err != nil {
		return ConnectionSpec{}, err
	}
	return ConnectionSpec{
		Name:       name,
		Owner:      owner,
		Readers:    readers,
		Driver:     driver,
		Endpoint:   endpoint,
		Database:   database,
		SecretName: secretName,
		TLS:        spec.TLS,
	}, nil
}

func normalizeLookup(name, principal string, options resourceRegistryOptions) (string, string, error) {
	normalizedName, err := normalizeResourceName(name, options.maxNameBytes)
	if err != nil {
		return "", "", err
	}
	normalizedPrincipal, err := normalizePrincipal(principal, options)
	if err != nil {
		return "", "", err
	}
	return normalizedName, normalizedPrincipal, nil
}

func normalizeResourceName(value string, maxBytes int) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > maxBytes || containsResourceControl(value) {
		return "", ErrResourceInvalid
	}
	return value, nil
}

func normalizePrincipal(value string, options resourceRegistryOptions) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > options.maxPrincipalBytes || containsResourceControl(value) {
		return "", ErrResourceInvalid
	}
	return value, nil
}

func normalizeReaders(readers []string, options resourceRegistryOptions) ([]string, error) {
	if len(readers) == 0 {
		return nil, nil
	}
	if len(readers) > options.maxReaders {
		return nil, ErrResourceLimit
	}
	normalized := make([]string, len(readers))
	seen := make(map[string]struct{}, len(readers))
	for index, reader := range readers {
		reader, err := normalizePrincipal(reader, options)
		if err != nil {
			return nil, err
		}
		if _, found := seen[reader]; found {
			return nil, ErrResourceInvalid
		}
		seen[reader] = struct{}{}
		normalized[index] = reader
	}
	sort.Strings(normalized)
	return normalized, nil
}

func validateSecretValue(value []byte, maxBytes int) error {
	if len(value) == 0 || len(value) > maxBytes {
		return ErrResourceInvalid
	}
	return nil
}

func validateConnectionEndpoint(endpoint string) error {
	if strings.Contains(endpoint, "@") {
		return ErrResourceInvalid
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.User != nil {
		return ErrResourceInvalid
	}
	for key := range parsed.Query() {
		switch strings.ToLower(key) {
		case "password", "passwd", "secret", "token", "access_token", "api_key":
			return ErrResourceInvalid
		}
	}
	return nil
}

func containsResourceControl(value string) bool {
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return true
		}
	}
	return false
}

func resourcePrincipalAllowed(owner string, readers []string, principal string) bool {
	if owner == principal {
		return true
	}
	for _, reader := range readers {
		if reader == principal {
			return true
		}
	}
	return false
}

func cloneSecretMetadata(metadata SecretMetadata) SecretMetadata {
	metadata.Readers = append([]string(nil), metadata.Readers...)
	return metadata
}

func cloneConnectionMetadata(metadata ConnectionMetadata) ConnectionMetadata {
	metadata.Readers = append([]string(nil), metadata.Readers...)
	return metadata
}
