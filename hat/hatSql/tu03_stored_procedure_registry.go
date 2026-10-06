package hatSql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
)

const (
	// DefaultStoredProcedureRegistryMaxProcedures bounds the active procedure
	// set when callers leave the option at zero.
	DefaultStoredProcedureRegistryMaxProcedures = 256
	// DefaultStoredProcedureMaxArguments bounds one call's positional inputs.
	DefaultStoredProcedureMaxArguments = 64
	// DefaultStoredProcedureMaxInputBytes bounds the JSON representation of
	// one call's arguments.
	DefaultStoredProcedureMaxInputBytes = 1 << 20
	// DefaultStoredProcedureMaxOutputBytes bounds the JSON representation of a
	// procedure result.
	DefaultStoredProcedureMaxOutputBytes = 1 << 20
	// DefaultStoredProcedureMaxNameBytes bounds procedure names and versions.
	DefaultStoredProcedureMaxNameBytes = 256
	// DefaultStoredProcedureMaxPrincipalBytes bounds authenticated principals.
	DefaultStoredProcedureMaxPrincipalBytes = 256
	// DefaultStoredProcedureMaxCapabilityBytes bounds authorization labels.
	DefaultStoredProcedureMaxCapabilityBytes = 256
)

var (
	// ErrStoredProcedureRegistryNil reports a nil registry receiver.
	ErrStoredProcedureRegistryNil = errors.New("stored procedure registry is nil")
	// ErrStoredProcedureInvalid reports malformed procedure metadata or call
	// identity.
	ErrStoredProcedureInvalid = errors.New("stored procedure is invalid")
	// ErrStoredProcedureVersionRequired reports a missing compare-and-swap
	// version for a replacement or unload.
	ErrStoredProcedureVersionRequired = errors.New("stored procedure expected version is required")
	// ErrStoredProcedureVersionConflict reports a stale compare-and-swap
	// version.
	ErrStoredProcedureVersionConflict = errors.New("stored procedure version conflict")
	// ErrStoredProcedureVersionUnchanged reports a replacement with the active
	// version.
	ErrStoredProcedureVersionUnchanged = errors.New("stored procedure version is unchanged")
	// ErrStoredProcedureNotFound reports a missing active procedure.
	ErrStoredProcedureNotFound = errors.New("stored procedure was not found")
	// ErrStoredProcedureAuthorizationRequired reports that callers did not opt
	// into an authorization policy.
	ErrStoredProcedureAuthorizationRequired = errors.New("stored procedure authorization is required")
	// ErrStoredProcedureAccessDenied reports a rejected principal.
	ErrStoredProcedureAccessDenied = errors.New("stored procedure access denied")
	// ErrStoredProcedureLimit reports an active procedure or generation limit.
	ErrStoredProcedureLimit = errors.New("stored procedure limit exceeded")
	// ErrStoredProcedureArgumentLimit reports too many positional arguments.
	ErrStoredProcedureArgumentLimit = errors.New("stored procedure argument limit exceeded")
	// ErrStoredProcedureInputLimit reports an oversized serialized argument
	// vector.
	ErrStoredProcedureInputLimit = errors.New("stored procedure input limit exceeded")
	// ErrStoredProcedureOutputLimit reports an oversized serialized result.
	ErrStoredProcedureOutputLimit = errors.New("stored procedure output limit exceeded")
	// ErrStoredProcedureValue reports a value that cannot be bounded using the
	// registry's JSON size contract.
	ErrStoredProcedureValue = errors.New("stored procedure value is not serializable")
	// ErrStoredProcedurePanic reports a handler or policy panic converted to an
	// error at the registry boundary.
	ErrStoredProcedurePanic = errors.New("stored procedure panicked")
)

// StoredProcedureHandler is the trusted in-process implementation of one
// procedure generation. It receives a context and a copy of the top-level
// argument slice; it is never loaded from serialized input.
type StoredProcedureHandler func(context.Context, []interface{}) (interface{}, error)

// StoredProcedure declares one versioned callable. RequiredCapability is an
// opaque policy label interpreted by the configured authorizer.
type StoredProcedure struct {
	Name               string
	Version            string
	RequiredCapability string
	Execute            StoredProcedureHandler
}

// StoredProcedureAuthorization is the complete authorization decision input.
// The registry does not invent principals or default grants.
type StoredProcedureAuthorization struct {
	Principal          string
	Name               string
	Version            string
	RequiredCapability string
}

// StoredProcedureAuthorizer decides whether one principal may invoke one
// active procedure. A nil authorizer fails closed at invocation time.
type StoredProcedureAuthorizer func(StoredProcedureAuthorization) bool

// StoredProcedureRegistryOptions bounds the registry and installs its
// authorization policy. Zero limits select the documented defaults; negative
// limits are rejected.
type StoredProcedureRegistryOptions struct {
	MaxProcedures      int
	MaxArguments       int
	MaxInputBytes      int
	MaxOutputBytes     int
	MaxNameBytes       int
	MaxPrincipalBytes  int
	MaxCapabilityBytes int
	Authorize          StoredProcedureAuthorizer
}

type storedProcedureRegistryOptions struct {
	maxProcedures      int
	maxArguments       int
	maxInputBytes      int
	maxOutputBytes     int
	maxNameBytes       int
	maxPrincipalBytes  int
	maxCapabilityBytes int
	authorize          StoredProcedureAuthorizer
}

// StoredProcedureMetadata is safe to expose in catalogs and snapshots. It
// intentionally contains no executable callback.
type StoredProcedureMetadata struct {
	Name               string `json:"name"`
	Version            string `json:"version"`
	RequiredCapability string `json:"required_capability,omitempty"`
	Generation         uint64 `json:"generation"`
}

type storedProcedureEntry struct {
	metadata StoredProcedureMetadata
	execute  StoredProcedureHandler
}

// StoredProcedureRegistry owns the active version of each trusted procedure.
// Registration and replacement are concurrency-safe; invocation never holds
// the registry lock while user code runs.
type StoredProcedureRegistry struct {
	mu             sync.RWMutex
	options        storedProcedureRegistryOptions
	procedures     map[string]storedProcedureEntry
	nextGeneration uint64
}

// NewStoredProcedureRegistry creates a bounded, opt-in registry. The absence
// of an authorizer is valid for registration and metadata inspection, but
// Invoke returns ErrStoredProcedureAuthorizationRequired.
func NewStoredProcedureRegistry(options StoredProcedureRegistryOptions) (*StoredProcedureRegistry, error) {
	normalized, err := normalizeStoredProcedureRegistryOptions(options)
	if err != nil {
		return nil, err
	}
	return &StoredProcedureRegistry{
		options:    normalized,
		procedures: make(map[string]storedProcedureEntry),
	}, nil
}

// Register installs an initial procedure when expectedVersion is empty. A
// replacement must name the currently active version, preventing stale
// owners from silently replacing a newer generation.
func (registry *StoredProcedureRegistry) Register(procedure StoredProcedure, expectedVersion string) (StoredProcedureMetadata, error) {
	if registry == nil {
		return StoredProcedureMetadata{}, ErrStoredProcedureRegistryNil
	}
	normalized, err := normalizeStoredProcedure(procedure, registry.options)
	if err != nil {
		return StoredProcedureMetadata{}, err
	}
	expectedVersion = strings.TrimSpace(expectedVersion)

	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.procedures == nil {
		registry.procedures = make(map[string]storedProcedureEntry)
	}
	current, exists := registry.procedures[normalized.Name]
	if !exists {
		if expectedVersion != "" {
			return StoredProcedureMetadata{}, ErrStoredProcedureVersionConflict
		}
		if len(registry.procedures) >= registry.options.maxProcedures {
			return StoredProcedureMetadata{}, ErrStoredProcedureLimit
		}
		if registry.nextGeneration == math.MaxUint64 {
			return StoredProcedureMetadata{}, ErrStoredProcedureLimit
		}
		return registry.installLocked(normalized), nil
	}
	if expectedVersion == "" {
		return StoredProcedureMetadata{}, ErrStoredProcedureVersionRequired
	}
	if expectedVersion != current.metadata.Version {
		return StoredProcedureMetadata{}, ErrStoredProcedureVersionConflict
	}
	if normalized.Version == current.metadata.Version {
		return StoredProcedureMetadata{}, ErrStoredProcedureVersionUnchanged
	}
	if registry.nextGeneration == math.MaxUint64 {
		return StoredProcedureMetadata{}, ErrStoredProcedureLimit
	}
	return registry.installLocked(normalized), nil
}

// Resolve returns metadata for the active generation without exposing its
// executable callback.
func (registry *StoredProcedureRegistry) Resolve(name string) (StoredProcedureMetadata, bool) {
	if registry == nil {
		return StoredProcedureMetadata{}, false
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return StoredProcedureMetadata{}, false
	}
	registry.mu.RLock()
	entry, found := registry.procedures[name]
	registry.mu.RUnlock()
	if !found {
		return StoredProcedureMetadata{}, false
	}
	return entry.metadata, true
}

// Snapshot returns active procedure metadata in deterministic name order.
func (registry *StoredProcedureRegistry) Snapshot() []StoredProcedureMetadata {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	snapshot := make([]StoredProcedureMetadata, 0, len(registry.procedures))
	for _, entry := range registry.procedures {
		snapshot = append(snapshot, entry.metadata)
	}
	registry.mu.RUnlock()
	sort.Slice(snapshot, func(left, right int) bool { return snapshot[left].Name < snapshot[right].Name })
	return snapshot
}

// Unload removes a procedure only when expectedVersion matches its active
// version.
func (registry *StoredProcedureRegistry) Unload(name, expectedVersion string) error {
	if registry == nil {
		return ErrStoredProcedureRegistryNil
	}
	name = strings.TrimSpace(name)
	expectedVersion = strings.TrimSpace(expectedVersion)
	if name == "" || len(name) > registry.options.maxNameBytes {
		return ErrStoredProcedureInvalid
	}
	if expectedVersion == "" {
		return ErrStoredProcedureVersionRequired
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	entry, found := registry.procedures[name]
	if !found {
		return ErrStoredProcedureNotFound
	}
	if entry.metadata.Version != expectedVersion {
		return ErrStoredProcedureVersionConflict
	}
	delete(registry.procedures, name)
	return nil
}

// Invoke authorizes and executes the active procedure. It checks context
// cancellation before user code and again after it; an arbitrary handler
// remains responsible for observing cancellation while it runs.
func (registry *StoredProcedureRegistry) Invoke(ctx context.Context, principal, name string, arguments []interface{}) (value interface{}, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			value = nil
			err = fmt.Errorf("%w: %v", ErrStoredProcedurePanic, recovered)
		}
	}()
	if registry == nil {
		return nil, ErrStoredProcedureRegistryNil
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", ErrStoredProcedureInvalid)
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return nil, contextErr
	}
	principal = strings.TrimSpace(principal)
	name = strings.TrimSpace(name)
	if principal == "" || len(principal) > registry.options.maxPrincipalBytes || name == "" || len(name) > registry.options.maxNameBytes {
		return nil, ErrStoredProcedureInvalid
	}
	if len(arguments) > registry.options.maxArguments {
		return nil, ErrStoredProcedureArgumentLimit
	}

	registry.mu.RLock()
	entry, found := registry.procedures[name]
	authorize := registry.options.authorize
	registry.mu.RUnlock()
	if !found {
		return nil, ErrStoredProcedureNotFound
	}
	if authorize == nil {
		return nil, ErrStoredProcedureAuthorizationRequired
	}
	if !authorize(StoredProcedureAuthorization{
		Principal:          principal,
		Name:               entry.metadata.Name,
		Version:            entry.metadata.Version,
		RequiredCapability: entry.metadata.RequiredCapability,
	}) {
		return nil, ErrStoredProcedureAccessDenied
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return nil, contextErr
	}
	if err := checkStoredProcedureJSONSize(arguments, registry.options.maxInputBytes, ErrStoredProcedureInputLimit); err != nil {
		return nil, err
	}
	value, err = entry.execute(ctx, append([]interface{}(nil), arguments...))
	if err != nil {
		return nil, err
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return nil, contextErr
	}
	if err := checkStoredProcedureJSONSize(value, registry.options.maxOutputBytes, ErrStoredProcedureOutputLimit); err != nil {
		return nil, err
	}
	return value, nil
}

func (registry *StoredProcedureRegistry) installLocked(procedure StoredProcedure) StoredProcedureMetadata {
	registry.nextGeneration++
	metadata := StoredProcedureMetadata{
		Name:               procedure.Name,
		Version:            procedure.Version,
		RequiredCapability: procedure.RequiredCapability,
		Generation:         registry.nextGeneration,
	}
	registry.procedures[metadata.Name] = storedProcedureEntry{metadata: metadata, execute: procedure.Execute}
	return metadata
}

func normalizeStoredProcedureRegistryOptions(options StoredProcedureRegistryOptions) (storedProcedureRegistryOptions, error) {
	values := storedProcedureRegistryOptions{
		maxProcedures:      options.MaxProcedures,
		maxArguments:       options.MaxArguments,
		maxInputBytes:      options.MaxInputBytes,
		maxOutputBytes:     options.MaxOutputBytes,
		maxNameBytes:       options.MaxNameBytes,
		maxPrincipalBytes:  options.MaxPrincipalBytes,
		maxCapabilityBytes: options.MaxCapabilityBytes,
		authorize:          options.Authorize,
	}
	if values.maxProcedures == 0 {
		values.maxProcedures = DefaultStoredProcedureRegistryMaxProcedures
	}
	if values.maxArguments == 0 {
		values.maxArguments = DefaultStoredProcedureMaxArguments
	}
	if values.maxInputBytes == 0 {
		values.maxInputBytes = DefaultStoredProcedureMaxInputBytes
	}
	if values.maxOutputBytes == 0 {
		values.maxOutputBytes = DefaultStoredProcedureMaxOutputBytes
	}
	if values.maxNameBytes == 0 {
		values.maxNameBytes = DefaultStoredProcedureMaxNameBytes
	}
	if values.maxPrincipalBytes == 0 {
		values.maxPrincipalBytes = DefaultStoredProcedureMaxPrincipalBytes
	}
	if values.maxCapabilityBytes == 0 {
		values.maxCapabilityBytes = DefaultStoredProcedureMaxCapabilityBytes
	}
	if values.maxProcedures < 0 || values.maxArguments < 0 || values.maxInputBytes < 0 || values.maxOutputBytes < 0 || values.maxNameBytes < 0 || values.maxPrincipalBytes < 0 || values.maxCapabilityBytes < 0 {
		return storedProcedureRegistryOptions{}, fmt.Errorf("%w: limits cannot be negative", ErrStoredProcedureInvalid)
	}
	return values, nil
}

func normalizeStoredProcedure(procedure StoredProcedure, options storedProcedureRegistryOptions) (StoredProcedure, error) {
	procedure.Name = strings.TrimSpace(procedure.Name)
	procedure.Version = strings.TrimSpace(procedure.Version)
	procedure.RequiredCapability = strings.TrimSpace(procedure.RequiredCapability)
	if procedure.Name == "" || procedure.Version == "" || procedure.Execute == nil || len(procedure.Name) > options.maxNameBytes || len(procedure.Version) > options.maxNameBytes || len(procedure.RequiredCapability) > options.maxCapabilityBytes {
		return StoredProcedure{}, ErrStoredProcedureInvalid
	}
	return procedure, nil
}

func checkStoredProcedureJSONSize(value interface{}, limit int, limitError error) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrStoredProcedureValue, err)
	}
	if len(encoded) > limit {
		return fmt.Errorf("%w: %d bytes exceeds %d", limitError, len(encoded), limit)
	}
	return nil
}
