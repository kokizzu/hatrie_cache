package hatProcedure

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
)

const (
	DefaultMaxProcedures           = 256
	MaxMaxProcedures               = 4096
	DefaultMaxVersionsPerProcedure = 8
	MaxVersionsPerProcedure        = 64
	DefaultMaxPayloadBytes         = 1 << 20
	MaxPayloadBytes                = 64 << 20
	MaxProcedureNameBytes          = 128
)

var (
	ErrInvalidOptions        = errors.New("hatProcedure: invalid options")
	ErrInvalidDefinition     = errors.New("hatProcedure: invalid definition")
	ErrInvalidCall           = errors.New("hatProcedure: invalid call")
	ErrAlreadyRegistered     = errors.New("hatProcedure: procedure version already registered")
	ErrProcedureLimit        = errors.New("hatProcedure: procedure limit reached")
	ErrVersionLimit          = errors.New("hatProcedure: version limit reached")
	ErrProcedureNotFound     = errors.New("hatProcedure: procedure not found")
	ErrVersionNotFound       = errors.New("hatProcedure: procedure version not found")
	ErrAuthorizationRequired = errors.New("hatProcedure: authorization is required")
	ErrUnauthorized          = errors.New("hatProcedure: call unauthorized")
	ErrAuthorizationPanic    = errors.New("hatProcedure: authorization failed")
	ErrProcedurePanic        = errors.New("hatProcedure: procedure panicked")
	ErrPayloadTooLarge       = errors.New("hatProcedure: payload too large")
	ErrResultTooLarge        = errors.New("hatProcedure: result too large")
)

// Handler executes one registered procedure. The payload belongs to the
// registry and must not be retained after the handler returns.
type Handler func(context.Context, []byte) ([]byte, error)

// Authorizer approves a resolved procedure call. The payload is an isolated
// copy and must not be retained after the authorizer returns.
type Authorizer func(context.Context, Call) error

// Call identifies one procedure invocation. Version zero selects the newest
// registered version.
type Call struct {
	Name    string
	Version uint32
	Payload []byte
}

// Definition registers one immutable procedure version.
type Definition struct {
	Name    string
	Version uint32
	Handler Handler
}

// Metadata is the deterministic public listing for one procedure name.
type Metadata struct {
	Name     string
	Versions []uint32
}

// Options bounds registry state and supplies the call authorizer. A nil
// authorizer intentionally denies every invocation.
type Options struct {
	MaxProcedures           int
	MaxVersionsPerProcedure int
	MaxPayloadBytes         int
	Authorize               Authorizer
}

// Registry stores versioned procedure handlers. Registered handlers are
// looked up under a read lock and executed after the lock is released.
type Registry struct {
	mu                      sync.RWMutex
	maxProcedures           int
	maxVersionsPerProcedure int
	maxPayloadBytes         int
	authorize               Authorizer
	procedures              map[string]map[uint32]Handler
}

// NewRegistry validates options and returns an empty registry.
func NewRegistry(options Options) (*Registry, error) {
	if options.MaxProcedures == 0 {
		options.MaxProcedures = DefaultMaxProcedures
	}
	if options.MaxVersionsPerProcedure == 0 {
		options.MaxVersionsPerProcedure = DefaultMaxVersionsPerProcedure
	}
	if options.MaxPayloadBytes == 0 {
		options.MaxPayloadBytes = DefaultMaxPayloadBytes
	}
	if options.MaxProcedures < 1 || options.MaxProcedures > MaxMaxProcedures {
		return nil, fmt.Errorf("%w: max procedures must be between 1 and %d", ErrInvalidOptions, MaxMaxProcedures)
	}
	if options.MaxVersionsPerProcedure < 1 || options.MaxVersionsPerProcedure > MaxVersionsPerProcedure {
		return nil, fmt.Errorf("%w: max versions must be between 1 and %d", ErrInvalidOptions, MaxVersionsPerProcedure)
	}
	if options.MaxPayloadBytes < 1 || options.MaxPayloadBytes > MaxPayloadBytes {
		return nil, fmt.Errorf("%w: max payload bytes must be between 1 and %d", ErrInvalidOptions, MaxPayloadBytes)
	}
	return &Registry{
		maxProcedures:           options.MaxProcedures,
		maxVersionsPerProcedure: options.MaxVersionsPerProcedure,
		maxPayloadBytes:         options.MaxPayloadBytes,
		authorize:               options.Authorize,
		procedures:              make(map[string]map[uint32]Handler),
	}, nil
}

// AllowAll is an explicit authorizer for callers that have already secured
// the registry boundary through another mechanism.
func AllowAll(context.Context, Call) error { return nil }

// Register adds one procedure version. Existing versions are immutable and
// must be revoked before a name/version pair can be reused.
func (registry *Registry) Register(definition Definition) error {
	if registry == nil {
		return fmt.Errorf("%w: registry is nil", ErrInvalidDefinition)
	}
	if err := validateDefinition(definition); err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	versions, exists := registry.procedures[definition.Name]
	if !exists {
		if len(registry.procedures) >= registry.maxProcedures {
			return ErrProcedureLimit
		}
		versions = make(map[uint32]Handler)
		registry.procedures[definition.Name] = versions
	}
	if _, exists := versions[definition.Version]; exists {
		return ErrAlreadyRegistered
	}
	if len(versions) >= registry.maxVersionsPerProcedure {
		return ErrVersionLimit
	}
	versions[definition.Version] = definition.Handler
	return nil
}

// Unregister removes one version. A zero version removes every version under
// the name, which is useful for revoking a procedure atomically.
func (registry *Registry) Unregister(name string, version uint32) error {
	if registry == nil {
		return fmt.Errorf("%w: registry is nil", ErrInvalidCall)
	}
	if err := validateName(name); err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	versions, exists := registry.procedures[name]
	if !exists {
		return ErrProcedureNotFound
	}
	if version == 0 {
		delete(registry.procedures, name)
		return nil
	}
	if _, exists := versions[version]; !exists {
		return ErrVersionNotFound
	}
	delete(versions, version)
	if len(versions) == 0 {
		delete(registry.procedures, name)
	}
	return nil
}

// Invoke authorizes and executes one procedure. The caller's payload and the
// returned result are never shared with registry internals or handlers.
func (registry *Registry) Invoke(ctx context.Context, call Call) ([]byte, error) {
	if registry == nil {
		return nil, fmt.Errorf("%w: registry is nil", ErrInvalidCall)
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", ErrInvalidCall)
	}
	if err := validateName(call.Name); err != nil {
		return nil, err
	}
	if len(call.Payload) > registry.maxPayloadBytes {
		return nil, ErrPayloadTooLarge
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if registry.authorize == nil {
		return nil, ErrAuthorizationRequired
	}

	handler, version, err := registry.lookup(call.Name, call.Version)
	if err != nil {
		return nil, err
	}

	authorizationPayload := cloneBytes(call.Payload)
	resolvedCall := Call{Name: call.Name, Version: version, Payload: authorizationPayload}
	if err := safeAuthorize(ctx, registry.authorize, resolvedCall); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	handlerPayload := cloneBytes(call.Payload)
	result, err := safeHandle(ctx, handler, handlerPayload)
	if err != nil {
		return nil, err
	}
	if len(result) > registry.maxPayloadBytes {
		return nil, ErrResultTooLarge
	}
	return cloneBytes(result), nil
}

// List returns a sorted copy of all registered names and versions.
func (registry *Registry) List() []Metadata {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	result := make([]Metadata, 0, len(registry.procedures))
	for name, versions := range registry.procedures {
		metadata := Metadata{Name: name, Versions: make([]uint32, 0, len(versions))}
		for version := range versions {
			metadata.Versions = append(metadata.Versions, version)
		}
		sort.Slice(metadata.Versions, func(left, right int) bool { return metadata.Versions[left] < metadata.Versions[right] })
		result = append(result, metadata)
	}
	sort.Slice(result, func(left, right int) bool { return result[left].Name < result[right].Name })
	return result
}

func (registry *Registry) lookup(name string, requestedVersion uint32) (Handler, uint32, error) {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	versions, exists := registry.procedures[name]
	if !exists {
		return nil, 0, ErrProcedureNotFound
	}
	if requestedVersion != 0 {
		handler, exists := versions[requestedVersion]
		if !exists {
			return nil, 0, ErrVersionNotFound
		}
		return handler, requestedVersion, nil
	}
	var latest uint32
	var handler Handler
	for version, candidate := range versions {
		if version > latest {
			latest = version
			handler = candidate
		}
	}
	return handler, latest, nil
}

func validateDefinition(definition Definition) error {
	if err := validateName(definition.Name); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidDefinition, err)
	}
	if definition.Version == 0 {
		return fmt.Errorf("%w: version must be positive", ErrInvalidDefinition)
	}
	if definition.Handler == nil {
		return fmt.Errorf("%w: handler is nil", ErrInvalidDefinition)
	}
	return nil
}

func validateName(name string) error {
	if len(name) == 0 || len(name) > MaxProcedureNameBytes {
		return fmt.Errorf("%w: name length must be between 1 and %d", ErrInvalidCall, MaxProcedureNameBytes)
	}
	for index := 0; index < len(name); index++ {
		value := name[index]
		if !isNameCharacter(value) || (index == 0 && !isAlphaNumeric(value)) || (index == len(name)-1 && !isAlphaNumeric(value)) {
			return fmt.Errorf("%w: invalid procedure name %q", ErrInvalidCall, name)
		}
	}
	return nil
}

func isNameCharacter(value byte) bool {
	return isAlphaNumeric(value) || value == '.' || value == '_' || value == '-'
}

func isAlphaNumeric(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9'
}

func safeAuthorize(ctx context.Context, authorize Authorizer, call Call) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrAuthorizationPanic
		}
	}()
	if err := authorize(ctx, call); err != nil {
		if errors.Is(err, ErrUnauthorized) {
			return err
		}
		return fmt.Errorf("%w: %v", ErrUnauthorized, err)
	}
	return nil
}

func safeHandle(ctx context.Context, handler Handler, payload []byte) (result []byte, err error) {
	defer func() {
		if recover() != nil {
			result = nil
			err = ErrProcedurePanic
		}
	}()
	return handler(ctx, payload)
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	return append([]byte(nil), value...)
}
