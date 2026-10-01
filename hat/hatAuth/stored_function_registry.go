package hatAuth

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
)

const (
	DefaultStoredFunctionRegistryMaxFunctions   = 1024
	DefaultStoredFunctionRegistryMaxNameBytes   = 256
	DefaultStoredFunctionRegistryMaxInputBytes  = 1 << 20
	DefaultStoredFunctionRegistryMaxOutputBytes = 1 << 20
	StoredFunctionOperationRegister             = "REGISTER"
	StoredFunctionOperationCall                 = "CALL"
)

var (
	ErrStoredFunctionRegistryInvalid = errors.New("hatriecache: stored function registry is invalid")
	ErrStoredFunctionAccessDenied    = errors.New("hatriecache: stored function access denied")
	ErrStoredFunctionNotFound        = errors.New("hatriecache: stored function not found")
	ErrStoredFunctionVersionConflict = errors.New("hatriecache: stored function version conflict")
	ErrStoredFunctionVersionMismatch = errors.New("hatriecache: stored function version mismatch")
	ErrStoredFunctionInputLimit      = errors.New("hatriecache: stored function input limit exceeded")
	ErrStoredFunctionOutputLimit     = errors.New("hatriecache: stored function output limit exceeded")
	ErrStoredFunctionPanic           = errors.New("hatriecache: stored function panicked")
)

// StoredFunctionHandler is a trusted in-process function implementation. The
// registry supplies an owned input slice; handlers should honor ctx.
type StoredFunctionHandler func(ctx context.Context, input []byte) ([]byte, error)

// StoredFunctionAuthorizer authorizes control-plane registration and calls.
// A nil authorizer is rejected so a new registry cannot silently run functions
// without an authorization boundary.
type StoredFunctionAuthorizer func(principal, operation, function string) bool

// StoredFunctionRegistryOptions bounds a registry and supplies its policy.
type StoredFunctionRegistryOptions struct {
	MaxFunctions   int
	MaxNameBytes   int
	MaxInputBytes  int
	MaxOutputBytes int
	Authorize      StoredFunctionAuthorizer
}

// StoredFunctionSpec defines one version of a named function. Registering a
// higher version atomically replaces the current handler; old versions are no
// longer callable, so callers can pin a version and fail closed on rollout.
type StoredFunctionSpec struct {
	Name    string
	Version uint64
	Handler StoredFunctionHandler
}

// StoredFunctionMetadata is the clone-safe public registry view.
type StoredFunctionMetadata struct {
	Name    string
	Version uint64
}

type storedFunctionEntry struct {
	metadata StoredFunctionMetadata
	handler  StoredFunctionHandler
}

type storedFunctionLimits struct {
	maxFunctions   int
	maxNameBytes   int
	maxInputBytes  int
	maxOutputBytes int
}

// StoredFunctionRegistry is an opt-in, concurrency-safe registry for bounded
// trusted function calls. Registration and execution are separate operations so
// callers can map them to different grants.
type StoredFunctionRegistry struct {
	mu        sync.RWMutex
	limits    storedFunctionLimits
	authorize StoredFunctionAuthorizer
	functions map[string]storedFunctionEntry
}

// NewStoredFunctionRegistry creates a fail-closed registry.
func NewStoredFunctionRegistry(options StoredFunctionRegistryOptions) (*StoredFunctionRegistry, error) {
	if options.Authorize == nil {
		return nil, ErrStoredFunctionRegistryInvalid
	}
	limits, err := newStoredFunctionLimits(options)
	if err != nil {
		return nil, err
	}
	return &StoredFunctionRegistry{
		limits:    limits,
		authorize: options.Authorize,
		functions: make(map[string]storedFunctionEntry),
	}, nil
}

func newStoredFunctionLimits(options StoredFunctionRegistryOptions) (storedFunctionLimits, error) {
	limits := storedFunctionLimits{
		maxFunctions:   options.MaxFunctions,
		maxNameBytes:   options.MaxNameBytes,
		maxInputBytes:  options.MaxInputBytes,
		maxOutputBytes: options.MaxOutputBytes,
	}
	if limits.maxFunctions == 0 {
		limits.maxFunctions = DefaultStoredFunctionRegistryMaxFunctions
	}
	if limits.maxNameBytes == 0 {
		limits.maxNameBytes = DefaultStoredFunctionRegistryMaxNameBytes
	}
	if limits.maxInputBytes == 0 {
		limits.maxInputBytes = DefaultStoredFunctionRegistryMaxInputBytes
	}
	if limits.maxOutputBytes == 0 {
		limits.maxOutputBytes = DefaultStoredFunctionRegistryMaxOutputBytes
	}
	if limits.maxFunctions < 0 || limits.maxNameBytes <= 0 || limits.maxInputBytes < 0 || limits.maxOutputBytes < 0 {
		return storedFunctionLimits{}, ErrStoredFunctionRegistryInvalid
	}
	return limits, nil
}

// Register validates and atomically publishes a function version.
func (registry *StoredFunctionRegistry) Register(actor string, spec StoredFunctionSpec) (StoredFunctionMetadata, error) {
	if registry == nil {
		return StoredFunctionMetadata{}, ErrStoredFunctionRegistryInvalid
	}
	name, err := registry.normalizeName(spec.Name)
	if err != nil || strings.TrimSpace(actor) == "" || spec.Version == 0 || spec.Handler == nil {
		return StoredFunctionMetadata{}, ErrStoredFunctionRegistryInvalid
	}
	if !registry.authorize(actor, StoredFunctionOperationRegister, name) {
		return StoredFunctionMetadata{}, ErrStoredFunctionAccessDenied
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	previous, exists := registry.functions[name]
	if !exists && len(registry.functions) >= registry.limits.maxFunctions {
		return StoredFunctionMetadata{}, ErrStoredFunctionRegistryInvalid
	}
	if exists && spec.Version <= previous.metadata.Version {
		return StoredFunctionMetadata{}, ErrStoredFunctionVersionConflict
	}
	metadata := StoredFunctionMetadata{Name: name, Version: spec.Version}
	registry.functions[name] = storedFunctionEntry{metadata: metadata, handler: spec.Handler}
	return metadata, nil
}

// Call authorizes and invokes a current function. Version zero selects the
// current version; a nonzero version must match exactly.
func (registry *StoredFunctionRegistry) Call(ctx context.Context, principal, name string, version uint64, input []byte) ([]byte, error) {
	if registry == nil {
		return nil, ErrStoredFunctionRegistryInvalid
	}
	name, err := registry.normalizeName(name)
	if err != nil || strings.TrimSpace(principal) == "" {
		return nil, ErrStoredFunctionRegistryInvalid
	}
	if !registry.authorize(principal, StoredFunctionOperationCall, name) {
		return nil, ErrStoredFunctionAccessDenied
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(input) > registry.limits.maxInputBytes {
		return nil, ErrStoredFunctionInputLimit
	}
	registry.mu.RLock()
	entry, exists := registry.functions[name]
	registry.mu.RUnlock()
	if !exists {
		return nil, ErrStoredFunctionNotFound
	}
	if version != 0 && version != entry.metadata.Version {
		return nil, ErrStoredFunctionVersionMismatch
	}
	ownedInput := append([]byte(nil), input...)
	output, err := invokeStoredFunction(ctx, entry.handler, ownedInput)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(output) > registry.limits.maxOutputBytes {
		return nil, ErrStoredFunctionOutputLimit
	}
	return append([]byte(nil), output...), nil
}

func invokeStoredFunction(ctx context.Context, handler StoredFunctionHandler, input []byte) (output []byte, err error) {
	defer func() {
		if recover() != nil {
			output = nil
			err = ErrStoredFunctionPanic
		}
	}()
	return handler(ctx, input)
}

// Lookup returns current metadata without exposing the handler.
func (registry *StoredFunctionRegistry) Lookup(name string) (StoredFunctionMetadata, bool) {
	if registry == nil {
		return StoredFunctionMetadata{}, false
	}
	name, err := registry.normalizeName(name)
	if err != nil {
		return StoredFunctionMetadata{}, false
	}
	registry.mu.RLock()
	entry, ok := registry.functions[name]
	registry.mu.RUnlock()
	if !ok {
		return StoredFunctionMetadata{}, false
	}
	return entry.metadata, true
}

// List returns current metadata in deterministic name order.
func (registry *StoredFunctionRegistry) List() []StoredFunctionMetadata {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	result := make([]StoredFunctionMetadata, 0, len(registry.functions))
	for _, entry := range registry.functions {
		result = append(result, entry.metadata)
	}
	registry.mu.RUnlock()
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

// PolicyStoredFunctionAuthorizer adapts the existing object-aware RBAC policy.
// Functions are addressed as objects named "function:<name>".
func PolicyStoredFunctionAuthorizer(policy Policy) StoredFunctionAuthorizer {
	return func(principal, operation, function string) bool {
		return policy.AuthorizeObject(principal, operation, "", "", "function:"+function)
	}
}

func (registry *StoredFunctionRegistry) normalizeName(name string) (string, error) {
	if strings.TrimSpace(name) != name || name == "" || len(name) > registry.limits.maxNameBytes || strings.IndexByte(name, 0) >= 0 {
		return "", ErrStoredFunctionRegistryInvalid
	}
	return name, nil
}
