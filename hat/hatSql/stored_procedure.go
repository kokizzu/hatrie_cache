package hatSql

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"unicode"
)

var (
	// ErrStoredProcedureInvalid reports a malformed definition or call name.
	ErrStoredProcedureInvalid = errors.New("hatSql: invalid stored procedure")
	// ErrStoredProcedureExists reports a duplicate immutable name/version pair.
	ErrStoredProcedureExists = errors.New("hatSql: stored procedure already exists")
	// ErrStoredProcedureNotFound reports an unknown name/version pair.
	ErrStoredProcedureNotFound = errors.New("hatSql: stored procedure not found")
	// ErrStoredProcedurePanic reports a recovered procedure or authorizer panic.
	ErrStoredProcedurePanic = errors.New("hatSql: stored procedure callback panicked")
	// ErrStoredProcedureArgumentLimit reports an oversized argument vector.
	ErrStoredProcedureArgumentLimit = errors.New("hatSql: stored procedure argument limit exceeded")
	// ErrStoredProcedureResultLimit reports an oversized result vector.
	ErrStoredProcedureResultLimit = errors.New("hatSql: stored procedure result limit exceeded")
)

const (
	// DefaultStoredProcedureMaxArguments bounds one call when no option is set.
	DefaultStoredProcedureMaxArguments = 64
	// DefaultStoredProcedureMaxResults bounds one response when no option is set.
	DefaultStoredProcedureMaxResults = 64
	maxStoredProcedureNameBytes      = 128
	maxStoredProcedureVersionBytes   = 64
)

// StoredProcedureRequest is the stable input contract for one versioned call.
// Arguments is owned by the registry and may be changed by the callback
// without changing the caller's slice.
type StoredProcedureRequest struct {
	Name      string
	Version   string
	Arguments []interface{}
}

// StoredProcedureResponse is the stable output contract for one call. Values
// are transferred to the caller; the registry never retains that slice.
type StoredProcedureResponse struct {
	Values []interface{}
}

// StoredProcedureFunc executes one trusted in-process procedure.
type StoredProcedureFunc func(context.Context, StoredProcedureRequest) (StoredProcedureResponse, error)

// StoredProcedureAuthorizer can reject a call before its procedure executes.
// It receives a separate request copy and cannot mutate the execution input.
type StoredProcedureAuthorizer func(context.Context, StoredProcedureRequest) error

// StoredProcedureDefinition describes one immutable name/version entry.
type StoredProcedureDefinition struct {
	Name    string
	Version string
	Execute StoredProcedureFunc
}

// StoredProcedureRegistryOptions configures one opt-in registry. A nil
// Authorize hook permits trusted local callers; network-facing adapters should
// provide an authorizer before forwarding requests here.
type StoredProcedureRegistryOptions struct {
	Authorize    StoredProcedureAuthorizer
	MaxArguments int
	MaxResults   int
}

type storedProcedureKey struct {
	name    string
	version string
}

// StoredProcedureRegistry stores immutable versioned callbacks and invokes
// them without holding its lock. The registry is safe for concurrent callers.
type StoredProcedureRegistry struct {
	mu           sync.RWMutex
	procedures   map[storedProcedureKey]StoredProcedureDefinition
	authorize    StoredProcedureAuthorizer
	maxArguments int
	maxResults   int
}

// NewStoredProcedureRegistry creates an empty, explicitly opt-in registry.
// Zero or negative limits use the bounded defaults.
func NewStoredProcedureRegistry(options StoredProcedureRegistryOptions) *StoredProcedureRegistry {
	maxArguments := options.MaxArguments
	if maxArguments <= 0 {
		maxArguments = DefaultStoredProcedureMaxArguments
	}
	maxResults := options.MaxResults
	if maxResults <= 0 {
		maxResults = DefaultStoredProcedureMaxResults
	}
	return &StoredProcedureRegistry{
		procedures:   make(map[storedProcedureKey]StoredProcedureDefinition),
		authorize:    options.Authorize,
		maxArguments: maxArguments,
		maxResults:   maxResults,
	}
}

// Register adds one immutable procedure. Replacing an existing version is
// deliberately rejected; publish a new version to preserve call semantics.
func (registry *StoredProcedureRegistry) Register(definition StoredProcedureDefinition) error {
	if registry == nil {
		return fmt.Errorf("%w: registry is required", ErrStoredProcedureInvalid)
	}
	normalized, key, err := normalizeStoredProcedureDefinition(definition)
	if err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, exists := registry.procedures[key]; exists {
		return fmt.Errorf("%w: %s@%s", ErrStoredProcedureExists, key.name, key.version)
	}
	registry.procedures[key] = normalized
	return nil
}

// Call authorizes and invokes one exact name/version pair.
func (registry *StoredProcedureRegistry) Call(ctx context.Context, name, version string, arguments []interface{}) ([]interface{}, error) {
	if registry == nil {
		return nil, fmt.Errorf("%w: registry is required", ErrStoredProcedureInvalid)
	}
	name, version, err := normalizeStoredProcedureCall(name, version)
	if err != nil {
		return nil, err
	}
	if len(arguments) > registry.maxArguments {
		return nil, fmt.Errorf("%w: got %d, max %d", ErrStoredProcedureArgumentLimit, len(arguments), registry.maxArguments)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key := storedProcedureKey{name: name, version: version}
	registry.mu.RLock()
	definition, exists := registry.procedures[key]
	authorize := registry.authorize
	maxResults := registry.maxResults
	registry.mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("%w: %s@%s", ErrStoredProcedureNotFound, name, version)
	}

	request := StoredProcedureRequest{Name: name, Version: version, Arguments: cloneStoredProcedureValues(arguments)}
	if authorize != nil {
		if err := invokeStoredProcedureAuthorizer(ctx, authorize, cloneStoredProcedureRequest(request)); err != nil {
			return nil, wrapStoredProcedureCallError(name, version, err)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	response, err := invokeStoredProcedure(ctx, definition.Execute, request)
	if err != nil {
		return nil, wrapStoredProcedureCallError(name, version, err)
	}
	if len(response.Values) > maxResults {
		return nil, fmt.Errorf("%w: got %d, max %d", ErrStoredProcedureResultLimit, len(response.Values), maxResults)
	}
	return response.Values, nil
}

// Versions returns the registered versions for name in deterministic order.
func (registry *StoredProcedureRegistry) Versions(name string) []string {
	if registry == nil {
		return nil
	}
	name, err := normalizeStoredProcedureName(name)
	if err != nil {
		return nil
	}
	registry.mu.RLock()
	versions := make([]string, 0)
	for key := range registry.procedures {
		if key.name == name {
			versions = append(versions, key.version)
		}
	}
	registry.mu.RUnlock()
	sort.Strings(versions)
	return versions
}

func normalizeStoredProcedureDefinition(definition StoredProcedureDefinition) (StoredProcedureDefinition, storedProcedureKey, error) {
	name, err := normalizeStoredProcedureName(definition.Name)
	if err != nil {
		return StoredProcedureDefinition{}, storedProcedureKey{}, err
	}
	version, err := normalizeStoredProcedureVersion(definition.Version)
	if err != nil {
		return StoredProcedureDefinition{}, storedProcedureKey{}, err
	}
	if definition.Execute == nil {
		return StoredProcedureDefinition{}, storedProcedureKey{}, fmt.Errorf("%w: execute callback is required", ErrStoredProcedureInvalid)
	}
	definition.Name, definition.Version = name, version
	return definition, storedProcedureKey{name: name, version: version}, nil
}

func normalizeStoredProcedureCall(name, version string) (string, string, error) {
	name, err := normalizeStoredProcedureName(name)
	if err != nil {
		return "", "", err
	}
	version, err = normalizeStoredProcedureVersion(version)
	if err != nil {
		return "", "", err
	}
	return name, version, nil
}

func normalizeStoredProcedureName(value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" || len(value) > maxStoredProcedureNameBytes || strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return "", fmt.Errorf("%w: name", ErrStoredProcedureInvalid)
	}
	return value, nil
}

func normalizeStoredProcedureVersion(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > maxStoredProcedureVersionBytes || strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return "", fmt.Errorf("%w: version", ErrStoredProcedureInvalid)
	}
	return value, nil
}

func cloneStoredProcedureRequest(request StoredProcedureRequest) StoredProcedureRequest {
	request.Arguments = cloneStoredProcedureValues(request.Arguments)
	return request
}

func cloneStoredProcedureValues(values []interface{}) []interface{} {
	if values == nil {
		return nil
	}
	return append([]interface{}(nil), values...)
}

func invokeStoredProcedureAuthorizer(ctx context.Context, authorize StoredProcedureAuthorizer, request StoredProcedureRequest) (err error) {
	completed := false
	defer func() {
		if !completed {
			recover()
			err = ErrStoredProcedurePanic
		}
	}()
	err = authorize(ctx, request)
	completed = true
	return err
}

func invokeStoredProcedure(ctx context.Context, execute StoredProcedureFunc, request StoredProcedureRequest) (response StoredProcedureResponse, err error) {
	completed := false
	defer func() {
		if !completed {
			recover()
			response = StoredProcedureResponse{}
			err = ErrStoredProcedurePanic
		}
	}()
	response, err = execute(ctx, request)
	completed = true
	return response, err
}

func wrapStoredProcedureCallError(name, version string, err error) error {
	return fmt.Errorf("stored procedure %s@%s: %w", name, version, err)
}
