package hatSql

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	// These defaults keep an accidentally exposed registry bounded. Callers
	// that need a different envelope must opt in explicitly.
	DefaultStoredProcedureMaxProcedures     = 1024
	DefaultStoredProcedureMaxArguments      = 64
	DefaultStoredProcedureMaxInputBytes     = 1 << 20
	DefaultStoredProcedureMaxOutputBytes    = 1 << 20
	DefaultStoredProcedureMaxConcurrentCall = 64
	DefaultStoredProcedureMaxCallDuration   = 5 * time.Second

	maxStoredProcedureIdentifierBytes = 128
	maxStoredProcedureValueDepth      = 32
)

var (
	// ErrStoredProcedureUnauthorized reports an authorization rejection.
	ErrStoredProcedureUnauthorized = errors.New("stored procedure unauthorized")
	// ErrStoredProcedureDuplicate reports a duplicate package/name/version.
	ErrStoredProcedureDuplicate = errors.New("stored procedure already registered")
	// ErrStoredProcedureNotFound reports an unknown package/name/version.
	ErrStoredProcedureNotFound = errors.New("stored procedure not found")
	// ErrStoredProcedureContextRequired reports a nil call context.
	ErrStoredProcedureContextRequired = errors.New("stored procedure context is required")
	// ErrStoredProcedureArgumentLimit reports too many positional arguments.
	ErrStoredProcedureArgumentLimit = errors.New("stored procedure argument limit exceeded")
	// ErrStoredProcedureInputLimit reports an oversized input payload.
	ErrStoredProcedureInputLimit = errors.New("stored procedure input limit exceeded")
	// ErrStoredProcedureOutputLimit reports an oversized output payload.
	ErrStoredProcedureOutputLimit = errors.New("stored procedure output limit exceeded")
	// ErrStoredProcedurePanic reports a panic converted into an ordinary error.
	ErrStoredProcedurePanic = errors.New("stored procedure panicked")
	// ErrStoredProcedureDefinitionInvalid reports an invalid registration.
	ErrStoredProcedureDefinitionInvalid = errors.New("stored procedure definition is invalid")
	// ErrStoredProcedureArgumentInvalid reports an unsupported or cyclic value.
	ErrStoredProcedureArgumentInvalid = errors.New("stored procedure argument is invalid")
	// ErrStoredProcedureOutputInvalid reports an unsupported or cyclic result.
	ErrStoredProcedureOutputInvalid = errors.New("stored procedure output is invalid")
	// ErrStoredProcedureRegistryLimit reports too many registered procedures.
	ErrStoredProcedureRegistryLimit = errors.New("stored procedure registry limit exceeded")
	// ErrStoredProcedureOptionsInvalid reports an invalid registry envelope.
	ErrStoredProcedureOptionsInvalid = errors.New("stored procedure registry options are invalid")
)

// StoredProcedureAuthorization is the normalized identity presented to an
// optional authorizer before a procedure is invoked.
type StoredProcedureAuthorization struct {
	Principal string
	Package   string
	Name      string
	Version   string
}

// StoredProcedureAuthorizer authorizes one procedure invocation. A nil
// authorizer permits calls for trusted in-process use; network-facing callers
// should provide one.
type StoredProcedureAuthorizer func(context.Context, StoredProcedureAuthorization) error

// StoredProcedureRegistryOptions bounds registration and invocation. Zero
// values use the safe defaults above.
type StoredProcedureRegistryOptions struct {
	MaxProcedures      int
	MaxArguments       int
	MaxInputBytes      int
	MaxOutputBytes     int
	MaxConcurrentCalls int
	MaxCallDuration    time.Duration
	Authorize          StoredProcedureAuthorizer
}

// StoredProcedureDefinition identifies one immutable procedure callback.
// Evaluate must treat arguments as read-only and should honor the context
// deadline because Go cannot forcibly interrupt arbitrary in-process code.
type StoredProcedureDefinition struct {
	Package  string
	Name     string
	Version  string
	Evaluate func(context.Context, []interface{}) (interface{}, error)
}

type storedProcedureConfig struct {
	maxProcedures      int
	maxArguments       int
	maxInputBytes      int
	maxOutputBytes     int
	maxConcurrentCalls int
	maxCallDuration    time.Duration
	authorize          StoredProcedureAuthorizer
}

type storedProcedureKey struct {
	pkg     string
	name    string
	version string
}

func (key storedProcedureKey) String() string {
	return key.pkg + "/" + key.name + "@" + key.version
}

// StoredProcedureRegistry provides bounded, concurrent registration and
// invocation for trusted in-process stored procedures. It is deliberately
// separate from SQL parsing and transport exposure.
type StoredProcedureRegistry struct {
	mu          sync.RWMutex
	config      storedProcedureConfig
	procedures  map[storedProcedureKey]StoredProcedureDefinition
	concurrency chan struct{}
}

// NewStoredProcedureRegistry creates an empty bounded registry.
func NewStoredProcedureRegistry(options StoredProcedureRegistryOptions) (*StoredProcedureRegistry, error) {
	config, err := normalizeStoredProcedureConfig(options)
	if err != nil {
		return nil, err
	}
	return &StoredProcedureRegistry{
		config:      config,
		procedures:  make(map[storedProcedureKey]StoredProcedureDefinition, config.maxProcedures),
		concurrency: make(chan struct{}, config.maxConcurrentCalls),
	}, nil
}

// Register validates and installs one procedure. Registrations are immutable;
// callers replace a version by registering a new version.
func (registry *StoredProcedureRegistry) Register(definition StoredProcedureDefinition) error {
	if registry == nil {
		return fmt.Errorf("%w: registry is required", ErrStoredProcedureDefinitionInvalid)
	}
	normalized, key, err := normalizeStoredProcedureDefinition(definition)
	if err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if len(registry.procedures) >= registry.config.maxProcedures {
		return fmt.Errorf("%w: maximum %d procedures", ErrStoredProcedureRegistryLimit, registry.config.maxProcedures)
	}
	if _, exists := registry.procedures[key]; exists {
		return fmt.Errorf("%w: %s", ErrStoredProcedureDuplicate, key)
	}
	registry.procedures[key] = normalized
	return nil
}

// Call authorizes and invokes one registered procedure under the configured
// argument, payload, concurrency, and duration limits.
func (registry *StoredProcedureRegistry) Call(ctx context.Context, principal, pkg, name, version string, arguments []interface{}) (result interface{}, err error) {
	if registry == nil {
		return nil, fmt.Errorf("%w: registry is required", ErrStoredProcedureNotFound)
	}
	if ctx == nil {
		return nil, ErrStoredProcedureContextRequired
	}
	key, err := normalizeStoredProcedureKey(pkg, name, version)
	if err != nil {
		return nil, err
	}
	registry.mu.RLock()
	definition, ok := registry.procedures[key]
	registry.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrStoredProcedureNotFound, key)
	}

	callCtx, cancel := context.WithTimeout(ctx, registry.config.maxCallDuration)
	defer cancel()
	authorization := StoredProcedureAuthorization{
		Principal: principal,
		Package:   key.pkg,
		Name:      key.name,
		Version:   key.version,
	}
	if registry.config.authorize != nil {
		if authErr := callStoredProcedureAuthorizer(callCtx, registry.config.authorize, authorization); authErr != nil {
			return nil, authErr
		}
	}

	if len(arguments) > registry.config.maxArguments {
		return nil, fmt.Errorf("%w: got %d, maximum %d", ErrStoredProcedureArgumentLimit, len(arguments), registry.config.maxArguments)
	}
	clonedArguments, inputBytes, cloneErr := cloneStoredProcedureArguments(arguments)
	if cloneErr != nil {
		return nil, cloneErr
	}
	if inputBytes > registry.config.maxInputBytes {
		return nil, fmt.Errorf("%w: got %d bytes, maximum %d", ErrStoredProcedureInputLimit, inputBytes, registry.config.maxInputBytes)
	}

	select {
	case registry.concurrency <- struct{}{}:
		defer func() { <-registry.concurrency }()
	case <-callCtx.Done():
		return nil, callCtx.Err()
	}

	result, err = callStoredProcedureEvaluator(callCtx, definition.Evaluate, clonedArguments)
	if err != nil {
		return nil, err
	}
	clonedResult, outputBytes, cloneErr := cloneStoredProcedureValue(result, 0, false)
	if cloneErr != nil {
		return nil, cloneErr
	}
	if outputBytes > registry.config.maxOutputBytes {
		return nil, fmt.Errorf("%w: got %d bytes, maximum %d", ErrStoredProcedureOutputLimit, outputBytes, registry.config.maxOutputBytes)
	}
	return clonedResult, nil
}

// Definition returns one registered definition with normalized identity.
func (registry *StoredProcedureRegistry) Definition(pkg, name, version string) (StoredProcedureDefinition, bool) {
	if registry == nil {
		return StoredProcedureDefinition{}, false
	}
	key, err := normalizeStoredProcedureKey(pkg, name, version)
	if err != nil {
		return StoredProcedureDefinition{}, false
	}
	registry.mu.RLock()
	definition, ok := registry.procedures[key]
	registry.mu.RUnlock()
	return definition, ok
}

// Definitions returns immutable-by-convention copies in deterministic order.
func (registry *StoredProcedureRegistry) Definitions() []StoredProcedureDefinition {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	definitions := make([]StoredProcedureDefinition, 0, len(registry.procedures))
	for _, definition := range registry.procedures {
		definitions = append(definitions, definition)
	}
	registry.mu.RUnlock()
	sort.Slice(definitions, func(left, right int) bool {
		if definitions[left].Package != definitions[right].Package {
			return definitions[left].Package < definitions[right].Package
		}
		if definitions[left].Name != definitions[right].Name {
			return definitions[left].Name < definitions[right].Name
		}
		return definitions[left].Version < definitions[right].Version
	})
	return definitions
}

// Close removes all definitions. In-flight calls continue with their copied
// callback and still release their admission slot.
func (registry *StoredProcedureRegistry) Close() {
	if registry == nil {
		return
	}
	registry.mu.Lock()
	registry.procedures = make(map[storedProcedureKey]StoredProcedureDefinition, registry.config.maxProcedures)
	registry.mu.Unlock()
}

func normalizeStoredProcedureConfig(options StoredProcedureRegistryOptions) (storedProcedureConfig, error) {
	if options.MaxProcedures < 0 || options.MaxArguments < 0 || options.MaxInputBytes < 0 || options.MaxOutputBytes < 0 || options.MaxConcurrentCalls < 0 || options.MaxCallDuration < 0 {
		return storedProcedureConfig{}, fmt.Errorf("%w: negative limits are not allowed", ErrStoredProcedureOptionsInvalid)
	}
	return storedProcedureConfig{
		maxProcedures:      positiveStoredProcedureOption(options.MaxProcedures, DefaultStoredProcedureMaxProcedures),
		maxArguments:       positiveStoredProcedureOption(options.MaxArguments, DefaultStoredProcedureMaxArguments),
		maxInputBytes:      positiveStoredProcedureOption(options.MaxInputBytes, DefaultStoredProcedureMaxInputBytes),
		maxOutputBytes:     positiveStoredProcedureOption(options.MaxOutputBytes, DefaultStoredProcedureMaxOutputBytes),
		maxConcurrentCalls: positiveStoredProcedureOption(options.MaxConcurrentCalls, DefaultStoredProcedureMaxConcurrentCall),
		maxCallDuration:    positiveStoredProcedureDuration(options.MaxCallDuration, DefaultStoredProcedureMaxCallDuration),
		authorize:          options.Authorize,
	}, nil
}

func positiveStoredProcedureOption(value, fallback int) int {
	if value <= 0 {
		return fallback
	}
	return value
}

func positiveStoredProcedureDuration(value, fallback time.Duration) time.Duration {
	if value <= 0 {
		return fallback
	}
	return value
}

func normalizeStoredProcedureDefinition(definition StoredProcedureDefinition) (StoredProcedureDefinition, storedProcedureKey, error) {
	key, err := normalizeStoredProcedureKey(definition.Package, definition.Name, definition.Version)
	if err != nil {
		return StoredProcedureDefinition{}, storedProcedureKey{}, err
	}
	if definition.Evaluate == nil {
		return StoredProcedureDefinition{}, storedProcedureKey{}, fmt.Errorf("%w: evaluator is required", ErrStoredProcedureDefinitionInvalid)
	}
	definition.Package = key.pkg
	definition.Name = key.name
	definition.Version = key.version
	return definition, key, nil
}

func normalizeStoredProcedureKey(pkg, name, version string) (storedProcedureKey, error) {
	normalize := func(label, value string, lower bool) (string, error) {
		value = strings.TrimSpace(value)
		if value == "" {
			return "", fmt.Errorf("%w: %s is required", ErrStoredProcedureDefinitionInvalid, label)
		}
		if len(value) > maxStoredProcedureIdentifierBytes {
			return "", fmt.Errorf("%w: %s exceeds %d bytes", ErrStoredProcedureDefinitionInvalid, label, maxStoredProcedureIdentifierBytes)
		}
		if !utf8.ValidString(value) {
			return "", fmt.Errorf("%w: %s must be valid UTF-8", ErrStoredProcedureDefinitionInvalid, label)
		}
		for _, character := range value {
			if unicode.IsControl(character) {
				return "", fmt.Errorf("%w: %s contains a control character", ErrStoredProcedureDefinitionInvalid, label)
			}
		}
		if lower {
			value = strings.ToLower(value)
		}
		return value, nil
	}
	packageName, err := normalize("package", pkg, true)
	if err != nil {
		return storedProcedureKey{}, err
	}
	procedureName, err := normalize("name", name, true)
	if err != nil {
		return storedProcedureKey{}, err
	}
	procedureVersion, err := normalize("version", version, false)
	if err != nil {
		return storedProcedureKey{}, err
	}
	return storedProcedureKey{pkg: packageName, name: procedureName, version: procedureVersion}, nil
}

func callStoredProcedureAuthorizer(ctx context.Context, authorize StoredProcedureAuthorizer, identity StoredProcedureAuthorization) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("%w: authorizer: %v", ErrStoredProcedurePanic, recovered)
		}
	}()
	err = authorize(ctx, identity)
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrStoredProcedureUnauthorized) {
		return err
	}
	return err
}

func callStoredProcedureEvaluator(ctx context.Context, evaluate func(context.Context, []interface{}) (interface{}, error), arguments []interface{}) (result interface{}, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = nil
			err = fmt.Errorf("%w: %v", ErrStoredProcedurePanic, recovered)
		}
	}()
	return evaluate(ctx, arguments)
}

func cloneStoredProcedureArguments(arguments []interface{}) ([]interface{}, int, error) {
	if arguments == nil {
		return nil, 0, nil
	}
	cloned := make([]interface{}, len(arguments))
	bytes := 0
	for index, argument := range arguments {
		value, valueBytes, err := cloneStoredProcedureValue(argument, 0, true)
		if err != nil {
			return nil, 0, fmt.Errorf("%w at argument %d: %v", ErrStoredProcedureArgumentInvalid, index, err)
		}
		cloned[index] = value
		bytes += valueBytes
	}
	return cloned, bytes, nil
}

func cloneStoredProcedureValue(value interface{}, depth int, argument bool) (interface{}, int, error) {
	if depth > maxStoredProcedureValueDepth {
		if argument {
			return nil, 0, fmt.Errorf("%w: nesting exceeds %d levels", ErrStoredProcedureArgumentInvalid, maxStoredProcedureValueDepth)
		}
		return nil, 0, fmt.Errorf("%w: nesting exceeds %d levels", ErrStoredProcedureOutputInvalid, maxStoredProcedureValueDepth)
	}
	switch typed := value.(type) {
	case nil:
		return nil, 0, nil
	case string:
		return typed, len(typed), nil
	case []byte:
		return append([]byte(nil), typed...), len(typed), nil
	case bool:
		return typed, 1, nil
	case int:
		return typed, 8, nil
	case int8:
		return typed, 1, nil
	case int16:
		return typed, 2, nil
	case int32:
		return typed, 4, nil
	case int64:
		return typed, 8, nil
	case uint:
		return typed, 8, nil
	case uint8:
		return typed, 1, nil
	case uint16:
		return typed, 2, nil
	case uint32:
		return typed, 4, nil
	case uint64:
		return typed, 8, nil
	case float32:
		return typed, 4, nil
	case float64:
		return typed, 8, nil
	case []interface{}:
		cloned := make([]interface{}, len(typed))
		bytes := len(typed) * 8
		for index, item := range typed {
			value, valueBytes, err := cloneStoredProcedureValue(item, depth+1, argument)
			if err != nil {
				return nil, 0, err
			}
			cloned[index] = value
			bytes += valueBytes
		}
		return cloned, bytes, nil
	case []string:
		cloned := append([]string(nil), typed...)
		bytes := len(typed) * 8
		for _, item := range typed {
			bytes += len(item)
		}
		return cloned, bytes, nil
	case []int64:
		return append([]int64(nil), typed...), len(typed) * 8, nil
	case []float64:
		return append([]float64(nil), typed...), len(typed) * 8, nil
	case []bool:
		return append([]bool(nil), typed...), len(typed), nil
	default:
		if argument {
			return nil, 0, fmt.Errorf("%w: unsupported type %T", ErrStoredProcedureArgumentInvalid, value)
		}
		return nil, 0, fmt.Errorf("%w: unsupported type %T", ErrStoredProcedureOutputInvalid, value)
	}
}
