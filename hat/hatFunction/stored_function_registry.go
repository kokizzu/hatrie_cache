// Package hatFunction contains opt-in trusted in-process function
// registries. It does not provide a sandbox or an authorization policy.
package hatFunction

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

var (
	// ErrStoredFunctionInvalid indicates an invalid function definition,
	// registry option, or function name.
	ErrStoredFunctionInvalid = errors.New("hatFunction: invalid stored function")
	// ErrStoredFunctionExists indicates that a normalized name is already
	// registered.
	ErrStoredFunctionExists = errors.New("hatFunction: stored function already exists")
	// ErrStoredFunctionNotFound indicates that a function name is absent.
	ErrStoredFunctionNotFound = errors.New("hatFunction: stored function not found")
	// ErrStoredFunctionCapacity indicates that the registry is full.
	ErrStoredFunctionCapacity = errors.New("hatFunction: stored function registry capacity reached")
	// ErrStoredFunctionArity indicates that the call argument count is wrong.
	ErrStoredFunctionArity = errors.New("hatFunction: stored function arity mismatch")
	// ErrStoredFunctionArgumentLimit indicates that a call exceeds the bounded
	// argument budget.
	ErrStoredFunctionArgumentLimit = errors.New("hatFunction: stored function argument limit exceeded")
	// ErrStoredFunctionPanic wraps a panic raised by a trusted handler.
	ErrStoredFunctionPanic = errors.New("hatFunction: stored function panicked")
)

const (
	// DefaultStoredFunctionMaxFunctions is used when the option is zero.
	DefaultStoredFunctionMaxFunctions = 1024
	// DefaultStoredFunctionMaxArguments is used when the option is zero.
	DefaultStoredFunctionMaxArguments = 64
	// MaxStoredFunctionNameBytes bounds normalized names.
	MaxStoredFunctionNameBytes = 128
	// MaxStoredFunctionFunctions bounds one registry allocation.
	MaxStoredFunctionFunctions = 1 << 16
	// MaxStoredFunctionArguments bounds one call.
	MaxStoredFunctionArguments = 1024
)

// StoredFunctionHandler is the trusted implementation of a registered
// function. The registry copies the argument slice but not values referenced
// by its elements.
type StoredFunctionHandler func(context.Context, []any) (any, error)

// StoredFunction describes one versioned function definition. Arity -1 means
// any argument count up to the registry limit.
type StoredFunction struct {
	Name          string
	Version       uint64
	Arity         int
	Deterministic bool
	Handler       StoredFunctionHandler
}

// StoredFunctionInfo is the handler-free metadata returned by introspection.
type StoredFunctionInfo struct {
	Name          string
	Version       uint64
	Arity         int
	Deterministic bool
}

// StoredFunctionRegistryOptions bounds one registry. Zero values select the
// documented defaults.
type StoredFunctionRegistryOptions struct {
	MaxFunctions int
	MaxArguments int
}

// StoredFunctionRegistry is a concurrency-safe name-to-handler registry.
// Callers must treat registered handlers as trusted code and enforce any
// authorization policy before calling Call.
type StoredFunctionRegistry struct {
	mu           sync.RWMutex
	maxArguments int
	maxFunctions int
	functions    map[string]StoredFunction
}

// NewStoredFunctionRegistry creates an empty bounded registry.
func NewStoredFunctionRegistry(options StoredFunctionRegistryOptions) (*StoredFunctionRegistry, error) {
	maxFunctions := options.MaxFunctions
	if maxFunctions == 0 {
		maxFunctions = DefaultStoredFunctionMaxFunctions
	}
	if maxFunctions < 1 || maxFunctions > MaxStoredFunctionFunctions {
		return nil, fmt.Errorf("%w: max functions %d", ErrStoredFunctionInvalid, maxFunctions)
	}
	maxArguments := options.MaxArguments
	if maxArguments == 0 {
		maxArguments = DefaultStoredFunctionMaxArguments
	}
	if maxArguments < 1 || maxArguments > MaxStoredFunctionArguments {
		return nil, fmt.Errorf("%w: max arguments %d", ErrStoredFunctionInvalid, maxArguments)
	}
	return &StoredFunctionRegistry{
		maxArguments: maxArguments,
		maxFunctions: maxFunctions,
		functions:    make(map[string]StoredFunction, maxFunctions),
	}, nil
}

// Register adds one function under its normalized name.
func (registry *StoredFunctionRegistry) Register(function StoredFunction) error {
	if registry == nil {
		return fmt.Errorf("%w: nil registry", ErrStoredFunctionInvalid)
	}
	if registry.maxArguments < 1 || registry.maxFunctions < 1 || registry.functions == nil {
		return fmt.Errorf("%w: uninitialized registry", ErrStoredFunctionInvalid)
	}
	name, err := normalizeStoredFunctionName(function.Name)
	if err != nil {
		return err
	}
	if function.Version == 0 || function.Handler == nil || function.Arity < -1 || function.Arity > registry.maxArguments {
		return fmt.Errorf("%w: definition %q", ErrStoredFunctionInvalid, name)
	}
	function.Name = name
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, exists := registry.functions[name]; exists {
		return fmt.Errorf("%w: %s", ErrStoredFunctionExists, name)
	}
	if len(registry.functions) >= registry.maxFunctions {
		return fmt.Errorf("%w: %d", ErrStoredFunctionCapacity, registry.maxFunctions)
	}
	registry.functions[name] = function
	return nil
}

// Unregister removes a function and reports whether it existed.
func (registry *StoredFunctionRegistry) Unregister(name string) bool {
	if registry == nil {
		return false
	}
	normalized, err := normalizeStoredFunctionName(name)
	if err != nil {
		return false
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, exists := registry.functions[normalized]; !exists {
		return false
	}
	delete(registry.functions, normalized)
	return true
}

// Lookup returns handler-free metadata for a normalized function name.
func (registry *StoredFunctionRegistry) Lookup(name string) (StoredFunctionInfo, bool) {
	if registry == nil {
		return StoredFunctionInfo{}, false
	}
	normalized, err := normalizeStoredFunctionName(name)
	if err != nil {
		return StoredFunctionInfo{}, false
	}
	registry.mu.RLock()
	function, exists := registry.functions[normalized]
	registry.mu.RUnlock()
	if !exists {
		return StoredFunctionInfo{}, false
	}
	return storedFunctionInfo(function), true
}

// RegisteredFunction returns handler-free metadata or a typed not-found
// error. It is useful for validating a function before a request is admitted.
func (registry *StoredFunctionRegistry) RegisteredFunction(name string) (StoredFunctionInfo, error) {
	info, ok := registry.Lookup(name)
	if !ok {
		return StoredFunctionInfo{}, ErrStoredFunctionNotFound
	}
	return info, nil
}

// List returns a name-sorted copy of all registered metadata.
func (registry *StoredFunctionRegistry) List() []StoredFunctionInfo {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	infos := make([]StoredFunctionInfo, 0, len(registry.functions))
	for _, function := range registry.functions {
		infos = append(infos, storedFunctionInfo(function))
	}
	registry.mu.RUnlock()
	sort.Slice(infos, func(left, right int) bool { return infos[left].Name < infos[right].Name })
	return infos
}

// Call invokes a registered function after checking context, arity, and the
// argument budget. Handler panics are converted to ErrStoredFunctionPanic.
func (registry *StoredFunctionRegistry) Call(ctx context.Context, name string, args []any) (result any, err error) {
	if registry == nil {
		return nil, fmt.Errorf("%w: nil registry", ErrStoredFunctionInvalid)
	}
	if registry.maxArguments < 1 || registry.maxFunctions < 1 || registry.functions == nil {
		return nil, fmt.Errorf("%w: uninitialized registry", ErrStoredFunctionInvalid)
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrStoredFunctionInvalid)
	}
	normalized, err := normalizeStoredFunctionName(name)
	if err != nil {
		return nil, err
	}
	registry.mu.RLock()
	function, exists := registry.functions[normalized]
	maxArguments := registry.maxArguments
	registry.mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrStoredFunctionNotFound, normalized)
	}
	if len(args) > maxArguments {
		return nil, fmt.Errorf("%w: got %d, max %d", ErrStoredFunctionArgumentLimit, len(args), maxArguments)
	}
	if function.Arity >= 0 && len(args) != function.Arity {
		return nil, fmt.Errorf("%w: got %d, want %d", ErrStoredFunctionArity, len(args), function.Arity)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ownedArgs := append([]any(nil), args...)
	return invokeStoredFunction(ctx, function.Handler, ownedArgs)
}

func invokeStoredFunction(ctx context.Context, handler StoredFunctionHandler, args []any) (result any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = nil
			err = fmt.Errorf("%w: %v", ErrStoredFunctionPanic, recovered)
		}
	}()
	return handler(ctx, args)
}

func storedFunctionInfo(function StoredFunction) StoredFunctionInfo {
	return StoredFunctionInfo{Name: function.Name, Version: function.Version, Arity: function.Arity, Deterministic: function.Deterministic}
}

func normalizeStoredFunctionName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if len(name) == 0 || len(name) > MaxStoredFunctionNameBytes {
		return "", fmt.Errorf("%w: name length %d", ErrStoredFunctionInvalid, len(name))
	}
	lowercase := false
	for index := 0; index < len(name); index++ {
		character := name[index]
		switch {
		case character >= 'A' && character <= 'Z':
			lowercase = true
		case character >= 'a' && character <= 'z', character >= '0' && character <= '9', character == '_', character == '.':
		default:
			return "", fmt.Errorf("%w: name %q", ErrStoredFunctionInvalid, name)
		}
	}
	if lowercase {
		name = strings.ToLower(name)
	}
	return name, nil
}
