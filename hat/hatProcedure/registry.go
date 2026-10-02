// Package hatProcedure provides a bounded registry for trusted in-process
// stored procedures. It does not execute source code or enable a scripting
// runtime; callers register ordinary Go handlers explicitly.
package hatProcedure

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

var (
	// ErrRegistryNil indicates a method call on a nil registry.
	ErrRegistryNil = errors.New("hatProcedure: registry is nil")
	// ErrAuthorizerRequired keeps an accidentally open registry from being
	// created without an explicit access policy.
	ErrAuthorizerRequired = errors.New("hatProcedure: authorizer is required")
	// ErrRegistryOptionsInvalid indicates an option outside its supported bound.
	ErrRegistryOptionsInvalid = errors.New("hatProcedure: registry options are invalid")
	// ErrDefinitionInvalid indicates an invalid name, version, or handler.
	ErrDefinitionInvalid = errors.New("hatProcedure: procedure definition is invalid")
	// ErrProcedureAlreadyRegistered indicates a duplicate name and version.
	ErrProcedureAlreadyRegistered = errors.New("hatProcedure: procedure is already registered")
	// ErrProcedureNotFound indicates that the exact name and version are absent.
	ErrProcedureNotFound = errors.New("hatProcedure: procedure is not found")
	// ErrProcedureLimit indicates that the registry has reached its bound.
	ErrProcedureLimit = errors.New("hatProcedure: procedure limit reached")
	// ErrArgumentsTooLarge indicates that a call exceeds the configured bound.
	ErrArgumentsTooLarge = errors.New("hatProcedure: procedure arguments are too large")
	// ErrResultTooLarge indicates that a handler returned too much data.
	ErrResultTooLarge = errors.New("hatProcedure: procedure result is too large")
	// ErrProcedurePanic wraps a panic raised by a registered handler.
	ErrProcedurePanic = errors.New("hatProcedure: procedure panic")
	// ErrProcedureUnauthorized is returned when the authorizer rejects a call.
	ErrProcedureUnauthorized = errors.New("hatProcedure: procedure authorization failed")
)

const (
	// DefaultMaxProcedures bounds the number of registered name/version pairs.
	DefaultMaxProcedures = 1024
	// DefaultMaxNameBytes bounds the UTF-8 byte length of a procedure name.
	DefaultMaxNameBytes = 128
	// DefaultMaxArgumentsBytes bounds one copied call argument payload.
	DefaultMaxArgumentsBytes = 1 << 20
	// DefaultMaxResultBytes bounds one copied handler result payload.
	DefaultMaxResultBytes = 1 << 20

	maxRegistryProcedures     = 1 << 16
	maxRegistryNameBytes      = 1 << 10
	maxRegistryArgumentsBytes = 1 << 30
	maxRegistryResultBytes    = 1 << 30
)

// ProcedureInfo identifies one immutable procedure version.
type ProcedureInfo struct {
	Name    string
	Version uint32
}

// Definition registers one trusted Go handler under an exact name and version.
type Definition struct {
	Name    string
	Version uint32
	Handler Handler
}

// Call is the logical request delivered to a handler. Arguments is a
// registry-owned copy and may be modified by the handler without changing the
// caller's buffer.
type Call struct {
	Principal string
	Name      string
	Version   uint32
	Arguments []byte
}

// Handler executes one procedure call. It must honor ctx cancellation.
type Handler func(context.Context, Call) ([]byte, error)

// AuthorizeFunc decides whether principal may invoke one procedure version.
// It is called before the handler and must not retain the supplied metadata.
type AuthorizeFunc func(context.Context, string, ProcedureInfo) error

// RegistryOptions bounds a Registry. Authorize is mandatory; a nil authorizer
// is rejected so creating a registry never silently creates an open endpoint.
type RegistryOptions struct {
	MaxProcedures     int
	MaxNameBytes      int
	MaxArgumentsBytes int
	MaxResultBytes    int
	Authorize         AuthorizeFunc
}

// Registry stores explicitly registered handlers. Calls may run concurrently
// with each other and with registration changes.
type Registry struct {
	mu                sync.RWMutex
	definitions       map[procedureKey]Definition
	maxProcedures     int
	maxNameBytes      int
	maxArgumentsBytes int
	maxResultBytes    int
	authorize         AuthorizeFunc
}

type procedureKey struct {
	name    string
	version uint32
}

// NewRegistry validates options and creates an empty procedure registry.
func NewRegistry(options RegistryOptions) (*Registry, error) {
	if options.Authorize == nil {
		return nil, ErrAuthorizerRequired
	}
	maxProcedures := options.MaxProcedures
	if maxProcedures == 0 {
		maxProcedures = DefaultMaxProcedures
	}
	maxNameBytesValue := options.MaxNameBytes
	if maxNameBytesValue == 0 {
		maxNameBytesValue = DefaultMaxNameBytes
	}
	maxArgumentsBytesValue := options.MaxArgumentsBytes
	if maxArgumentsBytesValue == 0 {
		maxArgumentsBytesValue = DefaultMaxArgumentsBytes
	}
	maxResultBytesValue := options.MaxResultBytes
	if maxResultBytesValue == 0 {
		maxResultBytesValue = DefaultMaxResultBytes
	}
	if maxProcedures < 1 || maxProcedures > maxRegistryProcedures ||
		maxNameBytesValue < 1 || maxNameBytesValue > maxRegistryNameBytes ||
		maxArgumentsBytesValue < 1 || maxArgumentsBytesValue > maxRegistryArgumentsBytes ||
		maxResultBytesValue < 1 || maxResultBytesValue > maxRegistryResultBytes {
		return nil, ErrRegistryOptionsInvalid
	}
	return &Registry{
		definitions:       make(map[procedureKey]Definition),
		maxProcedures:     maxProcedures,
		maxNameBytes:      maxNameBytesValue,
		maxArgumentsBytes: maxArgumentsBytesValue,
		maxResultBytes:    maxResultBytesValue,
		authorize:         options.Authorize,
	}, nil
}

// Register adds one handler. The exact name/version pair must be unique.
func (registry *Registry) Register(definition Definition) error {
	if registry == nil {
		return ErrRegistryNil
	}
	info, err := registry.normalizeDefinition(definition)
	if err != nil {
		return err
	}
	key := procedureKey{name: info.Name, version: info.Version}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, exists := registry.definitions[key]; exists {
		return ErrProcedureAlreadyRegistered
	}
	if len(registry.definitions) >= registry.maxProcedures {
		return ErrProcedureLimit
	}
	definition.Name = info.Name
	registry.definitions[key] = definition
	return nil
}

// Unregister removes one exact name/version pair and reports whether it was
// present. Calls already in progress retain their handler.
func (registry *Registry) Unregister(name string, version uint32) bool {
	if registry == nil || !validProcedureName(name, registry.maxNameBytes) || version == 0 {
		return false
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	key := procedureKey{name: name, version: version}
	if _, exists := registry.definitions[key]; !exists {
		return false
	}
	delete(registry.definitions, key)
	return true
}

// List returns a deterministic snapshot of registered procedure metadata.
func (registry *Registry) List() []ProcedureInfo {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	items := make([]ProcedureInfo, 0, len(registry.definitions))
	for _, definition := range registry.definitions {
		items = append(items, ProcedureInfo{Name: definition.Name, Version: definition.Version})
	}
	registry.mu.RUnlock()
	sort.Slice(items, func(i, j int) bool {
		if items[i].Name == items[j].Name {
			return items[i].Version < items[j].Version
		}
		return items[i].Name < items[j].Name
	})
	return items
}

// Call authorizes and invokes one exact procedure version. The input and
// output payloads are copied at the registry boundary and are bounded by the
// registry options.
func (registry *Registry) Call(ctx context.Context, principal, name string, version uint32, arguments []byte) ([]byte, error) {
	if registry == nil {
		return nil, ErrRegistryNil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !validProcedureName(name, registry.maxNameBytes) || version == 0 {
		return nil, ErrProcedureNotFound
	}
	if len(arguments) > registry.maxArgumentsBytes {
		return nil, ErrArgumentsTooLarge
	}
	key := procedureKey{name: name, version: version}
	registry.mu.RLock()
	definition, ok := registry.definitions[key]
	registry.mu.RUnlock()
	if !ok {
		return nil, ErrProcedureNotFound
	}
	info := ProcedureInfo{Name: definition.Name, Version: definition.Version}
	if err := registry.authorize(ctx, principal, info); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrProcedureUnauthorized, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	call := Call{
		Principal: principal,
		Name:      definition.Name,
		Version:   definition.Version,
		Arguments: append([]byte(nil), arguments...),
	}
	result, err := invoke(definition.Handler, ctx, call)
	if err != nil {
		return nil, err
	}
	if len(result) > registry.maxResultBytes {
		return nil, ErrResultTooLarge
	}
	return append([]byte(nil), result...), nil
}

func (registry *Registry) normalizeDefinition(definition Definition) (ProcedureInfo, error) {
	if !validProcedureName(definition.Name, registry.maxNameBytes) || definition.Version == 0 || definition.Handler == nil {
		return ProcedureInfo{}, ErrDefinitionInvalid
	}
	return ProcedureInfo{Name: definition.Name, Version: definition.Version}, nil
}

func validProcedureName(name string, maxBytes int) bool {
	if name == "" || len(name) > maxBytes || strings.TrimSpace(name) != name {
		return false
	}
	for index := 0; index < len(name); index++ {
		character := name[index]
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '_' || character == '-' || character == '.' {
			continue
		}
		return false
	}
	return true
}

func invoke(handler Handler, ctx context.Context, call Call) (result []byte, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = nil
			err = fmt.Errorf("%w: %v", ErrProcedurePanic, recovered)
		}
	}()
	return handler(ctx, call)
}
