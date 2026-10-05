package hatSql

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"
)

const (
	DefaultSQLDictionaryCapacity = 64
	maxSQLDictionaryCapacity     = 4096
	maxSQLDictionaryNameBytes    = 128
)

var (
	ErrSQLDictionaryInvalid       = errors.New("SQL dictionary is invalid")
	ErrSQLDictionaryNotFound      = errors.New("SQL dictionary was not found")
	ErrSQLDictionaryAlreadyExists = errors.New("SQL dictionary already exists")
	ErrSQLDictionaryVersion       = errors.New("SQL dictionary version is invalid")
)

// SQLDictionaryLookup resolves one key against a dictionary snapshot. The
// callback must not mutate the definition it was registered with.
type SQLDictionaryLookup func(key interface{}) (value interface{}, found bool, err error)

// SQLDictionaryDefinition is one immutable, versioned dictionary source.
// Refresh installs a replacement definition atomically for future lookups.
type SQLDictionaryDefinition struct {
	Name    string
	Version uint64
	Lookup  SQLDictionaryLookup
}

// SQLDictionaryRegistry provides bounded, versioned dictionary lookup
// functions through the SQLFunctionResolver contract. It does not retain or
// copy values returned by Lookup.
type SQLDictionaryRegistry struct {
	mu           sync.RWMutex
	capacity     int
	dictionaries map[string]SQLDictionaryDefinition
}

// NewSQLDictionaryRegistry creates a bounded registry. A nonpositive capacity
// selects DefaultSQLDictionaryCapacity.
func NewSQLDictionaryRegistry(capacity int) (*SQLDictionaryRegistry, error) {
	if capacity <= 0 {
		capacity = DefaultSQLDictionaryCapacity
	}
	if capacity > maxSQLDictionaryCapacity {
		return nil, fmt.Errorf("%w: capacity must be <= %d", ErrSQLDictionaryInvalid, maxSQLDictionaryCapacity)
	}
	return &SQLDictionaryRegistry{
		capacity:     capacity,
		dictionaries: make(map[string]SQLDictionaryDefinition, capacity),
	}, nil
}

// Register adds one dictionary. Names are case-insensitive and normalized to
// lower case. A registration cannot replace an existing version.
func (registry *SQLDictionaryRegistry) Register(definition SQLDictionaryDefinition) error {
	if registry == nil {
		return ErrSQLDictionaryInvalid
	}
	normalized, err := normalizeSQLDictionaryDefinition(definition)
	if err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, exists := registry.dictionaries[normalized.Name]; exists {
		return fmt.Errorf("%w: %s", ErrSQLDictionaryAlreadyExists, normalized.Name)
	}
	if len(registry.dictionaries) >= registry.capacity {
		return fmt.Errorf("%w: capacity %d reached", ErrSQLDictionaryInvalid, registry.capacity)
	}
	registry.dictionaries[normalized.Name] = normalized
	return nil
}

// Refresh replaces one registered dictionary only when its version advances.
// The old definition remains visible until the replacement is committed.
func (registry *SQLDictionaryRegistry) Refresh(definition SQLDictionaryDefinition) error {
	if registry == nil {
		return ErrSQLDictionaryInvalid
	}
	normalized, err := normalizeSQLDictionaryDefinition(definition)
	if err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	previous, exists := registry.dictionaries[normalized.Name]
	if !exists {
		return fmt.Errorf("%w: %s", ErrSQLDictionaryNotFound, normalized.Name)
	}
	if normalized.Version <= previous.Version {
		return fmt.Errorf("%w: %d does not advance %d", ErrSQLDictionaryVersion, normalized.Version, previous.Version)
	}
	registry.dictionaries[normalized.Name] = normalized
	return nil
}

// Remove deletes a dictionary by name. Future lookups fail explicitly rather
// than silently returning a miss.
func (registry *SQLDictionaryRegistry) Remove(name string) error {
	if registry == nil {
		return ErrSQLDictionaryInvalid
	}
	name, err := normalizeSQLDictionaryName(name)
	if err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, exists := registry.dictionaries[name]; !exists {
		return fmt.Errorf("%w: %s", ErrSQLDictionaryNotFound, name)
	}
	delete(registry.dictionaries, name)
	return nil
}

// Snapshot returns definitions in deterministic name order. Callback values
// are copied as function references and remain owned by the caller.
func (registry *SQLDictionaryRegistry) Snapshot() []SQLDictionaryDefinition {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	definitions := make([]SQLDictionaryDefinition, 0, len(registry.dictionaries))
	for _, definition := range registry.dictionaries {
		definitions = append(definitions, definition)
	}
	registry.mu.RUnlock()
	sort.Slice(definitions, func(left, right int) bool {
		return definitions[left].Name < definitions[right].Name
	})
	return definitions
}

// EvaluateSQLFunction implements SQLFunctionResolver. Supported functions:
// DICT_GET(name, key[, default]), DICT_HAS(name, key), and DICT_VERSION(name).
// A missing DICT_GET returns NULL (nil) unless an explicit default is given.
func (registry *SQLDictionaryRegistry) EvaluateSQLFunction(name string, calls []FunctionCall) ([]interface{}, error) {
	if registry == nil {
		return nil, ErrSQLDictionaryInvalid
	}
	name = strings.ToUpper(strings.TrimSpace(name))
	values := make([]interface{}, len(calls))
	for index, call := range calls {
		var value interface{}
		var err error
		switch name {
		case "DICT_GET":
			value, err = registry.evaluateGet(call.Arguments)
		case "DICT_HAS":
			value, err = registry.evaluateHas(call.Arguments)
		case "DICT_VERSION":
			value, err = registry.evaluateVersion(call.Arguments)
		default:
			return nil, fmt.Errorf("unknown SQL dictionary function %q", name)
		}
		if err != nil {
			return nil, fmt.Errorf("%s call %d: %w", name, index, err)
		}
		values[index] = value
	}
	return values, nil
}

func (registry *SQLDictionaryRegistry) evaluateGet(arguments []interface{}) (interface{}, error) {
	if len(arguments) != 2 && len(arguments) != 3 {
		return nil, fmt.Errorf("%w: DICT_GET expects 2 or 3 arguments, got %d", ErrSQLDictionaryInvalid, len(arguments))
	}
	definition, err := registry.definition(arguments[0])
	if err != nil {
		return nil, err
	}
	value, found, err := definition.Lookup(arguments[1])
	if err != nil {
		return nil, fmt.Errorf("dictionary %q lookup: %w", definition.Name, err)
	}
	if found {
		return value, nil
	}
	if len(arguments) == 3 {
		return arguments[2], nil
	}
	return nil, nil
}

func (registry *SQLDictionaryRegistry) evaluateHas(arguments []interface{}) (interface{}, error) {
	if len(arguments) != 2 {
		return nil, fmt.Errorf("%w: DICT_HAS expects 2 arguments, got %d", ErrSQLDictionaryInvalid, len(arguments))
	}
	definition, err := registry.definition(arguments[0])
	if err != nil {
		return nil, err
	}
	_, found, err := definition.Lookup(arguments[1])
	if err != nil {
		return nil, fmt.Errorf("dictionary %q lookup: %w", definition.Name, err)
	}
	return found, nil
}

func (registry *SQLDictionaryRegistry) evaluateVersion(arguments []interface{}) (interface{}, error) {
	if len(arguments) != 1 {
		return nil, fmt.Errorf("%w: DICT_VERSION expects 1 argument, got %d", ErrSQLDictionaryInvalid, len(arguments))
	}
	definition, err := registry.definition(arguments[0])
	if err != nil {
		return nil, err
	}
	return int64(definition.Version), nil
}

func (registry *SQLDictionaryRegistry) definition(value interface{}) (SQLDictionaryDefinition, error) {
	if registry == nil {
		return SQLDictionaryDefinition{}, ErrSQLDictionaryInvalid
	}
	name, ok := value.(string)
	if !ok {
		return SQLDictionaryDefinition{}, fmt.Errorf("%w: dictionary name must be a string, got %T", ErrSQLDictionaryInvalid, value)
	}
	name, err := normalizeSQLDictionaryName(name)
	if err != nil {
		return SQLDictionaryDefinition{}, err
	}
	registry.mu.RLock()
	definition, exists := registry.dictionaries[name]
	registry.mu.RUnlock()
	if !exists {
		return SQLDictionaryDefinition{}, fmt.Errorf("%w: %s", ErrSQLDictionaryNotFound, name)
	}
	return definition, nil
}

func normalizeSQLDictionaryDefinition(definition SQLDictionaryDefinition) (SQLDictionaryDefinition, error) {
	name, err := normalizeSQLDictionaryName(definition.Name)
	if err != nil {
		return SQLDictionaryDefinition{}, err
	}
	if definition.Version == 0 || definition.Version > math.MaxInt64 {
		return SQLDictionaryDefinition{}, fmt.Errorf("%w: version must be between 1 and %d", ErrSQLDictionaryVersion, math.MaxInt64)
	}
	if definition.Lookup == nil {
		return SQLDictionaryDefinition{}, fmt.Errorf("%w: lookup callback is required", ErrSQLDictionaryInvalid)
	}
	definition.Name = name
	return definition, nil
}

func normalizeSQLDictionaryName(name string) (string, error) {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" || len(name) > maxSQLDictionaryNameBytes || !utf8.ValidString(name) {
		return "", fmt.Errorf("%w: dictionary name must be nonempty, valid UTF-8, and <= %d bytes", ErrSQLDictionaryInvalid, maxSQLDictionaryNameBytes)
	}
	return name, nil
}
