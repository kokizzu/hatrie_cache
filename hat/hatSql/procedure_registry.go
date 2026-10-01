package hatSql

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

var (
	ErrSQLProcedureRegistryNil      = errors.New("hatSql: SQL procedure registry is nil")
	ErrSQLProcedureNameRequired     = errors.New("hatSql: SQL procedure name is required")
	ErrSQLProcedureSourceRequired   = errors.New("hatSql: SQL procedure source is required")
	ErrSQLProcedureParameterInvalid = errors.New("hatSql: SQL procedure parameter is invalid")
	ErrSQLProcedureExists           = errors.New("hatSql: SQL procedure already exists")
	ErrSQLProcedureLimit            = errors.New("hatSql: SQL procedure registry limit exceeded")
	ErrSQLProcedureNotFound         = errors.New("hatSql: SQL procedure was not found")
	ErrSQLProcedureReadOnly         = errors.New("hatSql: SQL procedure must be a read-only query")
	ErrSQLProcedureParameterCount   = errors.New("hatSql: SQL procedure parameter count mismatch")
)

const (
	DefaultSQLProcedureRegistryLimit = 256
	MaxSQLProcedureRegistryLimit     = 4096
)

// SQLProcedureRegistryOptions bounds one in-process read-only procedure
// registry. Zero selects DefaultSQLProcedureRegistryLimit.
type SQLProcedureRegistryOptions struct {
	MaxProcedures int
}

// SQLProcedureDefinition names one parameterized read-only SQL query. The
// Parameters slice documents the positional arguments accepted by Source;
// Source uses the existing '$1', '$2', ... parameter syntax.
type SQLProcedureDefinition struct {
	Name       string
	Source     string
	Parameters []string
}

type sqlProcedureEntry struct {
	definition SQLProcedureDefinition
	compiled   *CompiledSQLQuery
}

// SQLProcedureRegistry stores immutable compiled read-only SQL procedures.
// Registration and metadata lookup are synchronized; compiled queries are
// safe for concurrent calls. The registry is caller-owned and disabled unless
// a caller explicitly creates and uses one.
type SQLProcedureRegistry struct {
	mu      sync.RWMutex
	limit   int
	entries map[string]sqlProcedureEntry
}

// NewSQLProcedureRegistry creates a bounded empty procedure registry.
func NewSQLProcedureRegistry(options SQLProcedureRegistryOptions) (*SQLProcedureRegistry, error) {
	limit := options.MaxProcedures
	if limit == 0 {
		limit = DefaultSQLProcedureRegistryLimit
	}
	if limit < 0 || limit > MaxSQLProcedureRegistryLimit {
		return nil, fmt.Errorf("%w: maximum procedures must be between 1 and %d", ErrSQLProcedureLimit, MaxSQLProcedureRegistryLimit)
	}
	return &SQLProcedureRegistry{limit: limit, entries: make(map[string]sqlProcedureEntry, limit)}, nil
}

// Register compiles and atomically publishes one read-only procedure.
func (registry *SQLProcedureRegistry) Register(definition SQLProcedureDefinition) error {
	if registry == nil {
		return ErrSQLProcedureRegistryNil
	}
	normalized, key, err := normalizeSQLProcedureDefinition(definition)
	if err != nil {
		return err
	}
	parameterCount, err := sqlPreparedParameterCount(normalized.Source)
	if err != nil {
		return fmt.Errorf("SQL procedure %q: %w", normalized.Name, err)
	}
	if parameterCount != len(normalized.Parameters) {
		return fmt.Errorf("%w: %q declares %d names for %d SQL parameters", ErrSQLProcedureParameterCount, normalized.Name, len(normalized.Parameters), parameterCount)
	}
	compiled, err := CompileSQLQuery(normalized.Source)
	if err != nil {
		return fmt.Errorf("SQL procedure %q: %w", normalized.Name, err)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, exists := registry.entries[key]; exists {
		return fmt.Errorf("%w: %q", ErrSQLProcedureExists, normalized.Name)
	}
	if len(registry.entries) >= registry.limit {
		return fmt.Errorf("%w: maximum procedures %d", ErrSQLProcedureLimit, registry.limit)
	}
	registry.entries[key] = sqlProcedureEntry{definition: normalized, compiled: compiled}
	return nil
}

// Call binds positional parameters and executes one compiled procedure.
func (registry *SQLProcedureRegistry) Call(ctx context.Context, name string, resolver SQLSourceResolver, parameters []interface{}, options SQLQueryOptions) (SQLQueryResult, error) {
	if registry == nil {
		return SQLQueryResult{}, ErrSQLProcedureRegistryNil
	}
	key := normalizeSQLProcedureName(name)
	if key == "" {
		return SQLQueryResult{}, ErrSQLProcedureNameRequired
	}
	registry.mu.RLock()
	entry, ok := registry.entries[key]
	registry.mu.RUnlock()
	if !ok {
		return SQLQueryResult{}, fmt.Errorf("%w: %q", ErrSQLProcedureNotFound, name)
	}
	if len(parameters) != len(entry.definition.Parameters) {
		return SQLQueryResult{}, fmt.Errorf("%w: %q expects %d values, got %d", ErrSQLProcedureParameterCount, entry.definition.Name, len(entry.definition.Parameters), len(parameters))
	}
	return entry.compiled.Execute(ctx, resolver, parameters, options)
}

// Definition returns a clone-safe definition by case-insensitive name.
func (registry *SQLProcedureRegistry) Definition(name string) (SQLProcedureDefinition, bool) {
	if registry == nil {
		return SQLProcedureDefinition{}, false
	}
	key := normalizeSQLProcedureName(name)
	if key == "" {
		return SQLProcedureDefinition{}, false
	}
	registry.mu.RLock()
	entry, ok := registry.entries[key]
	registry.mu.RUnlock()
	if !ok {
		return SQLProcedureDefinition{}, false
	}
	return cloneSQLProcedureDefinition(entry.definition), true
}

// Definitions returns clone-safe definitions in deterministic name order.
func (registry *SQLProcedureRegistry) Definitions() []SQLProcedureDefinition {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	definitions := make([]SQLProcedureDefinition, 0, len(registry.entries))
	for _, entry := range registry.entries {
		definitions = append(definitions, cloneSQLProcedureDefinition(entry.definition))
	}
	registry.mu.RUnlock()
	sort.Slice(definitions, func(left, right int) bool { return definitions[left].Name < definitions[right].Name })
	return definitions
}

func normalizeSQLProcedureDefinition(definition SQLProcedureDefinition) (SQLProcedureDefinition, string, error) {
	definition.Name = strings.TrimSpace(definition.Name)
	if definition.Name == "" {
		return SQLProcedureDefinition{}, "", ErrSQLProcedureNameRequired
	}
	definition.Source = strings.TrimSpace(definition.Source)
	if definition.Source == "" {
		return SQLProcedureDefinition{}, "", ErrSQLProcedureSourceRequired
	}
	if !sqlProcedureSourceIsReadOnly(definition.Source) {
		return SQLProcedureDefinition{}, "", ErrSQLProcedureReadOnly
	}
	seen := make(map[string]struct{}, len(definition.Parameters))
	for index, parameter := range definition.Parameters {
		parameter = strings.TrimSpace(parameter)
		if parameter == "" {
			return SQLProcedureDefinition{}, "", fmt.Errorf("%w: parameter %d", ErrSQLProcedureParameterInvalid, index+1)
		}
		key := strings.ToUpper(parameter)
		if _, exists := seen[key]; exists {
			return SQLProcedureDefinition{}, "", fmt.Errorf("%w: duplicate parameter %q", ErrSQLProcedureParameterInvalid, parameter)
		}
		seen[key] = struct{}{}
		definition.Parameters[index] = parameter
	}
	definition.Parameters = append([]string(nil), definition.Parameters...)
	return definition, normalizeSQLProcedureName(definition.Name), nil
}

func normalizeSQLProcedureName(name string) string {
	return strings.ToUpper(strings.TrimSpace(name))
}

func sqlProcedureSourceIsReadOnly(source string) bool {
	fields := strings.Fields(source)
	if len(fields) == 0 {
		return false
	}
	switch strings.ToUpper(fields[0]) {
	case "FROM", "SELECT", "WITH":
		return true
	default:
		return false
	}
}

func cloneSQLProcedureDefinition(definition SQLProcedureDefinition) SQLProcedureDefinition {
	definition.Parameters = append([]string(nil), definition.Parameters...)
	return definition
}
