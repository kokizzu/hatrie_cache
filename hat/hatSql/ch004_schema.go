package hatSql

import (
	"errors"
	"strings"
	"sync"
)

var (
	// ErrSQLFinalSchemaKindRequired means a schema registration omitted its
	// source kind.
	ErrSQLFinalSchemaKindRequired = errors.New("hatSql: FINAL schema source kind is required")
	// ErrSQLFinalSchemaKeyRequired means a schema registration omitted its
	// source key.
	ErrSQLFinalSchemaKeyRequired = errors.New("hatSql: FINAL schema source key is required")
)

type sqlFinalSchemaKey struct {
	kind string
	key  string
}

// SQLFinalSchemaRegistry stores validated, schema-bound FINAL contracts.
// Registrations are exact by source kind and key, safe for concurrent query
// resolution, and owned by the caller for persistence or lifecycle.
type SQLFinalSchemaRegistry struct {
	mu      sync.RWMutex
	entries map[sqlFinalSchemaKey]SQLFinalOptions
}

// NewSQLFinalSchemaRegistry creates an empty FINAL metadata registry.
func NewSQLFinalSchemaRegistry() *SQLFinalSchemaRegistry {
	return &SQLFinalSchemaRegistry{
		entries: make(map[sqlFinalSchemaKey]SQLFinalOptions),
	}
}

// Register validates and replaces the contract for one source. Source kinds
// are matched case-insensitively; source keys retain their exact spelling.
func (registry *SQLFinalSchemaRegistry) Register(kind, key string, options SQLFinalOptions) error {
	normalizedKind, err := normalizeSQLFinalSchemaIdentity(kind, key)
	if err != nil {
		return err
	}
	if err := options.validate(); err != nil {
		return err
	}
	if registry == nil {
		return errors.New("hatSql: FINAL schema registry is nil")
	}
	registry.mu.Lock()
	if registry.entries == nil {
		registry.entries = make(map[sqlFinalSchemaKey]SQLFinalOptions)
	}
	registry.entries[sqlFinalSchemaKey{kind: normalizedKind, key: key}] = options
	registry.mu.Unlock()
	return nil
}

// Unregister removes one source contract and reports whether it existed.
func (registry *SQLFinalSchemaRegistry) Unregister(kind, key string) bool {
	if registry == nil {
		return false
	}
	normalizedKind, err := normalizeSQLFinalSchemaIdentity(kind, key)
	if err != nil {
		return false
	}
	registry.mu.Lock()
	_, existed := registry.entries[sqlFinalSchemaKey{kind: normalizedKind, key: key}]
	delete(registry.entries, sqlFinalSchemaKey{kind: normalizedKind, key: key})
	registry.mu.Unlock()
	return existed
}

// Resolve implements SQLFinalSourceOptionsFunc for schema-bound queries.
func (registry *SQLFinalSchemaRegistry) Resolve(kind, key string) (SQLFinalOptions, bool, error) {
	if registry == nil {
		return SQLFinalOptions{}, false, nil
	}
	normalizedKind, err := normalizeSQLFinalSchemaIdentity(kind, key)
	if err != nil {
		return SQLFinalOptions{}, false, nil
	}
	registry.mu.RLock()
	options, configured := registry.entries[sqlFinalSchemaKey{kind: normalizedKind, key: key}]
	registry.mu.RUnlock()
	return options, configured, nil
}

func normalizeSQLFinalSchemaIdentity(kind, key string) (string, error) {
	kind = strings.ToUpper(strings.TrimSpace(kind))
	if kind == "" {
		return "", ErrSQLFinalSchemaKindRequired
	}
	if strings.TrimSpace(key) == "" {
		return "", ErrSQLFinalSchemaKeyRequired
	}
	return kind, nil
}
