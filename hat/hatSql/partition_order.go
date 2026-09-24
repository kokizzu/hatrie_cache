package hatSql

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

const (
	// DefaultSQLPartitionOrderRegistryCapacity bounds the default number of
	// declarations retained by a registry. A zero capacity passed to the
	// constructor selects this value.
	DefaultSQLPartitionOrderRegistryCapacity = 256
	maxSQLPartitionOrderFields               = 64
	maxSQLPartitionOrderNameBytes            = 256
)

var (
	ErrSQLPartitionOrderInvalid      = errors.New("invalid SQL partition order declaration")
	ErrSQLPartitionOrderRegistryNil  = errors.New("SQL partition order registry is nil")
	ErrSQLPartitionOrderRegistryFull = errors.New("SQL partition order registry is full")
)

// SQLPartitionOrderField describes the order of one field inside every
// physical partition. Null ordering is explicit when either null flag is set;
// both flags cannot be set at the same time.
type SQLPartitionOrderField struct {
	Field      string
	Desc       bool
	NullsFirst bool
	NullsLast  bool
}

// SQLPartitionOrderDeclaration is metadata owned by a source adapter. It
// describes the logical partition key and the deterministic order guaranteed
// inside each partition. The query engine never assumes the declaration is
// true for execution; it only exposes it to planning and EXPLAIN.
type SQLPartitionOrderDeclaration struct {
	Source          string
	Key             string
	PartitionFields []string
	OrderFields     []SQLPartitionOrderField
}

// SQLPartitionOrderResolver optionally supplies a declaration for a logical
// source. It is intentionally separate from SourceResolver so callers can
// register metadata without wrapping or replacing an existing data source.
type SQLPartitionOrderResolver interface {
	ResolveSQLPartitionOrder(name string, key string) (SQLPartitionOrderDeclaration, bool, error)
}

// SQLPartitionOrderRegistry is a bounded, concurrent declaration catalog.
// Register copies all caller-owned slices, and ResolveSQLPartitionOrder
// returns fresh copies, so declarations cannot be changed through aliases.
type SQLPartitionOrderRegistry struct {
	mu           sync.RWMutex
	capacity     int
	declarations map[sqlPartitionOrderRegistryKey]SQLPartitionOrderDeclaration
}

type sqlPartitionOrderRegistryKey struct {
	source string
	key    string
}

// NewSQLPartitionOrderRegistry creates a bounded declaration registry. A zero
// capacity selects DefaultSQLPartitionOrderRegistryCapacity.
func NewSQLPartitionOrderRegistry(capacity int) *SQLPartitionOrderRegistry {
	if capacity <= 0 {
		capacity = DefaultSQLPartitionOrderRegistryCapacity
	}
	return &SQLPartitionOrderRegistry{
		capacity:     capacity,
		declarations: make(map[sqlPartitionOrderRegistryKey]SQLPartitionOrderDeclaration),
	}
}

// Register validates and stores a declaration. Registering an existing source
// and key replaces it without consuming another registry slot.
func (registry *SQLPartitionOrderRegistry) Register(declaration SQLPartitionOrderDeclaration) error {
	if registry == nil {
		return ErrSQLPartitionOrderRegistryNil
	}
	normalized, err := normalizeSQLPartitionOrderDeclaration(declaration)
	if err != nil {
		return err
	}
	key := sqlPartitionOrderRegistryKey{source: normalized.Source, key: normalized.Key}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.capacity <= 0 {
		registry.capacity = DefaultSQLPartitionOrderRegistryCapacity
	}
	if registry.declarations == nil {
		registry.declarations = make(map[sqlPartitionOrderRegistryKey]SQLPartitionOrderDeclaration)
	}
	if _, exists := registry.declarations[key]; !exists && len(registry.declarations) >= registry.capacity {
		return ErrSQLPartitionOrderRegistryFull
	}
	registry.declarations[key] = normalized
	return nil
}

// ResolveSQLPartitionOrder implements SQLPartitionOrderResolver.
func (registry *SQLPartitionOrderRegistry) ResolveSQLPartitionOrder(source string, key string) (SQLPartitionOrderDeclaration, bool, error) {
	if registry == nil {
		return SQLPartitionOrderDeclaration{}, false, ErrSQLPartitionOrderRegistryNil
	}
	source = strings.TrimSpace(source)
	key = strings.TrimSpace(key)
	registry.mu.RLock()
	declaration, found := registry.declarations[sqlPartitionOrderRegistryKey{source: source, key: key}]
	registry.mu.RUnlock()
	if !found {
		return SQLPartitionOrderDeclaration{}, false, nil
	}
	return cloneSQLPartitionOrderDeclaration(declaration), true, nil
}

// SupportsPartitionPredicate reports whether a literal predicate can be
// safely offered to the existing PartitionPruningSourceResolver contract.
// It does not prune rows itself and returns false for unsupported operators.
func (declaration SQLPartitionOrderDeclaration) SupportsPartitionPredicate(predicate SQLPartitionPredicate) bool {
	field := strings.TrimSpace(predicate.Field)
	if field == "" || len(predicate.Values) == 0 {
		return false
	}
	switch strings.ToUpper(strings.TrimSpace(predicate.Operator)) {
	case "=", "IN", "<", "<=", ">", ">=":
	default:
		return false
	}
	for _, partitionField := range declaration.PartitionFields {
		if strings.EqualFold(field, strings.TrimSpace(partitionField)) {
			return true
		}
	}
	return false
}

// String formats a declaration for EXPLAIN and operational diagnostics.
func (declaration SQLPartitionOrderDeclaration) String() string {
	parts := make([]string, 0, 2)
	if len(declaration.PartitionFields) > 0 {
		fields := make([]string, len(declaration.PartitionFields))
		for index, field := range declaration.PartitionFields {
			fields[index] = strings.TrimSpace(field)
		}
		parts = append(parts, "PARTITION BY "+strings.Join(fields, ", "))
	}
	if len(declaration.OrderFields) > 0 {
		fields := make([]string, len(declaration.OrderFields))
		for index, field := range declaration.OrderFields {
			name := strings.TrimSpace(field.Field)
			if field.Desc {
				name += " DESC"
			} else {
				name += " ASC"
			}
			if field.NullsFirst {
				name += " NULLS FIRST"
			} else if field.NullsLast {
				name += " NULLS LAST"
			}
			fields[index] = name
		}
		parts = append(parts, "ORDER BY "+strings.Join(fields, ", "))
	}
	return strings.Join(parts, " ")
}

func normalizeSQLPartitionOrderDeclaration(declaration SQLPartitionOrderDeclaration) (SQLPartitionOrderDeclaration, error) {
	declaration = cloneSQLPartitionOrderDeclaration(declaration)
	declaration.Source = strings.TrimSpace(declaration.Source)
	declaration.Key = strings.TrimSpace(declaration.Key)
	if !validSQLPartitionOrderName(declaration.Source) || !validSQLPartitionOrderName(declaration.Key) {
		return SQLPartitionOrderDeclaration{}, fmt.Errorf("%w: source and key are required", ErrSQLPartitionOrderInvalid)
	}
	if len(declaration.PartitionFields) == 0 && len(declaration.OrderFields) == 0 {
		return SQLPartitionOrderDeclaration{}, fmt.Errorf("%w: partition or order fields are required", ErrSQLPartitionOrderInvalid)
	}
	if len(declaration.PartitionFields) > maxSQLPartitionOrderFields || len(declaration.OrderFields) > maxSQLPartitionOrderFields {
		return SQLPartitionOrderDeclaration{}, fmt.Errorf("%w: too many fields", ErrSQLPartitionOrderInvalid)
	}
	if len(declaration.Source) > maxSQLPartitionOrderNameBytes || len(declaration.Key) > maxSQLPartitionOrderNameBytes {
		return SQLPartitionOrderDeclaration{}, fmt.Errorf("%w: source or key is too long", ErrSQLPartitionOrderInvalid)
	}
	seenPartitions := make(map[string]struct{}, len(declaration.PartitionFields))
	for index, field := range declaration.PartitionFields {
		field = strings.TrimSpace(field)
		if !validSQLPartitionOrderName(field) || len(field) > maxSQLPartitionOrderNameBytes {
			return SQLPartitionOrderDeclaration{}, fmt.Errorf("%w: invalid partition field", ErrSQLPartitionOrderInvalid)
		}
		canonical := strings.ToLower(field)
		if _, exists := seenPartitions[canonical]; exists {
			return SQLPartitionOrderDeclaration{}, fmt.Errorf("%w: duplicate partition field %q", ErrSQLPartitionOrderInvalid, field)
		}
		seenPartitions[canonical] = struct{}{}
		declaration.PartitionFields[index] = field
	}
	seenOrderFields := make(map[string]struct{}, len(declaration.OrderFields))
	for index, field := range declaration.OrderFields {
		field.Field = strings.TrimSpace(field.Field)
		if !validSQLPartitionOrderName(field.Field) || len(field.Field) > maxSQLPartitionOrderNameBytes || field.NullsFirst && field.NullsLast {
			return SQLPartitionOrderDeclaration{}, fmt.Errorf("%w: invalid order field", ErrSQLPartitionOrderInvalid)
		}
		canonical := strings.ToLower(field.Field)
		if _, exists := seenOrderFields[canonical]; exists {
			return SQLPartitionOrderDeclaration{}, fmt.Errorf("%w: duplicate order field %q", ErrSQLPartitionOrderInvalid, field.Field)
		}
		seenOrderFields[canonical] = struct{}{}
		declaration.OrderFields[index] = field
	}
	return cloneSQLPartitionOrderDeclaration(declaration), nil
}

func validSQLPartitionOrderName(value string) bool {
	return value != "" && !strings.ContainsRune(value, '\x00')
}

func cloneSQLPartitionOrderDeclaration(declaration SQLPartitionOrderDeclaration) SQLPartitionOrderDeclaration {
	declaration.PartitionFields = append([]string(nil), declaration.PartitionFields...)
	declaration.OrderFields = append([]SQLPartitionOrderField(nil), declaration.OrderFields...)
	return declaration
}
