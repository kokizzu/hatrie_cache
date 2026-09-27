package hatSql

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultSQLFinalSchemaRegistryMaxDefinitions bounds the number of source
	// contracts retained by a registry when the caller does not set a limit.
	DefaultSQLFinalSchemaRegistryMaxDefinitions = 256
	// MaxSQLFinalSchemaRegistryMaxDefinitions prevents an accidental unbounded
	// metadata registry from retaining arbitrary source contracts.
	MaxSQLFinalSchemaRegistryMaxDefinitions = 65536
	// MaxSQLFinalSchemaKeyFields keeps per-row key construction bounded.
	MaxSQLFinalSchemaKeyFields = 32
)

var (
	// ErrSQLFinalSchemaRegistryOptionsInvalid means registry construction
	// options are outside the supported bounds.
	ErrSQLFinalSchemaRegistryOptionsInvalid = errors.New("hatSql: invalid FINAL schema registry options")
	// ErrSQLFinalSchemaDefinitionInvalid means a source contract is incomplete
	// or mixes replacing and collapsing metadata.
	ErrSQLFinalSchemaDefinitionInvalid = errors.New("hatSql: invalid FINAL schema definition")
	// ErrSQLFinalSchemaRegistryFull means a new source contract would exceed the
	// registry's configured bound.
	ErrSQLFinalSchemaRegistryFull = errors.New("hatSql: FINAL schema registry is full")
	// ErrSQLFinalSchemaNameInvalid means a registry source name is empty.
	ErrSQLFinalSchemaNameInvalid = errors.New("hatSql: invalid FINAL schema source name")
	// ErrSQLFinalSchemaRowValue means a configured version or sign field cannot
	// be converted to the callback type required by FINAL.
	ErrSQLFinalSchemaRowValue = errors.New("hatSql: invalid FINAL schema row value")
)

// SQLFinalSchemaRegistryOptions configures a bounded source-contract registry.
// A zero MaxDefinitions selects DefaultSQLFinalSchemaRegistryMaxDefinitions.
type SQLFinalSchemaRegistryOptions struct {
	MaxDefinitions int
}

// SQLFinalSchemaDefinition maps named row fields to one FINAL reconciliation
// contract. Replacing definitions require VersionField; collapsing definitions
// require SignField. KeyFields are combined in declaration order.
type SQLFinalSchemaDefinition struct {
	Mode         SQLFinalMode
	KeyFields    []string
	VersionField string
	SignField    string
}

// SQLFinalSchemaRegistration is one immutable entry returned by Snapshot.
type SQLFinalSchemaRegistration struct {
	Kind       string
	Key        string
	Definition SQLFinalSchemaDefinition
}

type sqlFinalSchemaRegistryKey struct {
	kind string
	key  string
}

type sqlFinalSchemaRegistryEntry struct {
	definition SQLFinalSchemaDefinition
	options    SQLFinalOptions
}

// SQLFinalSchemaRegistry turns schema metadata into the callback contract
// already consumed by SQL FINAL. It is opt-in: callers must attach Resolver to
// SQLQueryOptions.FinalSourceOptions before FINAL queries use the registry.
type SQLFinalSchemaRegistry struct {
	mu             sync.RWMutex
	maxDefinitions int
	definitions    map[sqlFinalSchemaRegistryKey]sqlFinalSchemaRegistryEntry
}

// NewSQLFinalSchemaRegistry creates a bounded, empty FINAL schema registry.
func NewSQLFinalSchemaRegistry(options SQLFinalSchemaRegistryOptions) (*SQLFinalSchemaRegistry, error) {
	maxDefinitions := options.MaxDefinitions
	if maxDefinitions == 0 {
		maxDefinitions = DefaultSQLFinalSchemaRegistryMaxDefinitions
	}
	if maxDefinitions < 1 || maxDefinitions > MaxSQLFinalSchemaRegistryMaxDefinitions {
		return nil, fmt.Errorf("%w: MaxDefinitions=%d", ErrSQLFinalSchemaRegistryOptionsInvalid, maxDefinitions)
	}
	return &SQLFinalSchemaRegistry{
		maxDefinitions: maxDefinitions,
		definitions:    make(map[sqlFinalSchemaRegistryKey]sqlFinalSchemaRegistryEntry),
	}, nil
}

// Upsert validates and installs one source contract. Updating an existing
// entry does not consume another registry slot.
func (registry *SQLFinalSchemaRegistry) Upsert(kind, key string, definition SQLFinalSchemaDefinition) error {
	if registry == nil {
		return ErrSQLFinalSchemaRegistryOptionsInvalid
	}
	normalizedKind, err := normalizeSQLFinalSchemaKind(kind)
	if err != nil {
		return err
	}
	normalizedKey, err := normalizeSQLFinalSchemaName(key)
	if err != nil {
		return err
	}
	normalizedDefinition, err := normalizeSQLFinalSchemaDefinition(definition)
	if err != nil {
		return err
	}
	entry := sqlFinalSchemaRegistryEntry{
		definition: normalizedDefinition,
		options:    compileSQLFinalSchemaOptions(normalizedDefinition),
	}
	registryKey := sqlFinalSchemaRegistryKey{kind: normalizedKind, key: normalizedKey}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, exists := registry.definitions[registryKey]; !exists && len(registry.definitions) >= registry.maxDefinitions {
		return ErrSQLFinalSchemaRegistryFull
	}
	registry.definitions[registryKey] = entry
	return nil
}

// Delete removes one source contract and reports whether it existed.
func (registry *SQLFinalSchemaRegistry) Delete(kind, key string) bool {
	if registry == nil {
		return false
	}
	normalizedKind, err := normalizeSQLFinalSchemaKind(kind)
	if err != nil {
		return false
	}
	normalizedKey, err := normalizeSQLFinalSchemaName(key)
	if err != nil {
		return false
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	registryKey := sqlFinalSchemaRegistryKey{kind: normalizedKind, key: normalizedKey}
	if _, exists := registry.definitions[registryKey]; !exists {
		return false
	}
	delete(registry.definitions, registryKey)
	return true
}

// Resolve implements SQLFinalSourceOptionsFunc. Missing source contracts are
// deliberately reported as unconfigured so the existing FINAL error policy is
// preserved by the query engine.
func (registry *SQLFinalSchemaRegistry) Resolve(kind, key string) (SQLFinalOptions, bool, error) {
	if registry == nil {
		return SQLFinalOptions{}, false, nil
	}
	normalizedKind, err := normalizeSQLFinalSchemaKind(kind)
	if err != nil {
		return SQLFinalOptions{}, false, nil
	}
	normalizedKey, err := normalizeSQLFinalSchemaName(key)
	if err != nil {
		return SQLFinalOptions{}, false, nil
	}
	registry.mu.RLock()
	entry, configured := registry.definitions[sqlFinalSchemaRegistryKey{kind: normalizedKind, key: normalizedKey}]
	registry.mu.RUnlock()
	if !configured {
		return SQLFinalOptions{}, false, nil
	}
	return entry.options, true, nil
}

// Resolver adapts the registry to SQLQueryOptions.FinalSourceOptions.
func (registry *SQLFinalSchemaRegistry) Resolver() *SQLFinalSourceOptionsResolver {
	return &SQLFinalSourceOptionsResolver{Resolve: registry.Resolve}
}

// Snapshot returns a sorted, detached view of all registered contracts.
func (registry *SQLFinalSchemaRegistry) Snapshot() []SQLFinalSchemaRegistration {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	registrations := make([]SQLFinalSchemaRegistration, 0, len(registry.definitions))
	for registryKey, entry := range registry.definitions {
		registrations = append(registrations, SQLFinalSchemaRegistration{
			Kind:       registryKey.kind,
			Key:        registryKey.key,
			Definition: cloneSQLFinalSchemaDefinition(entry.definition),
		})
	}
	registry.mu.RUnlock()
	sort.Slice(registrations, func(left, right int) bool {
		if registrations[left].Kind != registrations[right].Kind {
			return registrations[left].Kind < registrations[right].Kind
		}
		return registrations[left].Key < registrations[right].Key
	})
	return registrations
}

func normalizeSQLFinalSchemaName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", ErrSQLFinalSchemaNameInvalid
	}
	return name, nil
}

func normalizeSQLFinalSchemaKind(kind string) (string, error) {
	kind, err := normalizeSQLFinalSchemaName(kind)
	if err != nil {
		return "", err
	}
	return strings.ToUpper(kind), nil
}

func normalizeSQLFinalSchemaDefinition(definition SQLFinalSchemaDefinition) (SQLFinalSchemaDefinition, error) {
	if definition.Mode != SQLFinalReplacing && definition.Mode != SQLFinalCollapsing {
		return SQLFinalSchemaDefinition{}, ErrSQLFinalSchemaDefinitionInvalid
	}
	if len(definition.KeyFields) == 0 || len(definition.KeyFields) > MaxSQLFinalSchemaKeyFields {
		return SQLFinalSchemaDefinition{}, ErrSQLFinalSchemaDefinitionInvalid
	}
	normalized := SQLFinalSchemaDefinition{
		Mode:      definition.Mode,
		KeyFields: make([]string, 0, len(definition.KeyFields)),
	}
	seen := make(map[string]struct{}, len(definition.KeyFields)+2)
	for _, field := range definition.KeyFields {
		field, err := normalizeSQLFinalSchemaField(field)
		if err != nil {
			return SQLFinalSchemaDefinition{}, err
		}
		if _, exists := seen[field]; exists {
			return SQLFinalSchemaDefinition{}, ErrSQLFinalSchemaDefinitionInvalid
		}
		seen[field] = struct{}{}
		normalized.KeyFields = append(normalized.KeyFields, field)
	}
	versionField, err := normalizeOptionalSQLFinalSchemaField(definition.VersionField)
	if err != nil {
		return SQLFinalSchemaDefinition{}, err
	}
	signField, err := normalizeOptionalSQLFinalSchemaField(definition.SignField)
	if err != nil {
		return SQLFinalSchemaDefinition{}, err
	}
	switch definition.Mode {
	case SQLFinalReplacing:
		if versionField == "" || signField != "" {
			return SQLFinalSchemaDefinition{}, ErrSQLFinalSchemaDefinitionInvalid
		}
		normalized.VersionField = versionField
	case SQLFinalCollapsing:
		if signField == "" || versionField != "" {
			return SQLFinalSchemaDefinition{}, ErrSQLFinalSchemaDefinitionInvalid
		}
		normalized.SignField = signField
	}
	if _, exists := seen[versionField]; versionField != "" && exists {
		return SQLFinalSchemaDefinition{}, ErrSQLFinalSchemaDefinitionInvalid
	}
	if _, exists := seen[signField]; signField != "" && exists {
		return SQLFinalSchemaDefinition{}, ErrSQLFinalSchemaDefinitionInvalid
	}
	return normalized, nil
}

func normalizeSQLFinalSchemaField(field string) (string, error) {
	field = strings.TrimSpace(field)
	if field == "" {
		return "", ErrSQLFinalSchemaDefinitionInvalid
	}
	return field, nil
}

func normalizeOptionalSQLFinalSchemaField(field string) (string, error) {
	if strings.TrimSpace(field) == "" {
		return "", nil
	}
	return normalizeSQLFinalSchemaField(field)
}

func cloneSQLFinalSchemaDefinition(definition SQLFinalSchemaDefinition) SQLFinalSchemaDefinition {
	definition.KeyFields = append([]string(nil), definition.KeyFields...)
	return definition
}

func compileSQLFinalSchemaOptions(definition SQLFinalSchemaDefinition) SQLFinalOptions {
	options := SQLFinalOptions{
		Mode: definition.Mode,
		Key:  compileSQLFinalSchemaKey(definition.KeyFields),
	}
	if definition.Mode == SQLFinalReplacing {
		options.Version = compileSQLFinalSchemaVersion(definition.VersionField)
	} else {
		options.Sign = compileSQLFinalSchemaSign(definition.SignField)
	}
	return options
}

func compileSQLFinalSchemaKey(fields []string) SQLReplacingMergeKeyFunc {
	fields = append([]string(nil), fields...)
	if len(fields) == 1 {
		field := fields[0]
		return func(row SQLRow) string {
			value, present := row[field]
			if !present {
				return "\x00m;"
			}
			if stringValue, ok := value.(string); ok && (len(stringValue) == 0 || stringValue[0] != 0) {
				return stringValue
			}
			var stack [256]byte
			encoded := append(stack[:0], 0)
			if stringValue, ok := value.(string); ok {
				encoded = append(encoded, 's')
				encoded = appendSQLFinalSchemaLengthValue(encoded, stringValue)
			} else {
				encoded = appendSQLFinalSchemaValue(encoded, value)
			}
			return string(encoded)
		}
	}
	return func(row SQLRow) string {
		var stack [256]byte
		encoded := stack[:0]
		for _, field := range fields {
			value, present := row[field]
			if !present {
				encoded = append(encoded, '0', ';')
				continue
			}
			encoded = append(encoded, '1', ';')
			encoded = appendSQLFinalSchemaValue(encoded, value)
		}
		return string(encoded)
	}
}

func appendSQLFinalSchemaValue(encoded []byte, value any) []byte {
	switch value := value.(type) {
	case nil:
		return append(encoded, 'n', ';')
	case string:
		encoded = append(encoded, 's')
		return appendSQLFinalSchemaLengthValue(encoded, value)
	case []byte:
		encoded = append(encoded, 'b')
		return appendSQLFinalSchemaLengthValue(encoded, string(value))
	case bool:
		if value {
			return append(encoded, 't', ';')
		} else {
			return append(encoded, 'f', ';')
		}
	case int:
		encoded = append(encoded, 'i')
		encoded = strconv.AppendInt(encoded, int64(value), 10)
		return append(encoded, ';')
	case int8:
		encoded = append(encoded, 'i')
		encoded = strconv.AppendInt(encoded, int64(value), 10)
		return append(encoded, ';')
	case int16:
		encoded = append(encoded, 'i')
		encoded = strconv.AppendInt(encoded, int64(value), 10)
		return append(encoded, ';')
	case int32:
		encoded = append(encoded, 'i')
		encoded = strconv.AppendInt(encoded, int64(value), 10)
		return append(encoded, ';')
	case int64:
		encoded = append(encoded, 'i')
		encoded = strconv.AppendInt(encoded, value, 10)
		return append(encoded, ';')
	case uint:
		encoded = append(encoded, 'u')
		encoded = strconv.AppendUint(encoded, uint64(value), 10)
		return append(encoded, ';')
	case uint8:
		encoded = append(encoded, 'u')
		encoded = strconv.AppendUint(encoded, uint64(value), 10)
		return append(encoded, ';')
	case uint16:
		encoded = append(encoded, 'u')
		encoded = strconv.AppendUint(encoded, uint64(value), 10)
		return append(encoded, ';')
	case uint32:
		encoded = append(encoded, 'u')
		encoded = strconv.AppendUint(encoded, uint64(value), 10)
		return append(encoded, ';')
	case uint64:
		encoded = append(encoded, 'u')
		encoded = strconv.AppendUint(encoded, value, 10)
		return append(encoded, ';')
	case uintptr:
		encoded = append(encoded, 'u')
		encoded = strconv.AppendUint(encoded, uint64(value), 10)
		return append(encoded, ';')
	case float32:
		encoded = append(encoded, 'f')
		encoded = strconv.AppendFloat(encoded, float64(value), 'g', -1, 32)
		return append(encoded, ';')
	case float64:
		encoded = append(encoded, 'f')
		encoded = strconv.AppendFloat(encoded, value, 'g', -1, 64)
		return append(encoded, ';')
	case time.Time:
		encoded = append(encoded, 'd')
		return appendSQLFinalSchemaLengthValue(encoded, value.UTC().Format(time.RFC3339Nano))
	default:
		encoded = append(encoded, 'x')
		valueType := reflect.TypeOf(value)
		if valueType == nil {
			encoded = appendSQLFinalSchemaLengthValue(encoded, "nil")
			return appendSQLFinalSchemaLengthValue(encoded, "<nil>")
		}
		encoded = appendSQLFinalSchemaLengthValue(encoded, valueType.String())
		return appendSQLFinalSchemaLengthValue(encoded, fmt.Sprintf("%#v", value))
	}
}

func appendSQLFinalSchemaLengthValue(encoded []byte, value string) []byte {
	encoded = strconv.AppendInt(encoded, int64(len(value)), 10)
	encoded = append(encoded, ':')
	encoded = append(encoded, value...)
	return append(encoded, ';')
}

func compileSQLFinalSchemaVersion(field string) SQLReplacingMergeVersionFunc {
	return func(row SQLRow) (uint64, error) {
		value, present := row[field]
		if !present || value == nil {
			return 0, fmt.Errorf("%w: field %q is missing or nil", ErrSQLFinalSchemaRowValue, field)
		}
		version, ok := sqlFinalSchemaUint64(value)
		if !ok {
			return 0, fmt.Errorf("%w: field %q has unsupported version value %T", ErrSQLFinalSchemaRowValue, field, value)
		}
		return version, nil
	}
}

func compileSQLFinalSchemaSign(field string) SQLCollapsingMergeSignFunc {
	return func(row SQLRow) (int, error) {
		value, present := row[field]
		if !present || value == nil {
			return 0, fmt.Errorf("%w: field %q is missing or nil", ErrSQLFinalSchemaRowValue, field)
		}
		sign, ok := sqlFinalSchemaInt(value)
		if !ok {
			return 0, fmt.Errorf("%w: field %q has unsupported sign value %T", ErrSQLFinalSchemaRowValue, field, value)
		}
		return sign, nil
	}
}

func sqlFinalSchemaUint64(value any) (uint64, bool) {
	switch value := value.(type) {
	case uint:
		return uint64(value), true
	case uint8:
		return uint64(value), true
	case uint16:
		return uint64(value), true
	case uint32:
		return uint64(value), true
	case uint64:
		return value, true
	case uintptr:
		return uint64(value), true
	case int:
		if value < 0 {
			return 0, false
		}
		return uint64(value), true
	case int8:
		if value < 0 {
			return 0, false
		}
		return uint64(value), true
	case int16:
		if value < 0 {
			return 0, false
		}
		return uint64(value), true
	case int32:
		if value < 0 {
			return 0, false
		}
		return uint64(value), true
	case int64:
		if value < 0 {
			return 0, false
		}
		return uint64(value), true
	case float32:
		converted := float64(value)
		if converted < 0 || math.IsNaN(converted) || math.IsInf(converted, 0) || math.Trunc(converted) != converted || converted >= 18446744073709551616.0 {
			return 0, false
		}
		return uint64(converted), true
	case float64:
		if value < 0 || math.IsNaN(value) || math.IsInf(value, 0) || math.Trunc(value) != value || value >= 18446744073709551616.0 {
			return 0, false
		}
		return uint64(value), true
	case string:
		converted, err := strconv.ParseUint(value, 10, 64)
		return converted, err == nil
	case []byte:
		converted, err := strconv.ParseUint(string(value), 10, 64)
		return converted, err == nil
	default:
		return 0, false
	}
}

func sqlFinalSchemaInt(value any) (int, bool) {
	const maxInt64 = int64(^uint64(0) >> 1)
	const minInt64 = -maxInt64 - 1
	maxInt := int64(^uint(0) >> 1)
	minInt := -maxInt - 1
	var converted int64
	switch value := value.(type) {
	case int:
		return value, true
	case int8:
		return int(value), true
	case int16:
		return int(value), true
	case int32:
		return int(value), true
	case int64:
		converted = value
	case uint:
		if uint64(value) > uint64(maxInt) {
			return 0, false
		}
		return int(value), true
	case uint8:
		return int(value), true
	case uint16:
		return int(value), true
	case uint32:
		if uint64(value) > uint64(maxInt) {
			return 0, false
		}
		return int(value), true
	case uint64:
		if value > uint64(maxInt) {
			return 0, false
		}
		return int(value), true
	case uintptr:
		if uint64(value) > uint64(maxInt) {
			return 0, false
		}
		return int(value), true
	case float32:
		floatValue := float64(value)
		if math.IsNaN(floatValue) || math.IsInf(floatValue, 0) || math.Trunc(floatValue) != floatValue || floatValue < float64(minInt64) || floatValue > float64(maxInt64) {
			return 0, false
		}
		converted = int64(floatValue)
	case float64:
		if math.IsNaN(value) || math.IsInf(value, 0) || math.Trunc(value) != value || value < float64(minInt64) || value > float64(maxInt64) {
			return 0, false
		}
		converted = int64(value)
	case string:
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return 0, false
		}
		converted = parsed
	case []byte:
		parsed, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			return 0, false
		}
		converted = parsed
	default:
		return 0, false
	}
	if converted < minInt || converted > maxInt {
		return 0, false
	}
	return int(converted), true
}
