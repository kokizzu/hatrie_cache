package hatSql

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	maxSQLDictionaryEntries = 1_000_000
	maxSQLDictionaryKeySize = 1 << 20
)

// SQLDictionaryInfo describes a dictionary without exposing its values.
// Version starts at one and increases after every successful refresh.
type SQLDictionaryInfo struct {
	Name    string
	Version uint64
	Entries int
}

type sqlDictionarySnapshot struct {
	version uint64
	values  map[string]interface{}
}

type sqlDictionary struct {
	mu        sync.RWMutex
	version   uint64
	values    map[string]interface{}
	canonical string
}

// SQLDictionaryRegistry stores named, atomically refreshed SQL dictionaries.
// It also implements FunctionResolver for DICT_GET, DICT_HAS, and
// DICT_VERSION. Values are restricted to immutable SQL scalar values so a
// published snapshot cannot be changed through an external reference.
type SQLDictionaryRegistry struct {
	mu           sync.RWMutex
	dictionaries map[string]*sqlDictionary
}

// NewSQLDictionaryRegistry creates an empty dictionary registry.
func NewSQLDictionaryRegistry() *SQLDictionaryRegistry {
	return &SQLDictionaryRegistry{dictionaries: make(map[string]*sqlDictionary)}
}

// Register installs a dictionary at version one. Names are case-insensitive
// and duplicate registration is rejected so callers cannot silently replace a
// live lookup table.
func (registry *SQLDictionaryRegistry) Register(name string, values map[string]interface{}) error {
	if registry == nil {
		return fmt.Errorf("SQL dictionary registry is required")
	}
	canonical, err := normalizeSQLDictionaryName(name)
	if err != nil {
		return err
	}
	prepared, err := cloneSQLDictionaryValues(values)
	if err != nil {
		return fmt.Errorf("register SQL dictionary %q: %w", canonical, err)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.dictionaries == nil {
		registry.dictionaries = make(map[string]*sqlDictionary)
	}
	if _, exists := registry.dictionaries[canonical]; exists {
		return fmt.Errorf("SQL dictionary %q already exists", canonical)
	}
	registry.dictionaries[canonical] = &sqlDictionary{version: 1, values: prepared, canonical: canonical}
	return nil
}

// Refresh atomically replaces every entry in an existing dictionary. The
// input map is copied before publication, so a failed refresh leaves the old
// version intact and concurrent readers see either the old or new snapshot.
func (registry *SQLDictionaryRegistry) Refresh(name string, values map[string]interface{}) error {
	if registry == nil {
		return fmt.Errorf("SQL dictionary registry is required")
	}
	canonical, err := normalizeSQLDictionaryName(name)
	if err != nil {
		return err
	}
	prepared, err := cloneSQLDictionaryValues(values)
	if err != nil {
		return fmt.Errorf("refresh SQL dictionary %q: %w", canonical, err)
	}
	registry.mu.RLock()
	dictionary := registry.dictionaries[canonical]
	registry.mu.RUnlock()
	if dictionary == nil {
		return fmt.Errorf("SQL dictionary %q not found", canonical)
	}
	dictionary.mu.Lock()
	dictionary.version++
	dictionary.values = prepared
	dictionary.mu.Unlock()
	return nil
}

// Info returns metadata for a dictionary without returning any stored value.
func (registry *SQLDictionaryRegistry) Info(name string) (SQLDictionaryInfo, error) {
	dictionary, err := registry.dictionary(name)
	if err != nil {
		return SQLDictionaryInfo{}, err
	}
	snapshot := dictionary.snapshot()
	return SQLDictionaryInfo{Name: dictionary.canonical, Version: snapshot.version, Entries: len(snapshot.values)}, nil
}

// Lookup performs one typed dictionary lookup for Go callers. A missing key is
// not an error; version identifies the immutable snapshot used for the read.
func (registry *SQLDictionaryRegistry) Lookup(name string, key interface{}) (value interface{}, found bool, version uint64, err error) {
	dictionary, err := registry.dictionary(name)
	if err != nil {
		return nil, false, 0, err
	}
	snapshot := dictionary.snapshot()
	return sqlDictionaryLookup(snapshot, key)
}

// EvaluateSQLFunction implements FunctionResolver. A complete call batch
// captures one immutable snapshot per referenced dictionary, preventing a
// concurrent refresh from producing mixed versions in one SQL expression.
func (registry *SQLDictionaryRegistry) EvaluateSQLFunction(name string, calls []FunctionCall) ([]interface{}, error) {
	if registry == nil {
		return nil, fmt.Errorf("SQL dictionary registry is required")
	}
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "DICT_GET":
		return registry.evaluateDictionaryGet(calls)
	case "DICT_HAS":
		return registry.evaluateDictionaryHas(calls)
	case "DICT_VERSION":
		return registry.evaluateDictionaryVersion(calls)
	default:
		return nil, fmt.Errorf("unknown SQL function %q", name)
	}
}

func (registry *SQLDictionaryRegistry) evaluateDictionaryGet(calls []FunctionCall) ([]interface{}, error) {
	batch := sqlDictionaryBatch{registry: registry}
	values := make([]interface{}, len(calls))
	for index, call := range calls {
		if len(call.Arguments) < 2 || len(call.Arguments) > 3 {
			return nil, fmt.Errorf("DICT_GET expects a dictionary name, key, and optional default")
		}
		snapshot, canonical, err := batch.snapshotForCall(call)
		if err != nil {
			return nil, err
		}
		value, found, _, err := sqlDictionaryLookup(snapshot, call.Arguments[1])
		if err != nil {
			return nil, fmt.Errorf("DICT_GET %q: %w", canonical, err)
		}
		if found {
			values[index] = value
		} else if len(call.Arguments) == 3 {
			values[index] = call.Arguments[2]
		}
	}
	return values, nil
}

func (registry *SQLDictionaryRegistry) evaluateDictionaryHas(calls []FunctionCall) ([]interface{}, error) {
	batch := sqlDictionaryBatch{registry: registry}
	values := make([]interface{}, len(calls))
	for index, call := range calls {
		if len(call.Arguments) != 2 {
			return nil, fmt.Errorf("DICT_HAS expects a dictionary name and key")
		}
		snapshot, canonical, err := batch.snapshotForCall(call)
		if err != nil {
			return nil, err
		}
		_, found, _, err := sqlDictionaryLookup(snapshot, call.Arguments[1])
		if err != nil {
			return nil, fmt.Errorf("DICT_HAS %q: %w", canonical, err)
		}
		values[index] = found
	}
	return values, nil
}

func (registry *SQLDictionaryRegistry) evaluateDictionaryVersion(calls []FunctionCall) ([]interface{}, error) {
	batch := sqlDictionaryBatch{registry: registry}
	values := make([]interface{}, len(calls))
	for index, call := range calls {
		if len(call.Arguments) != 1 {
			return nil, fmt.Errorf("DICT_VERSION expects a dictionary name")
		}
		snapshot, _, err := batch.snapshotForCall(call)
		if err != nil {
			return nil, err
		}
		values[index] = snapshot.version
	}
	return values, nil
}

type sqlDictionaryBatch struct {
	registry        *SQLDictionaryRegistry
	commonName      string
	commonCanonical string
	commonSnapshot  *sqlDictionarySnapshot
	snapshots       map[string]*sqlDictionarySnapshot
}

func (batch *sqlDictionaryBatch) snapshotForCall(call FunctionCall) (*sqlDictionarySnapshot, string, error) {
	name, ok := call.Arguments[0].(string)
	if !ok {
		return nil, "", fmt.Errorf("SQL dictionary name must be a string")
	}
	if batch.commonSnapshot != nil && name == batch.commonName {
		return batch.commonSnapshot, batch.commonCanonical, nil
	}
	canonical, err := normalizeSQLDictionaryName(name)
	if err != nil {
		return nil, "", err
	}
	if batch.commonSnapshot == nil {
		dictionary, err := batch.registry.dictionary(canonical)
		if err != nil {
			return nil, canonical, err
		}
		batch.commonName = name
		batch.commonCanonical = canonical
		batch.commonSnapshot = dictionary.snapshot()
		return batch.commonSnapshot, canonical, nil
	}
	if batch.snapshots == nil {
		batch.snapshots = map[string]*sqlDictionarySnapshot{batch.commonCanonical: batch.commonSnapshot}
	}
	if snapshot, ok := batch.snapshots[canonical]; ok {
		return snapshot, canonical, nil
	}
	dictionary, err := batch.registry.dictionary(canonical)
	if err != nil {
		return nil, canonical, err
	}
	snapshot := dictionary.snapshot()
	batch.snapshots[canonical] = snapshot
	return snapshot, canonical, nil
}

func (registry *SQLDictionaryRegistry) dictionary(name string) (*sqlDictionary, error) {
	if registry == nil {
		return nil, fmt.Errorf("SQL dictionary registry is required")
	}
	canonical, err := normalizeSQLDictionaryName(name)
	if err != nil {
		return nil, err
	}
	registry.mu.RLock()
	dictionary := registry.dictionaries[canonical]
	registry.mu.RUnlock()
	if dictionary == nil {
		return nil, fmt.Errorf("SQL dictionary %q not found", canonical)
	}
	return dictionary, nil
}

func (dictionary *sqlDictionary) snapshot() *sqlDictionarySnapshot {
	dictionary.mu.RLock()
	snapshot := &sqlDictionarySnapshot{version: dictionary.version, values: dictionary.values}
	dictionary.mu.RUnlock()
	return snapshot
}

func sqlDictionaryLookup(snapshot *sqlDictionarySnapshot, key interface{}) (interface{}, bool, uint64, error) {
	canonicalKey, ok, err := normalizeSQLDictionaryKey(key)
	if err != nil {
		return nil, false, snapshot.version, err
	}
	if !ok {
		return nil, false, snapshot.version, nil
	}
	value, found := snapshot.values[canonicalKey]
	if !found {
		return nil, false, snapshot.version, nil
	}
	return cloneSQLDictionaryResult(value), true, snapshot.version, nil
}

func normalizeSQLDictionaryName(name string) (string, error) {
	canonical := strings.ToLower(strings.TrimSpace(name))
	if canonical == "" {
		return "", fmt.Errorf("SQL dictionary name must not be empty")
	}
	return canonical, nil
}

func normalizeSQLDictionaryKey(value interface{}) (string, bool, error) {
	if value == nil {
		return "", false, nil
	}
	var key string
	switch typed := value.(type) {
	case string:
		key = typed
	case []byte:
		key = string(typed)
	case bool:
		key = strconv.FormatBool(typed)
	case int:
		key = strconv.FormatInt(int64(typed), 10)
	case int8:
		key = strconv.FormatInt(int64(typed), 10)
	case int16:
		key = strconv.FormatInt(int64(typed), 10)
	case int32:
		key = strconv.FormatInt(int64(typed), 10)
	case int64:
		key = strconv.FormatInt(typed, 10)
	case uint:
		key = strconv.FormatUint(uint64(typed), 10)
	case uint8:
		key = strconv.FormatUint(uint64(typed), 10)
	case uint16:
		key = strconv.FormatUint(uint64(typed), 10)
	case uint32:
		key = strconv.FormatUint(uint64(typed), 10)
	case uint64:
		key = strconv.FormatUint(typed, 10)
	case float32:
		if math.IsNaN(float64(typed)) || math.IsInf(float64(typed), 0) {
			return "", false, fmt.Errorf("dictionary key must be finite")
		}
		key = strconv.FormatFloat(float64(typed), 'g', -1, 32)
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) {
			return "", false, fmt.Errorf("dictionary key must be finite")
		}
		key = strconv.FormatFloat(typed, 'g', -1, 64)
	case time.Time:
		key = typed.UTC().Format(time.RFC3339Nano)
	default:
		return "", false, fmt.Errorf("unsupported dictionary key type %T", value)
	}
	if len(key) > maxSQLDictionaryKeySize {
		return "", false, fmt.Errorf("dictionary key exceeds %d bytes", maxSQLDictionaryKeySize)
	}
	return key, true, nil
}

func cloneSQLDictionaryValues(values map[string]interface{}) (map[string]interface{}, error) {
	if len(values) > maxSQLDictionaryEntries {
		return nil, fmt.Errorf("dictionary exceeds %d entries", maxSQLDictionaryEntries)
	}
	cloned := make(map[string]interface{}, len(values))
	for key, value := range values {
		canonicalKey, ok, err := normalizeSQLDictionaryKey(key)
		if err != nil || !ok {
			if err == nil {
				err = fmt.Errorf("dictionary key must not be null")
			}
			return nil, err
		}
		clonedValue, err := cloneSQLDictionaryValue(value)
		if err != nil {
			return nil, fmt.Errorf("unsupported dictionary value: %w", err)
		}
		cloned[canonicalKey] = clonedValue
	}
	return cloned, nil
}

func cloneSQLDictionaryValue(value interface{}) (interface{}, error) {
	switch typed := value.(type) {
	case nil, string, bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64, time.Time:
		return value, nil
	case []byte:
		return append([]byte(nil), typed...), nil
	default:
		return nil, fmt.Errorf("unsupported dictionary value type %T", value)
	}
}

func cloneSQLDictionaryResult(value interface{}) interface{} {
	if bytes, ok := value.([]byte); ok {
		return append([]byte(nil), bytes...)
	}
	return value
}
