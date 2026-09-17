package hatSql

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	json "github.com/goccy/go-json"
)

const (
	defaultSQLResultCachePersistenceMaxBytes  int64 = 64 << 20
	SQLResultCachePersistenceMaxBytes         int64 = defaultSQLResultCachePersistenceMaxBytes
	resultCachePersistenceVersion                   = 1
	resultCachePersistenceDependenciesVersion       = 2
	resultCachePersistenceHeaderSize                = 4 + 1 + 8 + sha256.Size
	maxResultCachePersistenceEntries                = 1 << 20
	maxResultCachePersistenceDependencies           = 1 << 16
	maxResultCachePersistenceRows                   = 1 << 20
	maxResultCachePersistenceFields                 = 1 << 16
	maxResultCachePersistenceStringBytes            = 1 << 20
	maxResultCachePersistenceScalarBytes            = 16 << 20
	maxResultCachePersistenceValueDepth             = 64
)

var resultCachePersistenceMagic = [4]byte{'H', 'S', 'C', '1'}

var (
	// ErrSQLResultCachePersistenceCorrupt identifies a cache file that cannot
	// be trusted because its envelope, checksum, or payload is invalid.
	ErrSQLResultCachePersistenceCorrupt = errors.New("hatSql: SQL result cache persistence is corrupt")
	// ErrSQLResultCachePersistenceTooLarge identifies a file or snapshot that
	// exceeds the configured persistence quota.
	ErrSQLResultCachePersistenceTooLarge = errors.New("hatSql: SQL result cache persistence is too large")
)

// SQLResultCachePersistenceOptions bounds one persistence operation. MaxBytes
// includes the file envelope. Zero selects the conservative 64 MiB default.
type SQLResultCachePersistenceOptions struct {
	MaxBytes int64
}

func (options SQLResultCachePersistenceOptions) maxBytes() (int64, error) {
	maxBytes := options.MaxBytes
	if maxBytes == 0 {
		maxBytes = defaultSQLResultCachePersistenceMaxBytes
	}
	if maxBytes < resultCachePersistenceHeaderSize {
		return 0, fmt.Errorf("%w: max bytes %d is smaller than the %d-byte envelope", ErrSQLResultCachePersistenceTooLarge, maxBytes, resultCachePersistenceHeaderSize)
	}
	return maxBytes, nil
}

// Persist writes the versioned SQL entries in cache to path. Only entries
// retained by ExecuteVersioned are persisted; unversioned generic entries are
// intentionally excluded because their numeric epochs are process-local.
// The file is checksummed, mode 0600, and atomically replaced on success.
func (cache *ResultCache) Persist(path string) error {
	return cache.PersistWithOptions(path, SQLResultCachePersistenceOptions{})
}

// PersistWithOptions is Persist with an explicit file-size quota.
func (cache *ResultCache) PersistWithOptions(path string, options SQLResultCachePersistenceOptions) error {
	if cache == nil {
		return errors.New("hatSql: SQL result cache is nil")
	}
	if path == "" {
		return errors.New("hatSql: SQL result cache persistence path is empty")
	}
	maxBytes, err := options.maxBytes()
	if err != nil {
		return err
	}

	entries, err := cache.snapshotPersistentEntries()
	if err != nil {
		return fmt.Errorf("hatSql: encode SQL result cache persistence: %w", err)
	}
	version := byte(resultCachePersistenceVersion)
	if resultCachePersistenceHasDependencies(entries) {
		version = resultCachePersistenceDependenciesVersion
	}
	payload, err := encodeResultCachePersistenceSnapshotVersion(entries, version)
	if err != nil {
		return fmt.Errorf("hatSql: encode SQL result cache persistence: %w", err)
	}
	fileSize := int64(resultCachePersistenceHeaderSize) + int64(len(payload))
	if fileSize > maxBytes {
		return fmt.Errorf("%w: snapshot is %d bytes, limit is %d", ErrSQLResultCachePersistenceTooLarge, fileSize, maxBytes)
	}

	return writeResultCachePersistenceFileVersion(path, payload, version)
}

// Restore loads versioned SQL entries from path. A missing file is treated as
// a cold start. Corrupt or incompatible data returns an error and leaves the
// in-memory cache unchanged.
func (cache *ResultCache) Restore(path string) error {
	return cache.RestoreWithOptions(path, SQLResultCachePersistenceOptions{})
}

// RestoreWithOptions is Restore with an explicit file-size quota.
func (cache *ResultCache) RestoreWithOptions(path string, options SQLResultCachePersistenceOptions) error {
	if cache == nil {
		return errors.New("hatSql: SQL result cache is nil")
	}
	if path == "" {
		return errors.New("hatSql: SQL result cache persistence path is empty")
	}
	maxBytes, err := options.maxBytes()
	if err != nil {
		return err
	}

	data, err := readResultCachePersistenceFile(path, maxBytes)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	loaded, err := decodeResultCachePersistence(data)
	if err != nil {
		return err
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()
	// Persistence is SQL-specific. Keep any generic epoch entries already
	// owned by this ResultCache while replacing old typed entries.
	entries := make(map[string]resultCacheEntry, len(cache.entries)+len(loaded))
	order := make([]string, 0, len(cache.order)+len(loaded))
	for _, key := range cache.order {
		entry, ok := cache.entries[key]
		if !ok || entry.typed {
			continue
		}
		if _, exists := entries[key]; exists {
			continue
		}
		entries[key] = entry
		order = append(order, key)
	}
	for key, entry := range cache.entries {
		if entry.typed {
			continue
		}
		if _, exists := entries[key]; exists {
			continue
		}
		entries[key] = entry
		order = append(order, key)
	}
	cache.entries = entries
	cache.order = order
	for _, entry := range loaded {
		if cache.capacity <= 0 {
			break
		}
		cache.entries[entry.key] = entry.entry
		cache.order = append(cache.order, entry.key)
		for len(cache.order) > cache.capacity {
			oldest := cache.order[0]
			cache.order = cache.order[1:]
			delete(cache.entries, oldest)
		}
	}
	cache.rebuildDependencyIndexLocked()
	return nil
}

type resultCachePersistenceEntry struct {
	Key          string
	Version      string
	Dependencies []resultCachePersistenceDependency
	Result       resultCachePersistenceResult
}

type resultCachePersistenceDependency struct {
	Kind string
	Key  string
}

type resultCachePersistenceLoadedEntry struct {
	key   string
	entry resultCacheEntry
}

type resultCachePersistenceResult struct {
	QueryID         string
	Columns         []string
	ColumnsPresent  bool
	Rows            []resultCachePersistenceRow
	RowsPresent     bool
	Plan            []byte
	PlanPresent     bool
	PlanSnapshot    []byte
	SnapshotPresent bool
	Stats           []byte
	StatsPresent    bool
	HasMore         bool
	NextCursor      string
}

type resultCachePersistenceRow struct {
	Nil    bool
	Fields []resultCachePersistenceField
}

type resultCachePersistenceField struct {
	Key   string
	Value resultCachePersistenceValue
}

const (
	resultCachePersistenceValueScalar byte = iota + 1
	resultCachePersistenceValueArray
	resultCachePersistenceValueObject
	resultCachePersistenceValueRow
)

type resultCachePersistenceValue struct {
	Kind   byte
	Nil    bool
	Scalar []byte
	Array  []resultCachePersistenceValue
	Fields []resultCachePersistenceField
}

const (
	resultCachePersistenceResultColumnsPresent byte = 1 << iota
	resultCachePersistenceResultRowsPresent
	resultCachePersistenceResultPlanPresent
	resultCachePersistenceResultSnapshotPresent
	resultCachePersistenceResultStatsPresent
	resultCachePersistenceResultHasMore
)

func encodeResultCachePersistenceSnapshot(entries []resultCachePersistenceEntry) ([]byte, error) {
	return encodeResultCachePersistenceSnapshotVersion(entries, resultCachePersistenceVersion)
}

func encodeResultCachePersistenceSnapshotVersion(entries []resultCachePersistenceEntry, version byte) ([]byte, error) {
	if version != resultCachePersistenceVersion && version != resultCachePersistenceDependenciesVersion {
		return nil, fmt.Errorf("unsupported persistence version %d", version)
	}
	payload := make([]byte, 0, 128)
	payload = appendResultCachePersistenceUvarint(payload, uint64(len(entries)))
	for index, entry := range entries {
		if len(entry.Key) == 0 || len(entry.Key) > maxResultCachePersistenceStringBytes {
			return nil, fmt.Errorf("entry %d has invalid key", index)
		}
		if len(entry.Version) == 0 || len(entry.Version) > maxResultCachePersistenceStringBytes {
			return nil, fmt.Errorf("entry %d has invalid source version", index)
		}
		payload = appendResultCachePersistenceString(payload, entry.Key)
		payload = appendResultCachePersistenceString(payload, entry.Version)
		if version >= resultCachePersistenceDependenciesVersion {
			if len(entry.Dependencies) > maxResultCachePersistenceDependencies {
				return nil, fmt.Errorf("entry %d has too many dependencies", index)
			}
			payload = appendResultCachePersistenceUvarint(payload, uint64(len(entry.Dependencies)))
			for dependencyIndex, dependency := range entry.Dependencies {
				if dependency.Kind == "" || dependency.Key == "" || len(dependency.Kind) > maxResultCachePersistenceStringBytes || len(dependency.Key) > maxResultCachePersistenceStringBytes {
					return nil, fmt.Errorf("entry %d dependency %d is invalid", index, dependencyIndex)
				}
				payload = appendResultCachePersistenceString(payload, dependency.Kind)
				payload = appendResultCachePersistenceString(payload, dependency.Key)
			}
		} else if len(entry.Dependencies) != 0 {
			return nil, fmt.Errorf("entry %d dependencies require persistence version %d", index, resultCachePersistenceDependenciesVersion)
		}
		var err error
		payload, err = appendResultCachePersistenceResult(payload, entry.Result)
		if err != nil {
			return nil, fmt.Errorf("entry %d: %w", index, err)
		}
	}
	return payload, nil
}

func appendResultCachePersistenceResult(payload []byte, result resultCachePersistenceResult) ([]byte, error) {
	if len(result.QueryID) > maxResultCachePersistenceStringBytes || len(result.NextCursor) > maxResultCachePersistenceStringBytes {
		return nil, fmt.Errorf("result metadata string exceeds %d bytes", maxResultCachePersistenceStringBytes)
	}
	if len(result.Columns) > maxResultCachePersistenceFields || len(result.Rows) > maxResultCachePersistenceRows {
		return nil, fmt.Errorf("result columns or rows exceed persistence limits")
	}
	if len(result.Plan) > maxResultCachePersistenceStringBytes || len(result.PlanSnapshot) > maxResultCachePersistenceStringBytes || len(result.Stats) > maxResultCachePersistenceStringBytes {
		return nil, fmt.Errorf("result metadata exceeds %d bytes", maxResultCachePersistenceStringBytes)
	}
	var flags byte
	if result.ColumnsPresent {
		flags |= resultCachePersistenceResultColumnsPresent
	}
	if result.RowsPresent {
		flags |= resultCachePersistenceResultRowsPresent
	}
	if result.PlanPresent {
		flags |= resultCachePersistenceResultPlanPresent
	}
	if result.SnapshotPresent {
		flags |= resultCachePersistenceResultSnapshotPresent
	}
	if result.StatsPresent {
		flags |= resultCachePersistenceResultStatsPresent
	}
	if result.HasMore {
		flags |= resultCachePersistenceResultHasMore
	}
	payload = appendResultCachePersistenceString(payload, result.QueryID)
	payload = append(payload, flags)
	if result.ColumnsPresent {
		payload = appendResultCachePersistenceUvarint(payload, uint64(len(result.Columns)))
		for _, column := range result.Columns {
			if len(column) > maxResultCachePersistenceStringBytes {
				return nil, fmt.Errorf("column name exceeds %d bytes", maxResultCachePersistenceStringBytes)
			}
			payload = appendResultCachePersistenceString(payload, column)
		}
	}
	if result.RowsPresent {
		payload = appendResultCachePersistenceUvarint(payload, uint64(len(result.Rows)))
		for index, row := range result.Rows {
			var err error
			payload, err = appendResultCachePersistenceRow(payload, row)
			if err != nil {
				return nil, fmt.Errorf("row %d: %w", index, err)
			}
		}
	}
	if result.PlanPresent {
		payload = appendResultCachePersistenceLengthBytes(payload, result.Plan)
	}
	if result.SnapshotPresent {
		payload = appendResultCachePersistenceLengthBytes(payload, result.PlanSnapshot)
	}
	if result.StatsPresent {
		payload = appendResultCachePersistenceLengthBytes(payload, result.Stats)
	}
	payload = appendResultCachePersistenceString(payload, result.NextCursor)
	return payload, nil
}

func appendResultCachePersistenceRow(payload []byte, row resultCachePersistenceRow) ([]byte, error) {
	if row.Nil {
		return append(payload, 1), nil
	}
	if len(row.Fields) > maxResultCachePersistenceFields {
		return nil, fmt.Errorf("field count %d exceeds limit %d", len(row.Fields), maxResultCachePersistenceFields)
	}
	payload = append(payload, 0)
	payload = appendResultCachePersistenceUvarint(payload, uint64(len(row.Fields)))
	for _, field := range row.Fields {
		if len(field.Key) > maxResultCachePersistenceStringBytes {
			return nil, fmt.Errorf("field name exceeds %d bytes", maxResultCachePersistenceStringBytes)
		}
		payload = appendResultCachePersistenceString(payload, field.Key)
		var err error
		payload, err = appendResultCachePersistenceValue(payload, field.Value, 0)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", field.Key, err)
		}
	}
	return payload, nil
}

func appendResultCachePersistenceValue(payload []byte, value resultCachePersistenceValue, depth int) ([]byte, error) {
	if depth > maxResultCachePersistenceValueDepth {
		return nil, fmt.Errorf("value nesting exceeds %d levels", maxResultCachePersistenceValueDepth)
	}
	switch value.Kind {
	case resultCachePersistenceValueScalar:
		if len(value.Scalar) > maxResultCachePersistenceScalarBytes {
			return nil, fmt.Errorf("scalar exceeds %d bytes", maxResultCachePersistenceScalarBytes)
		}
	case resultCachePersistenceValueArray:
		if len(value.Array) > maxResultCachePersistenceFields {
			return nil, fmt.Errorf("array length %d exceeds limit %d", len(value.Array), maxResultCachePersistenceFields)
		}
	case resultCachePersistenceValueObject, resultCachePersistenceValueRow:
		if len(value.Fields) > maxResultCachePersistenceFields {
			return nil, fmt.Errorf("object field count %d exceeds limit %d", len(value.Fields), maxResultCachePersistenceFields)
		}
	default:
		return nil, fmt.Errorf("unknown value kind %d", value.Kind)
	}
	flags := byte(0)
	if value.Nil {
		flags = 1
	}
	payload = append(payload, value.Kind, flags)
	switch value.Kind {
	case resultCachePersistenceValueScalar:
		payload = appendResultCachePersistenceLengthBytes(payload, value.Scalar)
	case resultCachePersistenceValueArray:
		payload = appendResultCachePersistenceUvarint(payload, uint64(len(value.Array)))
		for index, child := range value.Array {
			var err error
			payload, err = appendResultCachePersistenceValue(payload, child, depth+1)
			if err != nil {
				return nil, fmt.Errorf("array item %d: %w", index, err)
			}
		}
	case resultCachePersistenceValueObject, resultCachePersistenceValueRow:
		payload = appendResultCachePersistenceUvarint(payload, uint64(len(value.Fields)))
		for _, field := range value.Fields {
			if len(field.Key) > maxResultCachePersistenceStringBytes {
				return nil, fmt.Errorf("field name exceeds %d bytes", maxResultCachePersistenceStringBytes)
			}
			payload = appendResultCachePersistenceString(payload, field.Key)
			var err error
			payload, err = appendResultCachePersistenceValue(payload, field.Value, depth+1)
			if err != nil {
				return nil, fmt.Errorf("field %q: %w", field.Key, err)
			}
		}
	}
	return payload, nil
}

func appendResultCachePersistenceString(payload []byte, value string) []byte {
	return appendResultCachePersistenceLengthBytes(payload, []byte(value))
}

func appendResultCachePersistenceLengthBytes(payload, value []byte) []byte {
	payload = appendResultCachePersistenceUvarint(payload, uint64(len(value)))
	return append(payload, value...)
}

func appendResultCachePersistenceUvarint(payload []byte, value uint64) []byte {
	var buffer [10]byte
	n := binary.PutUvarint(buffer[:], value)
	return append(payload, buffer[:n]...)
}

func (cache *ResultCache) snapshotPersistentEntries() ([]resultCachePersistenceEntry, error) {
	cache.mu.Lock()
	live := make([]resultCachePersistenceLiveEntry, 0, len(cache.entries))
	seen := make(map[string]struct{}, len(cache.entries))
	for _, key := range cache.order {
		entry, ok := cache.entries[key]
		if !ok || !entry.typed || entry.version == "" {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		live = append(live, resultCachePersistenceLiveEntry{key: key, version: entry.version, dependencies: resultCachePersistenceDependencies(entry.dependencies), result: cloneResultCacheResult(entry.result)})
	}
	remaining := make([]string, 0)
	for key, entry := range cache.entries {
		if !entry.typed || entry.version == "" {
			continue
		}
		if _, exists := seen[key]; !exists {
			remaining = append(remaining, key)
		}
	}
	sort.Strings(remaining)
	for _, key := range remaining {
		entry := cache.entries[key]
		live = append(live, resultCachePersistenceLiveEntry{key: key, version: entry.version, dependencies: resultCachePersistenceDependencies(entry.dependencies), result: cloneResultCacheResult(entry.result)})
	}
	cache.mu.Unlock()

	if len(live) > maxResultCachePersistenceEntries {
		return nil, fmt.Errorf("%w: %d entries exceed limit %d", ErrSQLResultCachePersistenceTooLarge, len(live), maxResultCachePersistenceEntries)
	}
	ordered := make([]resultCachePersistenceEntry, len(live))
	for index, item := range live {
		result, err := encodeResultCachePersistenceResult(item.result)
		if err != nil {
			return nil, err
		}
		ordered[index] = resultCachePersistenceEntry{Key: item.key, Version: item.version, Dependencies: item.dependencies, Result: result}
	}
	return ordered, nil
}

type resultCachePersistenceLiveEntry struct {
	key          string
	version      string
	dependencies []resultCachePersistenceDependency
	result       QueryResult
}

func resultCachePersistenceHasDependencies(entries []resultCachePersistenceEntry) bool {
	for _, entry := range entries {
		if len(entry.Dependencies) != 0 {
			return true
		}
	}
	return false
}

func resultCachePersistenceDependencies(dependencies []resultCacheDependencyKey) []resultCachePersistenceDependency {
	if len(dependencies) == 0 {
		return nil
	}
	result := make([]resultCachePersistenceDependency, len(dependencies))
	for index, dependency := range dependencies {
		result[index] = resultCachePersistenceDependency{Kind: dependency.kind, Key: dependency.key}
	}
	return result
}

func encodeResultCachePersistenceResult(result QueryResult) (resultCachePersistenceResult, error) {
	encoded := resultCachePersistenceResult{
		QueryID:        result.QueryID,
		Columns:        append([]string(nil), result.Columns...),
		ColumnsPresent: result.Columns != nil,
		RowsPresent:    result.Rows != nil,
		HasMore:        result.HasMore,
		NextCursor:     result.NextCursor,
	}
	if result.Rows != nil {
		encoded.Rows = make([]resultCachePersistenceRow, len(result.Rows))
		for index, row := range result.Rows {
			persisted, err := encodeResultCachePersistenceRow(row)
			if err != nil {
				return resultCachePersistenceResult{}, fmt.Errorf("row %d: %w", index, err)
			}
			encoded.Rows[index] = persisted
		}
	}
	var err error
	if result.Plan != nil {
		encoded.Plan, err = json.Marshal(result.Plan)
		if err != nil {
			return resultCachePersistenceResult{}, fmt.Errorf("plan: %w", err)
		}
		encoded.PlanPresent = true
	}
	if result.PlanSnapshot != nil {
		encoded.PlanSnapshot, err = json.Marshal(result.PlanSnapshot)
		if err != nil {
			return resultCachePersistenceResult{}, fmt.Errorf("plan snapshot: %w", err)
		}
		encoded.SnapshotPresent = true
	}
	if result.Stats != nil {
		encoded.Stats, err = json.Marshal(result.Stats)
		if err != nil {
			return resultCachePersistenceResult{}, fmt.Errorf("stats: %w", err)
		}
		encoded.StatsPresent = true
	}
	return encoded, nil
}

func encodeResultCachePersistenceRow(row Row) (resultCachePersistenceRow, error) {
	if row == nil {
		return resultCachePersistenceRow{Nil: true}, nil
	}
	keys := make([]string, 0, len(row))
	for key := range row {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	fields := make([]resultCachePersistenceField, 0, len(keys))
	for _, key := range keys {
		value, err := encodeResultCachePersistenceValue(row[key], 0)
		if err != nil {
			return resultCachePersistenceRow{}, fmt.Errorf("field %q: %w", key, err)
		}
		fields = append(fields, resultCachePersistenceField{Key: key, Value: value})
	}
	return resultCachePersistenceRow{Fields: fields}, nil
}

func encodeResultCachePersistenceValue(value interface{}, depth int) (resultCachePersistenceValue, error) {
	if depth > maxResultCachePersistenceValueDepth {
		return resultCachePersistenceValue{}, fmt.Errorf("value nesting exceeds %d levels", maxResultCachePersistenceValueDepth)
	}
	switch value := value.(type) {
	case Row:
		if value == nil {
			return resultCachePersistenceValue{Kind: resultCachePersistenceValueRow, Nil: true}, nil
		}
		fields, err := encodeResultCachePersistenceFields(map[string]interface{}(value), depth+1)
		if err != nil {
			return resultCachePersistenceValue{}, err
		}
		return resultCachePersistenceValue{Kind: resultCachePersistenceValueRow, Fields: fields}, nil
	case map[string]interface{}:
		if value == nil {
			return resultCachePersistenceValue{Kind: resultCachePersistenceValueObject, Nil: true}, nil
		}
		fields, err := encodeResultCachePersistenceFields(value, depth+1)
		if err != nil {
			return resultCachePersistenceValue{}, err
		}
		return resultCachePersistenceValue{Kind: resultCachePersistenceValueObject, Fields: fields}, nil
	case []interface{}:
		if value == nil {
			return resultCachePersistenceValue{Kind: resultCachePersistenceValueArray, Nil: true}, nil
		}
		children := make([]resultCachePersistenceValue, len(value))
		for index, child := range value {
			encoded, err := encodeResultCachePersistenceValue(child, depth+1)
			if err != nil {
				return resultCachePersistenceValue{}, fmt.Errorf("array item %d: %w", index, err)
			}
			children[index] = encoded
		}
		return resultCachePersistenceValue{Kind: resultCachePersistenceValueArray, Array: children}, nil
	default:
		scalar, err := encodeResultCachePersistenceScalar(value)
		if err != nil {
			return resultCachePersistenceValue{}, err
		}
		return resultCachePersistenceValue{Kind: resultCachePersistenceValueScalar, Scalar: scalar}, nil
	}
}

func encodeResultCachePersistenceFields(values map[string]interface{}, depth int) ([]resultCachePersistenceField, error) {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	fields := make([]resultCachePersistenceField, 0, len(keys))
	for _, key := range keys {
		value, err := encodeResultCachePersistenceValue(values[key], depth)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", key, err)
		}
		fields = append(fields, resultCachePersistenceField{Key: key, Value: value})
	}
	return fields, nil
}

const (
	resultCachePersistenceScalarNil byte = iota
	resultCachePersistenceScalarString
	resultCachePersistenceScalarBytes
	resultCachePersistenceScalarFalse
	resultCachePersistenceScalarTrue
	resultCachePersistenceScalarTime
	resultCachePersistenceScalarInt
	resultCachePersistenceScalarInt8
	resultCachePersistenceScalarInt16
	resultCachePersistenceScalarInt32
	resultCachePersistenceScalarInt64
	resultCachePersistenceScalarUint
	resultCachePersistenceScalarUint8
	resultCachePersistenceScalarUint16
	resultCachePersistenceScalarUint32
	resultCachePersistenceScalarUint64
	resultCachePersistenceScalarFloat32
	resultCachePersistenceScalarFloat64
	resultCachePersistenceScalarDate
	resultCachePersistenceScalarDecimal
	resultCachePersistenceScalarUUID
	resultCachePersistenceScalarDuration
	resultCachePersistenceScalarIPv4
	resultCachePersistenceScalarIPv6
)

func encodeResultCachePersistenceScalar(value interface{}) ([]byte, error) {
	encoded := make([]byte, 0, 16)
	switch value := value.(type) {
	case nil:
		return []byte{resultCachePersistenceScalarNil}, nil
	case string:
		return appendResultCachePersistenceBytes(encoded, resultCachePersistenceScalarString, []byte(value))
	case []byte:
		return appendResultCachePersistenceBytes(encoded, resultCachePersistenceScalarBytes, value)
	case bool:
		if value {
			return []byte{resultCachePersistenceScalarTrue}, nil
		}
		return []byte{resultCachePersistenceScalarFalse}, nil
	case time.Time:
		data, err := value.MarshalBinary()
		if err != nil {
			return nil, err
		}
		return appendResultCachePersistenceBytes(encoded, resultCachePersistenceScalarTime, data)
	case int:
		return appendResultCachePersistenceInt(encoded, resultCachePersistenceScalarInt, int64(value)), nil
	case int8:
		return appendResultCachePersistenceInt(encoded, resultCachePersistenceScalarInt8, int64(value)), nil
	case int16:
		return appendResultCachePersistenceInt(encoded, resultCachePersistenceScalarInt16, int64(value)), nil
	case int32:
		return appendResultCachePersistenceInt(encoded, resultCachePersistenceScalarInt32, int64(value)), nil
	case int64:
		return appendResultCachePersistenceInt(encoded, resultCachePersistenceScalarInt64, value), nil
	case uint:
		return appendResultCachePersistenceUint(encoded, resultCachePersistenceScalarUint, uint64(value)), nil
	case uint8:
		return appendResultCachePersistenceUint(encoded, resultCachePersistenceScalarUint8, uint64(value)), nil
	case uint16:
		return appendResultCachePersistenceUint(encoded, resultCachePersistenceScalarUint16, uint64(value)), nil
	case uint32:
		return appendResultCachePersistenceUint(encoded, resultCachePersistenceScalarUint32, uint64(value)), nil
	case uint64:
		return appendResultCachePersistenceUint(encoded, resultCachePersistenceScalarUint64, value), nil
	case float32:
		var bits [4]byte
		binary.LittleEndian.PutUint32(bits[:], math.Float32bits(value))
		encoded = append(encoded, resultCachePersistenceScalarFloat32)
		return append(encoded, bits[:]...), nil
	case float64:
		var bits [8]byte
		binary.LittleEndian.PutUint64(bits[:], math.Float64bits(value))
		encoded = append(encoded, resultCachePersistenceScalarFloat64)
		return append(encoded, bits[:]...), nil
	case sqlDate:
		return appendResultCachePersistenceBytes(encoded, resultCachePersistenceScalarDate, []byte(value))
	case sqlDecimal:
		return appendResultCachePersistenceBytes(encoded, resultCachePersistenceScalarDecimal, []byte(value))
	case sqlUUID:
		return appendResultCachePersistenceBytes(encoded, resultCachePersistenceScalarUUID, []byte(value))
	case sqlDuration:
		return appendResultCachePersistenceBytes(encoded, resultCachePersistenceScalarDuration, []byte(value))
	case SQLIPv4:
		var bytes [4]byte
		binary.BigEndian.PutUint32(bytes[:], uint32(value))
		encoded = append(encoded, resultCachePersistenceScalarIPv4)
		return append(encoded, bytes[:]...), nil
	case SQLIPv6:
		return append(append(encoded, resultCachePersistenceScalarIPv6), value[:]...), nil
	default:
		return nil, fmt.Errorf("unsupported value type %T", value)
	}
}

func appendResultCachePersistenceBytes(encoded []byte, tag byte, value []byte) ([]byte, error) {
	if len(value) > maxResultCachePersistenceScalarBytes {
		return nil, fmt.Errorf("value is %d bytes, limit is %d", len(value), maxResultCachePersistenceScalarBytes)
	}
	encoded = append(encoded, tag)
	var length [10]byte
	n := binary.PutUvarint(length[:], uint64(len(value)))
	encoded = append(encoded, length[:n]...)
	return append(encoded, value...), nil
}

func appendResultCachePersistenceInt(encoded []byte, tag byte, value int64) []byte {
	return appendResultCachePersistenceUint(encoded, tag, uint64(value<<1)^uint64(value>>63))
}

func appendResultCachePersistenceUint(encoded []byte, tag byte, value uint64) []byte {
	encoded = append(encoded, tag)
	var buffer [10]byte
	n := binary.PutUvarint(buffer[:], value)
	return append(encoded, buffer[:n]...)
}

func decodeResultCachePersistenceScalar(encoded []byte) (interface{}, error) {
	if len(encoded) == 0 {
		return nil, fmt.Errorf("scalar is empty")
	}
	tag := encoded[0]
	offset := 1
	switch tag {
	case resultCachePersistenceScalarNil:
		if offset != len(encoded) {
			return nil, fmt.Errorf("nil scalar has trailing bytes")
		}
		return nil, nil
	case resultCachePersistenceScalarString:
		value, err := readResultCachePersistenceBytes(encoded, &offset)
		return string(value), err
	case resultCachePersistenceScalarBytes:
		value, err := readResultCachePersistenceBytes(encoded, &offset)
		return append([]byte(nil), value...), err
	case resultCachePersistenceScalarFalse:
		if offset != len(encoded) {
			return nil, fmt.Errorf("false scalar has trailing bytes")
		}
		return false, nil
	case resultCachePersistenceScalarTrue:
		if offset != len(encoded) {
			return nil, fmt.Errorf("true scalar has trailing bytes")
		}
		return true, nil
	case resultCachePersistenceScalarTime:
		value, err := readResultCachePersistenceBytes(encoded, &offset)
		if err != nil {
			return nil, err
		}
		var result time.Time
		if err := result.UnmarshalBinary(value); err != nil {
			return nil, fmt.Errorf("time scalar: %w", err)
		}
		return result, nil
	case resultCachePersistenceScalarInt, resultCachePersistenceScalarInt8, resultCachePersistenceScalarInt16, resultCachePersistenceScalarInt32, resultCachePersistenceScalarInt64:
		value, err := readResultCachePersistenceUint(encoded, &offset)
		if err != nil {
			return nil, err
		}
		if offset != len(encoded) {
			return nil, fmt.Errorf("signed scalar has trailing bytes")
		}
		decoded := int64(value>>1) ^ -int64(value&1)
		switch tag {
		case resultCachePersistenceScalarInt:
			if strconv.IntSize == 32 && (decoded < -1<<31 || decoded > 1<<31-1) {
				return nil, fmt.Errorf("int scalar overflows int32")
			}
			return int(decoded), nil
		case resultCachePersistenceScalarInt8:
			if decoded < -1<<7 || decoded > 1<<7-1 {
				return nil, fmt.Errorf("int8 scalar overflows int8")
			}
			return int8(decoded), nil
		case resultCachePersistenceScalarInt16:
			if decoded < -1<<15 || decoded > 1<<15-1 {
				return nil, fmt.Errorf("int16 scalar overflows int16")
			}
			return int16(decoded), nil
		case resultCachePersistenceScalarInt32:
			if decoded < -1<<31 || decoded > 1<<31-1 {
				return nil, fmt.Errorf("int32 scalar overflows int32")
			}
			return int32(decoded), nil
		default:
			return decoded, nil
		}
	case resultCachePersistenceScalarUint, resultCachePersistenceScalarUint8, resultCachePersistenceScalarUint16, resultCachePersistenceScalarUint32, resultCachePersistenceScalarUint64:
		value, err := readResultCachePersistenceUint(encoded, &offset)
		if err != nil {
			return nil, err
		}
		if offset != len(encoded) {
			return nil, fmt.Errorf("unsigned scalar has trailing bytes")
		}
		switch tag {
		case resultCachePersistenceScalarUint:
			if strconv.IntSize == 32 && value > 1<<32-1 {
				return nil, fmt.Errorf("uint scalar overflows uint32")
			}
			return uint(value), nil
		case resultCachePersistenceScalarUint8:
			if value > 1<<8-1 {
				return nil, fmt.Errorf("uint8 scalar overflows uint8")
			}
			return uint8(value), nil
		case resultCachePersistenceScalarUint16:
			if value > 1<<16-1 {
				return nil, fmt.Errorf("uint16 scalar overflows uint16")
			}
			return uint16(value), nil
		case resultCachePersistenceScalarUint32:
			if value > 1<<32-1 {
				return nil, fmt.Errorf("uint32 scalar overflows uint32")
			}
			return uint32(value), nil
		default:
			return value, nil
		}
	case resultCachePersistenceScalarFloat32:
		if len(encoded)-offset != 4 {
			return nil, fmt.Errorf("float32 scalar has invalid length")
		}
		return math.Float32frombits(binary.LittleEndian.Uint32(encoded[offset:])), nil
	case resultCachePersistenceScalarFloat64:
		if len(encoded)-offset != 8 {
			return nil, fmt.Errorf("float64 scalar has invalid length")
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(encoded[offset:])), nil
	case resultCachePersistenceScalarDate:
		value, err := readResultCachePersistenceBytes(encoded, &offset)
		return sqlDate(string(value)), err
	case resultCachePersistenceScalarDecimal:
		value, err := readResultCachePersistenceBytes(encoded, &offset)
		return sqlDecimal(string(value)), err
	case resultCachePersistenceScalarUUID:
		value, err := readResultCachePersistenceBytes(encoded, &offset)
		return sqlUUID(string(value)), err
	case resultCachePersistenceScalarDuration:
		value, err := readResultCachePersistenceBytes(encoded, &offset)
		return sqlDuration(string(value)), err
	case resultCachePersistenceScalarIPv4:
		if len(encoded)-offset != 4 {
			return nil, fmt.Errorf("IPv4 scalar has invalid length")
		}
		return SQLIPv4(binary.BigEndian.Uint32(encoded[offset:])), nil
	case resultCachePersistenceScalarIPv6:
		if len(encoded)-offset != 16 {
			return nil, fmt.Errorf("IPv6 scalar has invalid length")
		}
		var value SQLIPv6
		copy(value[:], encoded[offset:])
		return value, nil
	default:
		return nil, fmt.Errorf("unknown scalar tag %d", tag)
	}
}

func readResultCachePersistenceBytes(encoded []byte, offset *int) ([]byte, error) {
	length, err := readResultCachePersistenceUint(encoded, offset)
	if err != nil {
		return nil, err
	}
	if length > maxResultCachePersistenceScalarBytes {
		return nil, fmt.Errorf("scalar length %d exceeds limit %d", length, maxResultCachePersistenceScalarBytes)
	}
	if length > uint64(len(encoded)-*offset) {
		return nil, fmt.Errorf("scalar bytes are truncated")
	}
	start := *offset
	*offset += int(length)
	if *offset != len(encoded) {
		return nil, fmt.Errorf("scalar has trailing bytes")
	}
	return encoded[start:*offset], nil
}

func readResultCachePersistenceUint(encoded []byte, offset *int) (uint64, error) {
	if *offset >= len(encoded) {
		return 0, fmt.Errorf("scalar varint is truncated")
	}
	value, size := binary.Uvarint(encoded[*offset:])
	if size <= 0 {
		return 0, fmt.Errorf("scalar varint is invalid")
	}
	*offset += size
	return value, nil
}

func writeResultCachePersistenceFile(path string, payload []byte) error {
	return writeResultCachePersistenceFileVersion(path, payload, resultCachePersistenceVersion)
}

func writeResultCachePersistenceFileVersion(path string, payload []byte, version byte) error {
	if version != resultCachePersistenceVersion && version != resultCachePersistenceDependenciesVersion {
		return fmt.Errorf("unsupported persistence version %d", version)
	}
	directory := filepath.Dir(path)
	base := filepath.Base(path)
	temporary, err := os.CreateTemp(directory, "."+base+".tmp-*")
	if err != nil {
		return fmt.Errorf("hatSql: create SQL result cache persistence temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		if temporaryPath != "" {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("hatSql: set SQL result cache persistence permissions: %w", err)
	}
	var header [resultCachePersistenceHeaderSize]byte
	copy(header[:4], resultCachePersistenceMagic[:])
	header[4] = version
	binary.BigEndian.PutUint64(header[5:13], uint64(len(payload)))
	digest := sha256.Sum256(payload)
	copy(header[13:], digest[:])
	if _, err := temporary.Write(header[:]); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("hatSql: write SQL result cache persistence header: %w", err)
	}
	if _, err := temporary.Write(payload); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("hatSql: write SQL result cache persistence payload: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("hatSql: sync SQL result cache persistence: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("hatSql: close SQL result cache persistence: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("hatSql: publish SQL result cache persistence: %w", err)
	}
	temporaryPath = ""
	return nil
}

func readResultCachePersistenceFile(path string, maxBytes int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("hatSql: stat SQL result cache persistence: %w", err)
	}
	if !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, fmt.Errorf("%w: persistence path is not a regular file", ErrSQLResultCachePersistenceCorrupt)
	}
	if info.Size() > maxBytes {
		_ = file.Close()
		return nil, fmt.Errorf("%w: file is %d bytes, limit is %d", ErrSQLResultCachePersistenceTooLarge, info.Size(), maxBytes)
	}
	data, readErr := io.ReadAll(io.LimitReader(file, maxBytes+1))
	closeErr := file.Close()
	if readErr != nil {
		return nil, fmt.Errorf("hatSql: read SQL result cache persistence: %w", readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("hatSql: close SQL result cache persistence: %w", closeErr)
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("%w: file grew beyond %d bytes", ErrSQLResultCachePersistenceTooLarge, maxBytes)
	}
	return data, nil
}

func decodeResultCachePersistence(data []byte) ([]resultCachePersistenceLoadedEntry, error) {
	corrupt := func(format string, args ...interface{}) error {
		return fmt.Errorf("%w: %s", ErrSQLResultCachePersistenceCorrupt, fmt.Sprintf(format, args...))
	}
	if len(data) < resultCachePersistenceHeaderSize {
		return nil, corrupt("truncated envelope")
	}
	if !bytes.Equal(data[:4], resultCachePersistenceMagic[:]) {
		return nil, corrupt("invalid magic")
	}
	version := data[4]
	if version != resultCachePersistenceVersion && version != resultCachePersistenceDependenciesVersion {
		return nil, corrupt("unsupported version %d", data[4])
	}
	payloadLength := binary.BigEndian.Uint64(data[5:13])
	if payloadLength != uint64(len(data)-resultCachePersistenceHeaderSize) {
		return nil, corrupt("payload length mismatch")
	}
	payload := data[resultCachePersistenceHeaderSize:]
	digest := sha256.Sum256(payload)
	if subtle.ConstantTimeCompare(data[13:13+sha256.Size], digest[:]) != 1 {
		return nil, corrupt("checksum mismatch")
	}
	loaded, err := decodeResultCachePersistencePayloadVersion(payload, version)
	if err != nil {
		return nil, corrupt("decode payload: %v", err)
	}
	return loaded, nil
}

type resultCachePersistenceReader struct {
	data   []byte
	offset int
}

func decodeResultCachePersistencePayload(payload []byte) ([]resultCachePersistenceLoadedEntry, error) {
	return decodeResultCachePersistencePayloadVersion(payload, resultCachePersistenceVersion)
}

func decodeResultCachePersistencePayloadVersion(payload []byte, formatVersion byte) ([]resultCachePersistenceLoadedEntry, error) {
	reader := resultCachePersistenceReader{data: payload}
	entryCount, err := reader.count(maxResultCachePersistenceEntries, "entry")
	if err != nil {
		return nil, err
	}
	loaded := make([]resultCachePersistenceLoadedEntry, 0, entryCount)
	seen := make(map[string]struct{}, entryCount)
	for index := 0; index < entryCount; index++ {
		key, err := reader.string(maxResultCachePersistenceStringBytes, "entry key")
		if err != nil {
			return nil, fmt.Errorf("entry %d: %w", index, err)
		}
		if key == "" {
			return nil, fmt.Errorf("entry %d has an empty key", index)
		}
		version, err := reader.string(maxResultCachePersistenceStringBytes, "source version")
		if err != nil {
			return nil, fmt.Errorf("entry %d: %w", index, err)
		}
		if version == "" {
			return nil, fmt.Errorf("entry %d has an empty source version", index)
		}
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("duplicate key %q", key)
		}
		seen[key] = struct{}{}
		var dependencies []resultCacheDependencyKey
		if formatVersion >= resultCachePersistenceDependenciesVersion {
			dependencyCount, err := reader.count(maxResultCachePersistenceDependencies, "dependency")
			if err != nil {
				return nil, fmt.Errorf("entry %d dependencies: %w", index, err)
			}
			dependencies = make([]resultCacheDependencyKey, 0, dependencyCount)
			seenDependencies := make(map[resultCacheDependencyKey]struct{}, dependencyCount)
			for dependencyIndex := 0; dependencyIndex < dependencyCount; dependencyIndex++ {
				kind, err := reader.string(maxResultCachePersistenceStringBytes, "dependency kind")
				if err != nil {
					return nil, fmt.Errorf("entry %d dependency %d: %w", index, dependencyIndex, err)
				}
				dependencyKey, err := reader.string(maxResultCachePersistenceStringBytes, "dependency key")
				if err != nil {
					return nil, fmt.Errorf("entry %d dependency %d: %w", index, dependencyIndex, err)
				}
				dependency := resultCacheDependencyKey{kind: kind, key: dependencyKey}
				if kind == "" || dependencyKey == "" {
					return nil, fmt.Errorf("entry %d dependency %d is empty", index, dependencyIndex)
				}
				if _, exists := seenDependencies[dependency]; exists {
					return nil, fmt.Errorf("entry %d has duplicate dependency", index)
				}
				seenDependencies[dependency] = struct{}{}
				dependencies = append(dependencies, dependency)
			}
		}
		persisted, err := reader.result()
		if err != nil {
			return nil, fmt.Errorf("entry %d result: %w", index, err)
		}
		result, err := decodeResultCachePersistenceResult(persisted)
		if err != nil {
			return nil, fmt.Errorf("entry %d result: %w", index, err)
		}
		loaded = append(loaded, resultCachePersistenceLoadedEntry{
			key:   key,
			entry: resultCacheEntry{version: version, typed: true, dependencies: dependencies, result: result},
		})
	}
	if reader.offset != len(reader.data) {
		return nil, fmt.Errorf("trailing payload bytes")
	}
	return loaded, nil
}

func (reader *resultCachePersistenceReader) result() (resultCachePersistenceResult, error) {
	queryID, err := reader.string(maxResultCachePersistenceStringBytes, "query ID")
	if err != nil {
		return resultCachePersistenceResult{}, err
	}
	flags, err := reader.byte("result flags")
	if err != nil {
		return resultCachePersistenceResult{}, err
	}
	if flags&^(resultCachePersistenceResultColumnsPresent|resultCachePersistenceResultRowsPresent|resultCachePersistenceResultPlanPresent|resultCachePersistenceResultSnapshotPresent|resultCachePersistenceResultStatsPresent|resultCachePersistenceResultHasMore) != 0 {
		return resultCachePersistenceResult{}, fmt.Errorf("unknown result flags 0x%x", flags)
	}
	result := resultCachePersistenceResult{
		QueryID:         queryID,
		ColumnsPresent:  flags&resultCachePersistenceResultColumnsPresent != 0,
		RowsPresent:     flags&resultCachePersistenceResultRowsPresent != 0,
		PlanPresent:     flags&resultCachePersistenceResultPlanPresent != 0,
		SnapshotPresent: flags&resultCachePersistenceResultSnapshotPresent != 0,
		StatsPresent:    flags&resultCachePersistenceResultStatsPresent != 0,
		HasMore:         flags&resultCachePersistenceResultHasMore != 0,
	}
	if result.ColumnsPresent {
		count, err := reader.count(maxResultCachePersistenceFields, "column")
		if err != nil {
			return resultCachePersistenceResult{}, err
		}
		result.Columns = make([]string, count)
		for index := range result.Columns {
			result.Columns[index], err = reader.string(maxResultCachePersistenceStringBytes, "column name")
			if err != nil {
				return resultCachePersistenceResult{}, fmt.Errorf("column %d: %w", index, err)
			}
		}
	}
	if result.RowsPresent {
		count, err := reader.count(maxResultCachePersistenceRows, "row")
		if err != nil {
			return resultCachePersistenceResult{}, err
		}
		result.Rows = make([]resultCachePersistenceRow, count)
		for index := range result.Rows {
			result.Rows[index], err = reader.row()
			if err != nil {
				return resultCachePersistenceResult{}, fmt.Errorf("row %d: %w", index, err)
			}
		}
	}
	if result.PlanPresent {
		result.Plan, err = reader.bytes(maxResultCachePersistenceStringBytes, "plan")
		if err != nil {
			return resultCachePersistenceResult{}, err
		}
	}
	if result.SnapshotPresent {
		result.PlanSnapshot, err = reader.bytes(maxResultCachePersistenceStringBytes, "plan snapshot")
		if err != nil {
			return resultCachePersistenceResult{}, err
		}
	}
	if result.StatsPresent {
		result.Stats, err = reader.bytes(maxResultCachePersistenceStringBytes, "stats")
		if err != nil {
			return resultCachePersistenceResult{}, err
		}
	}
	result.NextCursor, err = reader.string(maxResultCachePersistenceStringBytes, "next cursor")
	if err != nil {
		return resultCachePersistenceResult{}, err
	}
	return result, nil
}

func (reader *resultCachePersistenceReader) row() (resultCachePersistenceRow, error) {
	flag, err := reader.byte("row flags")
	if err != nil {
		return resultCachePersistenceRow{}, err
	}
	if flag > 1 {
		return resultCachePersistenceRow{}, fmt.Errorf("invalid row flags %d", flag)
	}
	row := resultCachePersistenceRow{Nil: flag == 1}
	if row.Nil {
		return row, nil
	}
	count, err := reader.count(maxResultCachePersistenceFields, "field")
	if err != nil {
		return resultCachePersistenceRow{}, err
	}
	row.Fields = make([]resultCachePersistenceField, count)
	for index := range row.Fields {
		row.Fields[index].Key, err = reader.string(maxResultCachePersistenceStringBytes, "field name")
		if err != nil {
			return resultCachePersistenceRow{}, fmt.Errorf("field %d: %w", index, err)
		}
		row.Fields[index].Value, err = reader.value(0)
		if err != nil {
			return resultCachePersistenceRow{}, fmt.Errorf("field %q: %w", row.Fields[index].Key, err)
		}
	}
	return row, nil
}

func (reader *resultCachePersistenceReader) value(depth int) (resultCachePersistenceValue, error) {
	if depth > maxResultCachePersistenceValueDepth {
		return resultCachePersistenceValue{}, fmt.Errorf("value nesting exceeds %d levels", maxResultCachePersistenceValueDepth)
	}
	kind, err := reader.byte("value kind")
	if err != nil {
		return resultCachePersistenceValue{}, err
	}
	flags, err := reader.byte("value flags")
	if err != nil {
		return resultCachePersistenceValue{}, err
	}
	if flags > 1 {
		return resultCachePersistenceValue{}, fmt.Errorf("invalid value flags %d", flags)
	}
	value := resultCachePersistenceValue{Kind: kind, Nil: flags == 1}
	switch kind {
	case resultCachePersistenceValueScalar:
		value.Scalar, err = reader.bytes(maxResultCachePersistenceScalarBytes, "scalar")
	case resultCachePersistenceValueArray:
		count, countErr := reader.count(maxResultCachePersistenceFields, "array item")
		if countErr != nil {
			return resultCachePersistenceValue{}, countErr
		}
		value.Array = make([]resultCachePersistenceValue, count)
		for index := range value.Array {
			value.Array[index], err = reader.value(depth + 1)
			if err != nil {
				return resultCachePersistenceValue{}, fmt.Errorf("array item %d: %w", index, err)
			}
		}
	case resultCachePersistenceValueObject, resultCachePersistenceValueRow:
		count, countErr := reader.count(maxResultCachePersistenceFields, "object field")
		if countErr != nil {
			return resultCachePersistenceValue{}, countErr
		}
		value.Fields = make([]resultCachePersistenceField, count)
		for index := range value.Fields {
			value.Fields[index].Key, err = reader.string(maxResultCachePersistenceStringBytes, "field name")
			if err != nil {
				return resultCachePersistenceValue{}, fmt.Errorf("object field %d: %w", index, err)
			}
			value.Fields[index].Value, err = reader.value(depth + 1)
			if err != nil {
				return resultCachePersistenceValue{}, fmt.Errorf("field %q: %w", value.Fields[index].Key, err)
			}
		}
	default:
		return resultCachePersistenceValue{}, fmt.Errorf("unknown value kind %d", kind)
	}
	if err != nil {
		return resultCachePersistenceValue{}, err
	}
	return value, nil
}

func (reader *resultCachePersistenceReader) byte(name string) (byte, error) {
	if reader.offset >= len(reader.data) {
		return 0, fmt.Errorf("%s is truncated", name)
	}
	value := reader.data[reader.offset]
	reader.offset++
	return value, nil
}

func (reader *resultCachePersistenceReader) count(max int, name string) (int, error) {
	value, err := reader.uvarint(name + " count")
	if err != nil {
		return 0, err
	}
	if value > uint64(max) {
		return 0, fmt.Errorf("%s count %d exceeds limit %d", name, value, max)
	}
	return int(value), nil
}

func (reader *resultCachePersistenceReader) string(max int, name string) (string, error) {
	value, err := reader.bytes(max, name)
	if err != nil {
		return "", err
	}
	return string(value), nil
}

func (reader *resultCachePersistenceReader) bytes(max int, name string) ([]byte, error) {
	length, err := reader.uvarint(name + " length")
	if err != nil {
		return nil, err
	}
	if length > uint64(max) {
		return nil, fmt.Errorf("%s length %d exceeds limit %d", name, length, max)
	}
	if length > uint64(len(reader.data)-reader.offset) {
		return nil, fmt.Errorf("%s is truncated", name)
	}
	start := reader.offset
	reader.offset += int(length)
	return append([]byte(nil), reader.data[start:reader.offset]...), nil
}

func (reader *resultCachePersistenceReader) uvarint(name string) (uint64, error) {
	if reader.offset >= len(reader.data) {
		return 0, fmt.Errorf("%s is truncated", name)
	}
	value, size := binary.Uvarint(reader.data[reader.offset:])
	if size <= 0 {
		return 0, fmt.Errorf("%s is invalid", name)
	}
	reader.offset += size
	return value, nil
}

func decodeResultCachePersistenceResult(persisted resultCachePersistenceResult) (QueryResult, error) {
	if len(persisted.Rows) > maxResultCachePersistenceRows {
		return QueryResult{}, fmt.Errorf("row count %d exceeds limit %d", len(persisted.Rows), maxResultCachePersistenceRows)
	}
	result := QueryResult{
		QueryID:    persisted.QueryID,
		HasMore:    persisted.HasMore,
		NextCursor: persisted.NextCursor,
	}
	if persisted.ColumnsPresent {
		result.Columns = append([]string(nil), persisted.Columns...)
	}
	if persisted.RowsPresent {
		result.Rows = make([]Row, len(persisted.Rows))
		for rowIndex, persistedRow := range persisted.Rows {
			row, err := decodeResultCachePersistenceRow(persistedRow)
			if err != nil {
				return QueryResult{}, fmt.Errorf("row %d: %w", rowIndex, err)
			}
			result.Rows[rowIndex] = row
		}
	}
	if persisted.PlanPresent {
		if err := json.Unmarshal(persisted.Plan, &result.Plan); err != nil {
			return QueryResult{}, fmt.Errorf("plan: %w", err)
		}
	}
	if persisted.SnapshotPresent {
		result.PlanSnapshot = new(SQLPlanSnapshot)
		if err := json.Unmarshal(persisted.PlanSnapshot, result.PlanSnapshot); err != nil {
			return QueryResult{}, fmt.Errorf("plan snapshot: %w", err)
		}
	}
	if persisted.StatsPresent {
		result.Stats = new(QueryStats)
		if err := json.Unmarshal(persisted.Stats, result.Stats); err != nil {
			return QueryResult{}, fmt.Errorf("stats: %w", err)
		}
	}
	return result, nil
}

func decodeResultCachePersistenceRow(persisted resultCachePersistenceRow) (Row, error) {
	if persisted.Nil {
		return nil, nil
	}
	if len(persisted.Fields) > maxResultCachePersistenceFields {
		return nil, fmt.Errorf("field count %d exceeds limit %d", len(persisted.Fields), maxResultCachePersistenceFields)
	}
	row := make(Row, len(persisted.Fields))
	seen := make(map[string]struct{}, len(persisted.Fields))
	for _, field := range persisted.Fields {
		if _, exists := seen[field.Key]; exists {
			return nil, fmt.Errorf("duplicate field %q", field.Key)
		}
		seen[field.Key] = struct{}{}
		value, err := decodeResultCachePersistenceValue(field.Value, 0)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", field.Key, err)
		}
		row[field.Key] = value
	}
	return row, nil
}

func decodeResultCachePersistenceValue(persisted resultCachePersistenceValue, depth int) (interface{}, error) {
	if depth > maxResultCachePersistenceValueDepth {
		return nil, fmt.Errorf("value nesting exceeds %d levels", maxResultCachePersistenceValueDepth)
	}
	switch persisted.Kind {
	case resultCachePersistenceValueScalar:
		return decodeResultCachePersistenceScalar(persisted.Scalar)
	case resultCachePersistenceValueArray:
		if persisted.Nil {
			return []interface{}(nil), nil
		}
		if len(persisted.Array) > maxResultCachePersistenceFields {
			return nil, fmt.Errorf("array length %d exceeds limit %d", len(persisted.Array), maxResultCachePersistenceFields)
		}
		array := make([]interface{}, len(persisted.Array))
		for index, child := range persisted.Array {
			value, err := decodeResultCachePersistenceValue(child, depth+1)
			if err != nil {
				return nil, fmt.Errorf("array item %d: %w", index, err)
			}
			array[index] = value
		}
		return array, nil
	case resultCachePersistenceValueObject, resultCachePersistenceValueRow:
		if len(persisted.Fields) > maxResultCachePersistenceFields {
			return nil, fmt.Errorf("object field count %d exceeds limit %d", len(persisted.Fields), maxResultCachePersistenceFields)
		}
		if persisted.Nil {
			if persisted.Kind == resultCachePersistenceValueRow {
				return Row(nil), nil
			}
			return map[string]interface{}(nil), nil
		}
		values := make(map[string]interface{}, len(persisted.Fields))
		seen := make(map[string]struct{}, len(persisted.Fields))
		for _, field := range persisted.Fields {
			if _, exists := seen[field.Key]; exists {
				return nil, fmt.Errorf("duplicate field %q", field.Key)
			}
			seen[field.Key] = struct{}{}
			value, err := decodeResultCachePersistenceValue(field.Value, depth+1)
			if err != nil {
				return nil, fmt.Errorf("field %q: %w", field.Key, err)
			}
			values[field.Key] = value
		}
		if persisted.Kind == resultCachePersistenceValueRow {
			return Row(values), nil
		}
		return values, nil
	default:
		return nil, fmt.Errorf("unknown value kind %d", persisted.Kind)
	}
}
