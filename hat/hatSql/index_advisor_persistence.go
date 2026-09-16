package hatSql

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
)

const (
	// DefaultSQLIndexAdvisorSnapshotMaxBytes bounds a persisted advisor file
	// accepted by Load. The advisor remains bounded by its configured capacity.
	DefaultSQLIndexAdvisorSnapshotMaxBytes = 1 << 20
	maxSQLIndexAdvisorSnapshotEntries      = 4096
	maxSQLIndexAdvisorSnapshotStringBytes  = 1024
	maxSQLIndexAdvisorPrefixFields         = 64
)

// SQLIndexAdvisorSnapshotVersion is the persisted advisor snapshot format
// version written by Save.
const SQLIndexAdvisorSnapshotVersion = 3

type sqlIndexAdvisorSnapshot struct {
	Version     uint8                                `json:"version"`
	Entries     []sqlIndexAdvisorSnapshotEntry       `json:"entries"`
	Prefixes    []sqlIndexAdvisorPrefixSnapshotEntry `json:"prefixes,omitempty"`
	SkipIndexes []sqlIndexAdvisorSkipSnapshotEntry   `json:"skip_indexes,omitempty"`
}

type sqlIndexAdvisorSnapshotEntry struct {
	Key         string `json:"key"`
	Field       string `json:"field"`
	SlowQueries uint64 `json:"slow_queries"`
}

type sqlIndexAdvisorPrefixSnapshotEntry struct {
	Key         string   `json:"key"`
	Fields      []string `json:"fields"`
	SlowQueries uint64   `json:"slow_queries"`
}

type sqlIndexAdvisorSkipSnapshotEntry struct {
	Key               string `json:"key"`
	Field             string `json:"field"`
	Path              string `json:"path"`
	SlowQueries       uint64 `json:"slow_queries"`
	TotalElapsedNanos uint64 `json:"total_elapsed_nanos"`
}

// Save writes the advisor's bounded workload observations as a versioned JSON
// snapshot. It does not include SQL text, literal values, or row data.
func (advisor *SQLIndexAdvisor) Save(writer io.Writer) error {
	if advisor == nil {
		return fmt.Errorf("SQL index advisor is nil")
	}
	if writer == nil {
		return fmt.Errorf("SQL index advisor snapshot writer is nil")
	}
	recommendations := advisor.Recommendations()
	entries := make([]sqlIndexAdvisorSnapshotEntry, len(recommendations))
	for index, recommendation := range recommendations {
		entries[index] = sqlIndexAdvisorSnapshotEntry{
			Key:         recommendation.Key,
			Field:       recommendation.Field,
			SlowQueries: recommendation.SlowQueries,
		}
	}
	prefixes := advisor.prefixSnapshotEntries()
	skipIndexes := advisor.skipIndexSnapshotEntries()
	return json.NewEncoder(writer).Encode(sqlIndexAdvisorSnapshot{
		Version:     SQLIndexAdvisorSnapshotVersion,
		Entries:     entries,
		Prefixes:    prefixes,
		SkipIndexes: skipIndexes,
	})
}

// Load replaces the advisor's observations from a validated JSON snapshot.
// Validation completes before the live map is changed, so a rejected file
// cannot partially alter existing recommendations.
func (advisor *SQLIndexAdvisor) Load(reader io.Reader) error {
	if advisor == nil {
		return fmt.Errorf("SQL index advisor is nil")
	}
	if reader == nil {
		return fmt.Errorf("SQL index advisor snapshot reader is nil")
	}
	data, err := io.ReadAll(io.LimitReader(reader, DefaultSQLIndexAdvisorSnapshotMaxBytes+1))
	if err != nil {
		return fmt.Errorf("read SQL index advisor snapshot: %w", err)
	}
	if len(data) > DefaultSQLIndexAdvisorSnapshotMaxBytes {
		return fmt.Errorf("SQL index advisor snapshot exceeds %d bytes", DefaultSQLIndexAdvisorSnapshotMaxBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var snapshot sqlIndexAdvisorSnapshot
	if err := decoder.Decode(&snapshot); err != nil {
		return fmt.Errorf("decode SQL index advisor snapshot: %w", err)
	}
	var trailing interface{}
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("SQL index advisor snapshot contains trailing data")
		}
		return fmt.Errorf("decode trailing SQL index advisor snapshot data: %w", err)
	}
	if snapshot.Version != 1 && snapshot.Version != 2 && snapshot.Version != SQLIndexAdvisorSnapshotVersion {
		return fmt.Errorf("unsupported SQL index advisor snapshot version %d", snapshot.Version)
	}
	if len(snapshot.Entries) > maxSQLIndexAdvisorSnapshotEntries {
		return fmt.Errorf("SQL index advisor snapshot contains too many entries")
	}
	if len(snapshot.Prefixes) > maxSQLIndexAdvisorSnapshotEntries {
		return fmt.Errorf("SQL index advisor snapshot contains too many prefixes")
	}
	if len(snapshot.SkipIndexes) > maxSQLIndexAdvisorSnapshotEntries {
		return fmt.Errorf("SQL index advisor snapshot contains too many skip indexes")
	}
	if advisor.capacity <= 0 && (len(snapshot.Entries) > 0 || len(snapshot.Prefixes) > 0 || len(snapshot.SkipIndexes) > 0) {
		return fmt.Errorf("SQL index advisor snapshot contains entries but capacity is %d", advisor.capacity)
	}
	if advisor.capacity > 0 && len(snapshot.Entries) > advisor.capacity {
		return fmt.Errorf("SQL index advisor snapshot contains %d entries, capacity is %d", len(snapshot.Entries), advisor.capacity)
	}
	if advisor.capacity > 0 && len(snapshot.Prefixes) > advisor.capacity {
		return fmt.Errorf("SQL index advisor snapshot contains %d prefixes, capacity is %d", len(snapshot.Prefixes), advisor.capacity)
	}
	if advisor.capacity > 0 && len(snapshot.SkipIndexes) > advisor.capacity {
		return fmt.Errorf("SQL index advisor snapshot contains %d skip indexes, capacity is %d", len(snapshot.SkipIndexes), advisor.capacity)
	}
	counts := make(map[sqlIndexAdvisorKey]uint64, len(snapshot.Entries))
	for _, entry := range snapshot.Entries {
		if len(entry.Key) == 0 || len(entry.Key) > maxSQLIndexAdvisorSnapshotStringBytes {
			return fmt.Errorf("SQL index advisor snapshot key length is invalid")
		}
		if len(entry.Field) == 0 || len(entry.Field) > maxSQLIndexAdvisorSnapshotStringBytes {
			return fmt.Errorf("SQL index advisor snapshot field length is invalid")
		}
		if entry.SlowQueries == 0 {
			return fmt.Errorf("SQL index advisor snapshot slow query count must be positive")
		}
		key := sqlIndexAdvisorKey{key: entry.Key, field: entry.Field}
		if _, exists := counts[key]; exists {
			return fmt.Errorf("SQL index advisor snapshot contains duplicate key and field")
		}
		counts[key] = entry.SlowQueries
	}
	prefixCounts := make(map[sqlIndexAdvisorPrefixKey]uint64, len(snapshot.Prefixes))
	for _, entry := range snapshot.Prefixes {
		if len(entry.Key) == 0 || len(entry.Key) > maxSQLIndexAdvisorSnapshotStringBytes {
			return fmt.Errorf("SQL index advisor snapshot prefix key length is invalid")
		}
		if len(entry.Fields) == 0 || len(entry.Fields) > maxSQLIndexAdvisorPrefixFields {
			return fmt.Errorf("SQL index advisor snapshot prefix fields are invalid")
		}
		seenFields := make(map[string]struct{}, len(entry.Fields))
		for _, field := range entry.Fields {
			if len(field) == 0 || len(field) > maxSQLIndexAdvisorSnapshotStringBytes {
				return fmt.Errorf("SQL index advisor snapshot prefix field length is invalid")
			}
			if _, exists := seenFields[field]; exists {
				return fmt.Errorf("SQL index advisor snapshot prefix contains duplicate fields")
			}
			seenFields[field] = struct{}{}
		}
		if entry.SlowQueries == 0 {
			return fmt.Errorf("SQL index advisor snapshot prefix count must be positive")
		}
		key := sqlIndexAdvisorPrefixKey{key: entry.Key, fields: strings.Join(entry.Fields, "\x00")}
		if _, exists := prefixCounts[key]; exists {
			return fmt.Errorf("SQL index advisor snapshot contains duplicate prefix")
		}
		prefixCounts[key] = entry.SlowQueries
	}
	skipCounts := make(map[sqlIndexAdvisorSkipKey]sqlIndexAdvisorSkipStats, len(snapshot.SkipIndexes))
	for _, entry := range snapshot.SkipIndexes {
		if len(entry.Key) == 0 || len(entry.Key) > maxSQLIndexAdvisorSnapshotStringBytes {
			return fmt.Errorf("SQL index advisor snapshot skip key length is invalid")
		}
		if len(entry.Field) == 0 || len(entry.Field) > maxSQLIndexAdvisorSnapshotStringBytes {
			return fmt.Errorf("SQL index advisor snapshot skip field length is invalid")
		}
		if len(entry.Path) == 0 || len(entry.Path) > maxSQLIndexAdvisorSnapshotStringBytes {
			return fmt.Errorf("SQL index advisor snapshot skip path length is invalid")
		}
		canonicalPath, err := NormalizeJSONPath(entry.Path)
		if err != nil || canonicalPath != entry.Path {
			return fmt.Errorf("SQL index advisor snapshot skip path is invalid")
		}
		if entry.SlowQueries == 0 {
			return fmt.Errorf("SQL index advisor snapshot skip count must be positive")
		}
		key := sqlIndexAdvisorSkipKey{key: entry.Key, field: entry.Field, path: entry.Path}
		if _, exists := skipCounts[key]; exists {
			return fmt.Errorf("SQL index advisor snapshot contains duplicate skip index")
		}
		skipCounts[key] = sqlIndexAdvisorSkipStats{
			slowQueries:       entry.SlowQueries,
			totalElapsedNanos: entry.TotalElapsedNanos,
		}
	}
	advisor.mu.Lock()
	advisor.counts = counts
	advisor.prefixCounts = prefixCounts
	advisor.skipCounts = skipCounts
	advisor.mu.Unlock()
	return nil
}

func (advisor *SQLIndexAdvisor) prefixSnapshotEntries() []sqlIndexAdvisorPrefixSnapshotEntry {
	if advisor == nil {
		return nil
	}
	advisor.mu.RLock()
	entries := make([]sqlIndexAdvisorPrefixSnapshotEntry, 0, len(advisor.prefixCounts))
	for key, count := range advisor.prefixCounts {
		entries = append(entries, sqlIndexAdvisorPrefixSnapshotEntry{
			Key:         key.key,
			Fields:      strings.Split(key.fields, "\x00"),
			SlowQueries: count,
		})
	}
	advisor.mu.RUnlock()
	sort.Slice(entries, func(left, right int) bool {
		if entries[left].Key != entries[right].Key {
			return entries[left].Key < entries[right].Key
		}
		return strings.Join(entries[left].Fields, "\x00") < strings.Join(entries[right].Fields, "\x00")
	})
	return entries
}

func (advisor *SQLIndexAdvisor) skipIndexSnapshotEntries() []sqlIndexAdvisorSkipSnapshotEntry {
	if advisor == nil {
		return nil
	}
	advisor.mu.RLock()
	entries := make([]sqlIndexAdvisorSkipSnapshotEntry, 0, len(advisor.skipCounts))
	for key, stats := range advisor.skipCounts {
		entries = append(entries, sqlIndexAdvisorSkipSnapshotEntry{
			Key:               key.key,
			Field:             key.field,
			Path:              key.path,
			SlowQueries:       stats.slowQueries,
			TotalElapsedNanos: stats.totalElapsedNanos,
		})
	}
	advisor.mu.RUnlock()
	sort.Slice(entries, func(left, right int) bool {
		if entries[left].Key != entries[right].Key {
			return entries[left].Key < entries[right].Key
		}
		if entries[left].Field != entries[right].Field {
			return entries[left].Field < entries[right].Field
		}
		return entries[left].Path < entries[right].Path
	})
	return entries
}
