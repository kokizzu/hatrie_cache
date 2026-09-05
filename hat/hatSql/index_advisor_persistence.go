package hatSql

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

const (
	// DefaultSQLIndexAdvisorSnapshotMaxBytes bounds a persisted advisor file
	// accepted by Load. The advisor remains bounded by its configured capacity.
	DefaultSQLIndexAdvisorSnapshotMaxBytes = 1 << 20
	maxSQLIndexAdvisorSnapshotEntries      = 4096
	maxSQLIndexAdvisorSnapshotStringBytes  = 1024
)

// SQLIndexAdvisorSnapshotVersion is the persisted advisor snapshot format
// version written by Save.
const SQLIndexAdvisorSnapshotVersion = 1

type sqlIndexAdvisorSnapshot struct {
	Version uint8                          `json:"version"`
	Entries []sqlIndexAdvisorSnapshotEntry `json:"entries"`
}

type sqlIndexAdvisorSnapshotEntry struct {
	Key         string `json:"key"`
	Field       string `json:"field"`
	SlowQueries uint64 `json:"slow_queries"`
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
	return json.NewEncoder(writer).Encode(sqlIndexAdvisorSnapshot{
		Version: SQLIndexAdvisorSnapshotVersion,
		Entries: entries,
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
	if snapshot.Version != SQLIndexAdvisorSnapshotVersion {
		return fmt.Errorf("unsupported SQL index advisor snapshot version %d", snapshot.Version)
	}
	if len(snapshot.Entries) > maxSQLIndexAdvisorSnapshotEntries {
		return fmt.Errorf("SQL index advisor snapshot contains too many entries")
	}
	if advisor.capacity <= 0 && len(snapshot.Entries) > 0 {
		return fmt.Errorf("SQL index advisor snapshot contains entries but capacity is %d", advisor.capacity)
	}
	if advisor.capacity > 0 && len(snapshot.Entries) > advisor.capacity {
		return fmt.Errorf("SQL index advisor snapshot contains %d entries, capacity is %d", len(snapshot.Entries), advisor.capacity)
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
	advisor.mu.Lock()
	advisor.counts = counts
	advisor.mu.Unlock()
	return nil
}
