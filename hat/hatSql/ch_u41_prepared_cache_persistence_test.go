package hatSql

import (
	"os"
	"path/filepath"
	"testing"
)

const chu41PreparedQuerySource = "SELECT value FROM CACHE('users') WHERE id = $1"

func TestSQLPreparedQueryCachePersistenceRoundTrip(t *testing.T) {
	cache := NewSQLPreparedQueryCache(4)
	if _, err := PrepareSQLQueryWithSchemaVersion(chu41PreparedQuerySource, []ParameterSpec{{Type: ParameterInteger}}, "schema-1", cache); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	path := filepath.Join(t.TempDir(), "prepared.cache")
	if err := cache.Save(path, SQLPreparedQueryCachePersistenceOptions{}); err != nil {
		t.Fatalf("save: %v", err)
	}

	restored := NewSQLPreparedQueryCache(4)
	report, err := restored.Load(path, SQLPreparedQueryCachePersistenceOptions{SchemaVersion: "schema-1"})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if report.Loaded != 1 || report.SkippedSchema != 0 {
		t.Fatalf("load report = %#v, want one loaded entry", report)
	}
	if stats := restored.Stats(); stats.Entries != 1 {
		t.Fatalf("restored stats = %#v, want one entry", stats)
	}
	if _, err := PrepareSQLQueryWithSchemaVersion(chu41PreparedQuerySource, []ParameterSpec{{Type: ParameterInteger}}, "schema-1", restored); err != nil {
		t.Fatalf("prepare after restore: %v", err)
	}
	if stats := restored.Stats(); stats.Hits == 0 {
		t.Fatalf("restored lookup stats = %#v, want a cache hit", stats)
	}
}

func TestSQLPreparedQueryCachePersistenceValidatesSchemaAndIsolatesParameters(t *testing.T) {
	cache := NewSQLPreparedQueryCache(4)
	if _, err := PrepareSQLQueryWithSchemaVersion(chu41PreparedQuerySource, []ParameterSpec{{Type: ParameterInteger}}, "schema-1", cache); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	path := filepath.Join(t.TempDir(), "prepared.cache")
	if err := cache.Save(path, SQLPreparedQueryCachePersistenceOptions{}); err != nil {
		t.Fatalf("save: %v", err)
	}

	restored := NewSQLPreparedQueryCache(4)
	report, err := restored.Load(path, SQLPreparedQueryCachePersistenceOptions{SchemaVersion: "schema-2"})
	if err != nil {
		t.Fatalf("load mismatched schema: %v", err)
	}
	if report.Loaded != 0 || report.SkippedSchema != 1 || restored.Stats().Entries != 0 {
		t.Fatalf("mismatched load report/cache = %#v/%#v", report, restored.Stats())
	}

	if _, err := restored.Load(path, SQLPreparedQueryCachePersistenceOptions{SchemaVersion: "schema-1"}); err != nil {
		t.Fatalf("load matching schema: %v", err)
	}
	query, err := PrepareSQLQueryWithSchemaVersion(chu41PreparedQuerySource, []ParameterSpec{{Type: ParameterText}}, "schema-1", restored)
	if err != nil {
		t.Fatalf("prepare with independent parameter schema: %v", err)
	}
	if parameters := query.Parameters(); len(parameters) != 1 || parameters[0].Type != ParameterText {
		t.Fatalf("restored parameter schema = %#v, want TEXT", parameters)
	}
}

func TestSQLPreparedQueryCachePersistenceRejectsCorruptionAndBounds(t *testing.T) {
	cache := NewSQLPreparedQueryCache(4)
	if _, err := PrepareSQLQueryWithSchemaVersion(chu41PreparedQuerySource, []ParameterSpec{{Type: ParameterInteger}}, "schema-1", cache); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	path := filepath.Join(t.TempDir(), "prepared.cache")
	if err := cache.Save(path, SQLPreparedQueryCachePersistenceOptions{}); err != nil {
		t.Fatalf("save: %v", err)
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read saved cache: %v", err)
	}
	payload[len(payload)-1] ^= 0xff
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("corrupt cache: %v", err)
	}

	restored := NewSQLPreparedQueryCache(4)
	if _, err := restored.Load(path, SQLPreparedQueryCachePersistenceOptions{SchemaVersion: "schema-1"}); err == nil {
		t.Fatal("corrupt cache load succeeded")
	}
	if stats := restored.Stats(); stats.Entries != 0 {
		t.Fatalf("cache changed after corrupt load = %#v", stats)
	}

	if err := cache.Save(path, SQLPreparedQueryCachePersistenceOptions{}); err != nil {
		t.Fatalf("restore valid cache: %v", err)
	}
	if _, err := restored.Load(path, SQLPreparedQueryCachePersistenceOptions{MaxBytes: 1, SchemaVersion: "schema-1"}); err == nil {
		t.Fatal("over-limit cache load succeeded")
	}
}
