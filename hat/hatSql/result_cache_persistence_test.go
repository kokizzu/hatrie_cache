package hatSql

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestSQLResultCachePersistencePreservesTypedResultsAndInvalidates(t *testing.T) {
	now := time.Date(2026, 9, 16, 12, 34, 56, 789, time.UTC)
	want := QueryResult{
		QueryID: "query-1",
		Columns: []string{"id", "payload"},
		Rows: []Row{{
			"nil":       nil,
			"bool":      true,
			"text":      "cached",
			"bytes":     []byte{1, 2, 3},
			"int":       int64(-7),
			"int8":      int8(-8),
			"int16":     int16(-16),
			"int32":     int32(-32),
			"uint":      uint64(9),
			"uint8":     uint8(8),
			"uint16":    uint16(16),
			"uint32":    uint32(32),
			"float":     float64(1.25),
			"float32":   float32(1.5),
			"time":      now,
			"date":      sqlDate("2026-09-16"),
			"decimal":   sqlDecimal("12.3400"),
			"uuid":      sqlUUID("01234567-89ab-cdef-0123-456789abcdef"),
			"duration":  sqlDuration("1h2m3s"),
			"ipv4":      SQLIPv4(0x7f000001),
			"ipv6":      SQLIPv6{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			"array":     []interface{}{int64(3), "nested"},
			"object":    map[string]interface{}{"count": int64(2)},
			"nestedRow": Row{"value": int32(4)},
		}},
		Stats: &QueryStats{OutputRows: 1, OutputColumns: 2, ResultBytes: 128},
	}
	cache := NewSQLResultCache(4)
	version := "schema-v1/source-v1"
	calls := 0
	execute := func(context.Context) (QueryResult, error) {
		calls++
		return want, nil
	}
	key := "schema-v1/query"
	if _, err := cache.ExecuteVersioned(context.Background(), key, func() (string, bool) {
		return version, true
	}, execute); err != nil {
		t.Fatalf("seed ExecuteVersioned() error = %v", err)
	}

	path := filepath.Join(t.TempDir(), "result-cache.bin")
	if err := cache.Persist(path); err != nil {
		t.Fatalf("Persist() error = %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("persisted cache stat error = %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("persisted cache mode = %o, want 600", got)
	}

	restored := NewSQLResultCache(4)
	if err := restored.Restore(path); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	hit, err := restored.ExecuteVersioned(context.Background(), key, func() (string, bool) {
		return version, true
	}, func(context.Context) (QueryResult, error) {
		t.Fatal("restored cache miss for matching source version")
		return QueryResult{}, nil
	})
	if err != nil {
		t.Fatalf("restored ExecuteVersioned() error = %v", err)
	}
	wantHit := cloneResultCacheResult(want)
	if !reflect.DeepEqual(hit, wantHit) {
		t.Fatalf("restored result = %#v, want %#v", hit, wantHit)
	}
	if got := hit.Rows[0]["time"].(time.Time); !got.Equal(now) {
		t.Fatalf("restored time = %v, want %v", got, now)
	}
	if _, ok := hit.Rows[0]["int"].(int64); !ok {
		t.Fatalf("restored int type = %T, want int64", hit.Rows[0]["int"])
	}

	version = "schema-v1/source-v2"
	updated, err := restored.ExecuteVersioned(context.Background(), key, func() (string, bool) {
		return version, true
	}, func(context.Context) (QueryResult, error) {
		return QueryResult{Columns: []string{"id"}, Rows: []Row{{"id": int64(2)}}}, nil
	})
	if err != nil {
		t.Fatalf("source-version invalidation error = %v", err)
	}
	if got := updated.Rows[0]["id"]; got != int64(2) {
		t.Fatalf("source-version invalidation result = %#v, want 2", got)
	}

	version = "schema-v1/source-v2"
	_, err = restored.ExecuteVersioned(context.Background(), "schema-v2/query", func() (string, bool) {
		return version, true
	}, func(context.Context) (QueryResult, error) {
		return QueryResult{Columns: []string{"id"}, Rows: []Row{{"id": int64(3)}}}, nil
	})
	if err != nil {
		t.Fatalf("schema-key invalidation error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("original executor calls = %d, want 1", calls)
	}
}

func TestSQLResultCachePersistenceRejectsCorruptionWithoutReplacingCache(t *testing.T) {
	cache := NewSQLResultCache(1)
	if _, err := cache.ExecuteVersioned(context.Background(), "key", func() (string, bool) {
		return "v1", true
	}, func(context.Context) (QueryResult, error) {
		return QueryResult{Columns: []string{"id"}, Rows: []Row{{"id": int64(1)}}}, nil
	}); err != nil {
		t.Fatalf("seed ExecuteVersioned() error = %v", err)
	}
	path := filepath.Join(t.TempDir(), "result-cache.bin")
	if err := cache.Persist(path); err != nil {
		t.Fatalf("Persist() error = %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	data[len(data)/2] ^= 1
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("tamper WriteFile() error = %v", err)
	}

	restored := NewSQLResultCache(1)
	if err := restored.Restore(path); !errors.Is(err, ErrSQLResultCachePersistenceCorrupt) {
		t.Fatalf("Restore() corruption error = %v, want %v", err, ErrSQLResultCachePersistenceCorrupt)
	}
	if got := restored.Stats().Entries; got != 0 {
		t.Fatalf("cache entries after rejected restore = %d, want 0", got)
	}
}

func TestSQLResultCachePersistenceSkipsProcessLocalEntriesAndTreatsMissingAsColdStart(t *testing.T) {
	cache := NewResultCache(2)
	if _, err := cache.Execute(context.Background(), "local", func() uint64 { return 1 }, func(context.Context) (QueryResult, error) {
		return QueryResult{Columns: []string{"id"}, Rows: []Row{{"id": int64(1)}}}, nil
	}); err != nil {
		t.Fatalf("seed generic Execute() error = %v", err)
	}
	path := filepath.Join(t.TempDir(), "result-cache.bin")
	if err := cache.Persist(path); err != nil {
		t.Fatalf("Persist() error = %v", err)
	}
	restored := NewResultCache(2)
	if err := restored.Restore(path); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	if got := restored.Stats().Entries; got != 0 {
		t.Fatalf("restored process-local entries = %d, want 0", got)
	}
	missing := filepath.Join(t.TempDir(), "missing-cache.bin")
	if err := restored.Restore(missing); err != nil {
		t.Fatalf("missing Restore() error = %v", err)
	}
	if err := cache.PersistWithOptions(path, SQLResultCachePersistenceOptions{MaxBytes: resultCachePersistenceHeaderSize}); !errors.Is(err, ErrSQLResultCachePersistenceTooLarge) {
		t.Fatalf("small-quota Persist() error = %v, want %v", err, ErrSQLResultCachePersistenceTooLarge)
	}
}
