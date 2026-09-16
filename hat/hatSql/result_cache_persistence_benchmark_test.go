package hatSql

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	json "github.com/goccy/go-json"
)

func benchmarkSQLResultCachePersistenceResult() QueryResult {
	rows := make([]Row, 1024)
	for index := range rows {
		rows[index] = Row{
			"id":      int64(index),
			"group":   "hot",
			"payload": []byte("cached-payload"),
			"active":  index%2 == 0,
			"at":      time.Unix(int64(index), 0).UTC(),
		}
	}
	return QueryResult{Columns: []string{"id", "group", "payload", "active", "at"}, Rows: rows}
}

func benchmarkSeedSQLResultCache(b *testing.B) (*SQLResultCache, string, QueryResult) {
	b.Helper()
	cache := NewSQLResultCache(1)
	result := benchmarkSQLResultCachePersistenceResult()
	if _, err := cache.ExecuteVersioned(context.Background(), "query", func() (string, bool) {
		return "source-v1", true
	}, func(context.Context) (QueryResult, error) {
		return result, nil
	}); err != nil {
		b.Fatal(err)
	}
	path := filepath.Join(b.TempDir(), "result-cache.bin")
	if err := cache.Persist(path); err != nil {
		b.Fatal(err)
	}
	return cache, path, result
}

func BenchmarkSQLResultCacheMemoryHit1KRows(b *testing.B) {
	cache, _, _ := benchmarkSeedSQLResultCache(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := cache.ExecuteVersioned(context.Background(), "query", func() (string, bool) {
			return "source-v1", true
		}, func(context.Context) (QueryResult, error) {
			b.Fatal("memory cache miss")
			return QueryResult{}, nil
		})
		if err != nil || len(result.Rows) != 1024 {
			b.Fatalf("memory hit result rows/error = %d/%v", len(result.Rows), err)
		}
	}
}

func BenchmarkSQLResultCachePersist1KRows(b *testing.B) {
	cache, path, _ := benchmarkSeedSQLResultCache(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := cache.Persist(path); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	info, err := os.Stat(path)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(info.Size()), "bytes/snapshot")
}

func BenchmarkSQLResultCacheRestore1KRows(b *testing.B) {
	_, path, _ := benchmarkSeedSQLResultCache(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		restored := NewSQLResultCache(1)
		if err := restored.Restore(path); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLResultCacheJSONEncoding1KRows(b *testing.B) {
	_, _, result := benchmarkSeedSQLResultCache(b)
	encoded, err := json.Marshal(result)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		encoded, err = json.Marshal(result)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(len(encoded)), "bytes/json")
}
