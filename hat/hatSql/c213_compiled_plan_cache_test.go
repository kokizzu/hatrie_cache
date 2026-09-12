package hatSql

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
)

const c213CompiledPlanCacheQuery = "FROM VALUES (1), (2), (3) AS values(id) SELECT id WHERE id >= 2"

func TestC213CompiledPlanCacheReusesImmutableHandle(t *testing.T) {
	cache, err := NewSQLCompiledQueryCache(SQLCompiledQueryCacheOptions{MaxEntries: 2, MaxBytes: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	first, err := cache.Compile(c213CompiledPlanCacheQuery)
	if err != nil {
		t.Fatal(err)
	}
	second, err := cache.Compile(c213CompiledPlanCacheQuery)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("cache returned different compiled handles for the same source")
	}
	result, err := second.Execute(context.Background(), nil, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []SQLRow{{"id": int64(2)}, {"id": int64(3)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("compiled rows = %#v, want %#v", result.Rows, want)
	}
	stats := cache.Stats()
	if stats.Entries != 1 || stats.Hits != 1 || stats.Misses != 1 || stats.Bytes <= 0 || stats.Bytes > stats.MaxBytes {
		t.Fatalf("cache stats = %#v, want one bounded entry and one hit/miss", stats)
	}
}

func TestC213CompiledPlanCacheOptionReusesPlanForQueryExecution(t *testing.T) {
	cache, err := NewSQLCompiledQueryCache(SQLCompiledQueryCacheOptions{MaxEntries: 2, MaxBytes: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	options := SQLQueryOptions{CompiledCache: cache}
	for range 2 {
		result, err := ExecuteSQLQueryParameters(context.Background(), c213CompiledPlanCacheQuery, nil, nil, options)
		if err != nil {
			t.Fatal(err)
		}
		if want := []SQLRow{{"id": int64(2)}, {"id": int64(3)}}; !reflect.DeepEqual(result.Rows, want) {
			t.Fatalf("compiled option rows = %#v, want %#v", result.Rows, want)
		}
	}
	stats := cache.Stats()
	if stats.Misses != 1 || stats.Hits != 1 {
		t.Fatalf("compiled option stats = %#v, want one miss and one hit", stats)
	}
}

func TestC213CompiledPlanCacheOptionPreservesParameterizedAndStreamExecution(t *testing.T) {
	cache, err := NewSQLCompiledQueryCache(SQLCompiledQueryCacheOptions{MaxEntries: 4, MaxBytes: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	parameterized := "FROM VALUES ($1) AS values(id) SELECT id"
	options := SQLQueryOptions{CompiledCache: cache}
	for _, value := range []int64{7, 11} {
		result, err := ExecuteSQLQueryParameters(context.Background(), parameterized, nil, []interface{}{value}, options)
		if err != nil {
			t.Fatal(err)
		}
		if want := []SQLRow{{"id": value}}; !reflect.DeepEqual(result.Rows, want) {
			t.Fatalf("parameterized rows = %#v, want %#v", result.Rows, want)
		}
	}
	var rows []SQLRow
	err = ExecuteSQLQueryRows(context.Background(), c213CompiledPlanCacheQuery, nil, nil, options, func(_ []string, row SQLRow) error {
		rows = append(rows, row)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := []SQLRow{{"id": int64(2)}, {"id": int64(3)}}; !reflect.DeepEqual(rows, want) {
		t.Fatalf("streamed rows = %#v, want %#v", rows, want)
	}
	stats := cache.Stats()
	if stats.Misses != 2 || stats.Hits != 1 {
		t.Fatalf("parameterized/stream cache stats = %#v, want two misses and one hit", stats)
	}
}

func TestC213CompiledPlanCacheOptionConcurrentExecutionIsReadOnly(t *testing.T) {
	cache, err := NewSQLCompiledQueryCache(SQLCompiledQueryCacheOptions{MaxEntries: 2, MaxBytes: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	options := SQLQueryOptions{CompiledCache: cache}
	const workers = 8
	const executions = 25
	errors := make(chan error, workers)
	var wait sync.WaitGroup
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for range executions {
				result, err := ExecuteSQLQueryParameters(context.Background(), c213CompiledPlanCacheQuery, nil, nil, options)
				if err != nil {
					errors <- err
					return
				}
				if len(result.Rows) != 2 {
					errors <- fmt.Errorf("rows = %d, want 2", len(result.Rows))
					return
				}
			}
			errors <- nil
		}()
	}
	wait.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestC213CompiledPlanCacheRejectsInvalidBounds(t *testing.T) {
	for _, options := range []SQLCompiledQueryCacheOptions{
		{MaxEntries: 0, MaxBytes: 1},
		{MaxEntries: 1, MaxBytes: 0},
	} {
		if _, err := NewSQLCompiledQueryCache(options); err == nil {
			t.Fatalf("options %#v unexpectedly accepted", options)
		}
	}
}

func TestC213CompiledPlanCacheEvictsByEntryAndByteBounds(t *testing.T) {
	cache, err := NewSQLCompiledQueryCache(SQLCompiledQueryCacheOptions{MaxEntries: 2, MaxBytes: 10 << 10})
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{
		"FROM VALUES (1) AS values(id) SELECT id",
		"FROM VALUES (2) AS values(id) SELECT id",
		"FROM VALUES (3) AS values(id) SELECT id",
	} {
		if _, err := cache.Compile(source); err != nil {
			t.Fatal(err)
		}
	}
	stats := cache.Stats()
	if stats.Entries > 2 || stats.Bytes > stats.MaxBytes || stats.Evictions == 0 {
		t.Fatalf("cache exceeded bounds or did not evict: %#v", stats)
	}
}

func TestC213CompiledPlanCacheRejectsOversizedPlanWithoutStorage(t *testing.T) {
	cache, err := NewSQLCompiledQueryCache(SQLCompiledQueryCacheOptions{MaxEntries: 2, MaxBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	query, err := cache.Compile(c213CompiledPlanCacheQuery)
	if err != nil {
		t.Fatal(err)
	}
	if query == nil {
		t.Fatal("oversized compilation returned nil")
	}
	if stats := cache.Stats(); stats.Entries != 0 || stats.Bytes != 0 || stats.Oversized != 1 {
		t.Fatalf("oversized cache stats = %#v, want no stored plan", stats)
	}
}

func TestC213CompiledPlanCacheSeparatesSchemaVersions(t *testing.T) {
	cache, err := NewSQLCompiledQueryCache(SQLCompiledQueryCacheOptions{MaxEntries: 4, MaxBytes: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	first, err := cache.CompileWithSchemaVersion(c213CompiledPlanCacheQuery, "schema-1")
	if err != nil {
		t.Fatal(err)
	}
	second, err := cache.CompileWithSchemaVersion(c213CompiledPlanCacheQuery, "schema-2")
	if err != nil {
		t.Fatal(err)
	}
	if first == second || cache.Stats().Entries != 2 {
		t.Fatalf("schema-version cache did not isolate plans: %#v", cache.Stats())
	}
	if removed := cache.InvalidateSchemaVersion("schema-1"); removed != 1 {
		t.Fatalf("removed schema-1 entries = %d, want 1", removed)
	}
}

func TestC213CompiledPlanCacheConcurrentCompileSharesHandle(t *testing.T) {
	cache, err := NewSQLCompiledQueryCache(SQLCompiledQueryCacheOptions{MaxEntries: 2, MaxBytes: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	const workers = 8
	handles := make(chan *CompiledSQLQuery, workers)
	errors := make(chan error, workers)
	var wait sync.WaitGroup
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			query, err := cache.Compile(c213CompiledPlanCacheQuery)
			if err != nil {
				errors <- err
				return
			}
			handles <- query
		}()
	}
	wait.Wait()
	close(handles)
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
	var first *CompiledSQLQuery
	for query := range handles {
		if first == nil {
			first = query
		} else if query != first {
			t.Fatal("concurrent cache misses returned different handles")
		}
	}
}

var c213CompiledPlanCacheBenchmarkSink *CompiledSQLQuery

func BenchmarkC213CompilePlanBaseline(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		query, err := CompileSQLQuery(c213CompiledPlanCacheQuery)
		if err != nil {
			b.Fatal(err)
		}
		c213CompiledPlanCacheBenchmarkSink = query
	}
}

func BenchmarkC213CompilePlanCached(b *testing.B) {
	cache, err := NewSQLCompiledQueryCache(SQLCompiledQueryCacheOptions{MaxEntries: 8, MaxBytes: 64 << 10})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := cache.Compile(c213CompiledPlanCacheQuery); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for range b.N {
		query, err := cache.Compile(c213CompiledPlanCacheQuery)
		if err != nil {
			b.Fatal(err)
		}
		c213CompiledPlanCacheBenchmarkSink = query
	}
}
