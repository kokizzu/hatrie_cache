package hatSql

import (
	"sync"
	"testing"
	"time"
)

const m049CompiledQueryCacheSingleflightSource = "FROM VALUES (1), (2) AS src(id) SELECT src.id WHERE src.id > 0"

func TestSQLCompiledQueryCacheCoalescesConcurrentExactMisses(t *testing.T) {
	cache, err := NewSQLCompiledQueryCache(SQLCompiledQueryCacheOptions{MaxEntries: 8, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("NewSQLCompiledQueryCache() error = %v", err)
	}

	const workers = 32
	compileStarted := make(chan struct{})
	releaseCompile := make(chan struct{})
	cache.compile = func(source string) (*CompiledSQLQuery, error) {
		close(compileStarted)
		<-releaseCompile
		return CompileSQLQuery(source)
	}
	results := make(chan *CompiledSQLQuery, workers)
	errors := make(chan error, workers)
	var group sync.WaitGroup
	group.Add(1)
	go func() {
		defer group.Done()
		query, compileErr := cache.Compile(m049CompiledQueryCacheSingleflightSource)
		results <- query
		errors <- compileErr
	}()
	<-compileStarted
	for index := 1; index < workers; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			query, compileErr := cache.Compile(m049CompiledQueryCacheSingleflightSource)
			results <- query
			errors <- compileErr
		}()
	}
	waitForCompiledQueryCacheCoalesced(t, cache, workers-1)
	close(releaseCompile)
	group.Wait()
	close(results)
	close(errors)

	var first *CompiledSQLQuery
	for query := range results {
		if query == nil {
			t.Fatal("Compile() returned nil query")
		}
		if first == nil {
			first = query
			continue
		}
		if query != first {
			t.Fatal("concurrent cache misses returned different compiled query pointers")
		}
	}
	for compileErr := range errors {
		if compileErr != nil {
			t.Fatalf("Compile() error = %v", compileErr)
		}
	}

	stats := cache.Stats()
	if stats.Misses != 1 {
		t.Fatalf("cache misses = %d, want one leader miss", stats.Misses)
	}
	if stats.Coalesced != workers-1 {
		t.Fatalf("coalesced calls = %d, want %d", stats.Coalesced, workers-1)
	}
}

func TestSQLCompiledQueryCacheCoalescesConcurrentCompileErrors(t *testing.T) {
	cache, err := NewSQLCompiledQueryCache(SQLCompiledQueryCacheOptions{MaxEntries: 8, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("NewSQLCompiledQueryCache() error = %v", err)
	}

	const workers = 16
	compileStarted := make(chan struct{})
	releaseCompile := make(chan struct{})
	cache.compile = func(source string) (*CompiledSQLQuery, error) {
		close(compileStarted)
		<-releaseCompile
		return CompileSQLQuery(source)
	}
	errors := make(chan error, workers)
	var group sync.WaitGroup
	group.Add(1)
	go func() {
		defer group.Done()
		_, compileErr := cache.Compile("SELECT")
		errors <- compileErr
	}()
	<-compileStarted
	for index := 1; index < workers; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			_, compileErr := cache.Compile("SELECT")
			errors <- compileErr
		}()
	}
	waitForCompiledQueryCacheCoalesced(t, cache, workers-1)
	close(releaseCompile)
	group.Wait()
	close(errors)
	for compileErr := range errors {
		if compileErr == nil {
			t.Fatal("Compile() accepted invalid SQL")
		}
	}
	if stats := cache.Stats(); stats.Coalesced != workers-1 {
		t.Fatalf("coalesced error calls = %d, want %d", stats.Coalesced, workers-1)
	}
}

func waitForCompiledQueryCacheCoalesced(t *testing.T, cache *SQLCompiledQueryCache, want uint64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cache.Stats().Coalesced == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("cache coalesced calls did not reach %d; got %d", want, cache.Stats().Coalesced)
}

func BenchmarkSQLCompiledQueryCacheConcurrentMiss(b *testing.B) {
	const workers = 16
	for iteration := 0; iteration < b.N; iteration++ {
		cache, err := NewSQLCompiledQueryCache(SQLCompiledQueryCacheOptions{MaxEntries: 8, MaxBytes: 1 << 20})
		if err != nil {
			b.Fatalf("NewSQLCompiledQueryCache() error = %v", err)
		}
		start := make(chan struct{})
		var group sync.WaitGroup
		group.Add(workers)
		for index := 0; index < workers; index++ {
			go func() {
				defer group.Done()
				<-start
				if _, err := cache.Compile(m049CompiledQueryCacheSingleflightSource); err != nil {
					b.Errorf("Compile() error = %v", err)
				}
			}()
		}
		close(start)
		group.Wait()
	}
}
