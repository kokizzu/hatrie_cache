package hatSql

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func BenchmarkSQLResultCacheVersionedMiss(b *testing.B) {
	result := QueryResult{Columns: []string{"value"}, Rows: []Row{{"value": int64(1)}}}
	cache := NewSQLResultCache(1)
	version := "v1"
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if index%2 != 0 {
			version = "v2"
		} else {
			version = "v1"
		}
		_, err := cache.ExecuteVersioned(
			context.Background(),
			"baseline-read-expression",
			func() (string, bool) { return version, true },
			func(context.Context) (QueryResult, error) { return result, nil },
		)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMaintainedResultCacheVersionedMiss(b *testing.B) {
	result := QueryResult{Columns: []string{"value"}, Rows: []Row{{"value": int64(1)}}}
	cache := NewMaintainedResultCache(1)
	version := "v1"
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if index%2 != 0 {
			version = "v2"
		} else {
			version = "v1"
		}
		_, err := cache.ExecuteVersioned(
			context.Background(),
			"maintained-read-expression",
			func() (string, bool) { return version, true },
			func(context.Context) (QueryResult, error) { return result, nil },
		)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkResultCacheConcurrentDuplicateMiss(b *testing.B) {
	benchmarkConcurrentDuplicateMiss(b, NewSQLResultCache, false)
}

func BenchmarkMaintainedResultCacheConcurrentDuplicateMiss(b *testing.B) {
	benchmarkConcurrentDuplicateMiss(b, NewMaintainedResultCache, true)
}

func BenchmarkResultCacheConcurrentSerializedMiss(b *testing.B) {
	benchmarkConcurrentDuplicateMissWithWork(b, NewSQLResultCache, false, true)
}

func BenchmarkMaintainedResultCacheConcurrentSerializedMiss(b *testing.B) {
	benchmarkConcurrentDuplicateMissWithWork(b, NewMaintainedResultCache, true, true)
}

func benchmarkConcurrentDuplicateMiss(b *testing.B, newCache func(int) *SQLResultCache, waitForCoalescing bool) {
	benchmarkConcurrentDuplicateMissWithWork(b, newCache, waitForCoalescing, false)
}

var benchmarkMaintainedResultCacheWork uint64

func benchmarkConcurrentDuplicateMissWithWork(b *testing.B, newCache func(int) *SQLResultCache, waitForCoalescing bool, serializeExecution bool) {
	const callers = 8
	result := QueryResult{Columns: []string{"value"}, Rows: []Row{{"value": int64(1)}}}
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		cache := newCache(callers)
		started := make(chan struct{}, callers)
		release := make(chan struct{})
		var executionMu sync.Mutex
		var executions atomic.Int32
		execute := func(context.Context) (QueryResult, error) {
			executions.Add(1)
			started <- struct{}{}
			<-release
			if serializeExecution {
				executionMu.Lock()
				for index := uint64(0); index < 20000; index++ {
					benchmarkMaintainedResultCacheWork += index
				}
				executionMu.Unlock()
			}
			return result, nil
		}
		var waiters sync.WaitGroup
		for index := 0; index < callers; index++ {
			waiters.Add(1)
			go func() {
				defer waiters.Done()
				_, _ = cache.ExecuteVersioned(
					context.Background(),
					"same-concurrent-expression",
					func() (string, bool) { return "v1", true },
					execute,
				)
			}()
		}
		if waitForCoalescing {
			for cache.Stats().Coalesced < callers-1 {
				runtime.Gosched()
			}
		} else {
			for executions.Load() < callers {
				runtime.Gosched()
			}
		}
		close(release)
		waiters.Wait()
	}
}
