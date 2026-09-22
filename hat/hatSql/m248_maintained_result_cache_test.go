package hatSql

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMaintainedResultCacheCoalescesIdenticalVersionedMisses(t *testing.T) {
	const callers = 8

	cache := NewMaintainedResultCache(callers)
	started := make(chan struct{})
	release := make(chan struct{})
	var startOnce sync.Once
	var executions atomic.Int32
	execute := func(context.Context) (QueryResult, error) {
		executions.Add(1)
		startOnce.Do(func() { close(started) })
		<-release
		return QueryResult{
			Columns: []string{"value"},
			Rows:    []Row{{"value": "shared"}},
		}, nil
	}

	results := make([]QueryResult, callers)
	errors := make([]error, callers)
	var waiters sync.WaitGroup
	call := func(index int) {
		defer waiters.Done()
		results[index], errors[index] = cache.ExecuteVersioned(
			context.Background(),
			"same-read-expression",
			func() (string, bool) { return "v1", true },
			execute,
		)
	}

	waiters.Add(1)
	go call(0)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("maintained result cache executor did not start")
	}
	for index := 1; index < callers; index++ {
		waiters.Add(1)
		go call(index)
	}

	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		if cache.Stats().Coalesced == callers-1 {
			break
		}
		select {
		case <-deadline.C:
			t.Fatalf("coalesced calls = %d, want %d", cache.Stats().Coalesced, callers-1)
		default:
			time.Sleep(time.Millisecond)
		}
	}
	close(release)
	waiters.Wait()

	if got := executions.Load(); got != 1 {
		t.Fatalf("executor calls = %d, want 1", got)
	}
	for index := range results {
		if errors[index] != nil {
			t.Fatalf("call %d error = %v", index, errors[index])
		}
		if got := results[index].Rows[0]["value"]; got != "shared" {
			t.Fatalf("call %d value = %#v, want shared", index, got)
		}
	}
	stats := cache.Stats()
	if stats.Entries != 1 {
		t.Fatalf("cache entries = %d, want 1", stats.Entries)
	}
	if stats.Coalesced != callers-1 {
		t.Fatalf("coalesced calls = %d, want %d", stats.Coalesced, callers-1)
	}
}

func TestMaintainedResultCacheSharesExecutorErrors(t *testing.T) {
	const callers = 4

	cache := NewMaintainedResultCache(callers)
	started := make(chan struct{})
	release := make(chan struct{})
	wantErr := context.DeadlineExceeded
	var startOnce sync.Once
	var executions atomic.Int32
	execute := func(context.Context) (QueryResult, error) {
		executions.Add(1)
		startOnce.Do(func() { close(started) })
		<-release
		return QueryResult{}, wantErr
	}

	errors := make([]error, callers)
	var waiters sync.WaitGroup
	call := func(index int) {
		defer waiters.Done()
		_, errors[index] = cache.ExecuteVersioned(
			context.Background(),
			"same-error-expression",
			func() (string, bool) { return "v1", true },
			execute,
		)
	}
	waiters.Add(1)
	go call(0)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("maintained result cache error executor did not start")
	}
	for index := 1; index < callers; index++ {
		waiters.Add(1)
		go call(index)
	}
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		if cache.Stats().Coalesced == callers-1 {
			break
		}
		select {
		case <-deadline.C:
			t.Fatalf("coalesced error calls = %d, want %d", cache.Stats().Coalesced, callers-1)
		default:
			time.Sleep(time.Millisecond)
		}
	}
	close(release)
	waiters.Wait()

	if got := executions.Load(); got != 1 {
		t.Fatalf("executor calls = %d, want 1", got)
	}
	for index, err := range errors {
		if err != wantErr {
			t.Errorf("call %d error = %v, want %v", index, err, wantErr)
		}
	}
}

func TestMaintainedResultCacheDependencyInvalidation(t *testing.T) {
	cache := NewMaintainedResultCacheWithDependencies(2)
	dependencies := []ResultCacheDependency{{Kind: "table", Key: "people"}}
	executions := 0
	execute := func(context.Context) (QueryResult, error) {
		executions++
		return QueryResult{
			Columns: []string{"execution"},
			Rows:    []Row{{"execution": executions}},
		}, nil
	}

	first, err := cache.ExecuteWithDependencies(context.Background(), "people-read", dependencies, execute)
	if err != nil {
		t.Fatalf("first ExecuteWithDependencies() error = %v", err)
	}
	second, err := cache.ExecuteWithDependencies(context.Background(), "people-read", dependencies, execute)
	if err != nil {
		t.Fatalf("second ExecuteWithDependencies() error = %v", err)
	}
	if executions != 1 || first.Rows[0]["execution"] != 1 || second.Rows[0]["execution"] != 1 {
		t.Fatalf("cached results = %#v and %#v after %d executions", first, second, executions)
	}
	if removed := cache.InvalidateDependency("table", "people"); removed != 1 {
		t.Fatalf("InvalidateDependency() removed %d entries, want 1", removed)
	}
	third, err := cache.ExecuteWithDependencies(context.Background(), "people-read", dependencies, execute)
	if err != nil {
		t.Fatalf("third ExecuteWithDependencies() error = %v", err)
	}
	if executions != 2 || third.Rows[0]["execution"] != 2 {
		t.Fatalf("refreshed result = %#v after %d executions, want execution 2", third, executions)
	}
}

func TestMaintainedResultCacheRetriesWaitersAfterVersionChange(t *testing.T) {
	const callers = 4

	cache := NewMaintainedResultCache(callers)
	started := make(chan struct{})
	release := make(chan struct{})
	var startOnce sync.Once
	var executions atomic.Int32
	var version atomic.Value
	version.Store("v1")
	execute := func(context.Context) (QueryResult, error) {
		execution := executions.Add(1)
		if execution == 1 {
			startOnce.Do(func() {
				close(started)
				version.Store("v2")
			})
			<-release
		}
		return QueryResult{
			Columns: []string{"version"},
			Rows:    []Row{{"version": version.Load().(string)}},
		}, nil
	}

	results := make([]QueryResult, callers)
	errors := make([]error, callers)
	var waiters sync.WaitGroup
	call := func(index int) {
		defer waiters.Done()
		results[index], errors[index] = cache.ExecuteVersioned(
			context.Background(),
			"changing-read-expression",
			func() (string, bool) { return version.Load().(string), true },
			execute,
		)
	}

	waiters.Add(1)
	go call(0)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("version-changing executor did not start")
	}
	for index := 1; index < callers; index++ {
		waiters.Add(1)
		go call(index)
	}
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		if cache.Stats().Coalesced == callers-1 {
			break
		}
		select {
		case <-deadline.C:
			t.Fatalf("coalesced version-changing calls = %d, want %d", cache.Stats().Coalesced, callers-1)
		default:
			time.Sleep(time.Millisecond)
		}
	}
	close(release)
	waiters.Wait()

	if got := executions.Load(); got != 2 {
		t.Fatalf("executor calls = %d, want 2 after version change", got)
	}
	for index, err := range errors {
		if err != nil {
			t.Fatalf("call %d error = %v", index, err)
		}
	}
	final, err := cache.ExecuteVersioned(
		context.Background(),
		"changing-read-expression",
		func() (string, bool) { return "v2", true },
		execute,
	)
	if err != nil {
		t.Fatalf("final cached read error = %v", err)
	}
	if got := final.Rows[0]["version"]; got != "v2" {
		t.Fatalf("final cached version = %#v, want v2", got)
	}
}
