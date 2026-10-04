package hatSql

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestResultCacheAdmissionRejectsFastResultsWithoutChangingCorrectness(t *testing.T) {
	cache, err := NewSQLResultCacheWithAdmission(4, ResultCacheAdmissionPolicy{MinExecutionDuration: time.Millisecond})
	if err != nil {
		t.Fatalf("NewSQLResultCacheWithAdmission() error = %v", err)
	}
	version := func() (string, bool) { return "v1", true }
	executions := 0
	execute := func(context.Context) (QueryResult, error) {
		executions++
		return QueryResult{Rows: []Row{{"value": int64(executions)}}}, nil
	}
	first, err := cache.ExecuteVersioned(context.Background(), "fast", version, execute)
	if err != nil {
		t.Fatalf("first ExecuteVersioned() error = %v", err)
	}
	second, err := cache.ExecuteVersioned(context.Background(), "fast", version, execute)
	if err != nil {
		t.Fatalf("second ExecuteVersioned() error = %v", err)
	}
	if executions != 2 || first.Rows[0]["value"] != int64(1) || second.Rows[0]["value"] != int64(2) {
		t.Fatalf("fast admission executions/results = %d/%#v/%#v", executions, first, second)
	}
	if stats := cache.Stats(); stats.Entries != 0 || stats.Bypasses != 2 {
		t.Fatalf("fast admission cache stats = %#v", stats)
	}
	if stats := cache.AdmissionStats(); stats.Admitted != 0 || stats.Rejected != 2 {
		t.Fatalf("fast admission stats = %#v", stats)
	}
}

func TestResultCacheAdmissionAdmitsSlowResultsAndKeepsDefaultPath(t *testing.T) {
	cache, err := NewResultCacheWithAdmission(4, ResultCacheAdmissionPolicy{MinExecutionDuration: time.Millisecond})
	if err != nil {
		t.Fatalf("NewResultCacheWithAdmission() error = %v", err)
	}
	version := func() (string, bool) { return "v1", true }
	executions := 0
	executeSlow := func(context.Context) (QueryResult, error) {
		executions++
		time.Sleep(2 * time.Millisecond)
		return QueryResult{Rows: []Row{{"value": int64(executions)}}}, nil
	}
	if _, err := cache.ExecuteVersioned(context.Background(), "slow", version, executeSlow); err != nil {
		t.Fatalf("slow miss error = %v", err)
	}
	if got, err := cache.ExecuteVersioned(context.Background(), "slow", version, executeSlow); err != nil || got.Rows[0]["value"] != int64(1) {
		t.Fatalf("slow hit = %#v/%v", got, err)
	}
	if executions != 1 || cache.Stats().Entries != 1 {
		t.Fatalf("slow admission executions/entries = %d/%d", executions, cache.Stats().Entries)
	}
	if stats := cache.AdmissionStats(); stats.Admitted != 1 || stats.Rejected != 0 {
		t.Fatalf("slow admission stats = %#v", stats)
	}

	defaultCache := NewSQLResultCache(1)
	executions = 0
	fast := func(context.Context) (QueryResult, error) {
		executions++
		return QueryResult{Rows: []Row{{"value": int64(executions)}}}, nil
	}
	if _, err := defaultCache.ExecuteVersioned(context.Background(), "default", version, fast); err != nil {
		t.Fatal(err)
	}
	if _, err := defaultCache.ExecuteVersioned(context.Background(), "default", version, fast); err != nil {
		t.Fatal(err)
	}
	if executions != 1 || defaultCache.Stats().Entries != 1 {
		t.Fatalf("default path executions/entries = %d/%d", executions, defaultCache.Stats().Entries)
	}
}

func TestResultCacheAdmissionRejectsInvalidPolicy(t *testing.T) {
	if _, err := NewResultCacheWithAdmission(1, ResultCacheAdmissionPolicy{MinExecutionDuration: -time.Nanosecond}); !errors.Is(err, ErrResultCacheAdmissionInvalid) {
		t.Fatalf("negative duration error = %v", err)
	}
	if _, err := NewSQLResultCacheWithAdmission(1, ResultCacheAdmissionPolicy{MinExecutionDuration: -time.Nanosecond}); !errors.Is(err, ErrResultCacheAdmissionInvalid) {
		t.Fatalf("SQL negative duration error = %v", err)
	}
}

func TestResultCacheAdmissionAppliesToPortableExecute(t *testing.T) {
	cache, err := NewResultCacheWithAdmission(2, ResultCacheAdmissionPolicy{MinExecutionDuration: time.Second})
	if err != nil {
		t.Fatalf("NewResultCacheWithAdmission() error = %v", err)
	}
	executions := 0
	execute := func(context.Context) (QueryResult, error) {
		executions++
		return QueryResult{Rows: []Row{{"value": int64(executions)}}}, nil
	}
	epoch := func() uint64 { return 1 }
	for index := 0; index < 2; index++ {
		if _, err := cache.Execute(context.Background(), "portable", epoch, execute); err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
	}
	if executions != 2 || cache.Stats().Entries != 0 {
		t.Fatalf("portable admission executions/entries = %d/%d", executions, cache.Stats().Entries)
	}
	if stats := cache.AdmissionStats(); stats.Admitted != 0 || stats.Rejected != 2 {
		t.Fatalf("portable admission stats = %#v", stats)
	}
}
