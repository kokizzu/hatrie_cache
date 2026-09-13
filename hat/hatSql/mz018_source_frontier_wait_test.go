package hatSql

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type mz018SourceFrontierResolver struct {
	calls     atomic.Int32
	frontier  uint64
	readyAt   int32
	succeedAt int32
}

func (resolver *mz018SourceFrontierResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return []Row{{"id": int64(1)}}, nil
}

func (resolver *mz018SourceFrontierResolver) SQLSourceFrontier(string, string) (uint64, bool, bool, error) {
	call := resolver.calls.Add(1)
	if resolver.readyAt > 0 && call < resolver.readyAt {
		return resolver.frontier, false, true, nil
	}
	if resolver.succeedAt > 0 && call >= resolver.succeedAt {
		return resolver.frontier, true, true, nil
	}
	return resolver.frontier - 1, true, true, nil
}

func (resolver *mz018SourceFrontierResolver) StreamSQLOrderedSourceAfter(ctx context.Context, name, key, field string, desc, nullsFirst, nullsLast bool, after KeysetPosition, visit func(Row, KeysetPosition) error) (bool, error) {
	if err := ctx.Err(); err != nil {
		return true, err
	}
	if err := visit(Row{"id": int64(1)}, KeysetPosition{Value: int64(1), Valid: true}); err != nil {
		return true, err
	}
	return true, nil
}

func TestMZ018SourceFrontierWaitsUntilFresh(t *testing.T) {
	resolver := &mz018SourceFrontierResolver{frontier: 5, succeedAt: 2}
	result, err := ExecuteSQLQueryContext(context.Background(), "SELECT id FROM CACHE('users')", resolver, SQLQueryOptions{
		RequireSourceFrontier:      true,
		RequiredSourceFrontier:     5,
		SourceFrontierWaitTimeout:  100 * time.Millisecond,
		SourceFrontierWaitInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	if len(result.Rows) != 1 || resolver.calls.Load() < 2 {
		t.Fatalf("result/calls = %d/%d, want one row and at least two frontier checks", len(result.Rows), resolver.calls.Load())
	}
}

func TestMZ018SourceFrontierWaitsUntilReady(t *testing.T) {
	resolver := &mz018SourceFrontierResolver{frontier: 5, readyAt: 2, succeedAt: 2}
	result, err := ExecuteSQLQueryContext(context.Background(), "SELECT id FROM CACHE('users')", resolver, SQLQueryOptions{
		RequireSourceFrontier:      true,
		RequiredSourceFrontier:     5,
		SourceFrontierWaitTimeout:  100 * time.Millisecond,
		SourceFrontierWaitInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	if len(result.Rows) != 1 || resolver.calls.Load() < 2 {
		t.Fatalf("result/calls = %d/%d, want one row and at least two frontier checks", len(result.Rows), resolver.calls.Load())
	}
}

func TestMZ018SourceFrontierWaitTimesOut(t *testing.T) {
	resolver := &mz018SourceFrontierResolver{frontier: 5}
	_, err := ExecuteSQLQueryContext(context.Background(), "SELECT id FROM CACHE('users')", resolver, SQLQueryOptions{
		RequireSourceFrontier:      true,
		RequiredSourceFrontier:     5,
		SourceFrontierWaitTimeout:  20 * time.Millisecond,
		SourceFrontierWaitInterval: time.Millisecond,
	})
	if !errors.Is(err, ErrSQLSourceFrontierWaitTimeout) {
		t.Fatalf("error = %v, want ErrSQLSourceFrontierWaitTimeout", err)
	}
	if resolver.calls.Load() < 2 {
		t.Fatalf("frontier checks = %d, want polling", resolver.calls.Load())
	}
}

func TestMZ018SourceFrontierWaitHonorsContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	resolver := &mz018SourceFrontierResolver{frontier: 5}
	_, err := ExecuteSQLQueryContext(ctx, "SELECT id FROM CACHE('users')", resolver, SQLQueryOptions{
		RequireSourceFrontier:      true,
		RequiredSourceFrontier:     5,
		SourceFrontierWaitTimeout:  time.Second,
		SourceFrontierWaitInterval: 50 * time.Millisecond,
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context.DeadlineExceeded", err)
	}
}

func TestMZ018SourceFrontierDefaultStillRejectsWithoutWait(t *testing.T) {
	resolver := &mz018SourceFrontierResolver{frontier: 5}
	_, err := ExecuteSQLQueryContext(context.Background(), "SELECT id FROM CACHE('users')", resolver, SQLQueryOptions{
		RequireSourceFrontier:  true,
		RequiredSourceFrontier: 5,
	})
	if !errors.Is(err, ErrSQLSourceFrontierBehind) {
		t.Fatalf("error = %v, want ErrSQLSourceFrontierBehind", err)
	}
	if resolver.calls.Load() != 1 {
		t.Fatalf("frontier checks = %d, want one check in default mode", resolver.calls.Load())
	}
}

func TestMZ018SourceFrontierWaitAppliesToKeysetPage(t *testing.T) {
	resolver := &mz018SourceFrontierResolver{frontier: 5, succeedAt: 2}
	result, err := ExecuteSQLQueryKeysetPage(context.Background(), "SELECT e.id FROM CACHE('events') AS e ORDER BY e.id", resolver, nil, SQLQueryOptions{
		RequireSourceFrontier:      true,
		RequiredSourceFrontier:     5,
		SourceFrontierWaitTimeout:  100 * time.Millisecond,
		SourceFrontierWaitInterval: time.Millisecond,
	}, 5, "")
	if err != nil {
		t.Fatalf("ExecuteSQLQueryKeysetPage() error = %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("keyset rows = %#v, want one row", result.Rows)
	}
	if resolver.calls.Load() < 2 {
		t.Fatalf("frontier checks = %d, want at least two checks", resolver.calls.Load())
	}
}

func TestMZ018SourceFrontierDefaultRejectsStaleKeysetPage(t *testing.T) {
	resolver := &mz018SourceFrontierResolver{frontier: 5}
	_, err := ExecuteSQLQueryKeysetPage(context.Background(), "SELECT e.id FROM CACHE('events') AS e ORDER BY e.id", resolver, nil, SQLQueryOptions{
		RequireSourceFrontier:  true,
		RequiredSourceFrontier: 5,
	}, 5, "")
	if !errors.Is(err, ErrSQLSourceFrontierBehind) {
		t.Fatalf("error = %v, want ErrSQLSourceFrontierBehind", err)
	}
	if resolver.calls.Load() != 1 {
		t.Fatalf("frontier checks = %d, want one check in default mode", resolver.calls.Load())
	}
}
