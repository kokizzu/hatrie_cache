package hatSql

import (
	"context"
	"errors"
	"testing"
	"time"
)

type frontierBoundSnapshotProviderResult struct {
	resolver SQLSourceResolver
	err      error
}

type frontierBoundSnapshotProviderTestResolver struct {
	rows         []Row
	beginErr     error
	beginCalls   int
	seenFrontier uint64
	releaseCalls int
	nilSnapshot  bool
}

func (resolver *frontierBoundSnapshotProviderTestResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return resolver.rows, nil
}

func (resolver *frontierBoundSnapshotProviderTestResolver) BeginSQLSnapshotAt(_ context.Context, frontier uint64) (SQLSourceResolver, func(), error) {
	resolver.beginCalls++
	resolver.seenFrontier = frontier
	release := func() {
		resolver.releaseCalls++
	}
	if resolver.beginErr != nil {
		return nil, release, resolver.beginErr
	}
	if resolver.nilSnapshot {
		return nil, release, nil
	}
	return SourceResolverFunc(func(string, string) ([]Row, error) {
		return resolver.rows, nil
	}), release, nil
}

type legacyFrontierSnapshotResolver struct{}

func (legacyFrontierSnapshotResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, nil
}

func newFrontierSnapshotTestBarrier(t *testing.T, count int) *SQLSourceFrontierBarrier {
	t.Helper()
	partitions := make([]SQLSourceFrontierPartition, count)
	for i := range partitions {
		partitions[i] = SQLSourceFrontierPartition{Source: "source", Partition: string(rune('a' + i))}
	}
	barrier, err := NewSQLSourceFrontierBarrierFromPartitions(partitions)
	if err != nil {
		t.Fatal(err)
	}
	return barrier
}

func TestBeginSQLFrontierSnapshotWaitsAndPassesExactFrontier(t *testing.T) {
	barrier := newFrontierSnapshotTestBarrier(t, 2)
	provider := &frontierBoundSnapshotProviderTestResolver{rows: []Row{{"id": int64(1)}}}
	result := make(chan frontierBoundSnapshotProviderResult, 1)
	go func() {
		resolver, release, err := BeginSQLFrontierSnapshot(context.Background(), provider, barrier, 9)
		if release != nil {
			release()
		}
		result <- frontierBoundSnapshotProviderResult{resolver: resolver, err: err}
	}()

	select {
	case got := <-result:
		t.Fatalf("snapshot returned before frontier was ready: %+v", got)
	case <-time.After(20 * time.Millisecond):
	}
	if provider.beginCalls != 0 {
		t.Fatalf("begin calls = %d before frontier readiness, want 0", provider.beginCalls)
	}
	if _, err := barrier.Observe(SQLSourceFrontier{Source: "source", Partition: "a", Frontier: 9}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-result:
		t.Fatalf("snapshot returned with one partition missing: %+v", got)
	case <-time.After(20 * time.Millisecond):
	}
	if _, err := barrier.Observe(SQLSourceFrontier{Source: "source", Partition: "b", Frontier: 9}); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-result:
		if got.err != nil {
			t.Fatal(got.err)
		}
		if got.resolver == nil {
			t.Fatal("snapshot resolver is nil")
		}
		if provider.seenFrontier != 9 {
			t.Fatalf("provider frontier = %d, want 9", provider.seenFrontier)
		}
		if provider.beginCalls != 1 || provider.releaseCalls != 1 {
			t.Fatalf("provider lifecycle = begin %d release %d, want 1 and 1", provider.beginCalls, provider.releaseCalls)
		}
	case <-time.After(time.Second):
		t.Fatal("frontier-bound snapshot did not start after the common frontier was ready")
	}
}

func TestBeginSQLFrontierSnapshotRequiresFrontierProvider(t *testing.T) {
	barrier := newFrontierSnapshotTestBarrier(t, 1)
	if _, err := barrier.Observe(SQLSourceFrontier{Source: "source", Partition: "a", Frontier: 1}); err != nil {
		t.Fatal(err)
	}
	_, _, err := BeginSQLFrontierSnapshot(context.Background(), legacyFrontierSnapshotResolver{}, barrier, 1)
	if !errors.Is(err, ErrSQLFrontierSnapshotProviderUnsupported) {
		t.Fatalf("error = %v, want ErrSQLFrontierSnapshotProviderUnsupported", err)
	}
}

func TestBeginSQLFrontierSnapshotPreservesErrorsAndReleasesNilSnapshots(t *testing.T) {
	barrier := newFrontierSnapshotTestBarrier(t, 1)
	if _, err := barrier.Observe(SQLSourceFrontier{Source: "source", Partition: "a", Frontier: 1}); err != nil {
		t.Fatal(err)
	}

	beginErr := errors.New("begin failed")
	provider := &frontierBoundSnapshotProviderTestResolver{beginErr: beginErr}
	_, release, err := BeginSQLFrontierSnapshot(context.Background(), provider, barrier, 1)
	if !errors.Is(err, beginErr) || release != nil {
		t.Fatalf("begin error = %v release-non-nil=%t, want original error and nil release", err, release != nil)
	}
	if provider.releaseCalls != 0 {
		t.Fatalf("release calls after begin error = %d, want 0", provider.releaseCalls)
	}

	provider = &frontierBoundSnapshotProviderTestResolver{nilSnapshot: true}
	_, release, err = BeginSQLFrontierSnapshot(context.Background(), provider, barrier, 1)
	if !errors.Is(err, ErrSQLSnapshotResolverNil) || release != nil {
		t.Fatalf("nil snapshot error = %v release-non-nil=%t, want ErrSQLSnapshotResolverNil and nil release", err, release != nil)
	}
	if provider.releaseCalls != 1 {
		t.Fatalf("release calls after nil snapshot = %d, want 1", provider.releaseCalls)
	}
}

func TestBeginSQLFrontierSnapshotCancellationAndValidation(t *testing.T) {
	barrier := newFrontierSnapshotTestBarrier(t, 1)
	provider := &frontierBoundSnapshotProviderTestResolver{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, release, err := BeginSQLFrontierSnapshot(ctx, provider, barrier, 1)
	if !errors.Is(err, context.Canceled) || release != nil {
		t.Fatalf("canceled error = %v release-non-nil=%t, want context.Canceled and nil release", err, release != nil)
	}
	if provider.beginCalls != 0 {
		t.Fatalf("provider begin calls after cancellation = %d, want 0", provider.beginCalls)
	}

	_, _, err = BeginSQLFrontierSnapshot(nil, provider, barrier, 1)
	if !errors.Is(err, ErrSQLSourceFrontierBarrierContextNil) {
		t.Fatalf("nil context error = %v, want ErrSQLSourceFrontierBarrierContextNil", err)
	}
	_, _, err = BeginSQLFrontierSnapshot(context.Background(), provider, nil, 1)
	if !errors.Is(err, ErrSQLSourceFrontierBarrierNil) {
		t.Fatalf("nil barrier error = %v, want ErrSQLSourceFrontierBarrierNil", err)
	}
}
