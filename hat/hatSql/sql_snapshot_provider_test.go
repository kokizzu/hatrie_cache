package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

var errSnapshotProviderTest = errors.New("snapshot provider test failure")

type snapshotProviderTestResolver struct {
	liveRows      []Row
	snapshotRows  []Row
	snapshotErr   error
	beginErr      error
	beginCalls    int
	liveCalls     int
	snapshotCalls int
	releaseCalls  int
	seenContext   context.Context
}

func (resolver *snapshotProviderTestResolver) ResolveSQLSource(string, string) ([]Row, error) {
	resolver.liveCalls++
	return resolver.liveRows, nil
}

func (resolver *snapshotProviderTestResolver) BeginSQLSnapshot(ctx context.Context) (SQLSourceResolver, func(), error) {
	resolver.beginCalls++
	resolver.seenContext = ctx
	if resolver.beginErr != nil {
		return nil, nil, resolver.beginErr
	}
	snapshot := SourceResolverFunc(func(string, string) ([]Row, error) {
		resolver.snapshotCalls++
		return resolver.snapshotRows, resolver.snapshotErr
	})
	return snapshot, func() { resolver.releaseCalls++ }, nil
}

func TestSQLSnapshotProviderUsesOneImmutableView(t *testing.T) {
	resolver := &snapshotProviderTestResolver{
		liveRows:     []Row{{"id": int64(99), "state": "live"}},
		snapshotRows: []Row{{"id": int64(7), "state": "ready"}},
	}
	ctx := context.WithValue(context.Background(), "snapshot-test", "request")
	result, err := ExecuteSQLQueryParameters(ctx, "FROM CACHE('items') WHERE state = 'ready' SELECT id", resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []SQLRow{{"id": int64(7)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
	if resolver.beginCalls != 1 || resolver.snapshotCalls != 1 || resolver.liveCalls != 0 || resolver.releaseCalls != 1 {
		t.Fatalf("provider calls = begin:%d snapshot:%d live:%d release:%d", resolver.beginCalls, resolver.snapshotCalls, resolver.liveCalls, resolver.releaseCalls)
	}
	if resolver.seenContext != ctx {
		t.Fatal("snapshot provider did not receive query context")
	}
}

func TestSQLSnapshotProviderCoversMultipleSourceReads(t *testing.T) {
	resolver := &snapshotProviderTestResolver{
		liveRows:     []Row{{"id": int64(99)}},
		snapshotRows: []Row{{"id": int64(7)}},
	}
	result, err := ExecuteSQLQueryParameters(context.Background(), "FROM CACHE('items') AS left JOIN CACHE('owners') AS right ON left.id = right.id SELECT left.id", resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []SQLRow{{"id": int64(7)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
	if resolver.liveCalls != 0 || resolver.snapshotCalls != 2 || resolver.releaseCalls != 1 {
		t.Fatalf("provider calls = snapshot:%d live:%d release:%d", resolver.snapshotCalls, resolver.liveCalls, resolver.releaseCalls)
	}
}

func TestSQLSnapshotProviderReleasesAfterExecutionError(t *testing.T) {
	resolver := &snapshotProviderTestResolver{
		snapshotRows: []Row{{"id": int64(7)}},
		snapshotErr:  errSnapshotProviderTest,
	}
	_, err := ExecuteSQLQueryParameters(context.Background(), "FROM CACHE('items') SELECT id", resolver, nil, SQLQueryOptions{})
	if !errors.Is(err, errSnapshotProviderTest) {
		t.Fatalf("error = %v, want %v", err, errSnapshotProviderTest)
	}
	if resolver.releaseCalls != 1 {
		t.Fatalf("release calls = %d, want 1", resolver.releaseCalls)
	}
}

func TestSQLSnapshotProviderWorksForStreamedRows(t *testing.T) {
	resolver := &snapshotProviderTestResolver{
		snapshotRows: []Row{{"id": int64(1)}, {"id": int64(2)}},
	}
	var rows []SQLRow
	err := ExecuteSQLQueryRows(context.Background(), "FROM CACHE('items') SELECT id", resolver, nil, SQLQueryOptions{}, func(_ []string, row SQLRow) error {
		rows = append(rows, row)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := []SQLRow{{"id": int64(1)}, {"id": int64(2)}}; !reflect.DeepEqual(rows, want) {
		t.Fatalf("streamed rows = %#v, want %#v", rows, want)
	}
	if resolver.liveCalls != 0 || resolver.snapshotCalls != 1 || resolver.releaseCalls != 1 {
		t.Fatalf("provider calls = snapshot:%d live:%d release:%d", resolver.snapshotCalls, resolver.liveCalls, resolver.releaseCalls)
	}
}

func TestSQLSnapshotProviderPropagatesBeginError(t *testing.T) {
	resolver := &snapshotProviderTestResolver{beginErr: errSnapshotProviderTest}
	_, err := ExecuteSQLQueryParameters(context.Background(), "FROM CACHE('items') SELECT id", resolver, nil, SQLQueryOptions{})
	if !errors.Is(err, errSnapshotProviderTest) {
		t.Fatalf("error = %v, want %v", err, errSnapshotProviderTest)
	}
	if resolver.liveCalls != 0 || resolver.releaseCalls != 0 {
		t.Fatalf("provider calls = live:%d release:%d", resolver.liveCalls, resolver.releaseCalls)
	}
}

type nilSnapshotProviderTestResolver struct{}

func (nilSnapshotProviderTestResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, nil
}

func (nilSnapshotProviderTestResolver) BeginSQLSnapshot(context.Context) (SQLSourceResolver, func(), error) {
	return nil, nil, nil
}

func TestSQLSnapshotProviderRejectsNilSnapshotResolver(t *testing.T) {
	_, err := ExecuteSQLQueryParameters(context.Background(), "FROM CACHE('items') SELECT id", nilSnapshotProviderTestResolver{}, nil, SQLQueryOptions{})
	if err == nil {
		t.Fatal("query unexpectedly succeeded")
	}
}
