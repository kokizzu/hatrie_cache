package hatSql

import (
	"context"
	"errors"
	"testing"
)

type mz008TestResolver struct {
	live          []Row
	historical    []Row
	beginCalls    int
	beginFrontier uint64
	releases      int
	beginErr      error
}

func (resolver *mz008TestResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return resolver.live, nil
}

func (resolver *mz008TestResolver) SQLSourceVersion(string, string) (string, bool, error) {
	return "stable", true, nil
}

func (resolver *mz008TestResolver) BeginSQLSnapshotAt(ctx context.Context, frontier uint64) (SQLSourceResolver, func(), error) {
	resolver.beginCalls++
	resolver.beginFrontier = frontier
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if resolver.beginErr != nil {
		return nil, nil, resolver.beginErr
	}
	snapshot := mz008HistoricalResolver{rows: resolver.historical}
	return snapshot, func() { resolver.releases++ }, nil
}

type mz008HistoricalResolver struct {
	rows []Row
}

func (resolver mz008HistoricalResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return resolver.rows, nil
}

func (resolver mz008HistoricalResolver) SQLSourceVersion(string, string) (string, bool, error) {
	return "stable", true, nil
}

func (resolver mz008HistoricalResolver) StreamSQLOrderedSourceAfter(ctx context.Context, _ string, _ string, field string, _ bool, _ bool, _ bool, after KeysetPosition, visit func(Row, KeysetPosition) error) (bool, error) {
	for index, row := range resolver.rows {
		if err := ctx.Err(); err != nil {
			return true, err
		}
		position := KeysetPosition{Value: row[field], Tie: uint64(index), Valid: true}
		if after.Valid && position.Tie <= after.Tie {
			continue
		}
		if err := visit(row, position); err != nil {
			return true, err
		}
	}
	return true, nil
}

func TestExecuteSQLQueryAsOfUsesHistoricalResolverAndReleases(t *testing.T) {
	frontier := uint64(42)
	resolver := &mz008TestResolver{
		live:       []Row{{"id": int64(1), "value": "live"}},
		historical: []Row{{"id": int64(1), "value": "historical"}},
	}
	result, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('items') SELECT id, value", resolver, SQLQueryOptions{AsOfFrontier: &frontier})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["value"] != "historical" {
		t.Fatalf("historical result = %#v, want one historical row", result.Rows)
	}
	if resolver.beginCalls != 1 || resolver.beginFrontier != frontier || resolver.releases != 1 {
		t.Fatalf("snapshot lifecycle = calls %d frontier %d releases %d, want 1/%d/1", resolver.beginCalls, resolver.beginFrontier, resolver.releases, frontier)
	}
}

func TestExecuteSQLQueryAsOfSupportsStreamedRows(t *testing.T) {
	frontier := uint64(7)
	resolver := &mz008TestResolver{
		live:       []Row{{"id": int64(1), "value": "live"}},
		historical: []Row{{"id": int64(2), "value": "historical"}},
	}
	var rows []SQLRow
	err := ExecuteSQLQueryRows(context.Background(), "FROM CACHE('items') SELECT id, value", resolver, nil, SQLQueryOptions{AsOfFrontier: &frontier}, func(_ []string, row SQLRow) error {
		rows = append(rows, row)
		return nil
	})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryRows() error = %v", err)
	}
	if len(rows) != 1 || rows[0]["value"] != "historical" || resolver.releases != 1 {
		t.Fatalf("streamed historical rows = %#v releases=%d, want one row and one release", rows, resolver.releases)
	}
}

func TestExecuteSQLQueryPageAsOfUsesHistoricalResolver(t *testing.T) {
	frontier := uint64(5)
	resolver := &mz008TestResolver{
		live:       []Row{{"id": int64(1), "value": "live"}},
		historical: []Row{{"id": int64(2), "value": "historical"}},
	}
	result, err := ExecuteSQLQueryPage(context.Background(), "FROM CACHE('items') SELECT id, value", resolver, nil, SQLQueryOptions{AsOfFrontier: &frontier}, 1, "")
	if err != nil {
		t.Fatalf("ExecuteSQLQueryPage() error = %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["value"] != "historical" || resolver.releases != 1 {
		t.Fatalf("paged historical rows = %#v releases=%d, want one row and one release", result.Rows, resolver.releases)
	}
}

func TestExecuteSQLQueryKeysetPageAsOfUsesHistoricalResolver(t *testing.T) {
	frontier := uint64(6)
	resolver := &mz008TestResolver{
		live:       []Row{{"id": int64(1), "value": "live"}},
		historical: []Row{{"id": int64(2), "value": "historical"}},
	}
	result, err := ExecuteSQLQueryKeysetPage(context.Background(), "FROM CACHE('items') AS src SELECT src.id, src.value ORDER BY src.id", resolver, nil, SQLQueryOptions{AsOfFrontier: &frontier}, 1, "")
	if err != nil {
		t.Fatalf("ExecuteSQLQueryKeysetPage() error = %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["value"] != "historical" || resolver.releases != 1 {
		t.Fatalf("keyset historical rows = %#v releases=%d, want one row and one release", result.Rows, resolver.releases)
	}
}

func TestExecuteSQLQueryAsOfBypassesLiveResultCache(t *testing.T) {
	frontier := uint64(15)
	resolver := &mz008TestResolver{
		live:       []Row{{"id": int64(1), "value": "live"}},
		historical: []Row{{"id": int64(2), "value": "historical-1"}},
	}
	options := SQLQueryOptions{AsOfFrontier: &frontier, ResultCache: NewSQLResultCache(1)}
	query := "FROM CACHE('items') SELECT id, value"
	first, err := ExecuteSQLQueryContext(context.Background(), query, resolver, options)
	if err != nil {
		t.Fatalf("first historical query error = %v", err)
	}
	resolver.historical = []Row{{"id": int64(3), "value": "historical-2"}}
	second, err := ExecuteSQLQueryContext(context.Background(), query, resolver, options)
	if err != nil {
		t.Fatalf("second historical query error = %v", err)
	}
	if first.Rows[0]["value"] != "historical-1" || second.Rows[0]["value"] != "historical-2" || resolver.beginCalls != 2 {
		t.Fatalf("historical cache results = %#v/%#v provider calls=%d, want two fresh snapshots", first.Rows, second.Rows, resolver.beginCalls)
	}
}

func TestExecuteSQLQueryWithoutAsOfKeepsLivePath(t *testing.T) {
	resolver := &mz008TestResolver{
		live:       []Row{{"id": int64(1), "value": "live"}},
		historical: []Row{{"id": int64(2), "value": "historical"}},
	}
	result, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('items') SELECT id, value", resolver, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["value"] != "live" || resolver.beginCalls != 0 {
		t.Fatalf("live result = %#v provider calls=%d, want live row and zero calls", result.Rows, resolver.beginCalls)
	}
}

type mz008NilSnapshotResolver struct {
	beginCalls int
	releases   int
}

func (resolver *mz008NilSnapshotResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return []Row{{"id": int64(1)}}, nil
}

func (resolver *mz008NilSnapshotResolver) BeginSQLSnapshotAt(context.Context, uint64) (SQLSourceResolver, func(), error) {
	resolver.beginCalls++
	return nil, func() { resolver.releases++ }, nil
}

func TestExecuteSQLQueryAsOfRejectsNilSnapshotAndReleasesIt(t *testing.T) {
	frontier := uint64(13)
	resolver := &mz008NilSnapshotResolver{}
	_, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('items') SELECT id", resolver, SQLQueryOptions{AsOfFrontier: &frontier})
	if !errors.Is(err, ErrSQLSnapshotResolverNil) {
		t.Fatalf("error = %v, want ErrSQLSnapshotResolverNil", err)
	}
	if resolver.beginCalls != 1 || resolver.releases != 1 {
		t.Fatalf("nil snapshot lifecycle = calls %d releases %d, want 1/1", resolver.beginCalls, resolver.releases)
	}
}

func TestExecuteSQLQueryAsOfRejectsUnsupportedResolver(t *testing.T) {
	frontier := uint64(3)
	resolver := mz008HistoricalResolver{rows: []Row{{"id": int64(1)}}}
	_, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('items') SELECT id", resolver, SQLQueryOptions{AsOfFrontier: &frontier})
	if !errors.Is(err, ErrSQLAsOfUnsupported) {
		t.Fatalf("error = %v, want ErrSQLAsOfUnsupported", err)
	}
}

func TestExecuteSQLQueryAsOfPropagatesProviderError(t *testing.T) {
	frontier := uint64(9)
	want := errors.New("historical snapshot unavailable")
	resolver := &mz008TestResolver{beginErr: want}
	_, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('items') SELECT id", resolver, SQLQueryOptions{AsOfFrontier: &frontier})
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
	if resolver.releases != 0 {
		t.Fatalf("releases = %d, want 0 when provider fails", resolver.releases)
	}
}

func TestExecuteSQLQueryAsOfHonorsCanceledContextBeforeProvider(t *testing.T) {
	frontier := uint64(11)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	resolver := &mz008TestResolver{}
	_, err := ExecuteSQLQueryContext(ctx, "FROM CACHE('items') SELECT id", resolver, SQLQueryOptions{AsOfFrontier: &frontier})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if resolver.beginCalls != 0 {
		t.Fatalf("provider calls = %d, want 0 after cancellation", resolver.beginCalls)
	}
}
