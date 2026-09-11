package hatSql

import (
	"context"
	"errors"
	"testing"
)

type mz007FrontierResolver struct {
	rows      []Row
	frontier  uint64
	ready     bool
	available bool
	reads     int
	checks    int
}

func (resolver *mz007FrontierResolver) ResolveSQLSource(string, string) ([]Row, error) {
	resolver.reads++
	return resolver.rows, nil
}

func (resolver *mz007FrontierResolver) SQLSourceFrontier(string, string) (uint64, bool, bool, error) {
	resolver.checks++
	return resolver.frontier, resolver.ready, resolver.available, nil
}

func mz007FrontierOptions(frontier uint64) SQLQueryOptions {
	return SQLQueryOptions{RequireSourceFrontier: true, RequiredSourceFrontier: frontier}
}

func TestExecuteSQLQueryRejectsUnavailableSourceFrontier(t *testing.T) {
	resolver := &mz007FrontierResolver{rows: []Row{{"id": int64(1)}}, available: false}
	_, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('items') SELECT id", resolver, mz007FrontierOptions(5))
	if !errors.Is(err, ErrSQLSourceFrontierUnavailable) {
		t.Fatalf("error = %v, want ErrSQLSourceFrontierUnavailable", err)
	}
	if resolver.checks != 1 || resolver.reads != 0 {
		t.Fatalf("frontier checks/reads = %d/%d, want 1/0", resolver.checks, resolver.reads)
	}
}

func TestExecuteSQLQueryRejectsUnreadySourceFrontier(t *testing.T) {
	resolver := &mz007FrontierResolver{rows: []Row{{"id": int64(1)}}, available: true}
	_, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('items') SELECT id", resolver, mz007FrontierOptions(5))
	if !errors.Is(err, ErrSQLSourceFrontierNotReady) {
		t.Fatalf("error = %v, want ErrSQLSourceFrontierNotReady", err)
	}
	if resolver.checks != 1 || resolver.reads != 0 {
		t.Fatalf("frontier checks/reads = %d/%d, want 1/0", resolver.checks, resolver.reads)
	}
}

func TestExecuteSQLQueryRejectsStaleSourceFrontier(t *testing.T) {
	resolver := &mz007FrontierResolver{rows: []Row{{"id": int64(1)}}, frontier: 4, ready: true, available: true}
	_, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('items') SELECT id", resolver, mz007FrontierOptions(5))
	if !errors.Is(err, ErrSQLSourceFrontierBehind) {
		t.Fatalf("error = %v, want ErrSQLSourceFrontierBehind", err)
	}
	if resolver.checks != 1 || resolver.reads != 0 {
		t.Fatalf("frontier checks/reads = %d/%d, want 1/0", resolver.checks, resolver.reads)
	}
}

func TestExecuteSQLQueryAcceptsRequiredSourceFrontier(t *testing.T) {
	resolver := &mz007FrontierResolver{rows: []Row{{"id": int64(1)}}, frontier: 5, ready: true, available: true}
	result, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('items') SELECT id", resolver, mz007FrontierOptions(5))
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["id"] != int64(1) {
		t.Fatalf("rows = %#v, want one id row", result.Rows)
	}
	if resolver.checks != 1 || resolver.reads != 1 {
		t.Fatalf("frontier checks/reads = %d/%d, want 1/1", resolver.checks, resolver.reads)
	}
}

func TestExecuteSQLQueryFrontierRequirementIsDisabledByDefault(t *testing.T) {
	resolver := &mz007FrontierResolver{rows: []Row{{"id": int64(1)}}, available: false}
	result, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('items') SELECT id", resolver, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	if len(result.Rows) != 1 || resolver.checks != 0 || resolver.reads != 1 {
		t.Fatalf("rows/checks/reads = %#v/%d/%d, want one row/0/1", result.Rows, resolver.checks, resolver.reads)
	}
}

func TestExecuteSQLQueryRequiresFrontierResolverForRemoteSource(t *testing.T) {
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) {
		return []Row{{"id": int64(1)}}, nil
	})
	_, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('items') SELECT id", resolver, mz007FrontierOptions(1))
	if !errors.Is(err, ErrSQLSourceFrontierUnavailable) {
		t.Fatalf("error = %v, want ErrSQLSourceFrontierUnavailable", err)
	}
}

func TestExecuteSQLQueryAllowsLocalValuesWithRequiredFrontier(t *testing.T) {
	result, err := ExecuteSQLQueryContext(context.Background(), "SELECT * FROM VALUES (1)", SourceResolverFunc(nil), mz007FrontierOptions(100))
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("rows = %#v, want one local row", result.Rows)
	}
}

func TestExecuteSQLQueryPageRejectsStaleSourceFrontier(t *testing.T) {
	resolver := &mz007FrontierResolver{rows: []Row{{"id": int64(1)}}, frontier: 4, ready: true, available: true}
	_, err := ExecuteSQLQueryPage(context.Background(), "FROM CACHE('items') SELECT id", resolver, nil, mz007FrontierOptions(5), 1, "")
	if !errors.Is(err, ErrSQLSourceFrontierBehind) {
		t.Fatalf("ExecuteSQLQueryPage() error = %v, want ErrSQLSourceFrontierBehind", err)
	}
	if resolver.checks != 1 || resolver.reads != 0 {
		t.Fatalf("frontier checks/reads = %d/%d, want 1/0", resolver.checks, resolver.reads)
	}
}

func TestExecuteSQLQueryRowsRejectsStaleSourceFrontierBeforeCallback(t *testing.T) {
	resolver := &mz007FrontierResolver{rows: []Row{{"id": int64(1)}}, frontier: 4, ready: true, available: true}
	callbacks := 0
	err := ExecuteSQLQueryRows(context.Background(), "FROM CACHE('items') SELECT id", resolver, nil, mz007FrontierOptions(5), func([]string, Row) error {
		callbacks++
		return nil
	})
	if !errors.Is(err, ErrSQLSourceFrontierBehind) {
		t.Fatalf("ExecuteSQLQueryRows() error = %v, want ErrSQLSourceFrontierBehind", err)
	}
	if callbacks != 0 || resolver.reads != 0 {
		t.Fatalf("callbacks/reads = %d/%d, want 0/0", callbacks, resolver.reads)
	}
}
