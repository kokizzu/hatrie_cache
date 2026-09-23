package hatSql

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestM211AsOfBoundsRejectBeforeProvider(t *testing.T) {
	resolver := &mz008TestResolver{historical: []Row{{"id": int64(1)}}}
	asOf := uint64(9)
	since := uint64(10)
	_, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('items') SELECT id", resolver, SQLQueryOptions{
		AsOfFrontier: &asOf,
		AsOfSince:    &since,
	})
	if !errors.Is(err, ErrSQLAsOfBeforeSince) {
		t.Fatalf("before-since error = %v, want ErrSQLAsOfBeforeSince", err)
	}
	if resolver.beginCalls != 0 {
		t.Fatalf("provider begin calls = %d, want zero for rejected bound", resolver.beginCalls)
	}
}

func TestM211AsOfUpperIsExclusive(t *testing.T) {
	resolver := &mz008TestResolver{historical: []Row{{"id": int64(1)}}}
	asOf := uint64(10)
	upper := uint64(10)
	_, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('items') SELECT id", resolver, SQLQueryOptions{
		AsOfFrontier: &asOf,
		AsOfUpper:    &upper,
	})
	if !errors.Is(err, ErrSQLAsOfAtOrAfterUpper) {
		t.Fatalf("at-upper error = %v, want ErrSQLAsOfAtOrAfterUpper", err)
	}
	if resolver.beginCalls != 0 {
		t.Fatalf("provider begin calls = %d, want zero for rejected upper bound", resolver.beginCalls)
	}

	asOf = 9
	result, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('items') SELECT id", resolver, SQLQueryOptions{
		AsOfFrontier: &asOf,
		AsOfUpper:    &upper,
	})
	if err != nil {
		t.Fatalf("frontier below upper error = %v", err)
	}
	if len(result.Rows) != 1 || resolver.beginCalls != 1 {
		t.Fatalf("valid bounded read rows/calls = %#v/%d, want one row/one call", result.Rows, resolver.beginCalls)
	}

	asOf = 10
	since := uint64(10)
	upper = 11
	result, err = ExecuteSQLQueryContext(context.Background(), "FROM CACHE('items') SELECT id", resolver, SQLQueryOptions{
		AsOfFrontier: &asOf,
		AsOfSince:    &since,
		AsOfUpper:    &upper,
	})
	if err != nil || len(result.Rows) != 1 {
		t.Fatalf("frontier at inclusive since error/rows = %v/%#v, want success/one row", err, result.Rows)
	}
}

func TestM211AsOfBoundsRequireAFrontierAndHaveValidInterval(t *testing.T) {
	since := uint64(4)
	if _, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('items') SELECT id", &mz008TestResolver{}, SQLQueryOptions{AsOfSince: &since}); !errors.Is(err, ErrSQLAsOfBoundsRequireFrontier) {
		t.Fatalf("missing frontier error = %v, want ErrSQLAsOfBoundsRequireFrontier", err)
	}

	frontier := uint64(5)
	invalidSince := uint64(5)
	upper := uint64(5)
	if _, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('items') SELECT id", &mz008TestResolver{}, SQLQueryOptions{
		AsOfFrontier: &frontier,
		AsOfSince:    &invalidSince,
		AsOfUpper:    &upper,
	}); !errors.Is(err, ErrSQLAsOfBoundsInvalid) {
		t.Fatalf("invalid interval error = %v, want ErrSQLAsOfBoundsInvalid", err)
	}
}

func TestM211AsOfBoundsCoverEveryReadEntryPoint(t *testing.T) {
	frontier := uint64(3)
	since := uint64(4)
	options := SQLQueryOptions{AsOfFrontier: &frontier, AsOfSince: &since}
	resolver := &mz008TestResolver{historical: []Row{{"id": int64(1)}}}
	if _, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('items') SELECT id", resolver, options); !errors.Is(err, ErrSQLAsOfBeforeSince) {
		t.Fatalf("materialized error = %v", err)
	}
	if err := ExecuteSQLQueryRows(context.Background(), "FROM CACHE('items') SELECT id", resolver, nil, options, func([]string, SQLRow) error { return nil }); !errors.Is(err, ErrSQLAsOfBeforeSince) {
		t.Fatalf("row-stream error = %v", err)
	}
	if _, err := ExecuteSQLQueryPage(context.Background(), "FROM CACHE('items') SELECT id", resolver, nil, options, 1, ""); !errors.Is(err, ErrSQLAsOfBeforeSince) {
		t.Fatalf("offset-page error = %v", err)
	}
	if _, err := ExecuteSQLQueryKeysetPage(context.Background(), "FROM CACHE('items') SELECT id ORDER BY id", resolver, nil, options, 1, ""); !errors.Is(err, ErrSQLAsOfBeforeSince) {
		t.Fatalf("keyset-page error = %v", err)
	}
	if resolver.beginCalls != 0 {
		t.Fatalf("provider begin calls = %d, want zero across rejected entry points", resolver.beginCalls)
	}
}

func TestM211AsOfBoundsApplyAfterSnapshotTokenNormalization(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	codec, err := NewSQLSnapshotTokenCodec(SQLSnapshotTokenCodecOptions{
		Secret: []byte("m211-test-secret-0123456789"),
		Now:    func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	token, err := codec.Encode(3)
	if err != nil {
		t.Fatal(err)
	}
	since := uint64(4)
	resolver := &mz008TestResolver{}
	_, err = ExecuteSQLQueryContext(context.Background(), "FROM CACHE('items') SELECT id", resolver, SQLQueryOptions{
		SnapshotToken:      token,
		SnapshotTokenCodec: codec,
		AsOfSince:          &since,
	})
	if !errors.Is(err, ErrSQLAsOfBeforeSince) {
		t.Fatalf("token bound error = %v, want ErrSQLAsOfBeforeSince", err)
	}
	if resolver.beginCalls != 0 {
		t.Fatalf("provider begin calls = %d, want zero for token-bound rejection", resolver.beginCalls)
	}
}
