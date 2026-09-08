package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type sqlColumnarCountMetadataResolver struct {
	batch ColumnarBatch
}

func (resolver sqlColumnarCountMetadataResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, errors.New("row source must not be resolved for metadata count")
}

func (resolver sqlColumnarCountMetadataResolver) ResolveSQLColumnarSource(string, string, []string) (ColumnarBatch, bool, error) {
	return resolver.batch, true, nil
}

func TestSQLColumnarCountOnlyMetadataEligibility(t *testing.T) {
	query, err := parseSQLQueryWithCache("SELECT COUNT(*) AS total FROM CACHE('items')", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	aggregates, _, _, ok := sqlColumnarNumericAggregates(query, nil)
	if !ok || !sqlColumnarCountOnlyMetadata(aggregates, query.where) {
		t.Fatalf("count-only aggregates = %#v, where = %#v, accepted = %t", aggregates, query.where, ok)
	}
}

func TestSQLColumnarCountStarUsesBatchRowCount(t *testing.T) {
	resolver := sqlColumnarCountMetadataResolver{batch: ColumnarBatch{Rows: 17}}
	result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT COUNT(*) AS total FROM CACHE('items')", resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []SQLRow{{"total": int64(17)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("count result = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLColumnarCountStarPreservesEmptyResultAndLimits(t *testing.T) {
	resolver := sqlColumnarCountMetadataResolver{batch: ColumnarBatch{Rows: 0}}
	result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT COUNT(*) AS total FROM CACHE('items')", resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []SQLRow{{"total": int64(0)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("empty count result = %#v, want %#v", result.Rows, want)
	}
	resolver.batch.Rows = 17
	if _, err := ExecuteSQLQueryParameters(context.Background(), "SELECT COUNT(*) AS total FROM CACHE('items')", resolver, nil, SQLQueryOptions{MaxRows: 16}); err == nil {
		t.Fatal("count query exceeded MaxRows without an error")
	}
}

func TestSQLColumnarCountOnlyMetadataRejectsRicherShapes(t *testing.T) {
	for _, source := range []string{
		"SELECT COUNT(value) AS total FROM CACHE('items')",
		"SELECT COUNT(*) AS total FROM CACHE('items') WHERE value >= 2",
		"SELECT COUNT(*) AS total, SUM(value) AS sum FROM CACHE('items')",
	} {
		query, err := parseSQLQueryWithCache(source, nil, nil)
		if err != nil {
			t.Fatalf("parse %q: %v", source, err)
		}
		aggregates, _, _, ok := sqlColumnarNumericAggregates(query, nil)
		if !ok {
			t.Fatalf("numeric aggregate plan rejected %q", source)
		}
		if sqlColumnarCountOnlyMetadata(aggregates, query.where) {
			t.Fatalf("richer query incorrectly accepted for metadata count: %q", source)
		}
	}
}

func TestSQLColumnarCountStarFallbackPreservesFilterAndNullSemantics(t *testing.T) {
	resolver := sqlColumnarCountMetadataResolver{batch: ColumnarBatch{
		Columns: map[string][]interface{}{"value": {int64(1), int64(2), nil, int64(3)}},
		Rows:    4,
	}}
	filtered, err := ExecuteSQLQueryParameters(context.Background(), "SELECT COUNT(*) AS total FROM CACHE('items') WHERE value >= 2", resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []SQLRow{{"total": int64(2)}}; !reflect.DeepEqual(filtered.Rows, want) {
		t.Fatalf("filtered count = %#v, want %#v", filtered.Rows, want)
	}
	counted, err := ExecuteSQLQueryParameters(context.Background(), "SELECT COUNT(value) AS total FROM CACHE('items')", resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []SQLRow{{"total": int64(3)}}; !reflect.DeepEqual(counted.Rows, want) {
		t.Fatalf("nullable count = %#v, want %#v", counted.Rows, want)
	}
}
