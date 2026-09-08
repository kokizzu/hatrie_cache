package hatSql

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"
)

type sqlColumnarMinMaxMetadataResolver struct {
	batch    ColumnarBatch
	segments *ColumnarNumericSegments
}

func (resolver sqlColumnarMinMaxMetadataResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, errors.New("row source must not be resolved for columnar aggregate metadata")
}

func (resolver sqlColumnarMinMaxMetadataResolver) ResolveSQLColumnarSource(string, string, []string) (ColumnarBatch, bool, error) {
	return resolver.batch, true, nil
}

func (resolver sqlColumnarMinMaxMetadataResolver) BorrowSQLColumnarSourceSegments(string, string, []string) (ColumnarBatch, *ColumnarNumericSegments, bool, error) {
	return resolver.batch, resolver.segments, true, nil
}

func TestSQLColumnarMinMaxUsesCompleteSegmentMetadata(t *testing.T) {
	resolver := sqlColumnarMinMaxMetadataResolver{
		batch: ColumnarBatch{
			Columns: map[string][]interface{}{
				"score": {int64(9), int64(3), int64(7), int64(11), int64(4), int64(8)},
			},
			Rows: 6,
		},
		segments: &ColumnarNumericSegments{
			RowsPerSegment: 2,
			Columns: map[string][]ColumnarNumericSegment{
				"score": {
					{Minimum: 3, Maximum: 9, Valid: true},
					{Minimum: 7, Maximum: 11, Valid: true},
					{Minimum: 4, Maximum: 8, Valid: true},
				},
			},
		},
	}

	result, err := ExecuteSQLQueryContext(context.Background(), "SELECT MIN(score) AS low, MAX(score) AS high FROM CACHE('items')", resolver, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := []SQLRow{{"low": float64(3), "high": float64(11)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLColumnarMetadataSupportsCountAndMinMaxTogether(t *testing.T) {
	resolver := sqlColumnarMinMaxMetadataResolver{
		batch: ColumnarBatch{
			Columns: map[string][]interface{}{"score": {int64(12), int64(2), int64(8), int64(5)}},
			Rows:    4,
		},
		segments: &ColumnarNumericSegments{
			RowsPerSegment: 2,
			Columns: map[string][]ColumnarNumericSegment{
				"score": {{Minimum: 2, Maximum: 12, Valid: true}, {Minimum: 5, Maximum: 8, Valid: true}},
			},
		},
	}

	result, err := ExecuteSQLQueryContext(context.Background(), "SELECT COUNT(*) AS total, MIN(score) AS low, MAX(score) AS high FROM CACHE('items')", resolver, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := []SQLRow{{"total": int64(4), "low": float64(2), "high": float64(12)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLColumnarMinMaxMetadataIgnoresNullsInValidSegments(t *testing.T) {
	resolver := sqlColumnarMinMaxMetadataResolver{
		batch: ColumnarBatch{
			Columns: map[string][]interface{}{"score": {nil, int64(12), int64(8), nil}},
			Rows:    4,
		},
		segments: &ColumnarNumericSegments{
			RowsPerSegment: 2,
			Columns: map[string][]ColumnarNumericSegment{
				"score": {{Minimum: 12, Maximum: 12, Valid: true}, {Minimum: 8, Maximum: 8, Valid: true}},
			},
		},
	}

	result, err := ExecuteSQLQueryContext(context.Background(), "SELECT MIN(score) AS low, MAX(score) AS high FROM CACHE('items')", resolver, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := []SQLRow{{"low": float64(8), "high": float64(12)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLColumnarMinMaxFallsBackForIncompleteMetadata(t *testing.T) {
	resolver := sqlColumnarMinMaxMetadataResolver{
		batch: ColumnarBatch{
			Columns: map[string][]interface{}{"score": {int64(12), nil, int64(8), int64(5)}},
			Rows:    4,
		},
		segments: &ColumnarNumericSegments{
			RowsPerSegment: 2,
			Columns: map[string][]ColumnarNumericSegment{
				"score": {{Minimum: 12, Maximum: 12, Valid: true}},
			},
		},
	}

	result, err := ExecuteSQLQueryContext(context.Background(), "SELECT MIN(score) AS low, MAX(score) AS high FROM CACHE('items')", resolver, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := []SQLRow{{"low": float64(5), "high": float64(12)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLColumnarMinMaxFallsBackForInvalidSegment(t *testing.T) {
	resolver := sqlColumnarMinMaxMetadataResolver{
		batch: ColumnarBatch{
			Columns: map[string][]interface{}{"score": {math.NaN(), int64(2), int64(8), int64(5)}},
			Rows:    4,
		},
		segments: &ColumnarNumericSegments{
			RowsPerSegment: 2,
			Columns: map[string][]ColumnarNumericSegment{
				"score": {{Valid: false}, {Minimum: 5, Maximum: 8, Valid: true}},
			},
		},
	}

	result, err := ExecuteSQLQueryContext(context.Background(), "SELECT MIN(score) AS low, MAX(score) AS high FROM CACHE('items')", resolver, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || !math.IsNaN(result.Rows[0]["low"].(float64)) || !math.IsNaN(result.Rows[0]["high"].(float64)) {
		t.Fatalf("rows = %#v, want NaN results from the established scan", result.Rows)
	}
}

func TestSQLColumnarMinMaxFallsBackForNonFiniteMetadata(t *testing.T) {
	resolver := sqlColumnarMinMaxMetadataResolver{
		batch: ColumnarBatch{
			Columns: map[string][]interface{}{"score": {math.Inf(1), int64(2)}},
			Rows:    2,
		},
		segments: &ColumnarNumericSegments{
			RowsPerSegment: 2,
			Columns: map[string][]ColumnarNumericSegment{
				"score": {{Minimum: math.Inf(1), Maximum: math.Inf(1), Valid: true}},
			},
		},
	}

	result, err := ExecuteSQLQueryContext(context.Background(), "SELECT MIN(score) AS low, MAX(score) AS high FROM CACHE('items')", resolver, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := []SQLRow{{"low": float64(2), "high": math.Inf(1)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLColumnarMinMaxDoesNotUseMetadataForFilteredQuery(t *testing.T) {
	resolver := sqlColumnarMinMaxMetadataResolver{
		batch: ColumnarBatch{
			Columns: map[string][]interface{}{"score": {int64(1), int64(20), int64(3), int64(40)}},
			Rows:    4,
		},
		segments: &ColumnarNumericSegments{
			RowsPerSegment: 2,
			Columns: map[string][]ColumnarNumericSegment{
				"score": {{Minimum: 1, Maximum: 20, Valid: true}, {Minimum: 3, Maximum: 40, Valid: true}},
			},
		},
	}

	result, err := ExecuteSQLQueryContext(context.Background(), "SELECT MIN(score) AS low, MAX(score) AS high FROM CACHE('items') WHERE score >= 20", resolver, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := []SQLRow{{"low": float64(20), "high": float64(40)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLColumnarMinMaxPreservesEmptyAndLimitSemantics(t *testing.T) {
	for _, test := range []struct {
		name     string
		query    string
		batch    ColumnarBatch
		wantRows []SQLRow
	}{
		{
			name:     "empty",
			query:    "SELECT MIN(score) AS low, MAX(score) AS high FROM CACHE('items')",
			batch:    ColumnarBatch{Columns: map[string][]interface{}{"score": {}}, Rows: 0},
			wantRows: []SQLRow{{"low": nil, "high": nil}},
		},
		{
			name:     "limit zero",
			query:    "SELECT MIN(score) AS low, MAX(score) AS high FROM CACHE('items') LIMIT 0",
			batch:    ColumnarBatch{Columns: map[string][]interface{}{"score": {int64(1)}}, Rows: 1},
			wantRows: []SQLRow{},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := ExecuteSQLQueryContext(context.Background(), test.query, sqlColumnarMinMaxMetadataResolver{batch: test.batch}, SQLQueryOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(result.Rows, test.wantRows) {
				t.Fatalf("rows = %#v, want %#v", result.Rows, test.wantRows)
			}
		})
	}
}

func TestSQLColumnarMetadataEligibilityRejectsUnsupportedAggregates(t *testing.T) {
	queries := []string{
		"SELECT SUM(score) FROM CACHE('items')",
		"SELECT COUNT(score) FROM CACHE('items')",
		"SELECT MIN(score) FROM CACHE('items') WHERE score > 0",
	}
	for _, source := range queries {
		query, err := parseSQLQueryWithCache(source, nil, nil)
		if err != nil {
			t.Fatalf("parse %q: %v", source, err)
		}
		aggregates, _, _, ok := sqlColumnarNumericAggregates(query, nil)
		if !ok {
			t.Fatalf("sqlColumnarNumericAggregates(%q) unexpectedly rejected query", source)
		}
		if sqlColumnarMetadataAggregates(aggregates, query.where, nil, 4) {
			t.Fatalf("sqlColumnarMetadataAggregates(%q) = true, want false", source)
		}
	}
}
