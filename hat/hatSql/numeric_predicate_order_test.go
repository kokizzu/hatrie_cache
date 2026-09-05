package hatSql

import (
	"context"
	"reflect"
	"testing"
)

func TestSQLColumnarOrdersNumericPredicatesByMarkSelectivity(t *testing.T) {
	t.Parallel()
	segments := &ColumnarNumericSegments{
		RowsPerSegment: 2,
		Columns: map[string][]ColumnarNumericSegment{
			"broad": {
				{Minimum: 0, Maximum: 10, Valid: true},
				{Minimum: 0, Maximum: 10, Valid: true},
				{Minimum: 0, Maximum: 10, Valid: true},
				{Minimum: 0, Maximum: 10, Valid: true},
			},
			"narrow": {
				{Minimum: 0, Maximum: 1, Valid: true},
				{Minimum: 2, Maximum: 3, Valid: true},
				{Minimum: 20, Maximum: 21, Valid: true},
				{Minimum: 22, Maximum: 23, Valid: true},
			},
		},
	}
	predicates := []sqlColumnarNumericFilter{
		{field: "broad", operator: ">=", value: 0},
		{field: "narrow", operator: "=", value: 1},
	}
	got := sqlColumnarOrderNumericPredicates(segments, predicates)
	if len(got) != 2 || got[0].field != "narrow" || got[1].field != "broad" {
		t.Fatalf("predicate order = %#v, want narrow then broad", got)
	}
}

func TestSQLColumnarNumericPredicateOrderSortsKnownFieldsBeforeUnknownFields(t *testing.T) {
	t.Parallel()
	segments := &ColumnarNumericSegments{
		Columns: map[string][]ColumnarNumericSegment{
			"broad": {
				{Minimum: 0, Maximum: 10, Valid: true},
				{Minimum: 0, Maximum: 10, Valid: true},
				{Minimum: 0, Maximum: 10, Valid: true},
				{Minimum: 0, Maximum: 10, Valid: true},
			},
			"medium": {
				{Minimum: 0, Maximum: 1, Valid: true},
				{Minimum: 2, Maximum: 3, Valid: true},
				{Minimum: 20, Maximum: 21, Valid: true},
				{Minimum: 22, Maximum: 23, Valid: true},
			},
			"narrow": {
				{Minimum: 0, Maximum: 1, Valid: true},
				{Minimum: 2, Maximum: 3, Valid: true},
				{Minimum: 20, Maximum: 21, Valid: true},
				{Minimum: 22, Maximum: 23, Valid: true},
			},
		},
	}
	predicates := []sqlColumnarNumericFilter{
		{field: "broad", operator: ">=", value: 0},
		{field: "unknown", operator: "=", value: 1},
		{field: "medium", operator: ">=", value: 2},
		{field: "narrow", operator: "=", value: 1},
	}
	got := sqlColumnarOrderNumericPredicates(segments, predicates)
	want := []string{"narrow", "medium", "broad", "unknown"}
	for index, field := range want {
		if got[index].field != field {
			t.Fatalf("predicate order = %#v, want fields %#v", got, want)
		}
	}
}

func TestSQLColumnarNumericPredicateOrderPreservesUnknownAndLargeInputs(t *testing.T) {
	t.Parallel()
	withoutStats := []sqlColumnarNumericFilter{
		{field: "first", operator: "=", value: 1},
		{field: "second", operator: "=", value: 2},
	}
	if got := sqlColumnarOrderNumericPredicates(nil, withoutStats); got[0].field != "first" || got[1].field != "second" {
		t.Fatalf("predicate order without stats = %#v, want original order", got)
	}
	large := make([]sqlColumnarNumericFilter, 9)
	for index := range large {
		large[index] = sqlColumnarNumericFilter{field: "field"}
	}
	if got := sqlColumnarOrderNumericPredicates(&ColumnarNumericSegments{Columns: map[string][]ColumnarNumericSegment{"field": {{Valid: true}}}}, large); &got[0] != &large[0] || got[0].field != large[0].field {
		t.Fatalf("large predicate order should preserve the original slice: %#v", got)
	}
}

func TestSQLColumnarNumericPredicateOrderPreservesQueryResults(t *testing.T) {
	t.Parallel()
	probe := &sqlSegmentedColumnarSourceProbe{
		batch: ColumnarBatch{
			Columns: map[string][]interface{}{
				"broad":  {float64(0), float64(0), float64(0), float64(0)},
				"narrow": {float64(1), float64(0), float64(0), float64(0)},
			},
			Rows: 4,
		},
		segments: &ColumnarNumericSegments{
			RowsPerSegment: 2,
			Columns: map[string][]ColumnarNumericSegment{
				"broad":  {{Minimum: 0, Maximum: 0, Valid: true}, {Minimum: 0, Maximum: 0, Valid: true}},
				"narrow": {{Minimum: 0, Maximum: 1, Valid: true}, {Minimum: 0, Maximum: 0, Valid: true}},
			},
		},
	}
	query := "FROM CACHE('events') AS event WHERE event.broad >= 0 AND event.narrow = 1 SELECT event.broad, event.narrow"
	got, err := ExecuteSQLQueryParameters(context.Background(), query, probe, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryParameters() error = %v", err)
	}
	want := SQLQueryResult{Columns: []string{"broad", "narrow"}, Rows: []SQLRow{{"broad": float64(0), "narrow": float64(1)}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ExecuteSQLQueryParameters() result = %#v, want %#v", got, want)
	}
}
