package hatSql

import "testing"

func TestSQLColumnarSparsePrimarySegmentRange(t *testing.T) {
	t.Parallel()
	segments := &ColumnarNumericSegments{
		RowsPerSegment:     2,
		SparsePrimaryField: "id",
		Columns: map[string][]ColumnarNumericSegment{
			"id": {
				{Minimum: 0, Maximum: 1, Valid: true},
				{Minimum: 2, Maximum: 3, Valid: true},
				{Minimum: 4, Maximum: 5, Valid: true},
				{Minimum: 6, Maximum: 7, Valid: true},
			},
		},
	}
	tests := []struct {
		name       string
		predicates []sqlColumnarNumericFilter
		start      int
		end        int
		used       bool
	}{
		{name: "equality", predicates: []sqlColumnarNumericFilter{{field: "id", operator: "=", value: 3}}, start: 1, end: 2, used: true},
		{name: "less", predicates: []sqlColumnarNumericFilter{{field: "id", operator: "<", value: 4}}, start: 0, end: 2, used: true},
		{name: "less or equal", predicates: []sqlColumnarNumericFilter{{field: "id", operator: "<=", value: 4}}, start: 0, end: 3, used: true},
		{name: "greater", predicates: []sqlColumnarNumericFilter{{field: "id", operator: ">", value: 4}}, start: 2, end: 4, used: true},
		{name: "greater or equal", predicates: []sqlColumnarNumericFilter{{field: "id", operator: ">=", value: 4}}, start: 2, end: 4, used: true},
		{name: "intersected range", predicates: []sqlColumnarNumericFilter{{field: "id", operator: ">=", value: 3}, {field: "id", operator: "<", value: 6}}, start: 1, end: 3, used: true},
		{name: "not equal is not bounded", predicates: []sqlColumnarNumericFilter{{field: "id", operator: "!=", value: 3}}, used: false},
		{name: "unindexed field", predicates: []sqlColumnarNumericFilter{{field: "other", operator: ">=", value: 3}}, used: false},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			start, end, used := sqlColumnarSparsePrimarySegmentRange(segments, test.predicates, 4)
			if start != test.start || end != test.end || used != test.used {
				t.Fatalf("sqlColumnarSparsePrimarySegmentRange() = %d, %d, %t; want %d, %d, %t", start, end, used, test.start, test.end, test.used)
			}
		})
	}
}

func TestSQLColumnarSparsePrimarySegmentRangeRejectsIncompleteMetadata(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		segments *ColumnarNumericSegments
	}{
		{name: "missing primary field", segments: &ColumnarNumericSegments{RowsPerSegment: 2, Columns: map[string][]ColumnarNumericSegment{"id": {{Minimum: 0, Maximum: 1, Valid: true}}}}},
		{name: "missing segment", segments: &ColumnarNumericSegments{RowsPerSegment: 2, SparsePrimaryField: "id", Columns: map[string][]ColumnarNumericSegment{"id": {{Minimum: 0, Maximum: 1, Valid: true}}}}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			start, end, used := sqlColumnarSparsePrimarySegmentRange(test.segments, []sqlColumnarNumericFilter{{field: "id", operator: ">=", value: 0}}, 2)
			if start != 0 || end != 0 || used {
				t.Fatalf("sqlColumnarSparsePrimarySegmentRange() = %d, %d, %t; want 0, 0, false", start, end, used)
			}
		})
	}
}
