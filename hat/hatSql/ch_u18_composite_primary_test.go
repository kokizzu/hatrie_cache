package hatSql

import (
	"reflect"
	"testing"
)

func TestSQLColumnarCompositeSparsePrimarySegmentRange(t *testing.T) {
	segments := &ColumnarNumericSegments{
		RowsPerSegment:            2,
		SparsePrimaryFields:       []string{"tenant", "id"},
		SparsePrimaryTupleMinimum: []float64{1, 1, 2, 1, 3, 1},
		SparsePrimaryTupleMaximum: []float64{1, 2, 2, 2, 3, 3},
	}
	cases := []struct {
		name       string
		predicates []sqlColumnarNumericFilter
		start      int
		end        int
		used       bool
	}{
		{name: "leading equality", predicates: []sqlColumnarNumericFilter{{field: "tenant", operator: "=", value: 2}}, start: 1, end: 2, used: true},
		{name: "prefix equality plus range", predicates: []sqlColumnarNumericFilter{{field: "tenant", operator: "=", value: 2}, {field: "id", operator: ">", value: 1}}, start: 1, end: 2, used: true},
		{name: "strict leading lower bound", predicates: []sqlColumnarNumericFilter{{field: "tenant", operator: ">", value: 2}}, start: 2, end: 3, used: true},
		{name: "leading lower bound", predicates: []sqlColumnarNumericFilter{{field: "tenant", operator: ">=", value: 2}}, start: 1, end: 3, used: true},
		{name: "leading upper bound", predicates: []sqlColumnarNumericFilter{{field: "tenant", operator: "<", value: 2}}, start: 0, end: 1, used: true},
		{name: "inclusive leading upper bound", predicates: []sqlColumnarNumericFilter{{field: "tenant", operator: "<=", value: 2}}, start: 0, end: 2, used: true},
		{name: "exact tuple", predicates: []sqlColumnarNumericFilter{{field: "tenant", operator: "=", value: 2}, {field: "id", operator: "=", value: 1}}, start: 1, end: 2, used: true},
		{name: "empty tuple range", predicates: []sqlColumnarNumericFilter{{field: "tenant", operator: "=", value: 2}, {field: "id", operator: ">", value: 2}}, start: 2, end: 2, used: true},
		{name: "non-leading predicate", predicates: []sqlColumnarNumericFilter{{field: "id", operator: "=", value: 1}}, start: 0, end: 0, used: false},
		{name: "unsupported operator", predicates: []sqlColumnarNumericFilter{{field: "tenant", operator: "!=", value: 2}}, start: 0, end: 0, used: false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			start, end, used := sqlColumnarSparsePrimarySegmentRangeWithComposite(segments, testCase.predicates, 3)
			if start != testCase.start || end != testCase.end || used != testCase.used {
				t.Fatalf("sqlColumnarSparsePrimarySegmentRange() = %d, %d, %t; want %d, %d, %t", start, end, used, testCase.start, testCase.end, testCase.used)
			}
		})
	}
}

func TestSQLColumnarCompositeSparsePrimaryFallsBackForIncompleteMetadata(t *testing.T) {
	segments := &ColumnarNumericSegments{
		RowsPerSegment:            2,
		SparsePrimaryField:        "tenant",
		SparsePrimaryFields:       []string{"tenant", "id"},
		SparsePrimaryTupleMinimum: []float64{1, 1},
		SparsePrimaryTupleMaximum: []float64{1, 2},
		Columns: map[string][]ColumnarNumericSegment{
			"tenant": {
				{Minimum: 1, Maximum: 1, Valid: true},
				{Minimum: 2, Maximum: 2, Valid: true},
			},
		},
	}
	start, end, used := sqlColumnarSparsePrimarySegmentRangeWithComposite(segments, []sqlColumnarNumericFilter{{field: "tenant", operator: "=", value: 2}}, 2)
	if start != 1 || end != 2 || !used {
		t.Fatalf("sqlColumnarSparsePrimarySegmentRangeWithComposite() = %d, %d, %t; want 1, 2, true", start, end, used)
	}
}

func TestTypedTableBuildsCompositeSparsePrimaryMarks(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		Columns: []TypedTableColumn{
			{Name: "tenant", Kind: TypedTableInt64},
			{Name: "id", Kind: TypedTableInt64},
			{Name: "value", Kind: TypedTableInt64},
		},
		ColumnarCache: TypedTableColumnarCacheOptions{
			Enabled:            true,
			MinReads:           1,
			RowsPerSegment:     2,
			SparsePrimaryIndex: true,
			SparsePrimaryFields: []string{
				"tenant",
				"id",
			},
		},
	})
	if err != nil {
		t.Fatalf("NewTypedTable() error = %v", err)
	}
	batch := ColumnarBatch{
		Rows: 5,
		Columns: map[string][]interface{}{
			"tenant": {int64(1), int64(1), int64(2), int64(2), int64(3)},
			"id":     {int64(1), int64(2), int64(1), int64(2), int64(1)},
			"value":  {int64(10), int64(20), int64(30), int64(40), int64(50)},
		},
	}
	segments := table.columnarNumericSegmentsLocked(batch)
	if segments == nil {
		t.Fatal("columnarNumericSegmentsLocked() = nil, want metadata")
	}
	if !reflect.DeepEqual(segments.SparsePrimaryFields, []string{"tenant", "id"}) {
		t.Fatalf("SparsePrimaryFields = %#v, want [tenant id]", segments.SparsePrimaryFields)
	}
	if !reflect.DeepEqual(segments.SparsePrimaryTupleMinimum, []float64{1, 1, 2, 1, 3, 1}) {
		t.Fatalf("SparsePrimaryTupleMinimum = %#v, want [1 1 2 1 3 1]", segments.SparsePrimaryTupleMinimum)
	}
	if !reflect.DeepEqual(segments.SparsePrimaryTupleMaximum, []float64{1, 2, 2, 2, 3, 1}) {
		t.Fatalf("SparsePrimaryTupleMaximum = %#v, want [1 2 2 2 3 1]", segments.SparsePrimaryTupleMaximum)
	}
}

func TestTypedTableOmitsCompositeSparsePrimaryMarksWhenTupleOrderBreaks(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		Columns: []TypedTableColumn{
			{Name: "tenant", Kind: TypedTableInt64},
			{Name: "id", Kind: TypedTableInt64},
		},
		ColumnarCache: TypedTableColumnarCacheOptions{
			Enabled:            true,
			MinReads:           1,
			RowsPerSegment:     2,
			SparsePrimaryIndex: true,
			SparsePrimaryFields: []string{
				"tenant",
				"id",
			},
		},
	})
	if err != nil {
		t.Fatalf("NewTypedTable() error = %v", err)
	}
	segments := table.columnarNumericSegmentsLocked(ColumnarBatch{
		Rows: 2,
		Columns: map[string][]interface{}{
			"tenant": {int64(1), int64(1)},
			"id":     {int64(2), int64(1)},
		},
	})
	if segments == nil {
		t.Fatal("columnarNumericSegmentsLocked() = nil, want numeric metadata")
	}
	if len(segments.SparsePrimaryFields) != 0 || len(segments.SparsePrimaryTupleMinimum) != 0 || len(segments.SparsePrimaryTupleMaximum) != 0 {
		t.Fatalf("unsorted composite metadata = %#v/%#v/%#v, want empty", segments.SparsePrimaryFields, segments.SparsePrimaryTupleMinimum, segments.SparsePrimaryTupleMaximum)
	}
}

func TestTypedTableCompositeSparsePrimaryOptionsNormalizeAndStayOffByDefault(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		Columns: []TypedTableColumn{
			{Name: "tenant", Kind: TypedTableInt64},
			{Name: "id", Kind: TypedTableInt64},
		},
		ColumnarCache: TypedTableColumnarCacheOptions{
			Enabled:            true,
			SparsePrimaryIndex: true,
			SparsePrimaryFields: []string{
				" tenant ",
				"id",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(table.columnar.options.SparsePrimaryFields, []string{"tenant", "id"}) {
		t.Fatalf("SparsePrimaryFields = %#v, want [tenant id]", table.columnar.options.SparsePrimaryFields)
	}
	if table.columnar.options.SparsePrimaryField != "tenant" {
		t.Fatalf("SparsePrimaryField = %q, want tenant", table.columnar.options.SparsePrimaryField)
	}

	defaultTable, err := NewTypedTable(TypedTableSchema{
		Name: "default_events",
		Columns: []TypedTableColumn{
			{Name: "tenant", Kind: TypedTableInt64},
			{Name: "id", Kind: TypedTableInt64},
		},
		ColumnarCache: TypedTableColumnarCacheOptions{Enabled: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if defaultTable.columnar.options.SparsePrimaryIndex || len(defaultTable.columnar.options.SparsePrimaryFields) != 0 {
		t.Fatalf("default sparse primary options = %+v, want disabled", defaultTable.columnar.options)
	}
}
