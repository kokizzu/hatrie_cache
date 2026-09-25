package hatSql

import (
	"context"
	"math"
	"testing"
)

func TestCH002PrunesWholePartsBeforeMarkScan(t *testing.T) {
	parts := []ColumnarSourcePart{
		newCH002ColumnarSourcePart(0, 2),
		newCH002ColumnarSourcePart(100, 2),
		newCH002ColumnarSourcePart(200, 2),
	}
	predicates := []sqlColumnarNumericFilter{{field: "id", operator: ">=", value: 100}}

	selected := pruneSQLColumnarSourceParts(parts, predicates)
	if len(selected) != 2 {
		t.Fatalf("selected %d parts, want 2", len(selected))
	}
	if selected[0].Batch.Columns["id"][0] != int64(100) || selected[1].Batch.Columns["id"][0] != int64(200) {
		t.Fatalf("selected parts = %#v, want id ranges 100 and 200", selected)
	}
}

func TestCH002RetainsPartsWhenMetadataCannotProveExclusion(t *testing.T) {
	parts := []ColumnarSourcePart{
		newCH002ColumnarSourcePart(0, 2),
		{Batch: ColumnarBatch{Columns: map[string][]interface{}{"id": {int64(100), int64(101)}}, Rows: 2}},
		{Batch: ColumnarBatch{Columns: map[string][]interface{}{"id": {int64(200), int64(201)}}, Rows: 2}, Segments: &ColumnarNumericSegments{
			RowsPerSegment:     2,
			SparsePrimaryField: "id",
			Columns:            map[string][]ColumnarNumericSegment{"id": {{Minimum: 200, Maximum: 100, Valid: true}}},
		}},
	}
	predicates := []sqlColumnarNumericFilter{{field: "id", operator: "=", value: 100}}

	selected := pruneSQLColumnarSourceParts(parts, predicates)
	if len(selected) != 2 {
		t.Fatalf("selected %d parts, want the two parts with unavailable or invalid metadata", len(selected))
	}
}

func TestCH002RetainsPartWithNaNMetadata(t *testing.T) {
	part := newCH002ColumnarSourcePart(0, 2)
	part.Segments.Columns["id"][0].Minimum = math.NaN()
	selected := pruneSQLColumnarSourceParts([]ColumnarSourcePart{part, newCH002ColumnarSourcePart(100, 2)}, []sqlColumnarNumericFilter{{field: "id", operator: "=", value: 100}})
	if len(selected) != 2 {
		t.Fatalf("selected %d parts, want both parts when NaN invalidates the metadata", len(selected))
	}
}

func TestCH002ColumnarQuerySkipsNonMatchingPartBeforeMerge(t *testing.T) {
	resolver := &ch002PartsResolver{parts: []ColumnarSourcePart{
		{
			Batch: ColumnarBatch{Rows: 2},
			Segments: &ColumnarNumericSegments{
				RowsPerSegment:     2,
				SparsePrimaryField: "id",
				Columns:            map[string][]ColumnarNumericSegment{"id": {{Minimum: 0, Maximum: 1, Valid: true}}},
			},
		},
		newCH002ColumnarSourcePart(100, 2),
	}}

	result, err := ExecuteQueryParameters(context.Background(), "FROM CACHE('events') SELECT id WHERE id >= 100", resolver, nil, QueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteQueryParameters() error = %v", err)
	}
	if len(result.Rows) != 2 || result.Rows[0]["id"] != int64(100) || result.Rows[1]["id"] != int64(101) {
		t.Fatalf("result rows = %#v, want [[100] [101]]", result.Rows)
	}
}

func TestCH002MultipleCandidatePartsUseExistingResolverFallback(t *testing.T) {
	resolver := &ch002PartsResolver{
		parts: []ColumnarSourcePart{
			newCH002ColumnarSourcePart(100, 2),
			newCH002ColumnarSourcePart(200, 2),
		},
		fallback:          ColumnarBatch{Columns: map[string][]interface{}{"id": {int64(100), int64(101), int64(200), int64(201)}}, Rows: 4},
		fallbackAvailable: true,
	}

	result, err := ExecuteQueryParameters(context.Background(), "FROM CACHE('events') SELECT id WHERE id >= 100", resolver, nil, QueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteQueryParameters() error = %v", err)
	}
	if len(result.Rows) != 4 {
		t.Fatalf("result row count = %d, want 4", len(result.Rows))
	}
}

type ch002PartsResolver struct {
	parts             []ColumnarSourcePart
	fallback          ColumnarBatch
	fallbackAvailable bool
}

func (resolver *ch002PartsResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, nil
}

func (resolver *ch002PartsResolver) ResolveSQLColumnarSource(string, string, []string) (ColumnarBatch, bool, error) {
	return resolver.fallback, resolver.fallbackAvailable, nil
}

func (resolver *ch002PartsResolver) BorrowSQLColumnarSourceParts(string, string, []string) ([]ColumnarSourcePart, bool, error) {
	return resolver.parts, true, nil
}

func newCH002ColumnarSourcePart(start int64, rows int) ColumnarSourcePart {
	values := make([]interface{}, rows)
	for index := range values {
		values[index] = start + int64(index)
	}
	return ColumnarSourcePart{
		Batch: ColumnarBatch{Columns: map[string][]interface{}{"id": values}, Rows: rows},
		Segments: &ColumnarNumericSegments{
			RowsPerSegment:     rows,
			SparsePrimaryField: "id",
			Columns:            map[string][]ColumnarNumericSegment{"id": {{Minimum: float64(start), Maximum: float64(start + int64(rows) - 1), Valid: true}}},
		},
	}
}
