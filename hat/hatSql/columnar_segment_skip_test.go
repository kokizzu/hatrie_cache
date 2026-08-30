package hatSql

import (
	"context"
	"reflect"
	"testing"
)

type sqlSegmentedColumnarSourceProbe struct {
	batch           ColumnarBatch
	segments        *ColumnarNumericSegments
	rows            []Row
	segmentCalls    int
	resolutionCalls int
}

func (probe *sqlSegmentedColumnarSourceProbe) ResolveSQLSource(string, string) ([]Row, error) {
	return probe.rows, nil
}

func (probe *sqlSegmentedColumnarSourceProbe) ResolveSQLColumnarSource(string, string, []string) (ColumnarBatch, bool, error) {
	probe.resolutionCalls++
	return probe.batch, true, nil
}

func (probe *sqlSegmentedColumnarSourceProbe) BorrowSQLColumnarSourceSegments(string, string, []string) (ColumnarBatch, *ColumnarNumericSegments, bool, error) {
	probe.segmentCalls++
	return probe.batch, probe.segments, true, nil
}

func TestSQLColumnarNumericAggregateUsesSegmentedBatchWhenAvailable(t *testing.T) {
	t.Parallel()
	probe := &sqlSegmentedColumnarSourceProbe{
		batch: ColumnarBatch{Columns: map[string][]interface{}{"id": {float64(1), float64(2), float64(100), float64(101)}}, Rows: 4},
		segments: &ColumnarNumericSegments{
			RowsPerSegment: 2,
			Columns: map[string][]ColumnarNumericSegment{
				"id": {{Minimum: 1, Maximum: 2, Valid: true}, {Minimum: 100, Maximum: 101, Valid: true}},
			},
		},
		rows: []Row{{"id": float64(1)}, {"id": float64(2)}, {"id": float64(100)}, {"id": float64(101)}},
	}
	wantBatch := cloneSQLColumnarBatchForTest(probe.batch)
	result, err := ExecuteSQLQueryParameters(context.Background(), "FROM CACHE('events') AS event WHERE event.id >= 100 SELECT COUNT(*) AS total, SUM(event.id) AS sum, AVG(event.id) AS average, MIN(event.id) AS minimum, MAX(event.id) AS maximum", probe, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryParameters() error = %v", err)
	}
	if got, want := result.Rows, []Row{{"total": int64(2), "sum": float64(201), "average": float64(100.5), "minimum": float64(100), "maximum": float64(101)}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ExecuteSQLQueryParameters() rows = %#v, want %#v", got, want)
	}
	if probe.segmentCalls != 1 || probe.resolutionCalls != 0 {
		t.Fatalf("segment/resolution calls = %d/%d, want 1/0", probe.segmentCalls, probe.resolutionCalls)
	}
	if !reflect.DeepEqual(probe.batch, wantBatch) {
		t.Fatalf("segmented batch mutated = %#v, want %#v", probe.batch, wantBatch)
	}
}
