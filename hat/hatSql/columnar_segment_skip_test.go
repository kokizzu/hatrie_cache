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
			RowsPerSegment:     2,
			SparsePrimaryField: "id",
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
	analysis, err := ExecuteSQLQueryParameters(context.Background(), "EXPLAIN ANALYZE "+"FROM CACHE('events') AS event WHERE event.id >= 100 SELECT COUNT(*) AS total, SUM(event.id) AS sum, AVG(event.id) AS average, MIN(event.id) AS minimum, MAX(event.id) AS maximum", probe, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("EXPLAIN ANALYZE error = %v", err)
	}
	foundPrimaryMark := false
	for _, row := range analysis.Rows {
		if row["node"] == "COLUMNAR PRIMARY MARK SKIP" {
			foundPrimaryMark = true
			break
		}
	}
	if !foundPrimaryMark {
		t.Fatalf("EXPLAIN ANALYZE rows = %#v, want COLUMNAR PRIMARY MARK SKIP", analysis.Rows)
	}
}

func TestSQLColumnarNumericFilterSkipsDisjointSegments(t *testing.T) {
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
	result, err := ExecuteSQLQueryParameters(context.Background(), "FROM CACHE('events') AS event WHERE event.id >= 100 SELECT event.id", probe, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryParameters() error = %v", err)
	}
	if want := []Row{{"id": float64(100)}, {"id": float64(101)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("ExecuteSQLQueryParameters() rows = %#v, want %#v", result.Rows, want)
	}
	analysis, err := ExecuteSQLQueryParameters(context.Background(), "EXPLAIN ANALYZE FROM CACHE('events') AS event WHERE event.id >= 100 SELECT event.id", probe, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("EXPLAIN ANALYZE error = %v", err)
	}
	for _, row := range analysis.Rows {
		if row["node"] == "COLUMNAR NUMERIC SEGMENT SKIP" {
			return
		}
	}
	t.Fatalf("EXPLAIN ANALYZE rows = %#v, want COLUMNAR NUMERIC SEGMENT SKIP", analysis.Rows)
}

func TestExecuteSQLQueryRowsNumericFilterSkipsDisjointSegments(t *testing.T) {
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
	var rows []SQLRow
	err := ExecuteSQLQueryRows(context.Background(), "FROM CACHE('events') AS event WHERE event.id >= 100 SELECT event.id", probe, nil, SQLQueryOptions{}, func(_ []string, row SQLRow) error {
		rows = append(rows, row)
		return nil
	})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryRows() error = %v", err)
	}
	if want := []SQLRow{{"id": float64(100)}, {"id": float64(101)}}; !reflect.DeepEqual(rows, want) {
		t.Fatalf("ExecuteSQLQueryRows() rows = %#v, want %#v", rows, want)
	}
}

func TestSQLColumnarDictionarySegmentMasksPreservePredicateSemantics(t *testing.T) {
	t.Parallel()
	segments := &ColumnarNumericSegments{
		RowsPerSegment: 2,
		DictionaryCodeSets: map[string][]uint64{
			"state": {1, 2},
		},
	}
	if sqlColumnarDictionarySegmentMayMatch(segments, 0, "state", "=", 1, true) {
		t.Fatal("equality unexpectedly retained a segment without the target dictionary code")
	}
	if !sqlColumnarDictionarySegmentMayMatch(segments, 1, "state", "=", 1, true) {
		t.Fatal("equality unexpectedly skipped a segment containing the target dictionary code")
	}
	if sqlColumnarDictionarySegmentMayMatch(segments, 0, "state", "!=", 0, true) {
		t.Fatal("inequality unexpectedly retained a segment containing only the excluded dictionary code")
	}
	if !sqlColumnarDictionarySegmentMayMatch(segments, 1, "state", "!=", 0, true) {
		t.Fatal("inequality unexpectedly skipped a segment containing another dictionary code")
	}
	if sqlColumnarDictionaryINSegmentMayMatch(segments, 0, "state", []bool{false, true}) {
		t.Fatal("IN unexpectedly retained a segment disjoint from its dictionary codes")
	}
	if !sqlColumnarDictionaryINSegmentMayMatch(segments, 1, "state", []bool{false, true}) {
		t.Fatal("IN unexpectedly skipped a segment containing a requested dictionary code")
	}
}

func BenchmarkSQLColumnarNumericFilterSegmentSkip(b *testing.B) {
	const (
		segmentCount = 8
		rowsPerPart  = 256
	)
	batch := ColumnarBatch{Columns: map[string][]interface{}{"id": make([]interface{}, segmentCount*rowsPerPart)}, Rows: segmentCount * rowsPerPart}
	segments := &ColumnarNumericSegments{
		RowsPerSegment: rowsPerPart,
		Columns:        map[string][]ColumnarNumericSegment{"id": make([]ColumnarNumericSegment, segmentCount)},
	}
	for segmentIndex := 0; segmentIndex < segmentCount; segmentIndex++ {
		minimum := float64(segmentIndex * rowsPerPart)
		maximum := float64((segmentIndex+1)*rowsPerPart - 1)
		segments.Columns["id"][segmentIndex] = ColumnarNumericSegment{Minimum: minimum, Maximum: maximum, Valid: true}
		for rowIndex := 0; rowIndex < rowsPerPart; rowIndex++ {
			batch.Columns["id"][segmentIndex*rowsPerPart+rowIndex] = minimum + float64(rowIndex)
		}
	}
	query := "FROM CACHE('events') AS event WHERE event.id >= 1792 SELECT event.id"
	for _, benchmark := range []struct {
		name     string
		segments *ColumnarNumericSegments
	}{
		{name: "without_segment_skip"},
		{name: "with_segment_skip", segments: segments},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				probe := &sqlSegmentedColumnarSourceProbe{batch: batch, segments: benchmark.segments}
				result, err := ExecuteSQLQueryParameters(context.Background(), query, probe, nil, SQLQueryOptions{})
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != rowsPerPart {
					b.Fatalf("result rows = %d, want %d", len(result.Rows), rowsPerPart)
				}
			}
		})
	}
}
