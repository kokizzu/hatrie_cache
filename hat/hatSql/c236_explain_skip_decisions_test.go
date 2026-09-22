package hatSql

import (
	"context"
	"strings"
	"testing"
)

func TestC236ExplainSegmentDecisionsAndRejectedMarks(t *testing.T) {
	t.Parallel()
	probe := &sqlSegmentedColumnarSourceProbe{
		batch: ColumnarBatch{Columns: map[string][]interface{}{"id": {float64(1), float64(2), float64(50), float64(51), float64(100), float64(101)}}, Rows: 6},
		segments: &ColumnarNumericSegments{
			RowsPerSegment: 2,
			Columns: map[string][]ColumnarNumericSegment{
				"id": {
					{Minimum: 1, Maximum: 2, Valid: true},
					{Minimum: 50, Maximum: 51, Valid: true},
					{Minimum: 100, Maximum: 101, Valid: true},
				},
			},
		},
		rows: []Row{{"id": float64(1)}, {"id": float64(2)}, {"id": float64(50)}, {"id": float64(51)}, {"id": float64(100)}, {"id": float64(101)}},
	}
	result, err := ExecuteSQLQueryParameters(context.Background(), "EXPLAIN ANALYZE FROM CACHE('events') AS event WHERE event.id >= 100 SELECT event.id", probe, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("EXPLAIN ANALYZE error = %v", err)
	}
	var pruning *ExplainPruning
	for _, step := range result.Plan {
		if step.Node == "COLUMNAR NUMERIC SEGMENT SKIP" {
			pruning = step.Pruning
			break
		}
	}
	if pruning == nil {
		t.Fatalf("EXPLAIN ANALYZE plan = %#v, want numeric segment pruning", result.Plan)
	}
	if pruning.MarksExamined != 3 || pruning.RejectedMarks != 2 {
		t.Fatalf("pruning mark counts = %d/%d, want 3 examined and 2 rejected", pruning.MarksExamined, pruning.RejectedMarks)
	}
	if len(pruning.Decisions) != 3 {
		t.Fatalf("pruning decisions = %#v, want one decision per mark", pruning.Decisions)
	}
	if pruning.Decisions[0].Mark != 0 || pruning.Decisions[0].Action != "skip" || pruning.Decisions[1].Mark != 1 || pruning.Decisions[1].Action != "skip" || pruning.Decisions[2].Mark != 2 || pruning.Decisions[2].Action != "scan" {
		t.Fatalf("pruning decisions = %#v, want skip, skip, scan", pruning.Decisions)
	}
	encoded, err := MarshalExplainJSON(result.Plan)
	if err != nil {
		t.Fatalf("MarshalExplainJSON() error = %v", err)
	}
	for _, token := range []string{"\"marks_examined\":3", "\"rejected_marks\":2", "\"decisions\""} {
		if !strings.Contains(string(encoded), token) {
			t.Fatalf("explain JSON = %s, missing %s", encoded, token)
		}
	}
}

func TestC236ExplainPruningDecisionTraceIsBounded(t *testing.T) {
	t.Parallel()
	const segmentCount = 80
	const rowsPerSegment = 2
	values := make([]interface{}, segmentCount*rowsPerSegment)
	segments := make([]ColumnarNumericSegment, segmentCount)
	for segmentIndex := range segments {
		minimum := float64(segmentIndex * rowsPerSegment)
		values[segmentIndex*rowsPerSegment] = minimum
		values[segmentIndex*rowsPerSegment+1] = minimum + 1
		segments[segmentIndex] = ColumnarNumericSegment{Minimum: minimum, Maximum: minimum + 1, Valid: true}
	}
	probe := &sqlSegmentedColumnarSourceProbe{
		batch:    ColumnarBatch{Columns: map[string][]interface{}{"id": values}, Rows: len(values)},
		segments: &ColumnarNumericSegments{RowsPerSegment: rowsPerSegment, Columns: map[string][]ColumnarNumericSegment{"id": segments}},
	}
	result, err := ExecuteSQLQueryParameters(context.Background(), "EXPLAIN ANALYZE FROM CACHE('events') AS event WHERE event.id >= 160 SELECT event.id", probe, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("EXPLAIN ANALYZE error = %v", err)
	}
	var pruning *ExplainPruning
	for _, step := range result.Plan {
		if step.Node == "COLUMNAR NUMERIC SEGMENT SKIP" {
			pruning = step.Pruning
			break
		}
	}
	if pruning == nil {
		t.Fatalf("EXPLAIN ANALYZE plan = %#v, want numeric segment pruning", result.Plan)
	}
	if pruning.MarksExamined != segmentCount || pruning.RejectedMarks != 80 {
		t.Fatalf("pruning mark counts = %d/%d, want %d/80", pruning.MarksExamined, pruning.RejectedMarks, segmentCount)
	}
	if len(pruning.Decisions) != maxSQLExplainPruningDecisions || !pruning.DecisionsTruncated {
		t.Fatalf("pruning decision bound = len %d truncated %t, want %d/true", len(pruning.Decisions), pruning.DecisionsTruncated, maxSQLExplainPruningDecisions)
	}
}

func TestC236ExplainPruningCloneDoesNotAliasDecisions(t *testing.T) {
	original := &ExplainPruning{Decisions: []ExplainPruningDecision{{Mark: 1, Action: "skip"}}}
	clone := cloneExplainPruning(original)
	clone.Decisions[0].Action = "scan"
	if original.Decisions[0].Action != "skip" {
		t.Fatalf("clone mutated original decisions = %#v", original.Decisions)
	}
}
