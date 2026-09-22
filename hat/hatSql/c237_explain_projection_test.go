package hatSql

import (
	"context"
	"strings"
	"testing"
)

func TestC237ExplainProjectionSelectionAndIOEstimate(t *testing.T) {
	t.Parallel()
	probe := &sqlSegmentedColumnarSourceProbe{
		batch: ColumnarBatch{
			Columns: map[string][]interface{}{
				"id":      {float64(1), float64(2), float64(50), float64(51), float64(100), float64(101)},
				"payload": {"a", "b", "c", "d", "e", "f"},
			},
			Rows: 6,
		},
		rows: []Row{
			{"id": float64(1), "payload": "a"},
			{"id": float64(2), "payload": "b"},
			{"id": float64(50), "payload": "c"},
			{"id": float64(51), "payload": "d"},
			{"id": float64(100), "payload": "e"},
			{"id": float64(101), "payload": "f"},
		},
	}
	result, err := ExecuteSQLQueryParameters(context.Background(), "EXPLAIN ANALYZE FROM CACHE('events') AS event WHERE event.id >= 100 SELECT event.payload", probe, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("EXPLAIN ANALYZE error = %v", err)
	}
	var scan *ExplainStep
	for index := range result.Plan {
		if result.Plan[index].Node == "COLUMNAR SCAN" {
			scan = &result.Plan[index]
			break
		}
	}
	if scan == nil || scan.Projection == nil {
		t.Fatalf("EXPLAIN ANALYZE plan = %#v, want columnar projection metadata", result.Plan)
	}
	projection := scan.Projection
	if projection.Kind != "columnar" {
		t.Fatalf("projection kind = %q, want columnar", projection.Kind)
	}
	if got, want := strings.Join(projection.Fields, ","), "id,payload"; got != want {
		t.Fatalf("selected fields = %q, want %q", got, want)
	}
	if got, want := strings.Join(projection.PredicateFields, ","), "id"; got != want {
		t.Fatalf("predicate fields = %q, want %q", got, want)
	}
	if got, want := strings.Join(projection.OutputFields, ","), "payload"; got != want {
		t.Fatalf("output fields = %q, want %q", got, want)
	}
	if projection.EstimatedReadBytes <= 0 || projection.EstimatedReadBytesPerRow <= 0 {
		t.Fatalf("projection I/O estimate = %#v, want positive bytes", projection)
	}
	encoded, err := MarshalExplainJSON(result.Plan)
	if err != nil {
		t.Fatalf("MarshalExplainJSON() error = %v", err)
	}
	for _, token := range []string{"\"projection\"", "\"estimated_read_bytes\"", "\"predicate_fields\""} {
		if !strings.Contains(string(encoded), token) {
			t.Fatalf("explain JSON = %s, missing %s", encoded, token)
		}
	}
}

func TestC237ExplainPlanIncludesProjectionSelection(t *testing.T) {
	probe := &sqlSegmentedColumnarSourceProbe{
		batch: ColumnarBatch{
			Columns: map[string][]interface{}{
				"id":      {float64(1), float64(2)},
				"payload": {"a", "b"},
			},
			Rows: 2,
		},
	}
	result, err := ExecuteSQLQueryParameters(context.Background(), "EXPLAIN FROM CACHE('events') AS event WHERE event.id >= 1 SELECT event.payload", probe, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("EXPLAIN error = %v", err)
	}
	var scan *ExplainStep
	for index := range result.Plan {
		if result.Plan[index].Node == "SCAN" {
			scan = &result.Plan[index]
			break
		}
	}
	if scan == nil || scan.Projection == nil {
		t.Fatalf("EXPLAIN plan = %#v, want selected projection", result.Plan)
	}
	if scan.Projection.EstimatedReadBytes <= 0 {
		t.Fatalf("EXPLAIN projection = %#v, want estimated read bytes", scan.Projection)
	}
	var row SQLRow
	for _, candidate := range result.Rows {
		if candidate["node"] == "SCAN" {
			row = candidate
			break
		}
	}
	if _, ok := row["projection"]; !ok {
		t.Fatalf("EXPLAIN row = %#v, want projection column", row)
	}
}

func TestC237ExplainProjectionCloneDoesNotAliasFields(t *testing.T) {
	original := []ExplainStep{{Projection: &ExplainProjection{
		Fields:          []string{"id", "payload"},
		PredicateFields: []string{"id"},
		OutputFields:    []string{"payload"},
	}}}
	clone := cloneMaterializedExplainSteps(original)
	clone[0].Projection.Fields[0] = "changed"
	clone[0].Projection.PredicateFields[0] = "changed"
	clone[0].Projection.OutputFields[0] = "changed"
	if original[0].Projection.Fields[0] != "id" || original[0].Projection.PredicateFields[0] != "id" || original[0].Projection.OutputFields[0] != "payload" {
		t.Fatalf("projection clone mutated original = %#v", original[0].Projection)
	}
}
