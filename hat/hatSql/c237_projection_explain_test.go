package hatSql_test

import (
	"context"
	"encoding/json"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestC237ProjectionExplainReportsSelectionAndEstimatedIO(t *testing.T) {
	resolver := &projectionSelectionResolver{
		rows:    []hatSql.Row{{"name": "Ada"}, {"name": "Lin"}},
		version: "1",
	}
	views := hatSql.NewMaterializedViews()
	query := "FROM CACHE('events') SELECT name"
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "event_names",
		Query:        query,
		Dependencies: []string{"events"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}

	options := hatSql.QueryOptions{ProjectionCatalog: views, PlanSnapshot: &hatSql.SQLPlanSnapshotOptions{}}
	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), "EXPLAIN "+query, resolver, options)
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Steps []struct {
			Node        string `json:"node"`
			Projections []struct {
				Name             string `json:"name"`
				Selected         bool   `json:"selected"`
				RejectedReason   string `json:"rejected_reason"`
				EstimatedRows    int    `json:"estimated_rows"`
				EstimatedBytes   int    `json:"estimated_bytes"`
				EstimatedIOBytes int    `json:"estimated_io_bytes"`
			} `json:"projections"`
		} `json:"steps"`
	}
	payload, err := hatSql.MarshalExplainJSON(result.Plan)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(payload, &document); err != nil {
		t.Fatal(err)
	}
	var candidate struct {
		Name             string
		Selected         bool
		EstimatedRows    int
		EstimatedBytes   int
		EstimatedIOBytes int
	}
	found := false
	for _, step := range document.Steps {
		if step.Node == "PROJECTION" && len(step.Projections) == 1 {
			candidate.Name = step.Projections[0].Name
			candidate.Selected = step.Projections[0].Selected
			candidate.EstimatedRows = step.Projections[0].EstimatedRows
			candidate.EstimatedBytes = step.Projections[0].EstimatedBytes
			candidate.EstimatedIOBytes = step.Projections[0].EstimatedIOBytes
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("projection explain plan = %#v", result.Plan)
	}
	if candidate.Name != "event_names" || !candidate.Selected {
		t.Fatalf("projection candidate = %#v, want selected event_names", candidate)
	}
	if candidate.EstimatedRows != 2 || candidate.EstimatedBytes <= 0 || candidate.EstimatedIOBytes != candidate.EstimatedBytes {
		t.Fatalf("projection estimates = %#v, want rows and I/O bytes", candidate)
	}
	if _, ok := result.Rows[0]["projections"]; !ok {
		t.Fatalf("projection row = %#v, want projections column", result.Rows)
	}
	if result.PlanSnapshot == nil || len(result.PlanSnapshot.Steps) == 0 || len(result.PlanSnapshot.Steps[0].Projections) != 1 {
		t.Fatalf("projection plan snapshot = %#v", result.PlanSnapshot)
	}

	resolver.version = "2"
	result, err = hatSql.ExecuteSQLQueryContext(context.Background(), "EXPLAIN "+query, resolver, options)
	if err != nil {
		t.Fatal(err)
	}
	payload, err = hatSql.MarshalExplainJSON(result.Plan)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(payload, &document); err != nil {
		t.Fatal(err)
	}
	for _, step := range document.Steps {
		if step.Node != "PROJECTION" || len(step.Projections) != 1 {
			continue
		}
		if step.Projections[0].Selected || step.Projections[0].RejectedReason != "source_version_changed" {
			t.Fatalf("stale projection candidate = %#v", step.Projections[0])
		}
		return
	}
	t.Fatalf("stale projection explain plan = %#v", result.Plan)
}
