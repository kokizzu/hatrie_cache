package hatSql

import (
	"context"
	"testing"
)

type ch237VersionedResolver struct {
	rows    map[string][]Row
	version map[string]string
}

func (resolver *ch237VersionedResolver) ResolveSQLSource(_ string, key string) ([]Row, error) {
	rows := resolver.rows[key]
	copyRows := make([]Row, len(rows))
	for index, row := range rows {
		copyRows[index] = make(Row, len(row))
		for field, value := range row {
			copyRows[index][field] = value
		}
	}
	return copyRows, nil
}

func (resolver *ch237VersionedResolver) SQLSourceVersion(_ string, key string) (string, bool, error) {
	version, ok := resolver.version[key]
	return version, ok, nil
}

func (resolver *ch237VersionedResolver) SQLSourceSize(_ string, key string) (int, int64, bool, error) {
	rows := resolver.rows[key]
	return len(rows), sqlProjectionEstimatedRowsBytes(rows), true, nil
}

func TestCH237ExplainProjectionSelectionIncludesEstimatedIO(t *testing.T) {
	resolver := &ch237VersionedResolver{
		rows: map[string][]Row{
			"orders": {
				{"id": int64(1), "unused": "large-source-field"},
				{"id": int64(2), "unused": "another-source-field"},
			},
		},
		version: map[string]string{"orders": "v1"},
	}
	views := NewMaterializedViews()
	query := "FROM CACHE('orders') SELECT id"
	if _, err := views.Create(context.Background(), MaterializedViewDefinition{
		Name:         "orders_by_id",
		Query:        query,
		Dependencies: []string{"orders"},
	}, resolver, QueryOptions{}); err != nil {
		t.Fatalf("create projection: %v", err)
	}

	result, err := ExecuteSQLQueryContext(context.Background(), "EXPLAIN "+query, resolver, QueryOptions{ProjectionCatalog: views})
	if err != nil {
		t.Fatalf("EXPLAIN: %v", err)
	}
	var selection *SQLProjectionDiagnostics
	for index := range result.Plan {
		if result.Plan[index].Node == "PROJECTION SELECTION" {
			selection = result.Plan[index].Projection
			break
		}
	}
	if selection == nil {
		t.Fatalf("EXPLAIN plan = %#v, want projection selection diagnostics", result.Plan)
	}
	if !selection.Selected || selection.Name != "orders_by_id" {
		t.Fatalf("projection selection = %#v, want selected orders_by_id", *selection)
	}
	if selection.SourceRows != 2 || selection.ProjectionRows != 2 {
		t.Fatalf("projection rows = %#v, want source and projection rows 2", *selection)
	}
	if selection.SourceBytes <= selection.ProjectionBytes || selection.EstimatedSavedBytes <= 0 {
		t.Fatalf("projection byte estimate = %#v, want source > projection and saved bytes", *selection)
	}
	if selection.EstimatedReadBytes != selection.ProjectionBytes {
		t.Fatalf("estimated read bytes = %d, projection bytes = %d", selection.EstimatedReadBytes, selection.ProjectionBytes)
	}
	var row SQLRow
	for _, candidate := range result.Rows {
		if candidate["node"] == "PROJECTION SELECTION" {
			row = candidate
			break
		}
	}
	if row["projection"] == nil {
		t.Fatalf("EXPLAIN row = %#v, want projection column", row)
	}
}

func TestCH237ExplainProjectionSelectionRejectsStaleSource(t *testing.T) {
	resolver := &ch237VersionedResolver{
		rows:    map[string][]Row{"orders": {{"id": int64(1)}}},
		version: map[string]string{"orders": "v1"},
	}
	views := NewMaterializedViews()
	query := "FROM CACHE('orders') SELECT id"
	if _, err := views.Create(context.Background(), MaterializedViewDefinition{
		Name:         "orders_by_id",
		Query:        query,
		Dependencies: []string{"orders"},
	}, resolver, QueryOptions{}); err != nil {
		t.Fatalf("create projection: %v", err)
	}
	resolver.version["orders"] = "v2"
	result, err := ExecuteSQLQueryContext(context.Background(), "EXPLAIN "+query, resolver, QueryOptions{ProjectionCatalog: views})
	if err != nil {
		t.Fatalf("EXPLAIN: %v", err)
	}
	for _, step := range result.Plan {
		if step.Node == "PROJECTION SELECTION" {
			if step.Projection == nil || step.Projection.Selected || step.Projection.Reason != "projection source version is stale" {
				t.Fatalf("stale projection step = %#v", step)
			}
			return
		}
	}
	t.Fatalf("EXPLAIN plan = %#v, want stale projection diagnostic", result.Plan)
}
