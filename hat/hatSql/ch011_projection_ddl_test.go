package hatSql

import (
	"context"
	"fmt"
	"testing"
)

type ch011ProjectionSource struct {
	rows    []Row
	version uint64
}

func (source *ch011ProjectionSource) ResolveSQLSource(string, string) ([]Row, error) {
	rows := make([]Row, len(source.rows))
	for index, row := range source.rows {
		rows[index] = cloneRow(row)
	}
	return rows, nil
}

func (source *ch011ProjectionSource) SQLSourceVersion(string, string) (string, bool, error) {
	return fmt.Sprintf("%d", source.version), true, nil
}

func cloneRow(row Row) Row {
	clone := make(Row, len(row))
	for key, value := range row {
		clone[key] = value
	}
	return clone
}

func TestCH011ProjectionDDLCreatesVersionGuardedHitAndDrops(t *testing.T) {
	source := &ch011ProjectionSource{
		version: 1,
		rows: []Row{
			{"id": int64(1), "score": int64(10)},
			{"id": int64(2), "score": int64(30)},
			{"id": int64(3), "score": int64(20)},
		},
	}
	session := NewSQLSession(source)
	query := "FROM CACHE('people') SELECT id, score ORDER BY score DESC LIMIT 2"
	if _, err := session.Execute(context.Background(), "CREATE PROJECTION top_people AS "+query, nil, SQLQueryOptions{}); err != nil {
		t.Fatal(err)
	}
	result, err := session.Execute(context.Background(), query, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 2 || result.Rows[0]["id"] != int64(2) || len(result.Plan) != 1 || result.Plan[0].Node != "PROJECTION HIT" {
		t.Fatalf("projection result = %#v, plan = %#v", result.Rows, result.Plan)
	}

	source.version = 2
	source.rows = append(source.rows, Row{"id": int64(4), "score": int64(40)})
	result, err = session.Execute(context.Background(), query, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 2 || result.Rows[0]["id"] != int64(4) || len(result.Plan) == 1 && result.Plan[0].Node == "PROJECTION HIT" {
		t.Fatalf("stale projection result = %#v, plan = %#v", result.Rows, result.Plan)
	}
	if _, err := session.Execute(context.Background(), "REFRESH PROJECTION top_people", nil, SQLQueryOptions{}); err != nil {
		t.Fatal(err)
	}
	result, err = session.Execute(context.Background(), query, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 2 || result.Rows[0]["id"] != int64(4) || len(result.Plan) != 1 || result.Plan[0].Node != "PROJECTION HIT" {
		t.Fatalf("refreshed projection result = %#v, plan = %#v", result.Rows, result.Plan)
	}

	if _, err := session.Execute(context.Background(), "DROP PROJECTION top_people", nil, SQLQueryOptions{}); err != nil {
		t.Fatal(err)
	}
	source.version = 3
	result, err = session.Execute(context.Background(), query, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Plan) == 1 && result.Plan[0].Node == "PROJECTION HIT" {
		t.Fatalf("dropped projection was selected: %#v", result.Plan)
	}
}

func TestCH011ProjectionDDLRequiresVersionedSource(t *testing.T) {
	session := NewSQLSession(SourceResolverFunc(func(string, string) ([]Row, error) {
		return []Row{{"id": int64(1)}}, nil
	}))
	if _, err := session.Execute(context.Background(), "CREATE PROJECTION ids AS FROM CACHE('people') SELECT id", nil, SQLQueryOptions{}); err == nil {
		t.Fatal("projection creation accepted an unversioned source")
	}
}

func TestCH011ProjectionDropReleasesMaterializedStorage(t *testing.T) {
	source := &ch011ProjectionSource{
		version: 1,
		rows:    []Row{{"id": int64(1), "score": int64(10)}},
	}
	views := NewMaterializedViews()
	if _, err := views.Create(context.Background(), MaterializedViewDefinition{
		Name:         "projection",
		Query:        "FROM CACHE('people') SELECT id",
		Dependencies: []string{"people"},
	}, source, QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	if usage := views.Usage(); usage.Rows != 1 {
		t.Fatalf("stored usage before drop = %#v, want one row", usage)
	}
	if err := views.Drop("projection"); err != nil {
		t.Fatal(err)
	}
	if usage := views.Usage(); usage.Rows != 0 || usage.Bytes != 0 {
		t.Fatalf("stored usage after drop = %#v, want zero", usage)
	}
	if _, exists := views.Get("projection"); exists {
		t.Fatal("dropped projection remained readable")
	}
}
