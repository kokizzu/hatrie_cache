package hatSql

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFileSQLProjectionDefinitionStoreRoundTrip(t *testing.T) {
	store, err := NewFileSQLProjectionDefinitionStore(filepath.Join(t.TempDir(), "projections.spc"))
	if err != nil {
		t.Fatal(err)
	}
	want := []MaterializedViewDefinition{{
		Name:         "top_people",
		Query:        "FROM CACHE('people') SELECT id ORDER BY id DESC",
		Dependencies: []string{"people"},
	}}
	if err := store.SaveSQLProjectionDefinitions(context.Background(), want); err != nil {
		t.Fatal(err)
	}
	got, err := store.LoadSQLProjectionDefinitions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Name != want[0].Name || got[0].Query != want[0].Query || len(got[0].Dependencies) != 1 || got[0].Dependencies[0] != "people" {
		t.Fatalf("round trip = %#v, want %#v", got, want)
	}
}

func TestFileSQLProjectionDefinitionStoreRejectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "projections.spc")
	store, err := NewFileSQLProjectionDefinitionStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSQLProjectionDefinitions(context.Background(), []MaterializedViewDefinition{{Name: "p", Query: "FROM CACHE('people') SELECT id", Dependencies: []string{"people"}}}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1]++
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.LoadSQLProjectionDefinitions(context.Background()); !errors.Is(err, ErrSQLProjectionDefinitionStoreCorrupt) {
		t.Fatalf("corrupt load error = %v, want ErrSQLProjectionDefinitionStoreCorrupt", err)
	}
}

func TestSQLSessionRestoresDurableProjectionDefinitions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "projections.spc")
	store, err := NewFileSQLProjectionDefinitionStore(path)
	if err != nil {
		t.Fatal(err)
	}
	source := &ch011ProjectionSource{rows: []Row{{"id": int64(1), "name": "Ada"}}, version: 1}
	first := NewSQLSession(source)
	first.SetProjectionDefinitionStore(store)
	if err := first.CreateProjection(context.Background(), "people_projection", "FROM CACHE('people') SELECT id, name", SQLQueryOptions{}); err != nil {
		t.Fatal(err)
	}

	second, err := NewSQLSessionWithOptions(source, SQLSessionOptions{
		ProjectionDefinitionStore: store,
		AutoRestoreProjections:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := second.Execute(context.Background(), "FROM CACHE('people') SELECT id, name", nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["name"] != "Ada" {
		t.Fatalf("restored result = %#v", result.Rows)
	}
	if len(result.Plan) != 1 || result.Plan[0].Node != "PROJECTION HIT" {
		t.Fatalf("restored plan = %#v, want projection hit", result.Plan)
	}
}

func TestSQLSessionDropProjectionUpdatesDurableDefinitions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "projections.spc")
	store, err := NewFileSQLProjectionDefinitionStore(path)
	if err != nil {
		t.Fatal(err)
	}
	source := &ch011ProjectionSource{rows: []Row{{"id": int64(1), "name": "Ada"}}, version: 1}
	session := NewSQLSession(source)
	session.SetProjectionDefinitionStore(store)
	if err := session.CreateProjection(context.Background(), "people_projection", "FROM CACHE('people') SELECT id, name", SQLQueryOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := session.DropProjection("people_projection"); err != nil {
		t.Fatal(err)
	}
	definitions, err := store.LoadSQLProjectionDefinitions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(definitions) != 0 {
		t.Fatalf("durable definitions after drop = %#v, want empty", definitions)
	}
}

func TestSQLSessionProjectionStoreFailureRollsBackCreate(t *testing.T) {
	session := NewSQLSession(&ch011ProjectionSource{rows: []Row{{"id": int64(1)}}, version: 1})
	session.SetProjectionDefinitionStore(failingProjectionDefinitionStore{err: errors.New("disk full")})
	if err := session.CreateProjection(context.Background(), "people_projection", "FROM CACHE('people') SELECT id", SQLQueryOptions{}); err == nil {
		t.Fatal("CreateProjection succeeded with a failing definition store")
	}
	if _, exists := session.projectionCatalog().Get("people_projection"); exists {
		t.Fatal("projection remained after durable persistence failure")
	}
}

type failingProjectionDefinitionStore struct {
	err error
}

func (store failingProjectionDefinitionStore) LoadSQLProjectionDefinitions(context.Context) ([]MaterializedViewDefinition, error) {
	return nil, store.err
}

func (store failingProjectionDefinitionStore) SaveSQLProjectionDefinitions(context.Context, []MaterializedViewDefinition) error {
	return store.err
}
