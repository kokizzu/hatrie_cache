package hatSql_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

type m219BackgroundSource struct {
	mu   sync.RWMutex
	rows []hatSql.Row
}

func (source *m219BackgroundSource) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
	source.mu.RLock()
	defer source.mu.RUnlock()
	rows := make([]hatSql.Row, len(source.rows))
	for rowIndex, row := range source.rows {
		copyRow := make(hatSql.Row, len(row))
		for column, value := range row {
			copyRow[column] = value
		}
		rows[rowIndex] = copyRow
	}
	return rows, nil
}

func (source *m219BackgroundSource) setRows(rows []hatSql.Row) {
	source.mu.Lock()
	source.rows = rows
	source.mu.Unlock()
}

func m219CreatePeopleView(t *testing.T, source *m219BackgroundSource) *hatSql.MaterializedViews {
	t.Helper()
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "people_view",
		Query:        "FROM CACHE('people') SELECT id, name",
		Dependencies: []string{"people"},
	}, source, hatSql.QueryOptions{}); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	return views
}

func m219PointLookupDefinition(key hatSql.MaterializedViewPointLookupKeyFunc) hatSql.MaterializedViewPointLookupDefinition {
	return hatSql.MaterializedViewPointLookupDefinition{
		Name:     "people_by_id",
		ViewName: "people_view",
		Key:      key,
	}
}

func m219WaitForBuildStart(t *testing.T, started <-chan struct{}) {
	t.Helper()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("background point lookup build did not start")
	}
}

func TestM219BackgroundPointLookupBuildReportsFrontierAndPublishesAtomically(t *testing.T) {
	rows := make([]hatSql.Row, 65)
	for rowIndex := range rows {
		rows[rowIndex] = hatSql.Row{"id": int64(rowIndex), "name": fmt.Sprintf("person-%d", rowIndex)}
	}
	source := &m219BackgroundSource{rows: rows}
	views := m219CreatePeopleView(t, source)
	blocked := make(chan struct{})
	release := make(chan struct{})
	key := func(row hatSql.Row) (string, error) {
		if row["id"] == int64(64) {
			close(blocked)
			<-release
		}
		return fmt.Sprint(row["id"]), nil
	}

	build, err := views.StartPointLookupIndexBuild(context.Background(), m219PointLookupDefinition(key))
	if err != nil {
		t.Fatalf("StartPointLookupIndexBuild() error = %v", err)
	}
	m219WaitForBuildStart(t, blocked)
	status := build.Status()
	if status.State != hatSql.MaterializedViewPointLookupBuildStateBuilding {
		t.Fatalf("Status().State = %q, want building", status.State)
	}
	if status.Frontier != 64 || status.TotalRows != 65 {
		t.Fatalf("Status() frontier/total = %d/%d, want 64/65", status.Frontier, status.TotalRows)
	}
	if _, found, err := views.LookupPoint("people_by_id", "1"); !errors.Is(err, hatSql.ErrMaterializedViewPointLookupIndexMissing) || found {
		t.Fatalf("LookupPoint() while building = found %v, err %v, want missing index", found, err)
	}
	if err := views.CreatePointLookupIndex(m219PointLookupDefinition(func(row hatSql.Row) (string, error) {
		return fmt.Sprint(row["id"]), nil
	})); !errors.Is(err, hatSql.ErrMaterializedViewPointLookupBuildInProgress) {
		t.Fatalf("CreatePointLookupIndex() while building error = %v, want build-in-progress", err)
	}

	close(release)
	status, err = build.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if status.State != hatSql.MaterializedViewPointLookupBuildStateReady || status.Frontier != status.TotalRows {
		t.Fatalf("final Status() = %#v, want ready at total frontier", status)
	}
	result, found, err := views.LookupPoint("people_by_id", "2")
	if err != nil || !found || len(result.Rows) != 1 || result.Rows[0]["name"] != "person-2" {
		t.Fatalf("LookupPoint() after build = %#v, %v, %v", result, found, err)
	}
}

func TestM219BackgroundPointLookupBuildRejectsStaleSnapshot(t *testing.T) {
	source := &m219BackgroundSource{rows: []hatSql.Row{
		{"id": int64(1), "name": "old"},
		{"id": int64(2), "name": "two"},
	}}
	views := m219CreatePeopleView(t, source)
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	build, err := views.StartPointLookupIndexBuild(context.Background(), m219PointLookupDefinition(func(row hatSql.Row) (string, error) {
		once.Do(func() { close(started) })
		<-release
		return fmt.Sprint(row["id"]), nil
	}))
	if err != nil {
		t.Fatalf("StartPointLookupIndexBuild() error = %v", err)
	}
	m219WaitForBuildStart(t, started)

	source.setRows([]hatSql.Row{
		{"id": int64(1), "name": "new"},
		{"id": int64(3), "name": "three"},
	})
	if _, err := views.RefreshChanged(context.Background(), []string{"people"}, source, hatSql.QueryOptions{}); err != nil {
		t.Fatalf("RefreshChanged() error = %v", err)
	}
	close(release)
	status, err := build.Wait(context.Background())
	if !errors.Is(err, hatSql.ErrMaterializedViewPointLookupBuildStale) || status.State != hatSql.MaterializedViewPointLookupBuildStateFailed {
		t.Fatalf("Wait() = %#v, %v, want stale failed build", status, err)
	}
	if _, found, err := views.LookupPoint("people_by_id", "1"); !errors.Is(err, hatSql.ErrMaterializedViewPointLookupIndexMissing) || found {
		t.Fatalf("LookupPoint() after stale build = found %v, err %v, want missing index", found, err)
	}
}

func TestM219BackgroundPointLookupBuildCanBeCanceled(t *testing.T) {
	source := &m219BackgroundSource{rows: []hatSql.Row{{"id": int64(1), "name": "one"}}}
	views := m219CreatePeopleView(t, source)
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	build, err := views.StartPointLookupIndexBuild(context.Background(), m219PointLookupDefinition(func(row hatSql.Row) (string, error) {
		once.Do(func() { close(started) })
		<-release
		return fmt.Sprint(row["id"]), nil
	}))
	if err != nil {
		t.Fatalf("StartPointLookupIndexBuild() error = %v", err)
	}
	m219WaitForBuildStart(t, started)
	build.Cancel()
	close(release)
	status, err := build.Wait(context.Background())
	if !errors.Is(err, hatSql.ErrMaterializedViewPointLookupBuildCanceled) || status.State != hatSql.MaterializedViewPointLookupBuildStateCanceled {
		t.Fatalf("Wait() = %#v, %v, want canceled build", status, err)
	}
	if _, found, err := views.LookupPoint("people_by_id", "1"); !errors.Is(err, hatSql.ErrMaterializedViewPointLookupIndexMissing) || found {
		t.Fatalf("LookupPoint() after cancel = found %v, err %v, want missing index", found, err)
	}
}
