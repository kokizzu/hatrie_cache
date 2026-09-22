package hatSql

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

type m222MutableSource struct {
	mu   sync.RWMutex
	rows []Row
}

type m222TestHelper interface {
	Helper()
	Fatalf(string, ...interface{})
}

func (source *m222MutableSource) ResolveSQLSource(_ string, key string) ([]Row, error) {
	if key != "people" {
		return nil, fmt.Errorf("unknown source %q", key)
	}
	source.mu.RLock()
	defer source.mu.RUnlock()
	rows := make([]Row, len(source.rows))
	for index, row := range source.rows {
		clone := make(Row, len(row))
		for column, value := range row {
			clone[column] = value
		}
		rows[index] = clone
	}
	return rows, nil
}

func (source *m222MutableSource) replace(rows []Row) {
	source.mu.Lock()
	source.rows = rows
	source.mu.Unlock()
}

func m222CreateReplicaViews(t m222TestHelper, source SourceResolver) *MaterializedViews {
	t.Helper()
	views := NewMaterializedViews()
	if _, err := views.Create(context.Background(), MaterializedViewDefinition{
		Name:         "people_view",
		Query:        "FROM CACHE('people') AS person SELECT person.id, person.name",
		Dependencies: []string{"people"},
	}, source, QueryOptions{}); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if err := views.CreatePointLookupIndex(MaterializedViewPointLookupDefinition{
		Name:     "people_by_id",
		ViewName: "people_view",
		Key: func(row Row) (string, error) {
			value, ok := row["id"]
			if !ok {
				return "", fmt.Errorf("id is required")
			}
			return fmt.Sprint(value), nil
		},
	}); err != nil {
		t.Fatalf("CreatePointLookupIndex() error = %v", err)
	}
	return views
}

func TestM222MaterializedViewComputeReplicaRefreshAndFailover(t *testing.T) {
	source := &m222MutableSource{rows: []Row{{"id": int64(1), "name": "Ada"}}}
	set, err := NewMaterializedViewComputeReplicaSet(
		MaterializedViewComputeReplica{Name: "primary", Views: m222CreateReplicaViews(t, source)},
		MaterializedViewComputeReplica{Name: "standby", Views: m222CreateReplicaViews(t, source)},
	)
	if err != nil {
		t.Fatalf("NewMaterializedViewComputeReplicaSet() error = %v", err)
	}

	if result, found, err := set.LookupPoint("people_by_id", "1"); err != nil || !found || result.Rows[0]["name"] != "Ada" {
		t.Fatalf("initial LookupPoint() = %#v, %v, %v", result, found, err)
	}
	if err := set.SetReplicaAvailable("primary", false); err != nil {
		t.Fatal(err)
	}
	source.replace([]Row{{"id": int64(1), "name": "Grace"}})
	report, err := set.RefreshChanged(context.Background(), []string{"people"}, source, QueryOptions{})
	if err != nil {
		t.Fatalf("RefreshChanged() with one fenced replica = %v", err)
	}
	if len(report.Replicas) != 1 || report.Replicas[0].Name != "standby" || len(report.Replicas[0].Refreshed) != 1 {
		t.Fatalf("refresh report = %#v", report)
	}
	if result, found, err := set.LookupPoint("people_by_id", "1"); err != nil || !found || result.Rows[0]["name"] != "Grace" {
		t.Fatalf("failover LookupPoint() = %#v, %v, %v", result, found, err)
	}

	if err := set.SetReplicaAvailable("primary", true); err != nil {
		t.Fatal(err)
	}
	if _, err := set.RefreshChanged(context.Background(), []string{"people"}, source, QueryOptions{}); err != nil {
		t.Fatalf("recovery RefreshChanged() error = %v", err)
	}
	statuses := set.Status()
	if len(statuses) != 2 || statuses[0].State != MaterializedViewComputeReplicaStateHealthy || statuses[1].State != MaterializedViewComputeReplicaStateHealthy {
		t.Fatalf("recovered replica statuses = %#v", statuses)
	}
}

func TestM222MaterializedViewComputeReplicaSetRequiresHealthyReplica(t *testing.T) {
	views := NewMaterializedViews()
	set, err := NewMaterializedViewComputeReplicaSet(MaterializedViewComputeReplica{Name: "only", Views: views})
	if err != nil {
		t.Fatal(err)
	}
	if err := set.SetReplicaAvailable("only", false); err != nil {
		t.Fatal(err)
	}
	if _, _, err := set.LookupPoint("missing", "1"); !errors.Is(err, ErrMaterializedViewComputeReplicaNoHealthy) {
		t.Fatalf("LookupPoint() error = %v", err)
	}
	if _, err := set.RefreshChanged(context.Background(), []string{"people"}, &m222MutableSource{}, QueryOptions{}); !errors.Is(err, ErrMaterializedViewComputeReplicaNoHealthy) {
		t.Fatalf("RefreshChanged() error = %v", err)
	}
}

func TestM222MaterializedViewComputeReplicaSetRejectsInvalidReplicas(t *testing.T) {
	if _, err := NewMaterializedViewComputeReplicaSet(); !errors.Is(err, ErrMaterializedViewComputeReplicaSetInvalid) {
		t.Fatalf("empty replica set error = %v", err)
	}
	if _, err := NewMaterializedViewComputeReplicaSet(MaterializedViewComputeReplica{Name: "duplicate", Views: NewMaterializedViews()}, MaterializedViewComputeReplica{Name: "duplicate", Views: NewMaterializedViews()}); !errors.Is(err, ErrMaterializedViewComputeReplicaSetInvalid) {
		t.Fatalf("duplicate replica error = %v", err)
	}
	if _, err := NewMaterializedViewComputeReplicaSet(MaterializedViewComputeReplica{Name: "nil"}); !errors.Is(err, ErrMaterializedViewComputeReplicaSetInvalid) {
		t.Fatalf("nil views error = %v", err)
	}
}
