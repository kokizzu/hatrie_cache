package hatSql

import (
	"context"
	"encoding/json"
	"testing"
)

type mz050PlanSnapshotResolver struct{}

func (mz050PlanSnapshotResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return []Row{{"id": int64(1)}}, nil
}

func (mz050PlanSnapshotResolver) SQLSourceFrontier(string, string) (uint64, bool, bool, error) {
	return 7, true, true, nil
}

func (mz050PlanSnapshotResolver) SQLSourceVersion(string, string) (string, bool, error) {
	return "v1", true, nil
}

func (mz050PlanSnapshotResolver) BeginSQLSnapshotAt(context.Context, uint64) (SQLSourceResolver, func(), error) {
	return mz050PlanSnapshotResolver{}, nil, nil
}

func TestMZ050PlanSnapshotCapturesTemporalRequirements(t *testing.T) {
	required := uint64(5)
	result, err := ExecuteSQLQueryContext(context.Background(), "EXPLAIN SELECT id FROM CACHE('users')", mz050PlanSnapshotResolver{}, SQLQueryOptions{
		QueryID:                "query-mz050",
		RequireSourceFrontier:  true,
		RequiredSourceFrontier: required,
		PlanSnapshot:           &SQLPlanSnapshotOptions{},
	})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	if result.PlanSnapshot == nil {
		t.Fatal("PlanSnapshot is nil")
	}
	snapshot := result.PlanSnapshot
	if snapshot.Format != SQLPlanSnapshotFormat || snapshot.QueryID != "query-mz050" || snapshot.StartedAtUnixNano <= 0 {
		t.Fatalf("snapshot identity = %#v", snapshot)
	}
	if snapshot.RequiredSourceFrontier == nil || *snapshot.RequiredSourceFrontier != required {
		t.Fatalf("required frontier = %#v, want %d", snapshot.RequiredSourceFrontier, required)
	}
	if snapshot.AsOfFrontier != nil {
		t.Fatalf("unexpected as-of frontier = %#v", snapshot.AsOfFrontier)
	}
	if len(snapshot.Steps) == 0 || len(snapshot.Steps) != len(result.Plan) {
		t.Fatalf("snapshot steps = %d, result plan = %d", len(snapshot.Steps), len(result.Plan))
	}
	originalNode := result.Plan[0].Node
	snapshot.Steps[0].Node = "changed"
	if result.Plan[0].Node != originalNode {
		t.Fatal("snapshot shares mutable plan-step state with result")
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	var decoded QueryResult
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if decoded.PlanSnapshot == nil || decoded.PlanSnapshot.Format != SQLPlanSnapshotFormat || len(decoded.PlanSnapshot.Steps) != len(snapshot.Steps) {
		t.Fatalf("decoded snapshot = %#v", decoded.PlanSnapshot)
	}
}

func TestMZ050PlanSnapshotIsOptIn(t *testing.T) {
	result, err := ExecuteSQLQueryContext(context.Background(), "EXPLAIN SELECT id FROM CACHE('users')", mz050PlanSnapshotResolver{}, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	if result.PlanSnapshot != nil {
		t.Fatalf("default PlanSnapshot = %#v, want nil", result.PlanSnapshot)
	}
}

func TestMZ050PlanSnapshotOnOffsetPage(t *testing.T) {
	result, err := ExecuteSQLQueryPage(
		context.Background(),
		"SELECT id FROM CACHE('users')",
		mz050PlanSnapshotResolver{},
		nil,
		SQLQueryOptions{PlanSnapshot: &SQLPlanSnapshotOptions{}},
		1,
		"",
	)
	if err != nil {
		t.Fatalf("ExecuteSQLQueryPage() error = %v", err)
	}
	if result.PlanSnapshot == nil {
		t.Fatal("offset page PlanSnapshot is nil")
	}
	if result.PlanSnapshot.Format != SQLPlanSnapshotFormat {
		t.Fatalf("snapshot format = %q, want %q", result.PlanSnapshot.Format, SQLPlanSnapshotFormat)
	}
}

func TestMZ050PlanSnapshotCacheModeIsolated(t *testing.T) {
	cache := NewSQLResultCache(2)
	withSnapshot := SQLQueryOptions{ResultCache: cache, PlanSnapshot: &SQLPlanSnapshotOptions{}, QueryID: "with"}
	withoutSnapshot := SQLQueryOptions{ResultCache: cache, QueryID: "without"}
	query := "SELECT id FROM CACHE('users')"

	first, err := ExecuteSQLQueryContext(context.Background(), query, mz050PlanSnapshotResolver{}, withSnapshot)
	if err != nil {
		t.Fatalf("snapshot query error = %v", err)
	}
	if first.PlanSnapshot == nil {
		t.Fatal("snapshot query did not return a snapshot")
	}
	second, err := ExecuteSQLQueryContext(context.Background(), query, mz050PlanSnapshotResolver{}, withoutSnapshot)
	if err != nil {
		t.Fatalf("default query error = %v", err)
	}
	if second.PlanSnapshot != nil {
		t.Fatalf("default query reused snapshot = %#v", second.PlanSnapshot)
	}
	third, err := ExecuteSQLQueryContext(context.Background(), query, mz050PlanSnapshotResolver{}, withSnapshot)
	if err != nil {
		t.Fatalf("cached snapshot query error = %v", err)
	}
	if third.PlanSnapshot == nil || third.PlanSnapshot.QueryID != "with" {
		t.Fatalf("cached snapshot = %#v", third.PlanSnapshot)
	}
}

func TestMZ050PlanSnapshotCapturesExplicitZeroAsOfFrontier(t *testing.T) {
	frontier := uint64(0)
	result, err := ExecuteSQLQueryContext(context.Background(), "EXPLAIN SELECT id FROM CACHE('users')", mz050PlanSnapshotResolver{}, SQLQueryOptions{
		AsOfFrontier: &frontier,
		PlanSnapshot: &SQLPlanSnapshotOptions{},
	})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	if result.PlanSnapshot == nil || result.PlanSnapshot.AsOfFrontier == nil || *result.PlanSnapshot.AsOfFrontier != 0 {
		t.Fatalf("as-of snapshot = %#v", result.PlanSnapshot)
	}
}

func TestMZ050PlanSnapshotCloneCopiesFrontiersAndSteps(t *testing.T) {
	required, asOf := uint64(5), uint64(9)
	original := &SQLPlanSnapshot{
		Format:                 SQLPlanSnapshotFormat,
		RequiredSourceFrontier: &required,
		AsOfFrontier:           &asOf,
		Steps:                  []ExplainStep{{Node: "SCAN", Lineage: []ColumnLineage{{Output: "id", SourceFields: []string{"id"}}}}},
	}
	clone := cloneSQLPlanSnapshot(original)
	if clone == original || clone.RequiredSourceFrontier == original.RequiredSourceFrontier || clone.AsOfFrontier == original.AsOfFrontier || &clone.Steps[0] == &original.Steps[0] {
		t.Fatal("snapshot clone shares top-level storage")
	}
	clone.RequiredSourceFrontier = cloneSQLPlanSnapshotFrontier(nil)
	clone.Steps[0].Lineage[0].SourceFields[0] = "changed"
	if original.RequiredSourceFrontier == nil || *original.RequiredSourceFrontier != required || original.Steps[0].Lineage[0].SourceFields[0] != "id" {
		t.Fatalf("original snapshot mutated through clone = %#v", original)
	}
}
