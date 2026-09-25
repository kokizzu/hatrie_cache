package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestMZ023ClusterAwareEqualityIndexUsesRequestedComputeCluster(t *testing.T) {
	resolver := &mz023ClusterIndexedResolver{
		rows: []Row{{"id": int64(1), "name": "Ada"}, {"id": int64(2), "name": "Grace"}},
	}
	result, err := ExecuteSQLQueryParameters(context.Background(),
		`FROM CACHE('people') AS p WHERE p.id = $1 SELECT p.name`, resolver, []interface{}{int64(2)}, SQLQueryOptions{
			ComputeCluster:        "analytics",
			DisableNativeDataflow: true,
		})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryParameters() error = %v", err)
	}
	if got, want := result.Rows, []Row{{"name": "Grace"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("rows = %#v, want %#v", got, want)
	}
	if resolver.cluster != "analytics" {
		t.Fatalf("cluster = %q, want analytics", resolver.cluster)
	}
	if resolver.legacyCalls != 0 {
		t.Fatalf("legacy index calls = %d, want 0", resolver.legacyCalls)
	}
}

func TestMZ023ClusterAwareEqualityIndexFallsBackToScanWhenPlacementUnavailable(t *testing.T) {
	resolver := &mz023ClusterIndexedResolver{
		rows:             []Row{{"id": int64(1), "name": "Ada"}, {"id": int64(2), "name": "Grace"}},
		availableCluster: "other",
	}
	result, err := ExecuteSQLQueryParameters(context.Background(),
		`FROM CACHE('people') AS p WHERE p.id = $1 SELECT p.name`, resolver, []interface{}{int64(2)}, SQLQueryOptions{
			ComputeCluster:        "analytics",
			DisableNativeDataflow: true,
		})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryParameters() error = %v", err)
	}
	if got, want := result.Rows, []Row{{"name": "Grace"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("rows = %#v, want %#v", got, want)
	}
	if resolver.cluster != "analytics" {
		t.Fatalf("cluster = %q, want analytics", resolver.cluster)
	}
	if resolver.legacyCalls != 0 {
		t.Fatalf("legacy index calls = %d, want 0", resolver.legacyCalls)
	}
}

func TestMZ023ComputeClusterPreservesLegacyResolverFallback(t *testing.T) {
	resolver := &mz023LegacyIndexedResolver{
		rows: []Row{{"id": int64(2), "name": "Grace"}},
	}
	result, err := ExecuteSQLQueryParameters(context.Background(),
		`FROM CACHE('people') AS p WHERE p.id = $1 SELECT p.name`, resolver, []interface{}{int64(2)}, SQLQueryOptions{
			ComputeCluster:        "analytics",
			DisableNativeDataflow: true,
		})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryParameters() error = %v", err)
	}
	if got, want := result.Rows, []Row{{"name": "Grace"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("rows = %#v, want %#v", got, want)
	}
	if resolver.legacyCalls != 1 {
		t.Fatalf("legacy index calls = %d, want 1", resolver.legacyCalls)
	}
}

func TestMZ023IndexStrategyFiltersCandidatesByCluster(t *testing.T) {
	decision, err := ExplainSQLIndexStrategy("people", "id", SQLIndexHint{
		Field:   "id",
		Cluster: "analytics",
		Mode:    SQLIndexHintForce,
	}, []SQLIndexStrategyCandidate{
		{SQLIndexDefinition: SQLIndexDefinition{Key: "people_by_id", Field: "id", Kind: "HASH", Cluster: "analytics"}, Available: true},
		{SQLIndexDefinition: SQLIndexDefinition{Key: "people_by_id", Field: "id", Kind: "ORDERED", Cluster: "other"}, Available: true},
	})
	if err != nil {
		t.Fatalf("ExplainSQLIndexStrategy() error = %v", err)
	}
	if !decision.HasSelection || decision.Selected.Cluster != "analytics" {
		t.Fatalf("decision = %+v, want analytics selection", decision)
	}
	if len(decision.Candidates) != 2 || decision.Candidates[1].Reason != SQLIndexStrategyReasonClusterMismatch {
		t.Fatalf("candidates = %+v, want cluster mismatch", decision.Candidates)
	}
}

func TestMZ023IndexStrategyRejectsPlacedCandidateWithoutComputeCluster(t *testing.T) {
	decision, err := ExplainSQLIndexStrategy("people", "id", SQLIndexHint{Field: "id", Mode: SQLIndexHintForce}, []SQLIndexStrategyCandidate{
		{SQLIndexDefinition: SQLIndexDefinition{Key: "people_by_id", Field: "id", Kind: "HASH", Cluster: "analytics"}, Available: true},
	})
	if !errors.Is(err, ErrSQLIndexStrategyHintUnsupported) {
		t.Fatalf("error = %v, want unsupported forced strategy", err)
	}
	if decision.HasSelection || decision.Candidates[0].Reason != SQLIndexStrategyReasonClusterRequired {
		t.Fatalf("decision = %+v, want cluster required", decision)
	}
}

type mz023ClusterIndexedResolver struct {
	rows             []Row
	cluster          string
	availableCluster string
	legacyCalls      int
}

type mz023LegacyIndexedResolver struct {
	rows        []Row
	legacyCalls int
}

func (resolver *mz023LegacyIndexedResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return resolver.rows, nil
}

func (resolver *mz023LegacyIndexedResolver) ResolveSQLIndexedSource(_, _, _ string, value interface{}) ([]Row, bool, error) {
	resolver.legacyCalls++
	wanted, ok := value.(int64)
	if !ok {
		return nil, false, errors.New("unexpected indexed value type")
	}
	for _, row := range resolver.rows {
		if row["id"] == wanted {
			return []Row{row}, true, nil
		}
	}
	return nil, true, nil
}

func (resolver *mz023ClusterIndexedResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return resolver.rows, nil
}

func (resolver *mz023ClusterIndexedResolver) ResolveSQLIndexedSource(string, string, string, interface{}) ([]Row, bool, error) {
	resolver.legacyCalls++
	return nil, false, nil
}

func (resolver *mz023ClusterIndexedResolver) ResolveSQLIndexedSourceInCluster(_, _, _, cluster string, value interface{}) ([]Row, bool, error) {
	resolver.cluster = cluster
	if resolver.availableCluster != "" && resolver.availableCluster != cluster {
		return nil, false, nil
	}
	wanted, ok := value.(int64)
	if !ok {
		return nil, false, errors.New("unexpected indexed value type")
	}
	for _, row := range resolver.rows {
		if row["id"] == wanted {
			return []Row{row}, true, nil
		}
	}
	return nil, true, nil
}
