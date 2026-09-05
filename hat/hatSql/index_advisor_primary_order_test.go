package hatSql

import (
	"bytes"
	"context"
	"reflect"
	"testing"
	"time"
)

func TestSQLIndexAdvisorPrimaryOrderRecommendations(t *testing.T) {
	advisor := NewSQLIndexAdvisor(8)
	resolver := SourceResolverFunc(func(name, _ string) ([]Row, error) {
		return []Row{
			{"id": int64(1), "tenant": int64(1), "created_at": int64(10), "name": name},
		}, nil
	})
	options := QueryOptions{SlowQueryThreshold: time.Nanosecond, IndexAdvisor: advisor}
	queries := []string{
		"FROM CACHE('people') AS person WHERE person.tenant = 1 AND person.created_at >= 10 SELECT person.id",
		"FROM CACHE('people') AS person WHERE person.tenant = 2 SELECT person.id",
	}
	for _, query := range queries {
		if _, err := ExecuteQueryParameters(context.Background(), query, resolver, nil, options); err != nil {
			t.Fatalf("ExecuteQueryParameters() error = %v", err)
		}
	}

	recommendations := advisor.PrimaryOrderRecommendations()
	if len(recommendations) != 1 {
		t.Fatalf("PrimaryOrderRecommendations() = %#v, want one source", recommendations)
	}
	if recommendations[0].Key != "people" {
		t.Fatalf("recommendation key = %q, want people", recommendations[0].Key)
	}
	if want := []string{"tenant", "created_at"}; !reflect.DeepEqual(recommendations[0].Fields, want) {
		t.Fatalf("recommendation fields = %#v, want %#v", recommendations[0].Fields, want)
	}

	recommendations[0].Fields[0] = "mutated"
	if got := advisor.PrimaryOrderRecommendations()[0].Fields[0]; got != "tenant" {
		t.Fatalf("recommendation fields were not copied: %q", got)
	}
}

func TestSQLIndexAdvisorPrimaryOrderRecommendationsAfterSnapshotLoad(t *testing.T) {
	advisor := NewSQLIndexAdvisor(8)
	advisor.counts[sqlIndexAdvisorKey{key: "people", field: "tenant"}] = 2
	advisor.counts[sqlIndexAdvisorKey{key: "people", field: "created_at"}] = 1
	var snapshot bytes.Buffer
	if err := advisor.Save(&snapshot); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	loaded := NewSQLIndexAdvisor(8)
	if err := loaded.Load(&snapshot); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if want := []string{"tenant", "created_at"}; !reflect.DeepEqual(loaded.PrimaryOrderRecommendations()[0].Fields, want) {
		t.Fatalf("loaded recommendation fields = %#v, want %#v", loaded.PrimaryOrderRecommendations()[0].Fields, want)
	}
}
