package hatSql

import (
	"bytes"
	"context"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestCHU11SQLIndexAdvisorRecommendsJSONPathSkipIndex(t *testing.T) {
	advisor := NewSQLIndexAdvisor(8)
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) {
		return []Row{{
			"doc": map[string]interface{}{"profile": map[string]interface{}{"city": "Singapore"}},
		}}, nil
	})
	_, err := ExecuteQueryParameters(context.Background(), "FROM CACHE('people') AS person WHERE JSON_VALUE(person.doc, '$.profile.city') = 'Singapore' SELECT person.doc", resolver, nil, QueryOptions{
		SlowQueryThreshold: time.Nanosecond,
		IndexAdvisor:       advisor,
	})
	if err != nil {
		t.Fatal(err)
	}
	recommendations := advisor.SkipIndexRecommendations(4)
	if len(recommendations) != 1 {
		t.Fatalf("SkipIndexRecommendations() length = %d, want 1", len(recommendations))
	}
	recommendation := recommendations[0]
	if recommendation.Key != "people" || recommendation.Field != "doc" || recommendation.Path != "$.profile.city" || recommendation.SlowQueries != 1 {
		t.Fatalf("SkipIndexRecommendations() = %#v", recommendation)
	}
	if recommendation.TotalElapsed <= 0 || recommendation.AverageElapsed <= 0 {
		t.Fatalf("recommendation elapsed values = %#v, want positive values", recommendation)
	}
}

func TestCHU11SQLIndexAdvisorRejectsUnsupportedJSONPathSkipPredicate(t *testing.T) {
	advisor := NewSQLIndexAdvisor(8)
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) {
		return []Row{{"doc": map[string]interface{}{"profile": map[string]interface{}{"city": "Singapore"}}}}, nil
	})
	_, err := ExecuteQueryParameters(context.Background(), "FROM CACHE('people') AS person WHERE JSON_VALUE(person.doc, '$.profile.city') >= 'Singapore' SELECT person.doc", resolver, nil, QueryOptions{
		SlowQueryThreshold: time.Nanosecond,
		IndexAdvisor:       advisor,
	})
	if err != nil {
		t.Fatal(err)
	}
	if recommendations := advisor.SkipIndexRecommendations(4); len(recommendations) != 0 {
		t.Fatalf("SkipIndexRecommendations() = %#v, want none for range predicate", recommendations)
	}
}

func TestCHU11SQLIndexAdvisorSkipIndexSnapshotRoundTrip(t *testing.T) {
	advisor := NewSQLIndexAdvisor(4)
	advisor.skipCounts[sqlIndexAdvisorSkipKey{key: "people", field: "doc", path: "$.profile.city"}] = sqlIndexAdvisorSkipStats{
		slowQueries:       3,
		totalElapsedNanos: uint64(12 * time.Millisecond),
	}
	var encoded bytes.Buffer
	if err := advisor.Save(&encoded); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	if strings.Contains(encoded.String(), "Singapore") {
		t.Fatalf("Save() retained a predicate value: %q", encoded.String())
	}
	restored := NewSQLIndexAdvisor(4)
	if err := restored.Load(bytes.NewReader(encoded.Bytes())); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got, want := restored.SkipIndexRecommendations(0), advisor.SkipIndexRecommendations(0); !reflect.DeepEqual(got, want) {
		t.Fatalf("restored SkipIndexRecommendations() = %#v, want %#v", got, want)
	}
}

func TestCHU11SQLIndexAdvisorRejectsInvalidSkipSnapshotWithoutMutation(t *testing.T) {
	advisor := NewSQLIndexAdvisor(4)
	advisor.skipCounts[sqlIndexAdvisorSkipKey{key: "people", field: "doc", path: "$.profile.city"}] = sqlIndexAdvisorSkipStats{
		slowQueries:       1,
		totalElapsedNanos: 1,
	}
	before := advisor.SkipIndexRecommendations(0)
	cases := []string{
		`{"version":3,"entries":[],"skip_indexes":[{"key":"people","field":"doc","path":"city","slow_queries":1}]}`,
		`{"version":3,"entries":[],"skip_indexes":[{"key":"people","field":"doc","path":"$.profile.city","slow_queries":0}]}`,
		`{"version":3,"entries":[],"skip_indexes":[{"key":"people","field":"doc","path":"$.profile.city","slow_queries":1},{"key":"people","field":"doc","path":"$.profile.city","slow_queries":1}]}`,
	}
	for _, payload := range cases {
		if err := advisor.Load(strings.NewReader(payload)); err == nil {
			t.Fatalf("Load(%q) error = nil", payload)
		}
		if got := advisor.SkipIndexRecommendations(0); !reflect.DeepEqual(got, before) {
			t.Fatalf("Load(%q) mutated recommendations: got %#v, want %#v", payload, got, before)
		}
	}
}

func TestCHU11SQLIndexAdvisorSkipIndexRecommendationsLimitAndCostOrder(t *testing.T) {
	advisor := NewSQLIndexAdvisor(4)
	advisor.skipCounts = map[sqlIndexAdvisorSkipKey]sqlIndexAdvisorSkipStats{
		{key: "people", field: "doc", path: "$.profile.city"}:    {slowQueries: 2, totalElapsedNanos: 20},
		{key: "people", field: "doc", path: "$.profile.country"}: {slowQueries: 3, totalElapsedNanos: 30},
	}
	recommendations := advisor.SkipIndexRecommendations(1)
	if len(recommendations) != 1 || recommendations[0].Path != "$.profile.country" {
		t.Fatalf("limited cost recommendations = %#v", recommendations)
	}
}

func TestCHU11SQLIndexAdvisorJSONSkipCollectorIsAllocationFreeWithoutJSONPredicate(t *testing.T) {
	query, err := parseSQLQuery("FROM CACHE('people') AS person WHERE person.id >= 1 SELECT person.id")
	if err != nil {
		t.Fatalf("parseSQLQuery() error = %v", err)
	}
	var candidates []sqlIndexAdvisorSkipCandidate
	allocations := testing.AllocsPerRun(100, func() {
		candidates = sqlIndexAdvisorJSONSkipCandidates(query.where, "person")
	})
	if candidates != nil {
		t.Fatalf("JSON skip candidates = %#v, want nil", candidates)
	}
	if allocations != 0 {
		t.Fatalf("JSON skip collector allocations = %v, want 0", allocations)
	}
}
