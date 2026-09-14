package hatSql

import (
	"bytes"
	"context"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

var ch023PrimaryPrefixBenchmarkSink []SQLPrimaryPrefixRecommendation

func TestCH023SQLIndexAdvisorPrimaryPrefixRecommendations(t *testing.T) {
	advisor := NewSQLIndexAdvisor(8)
	resolver := SourceResolverFunc(func(name, _ string) ([]Row, error) {
		return []Row{{"id": int64(1), "tenant": int64(1), "created_at": int64(10), "name": name}}, nil
	})
	options := QueryOptions{SlowQueryThreshold: time.Nanosecond, IndexAdvisor: advisor}
	queries := []string{
		"FROM CACHE('people') AS person WHERE person.created_at >= 10 AND person.tenant = 1 SELECT person.id",
		"FROM CACHE('people') AS person WHERE person.tenant = 2 AND person.created_at >= 20 SELECT person.id",
		"FROM CACHE('people') AS person WHERE person.created_at >= 30 SELECT person.id",
	}
	for _, query := range queries {
		if _, err := ExecuteQueryParameters(context.Background(), query, resolver, nil, options); err != nil {
			t.Fatalf("ExecuteQueryParameters() error = %v", err)
		}
	}

	recommendations := advisor.PrimaryPrefixRecommendations(2)
	if len(recommendations) != 1 {
		t.Fatalf("PrimaryPrefixRecommendations() = %#v, want one source", recommendations)
	}
	recommendation := recommendations[0]
	if recommendation.Key != "people" || recommendation.SlowQueries != 2 {
		t.Fatalf("recommendation = %#v, want people and two matching queries", recommendation)
	}
	if want := []string{"tenant", "created_at"}; !reflect.DeepEqual(recommendation.Fields, want) {
		t.Fatalf("recommendation fields = %#v, want %#v", recommendation.Fields, want)
	}

	recommendation.Fields[0] = "mutated"
	if got := advisor.PrimaryPrefixRecommendations(2)[0].Fields[0]; got != "tenant" {
		t.Fatalf("recommendation fields were not copied: %q", got)
	}
}

func TestCH023SQLIndexAdvisorPrimaryPrefixSnapshotRoundTrip(t *testing.T) {
	advisor := NewSQLIndexAdvisor(8)
	advisor.counts[sqlIndexAdvisorKey{key: "people", field: "tenant"}] = 2
	advisor.prefixCounts[sqlIndexAdvisorPrefixKey{key: "people", fields: "tenant\x00created_at"}] = 2
	var snapshot bytes.Buffer
	if err := advisor.Save(&snapshot); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	loaded := NewSQLIndexAdvisor(8)
	if err := loaded.Load(&snapshot); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	got := loaded.PrimaryPrefixRecommendations(2)
	if len(got) != 1 || got[0].Key != "people" || got[0].SlowQueries != 2 || !reflect.DeepEqual(got[0].Fields, []string{"tenant", "created_at"}) {
		t.Fatalf("loaded prefix recommendations = %#v", got)
	}

	legacy := NewSQLIndexAdvisor(8)
	if err := legacy.Load(strings.NewReader(`{"version":1,"entries":[{"key":"people","field":"tenant","slow_queries":2}]}`)); err != nil {
		t.Fatalf("Load() legacy snapshot error = %v", err)
	}
	if got := legacy.PrimaryPrefixRecommendations(2); len(got) != 0 {
		t.Fatalf("legacy prefix recommendations = %#v, want none", got)
	}
}

func TestCH023SQLIndexAdvisorRejectsInvalidPrefixSnapshotWithoutMutation(t *testing.T) {
	advisor := NewSQLIndexAdvisor(8)
	advisor.prefixCounts[sqlIndexAdvisorPrefixKey{key: "people", fields: "tenant"}] = 3
	data := `{"version":2,"prefixes":[{"key":"orders","fields":["tenant","tenant"],"slow_queries":1}]}`
	if err := advisor.Load(strings.NewReader(data)); err == nil {
		t.Fatal("Load() accepted duplicate prefix fields")
	}
	got := advisor.PrimaryPrefixRecommendations(2)
	if len(got) != 1 || got[0].Key != "people" || got[0].SlowQueries != 3 {
		t.Fatalf("advisor changed after rejected load: %#v", got)
	}
}

func BenchmarkCH023SQLIndexAdvisorPrimaryPrefixRecommendations(b *testing.B) {
	advisor := NewSQLIndexAdvisor(128)
	for index := 0; index < 128; index++ {
		fields := "tenant_" + strconv.Itoa(index%8) + "\x00created_at"
		advisor.prefixCounts[sqlIndexAdvisorPrefixKey{key: "table_" + strconv.Itoa(index/8), fields: fields}] = uint64(index + 1)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		ch023PrimaryPrefixBenchmarkSink = advisor.PrimaryPrefixRecommendations(2)
	}
}
