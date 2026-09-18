package hatSchema

import (
	"context"
	"fmt"
	"hatrie_cache/hat/hatSql"
	"reflect"
	"strings"
	"testing"
)

type tr030CountingResolver struct {
	adapter       SQLResolverAdapter
	calls         []string
	estimateCalls int
	statsCalls    int
}

func (resolver *tr030CountingResolver) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
	return resolver.adapter.ResolveSQLSource(name, key)
}

func (resolver *tr030CountingResolver) ResolveSQLIndexedSource(name, key, field string, value interface{}) ([]hatSql.Row, bool, error) {
	resolver.calls = append(resolver.calls, field)
	return resolver.adapter.ResolveSQLIndexedSource(name, key, field, value)
}

func (resolver *tr030CountingResolver) SQLJSONIndexValueEstimate(key, field string, value interface{}) (int, bool, bool, error) {
	resolver.estimateCalls++
	return resolver.adapter.SQLJSONIndexValueEstimate(key, field, value)
}

func (resolver *tr030CountingResolver) SQLJSONIndexStats(key string, fields ...string) (hatSql.JSONIndexStats, bool, error) {
	resolver.statsCalls++
	return resolver.adapter.SQLJSONIndexStats(key, fields...)
}

func TestTR030MaterializedSourceExposesDistributionStatsAndSelectsMostSelectiveIndex(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{{Name: "id", Indexed: true}, {Name: "kind", Indexed: true}})
	for index := 0; index < 100; index++ {
		kind := "common"
		if index >= 90 {
			kind = "rare"
		}
		if _, err := source.Insert(Row{"id": fmt.Sprintf("id-%03d", index), "kind": kind}); err != nil {
			t.Fatal(err)
		}
	}
	adapter := SQLResolverAdapter{Sources: map[string]*MaterializedSource{"events": source}}
	stats, available, err := adapter.SQLJSONIndexStats("events", "kind")
	if err != nil || !available {
		t.Fatalf("SQLJSONIndexStats() = %#v/%t/%v, want available", stats, available, err)
	}
	wantStats := hatSql.JSONIndexStats{
		Key:               "events",
		Fields:            []string{"kind"},
		Rows:              100,
		DistinctKeys:      2,
		MinRowsPerKey:     10,
		MaxRowsPerKey:     90,
		AverageRowsPerKey: 50,
		FrequencyHistogram: []hatSql.JSONIndexFrequencyBucket{
			{RowsPerKey: 10, DistinctKeys: 1},
			{RowsPerKey: 90, DistinctKeys: 1},
		},
	}
	if !reflect.DeepEqual(stats, wantStats) {
		t.Fatalf("kind stats = %#v, want %#v", stats, wantStats)
	}
	if _, available, err := adapter.SQLJSONIndexStats("events", "missing"); err != nil || available {
		t.Fatalf("missing stats = available %t, error %v; want unavailable", available, err)
	}
	if rows, exact, available, err := adapter.SQLJSONIndexValueEstimate("events", "id", "id-007"); err != nil || !exact || !available || rows != 1 {
		t.Fatalf("id value estimate = %d/%t/%t/%v, want one exact row", rows, exact, available, err)
	}

	resolver := &tr030CountingResolver{adapter: adapter}
	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), "FROM CACHE('events') AS event WHERE event.kind = 'common' AND event.id = 'id-007' SELECT event.id", resolver, hatSql.SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Rows, []hatSql.Row{{"id": "id-007"}}) {
		t.Fatalf("query rows = %#v, want id-007", result.Rows)
	}
	if !reflect.DeepEqual(resolver.calls, []string{"id"}) {
		t.Fatalf("index probes = %#v, want most-selective id index", resolver.calls)
	}

	singleResolver := &tr030CountingResolver{adapter: adapter}
	result, err = hatSql.ExecuteSQLQueryContext(context.Background(), "FROM CACHE('events') AS event WHERE event.id = 'id-007' SELECT event.id", singleResolver, hatSql.SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Rows, []hatSql.Row{{"id": "id-007"}}) {
		t.Fatalf("single-predicate rows = %#v, want id-007", result.Rows)
	}
	if singleResolver.statsCalls != 0 {
		t.Fatalf("single-predicate stats calls = %d, want no statistics lookup without competing indexes", singleResolver.statsCalls)
	}
	if singleResolver.estimateCalls != 0 {
		t.Fatalf("single-predicate estimate calls = %d, want no estimate lookup without competing indexes", singleResolver.estimateCalls)
	}
}

func TestTR030MaterializedSourceStatsTrackNullsAndFunctionalIndexes(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{{Name: "name"}, {Name: "state", Indexed: true}})
	for _, row := range []Row{
		{"name": "Ada", "state": "open"},
		{"name": "ADA", "state": "open"},
		{"name": "Grace", "state": nil},
		{"name": "Linus", "state": "closed"},
	} {
		if _, err := source.Insert(row); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := source.BuildFunctionalIndex("lower_name", []string{"name"}, func(row Row) (interface{}, error) {
		return strings.ToLower(row["name"].(string)), nil
	}); err != nil {
		t.Fatal(err)
	}
	adapter := SQLResolverAdapter{Sources: map[string]*MaterializedSource{"people": source}}
	state, available, err := adapter.SQLJSONIndexStats("people", "state")
	if err != nil || !available || state.Rows != 4 || state.NullRows != 1 || state.DistinctKeys != 2 || state.MinRowsPerKey != 1 || state.MaxRowsPerKey != 2 {
		t.Fatalf("state stats = %#v/%t/%v, want non-null distribution and null count", state, available, err)
	}
	if _, err := source.Insert(Row{"name": "Ada", "state": "open"}); err != nil {
		t.Fatal(err)
	}
	state, available, err = adapter.SQLJSONIndexStats("people", "state")
	if err != nil || !available || state.Rows != 5 || state.NullRows != 1 || state.DistinctKeys != 2 || state.MinRowsPerKey != 1 || state.MaxRowsPerKey != 3 {
		t.Fatalf("updated state stats = %#v/%t/%v, want invalidated distribution and null count", state, available, err)
	}
	lower, available, err := adapter.SQLJSONIndexStats("people", "lower_name")
	if err != nil || !available || lower.Rows != 5 || lower.NullRows != 0 || lower.DistinctKeys != 3 || lower.MinRowsPerKey != 1 || lower.MaxRowsPerKey != 3 {
		t.Fatalf("functional stats = %#v/%t/%v, want lower-name distribution", lower, available, err)
	}
	if rows, exact, available, err := adapter.SQLJSONIndexValueEstimate("people", "lower_name", "ada"); err != nil || !exact || !available || rows != 3 {
		t.Fatalf("functional value estimate = %d/%t/%t/%v, want three lower-name rows", rows, exact, available, err)
	}
	if _, err := source.Insert(Row{"name": "Ada", "state": "open"}); err != nil {
		t.Fatal(err)
	}
	lower, available, err = adapter.SQLJSONIndexStats("people", "lower_name")
	if err != nil || !available || lower.Rows != 6 || lower.NullRows != 0 || lower.DistinctKeys != 3 || lower.MinRowsPerKey != 1 || lower.MaxRowsPerKey != 4 {
		t.Fatalf("updated functional stats = %#v/%t/%v, want invalidated lower-name distribution", lower, available, err)
	}
}
