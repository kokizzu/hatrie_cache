package hatCache

import (
	"context"
	"testing"
)

type adaptiveIndexTestResolver struct {
	rows       []SQLRow
	indexCalls int
}

func (resolver *adaptiveIndexTestResolver) ResolveSQLSource(name, key string) ([]SQLRow, error) {
	return resolver.rows, nil
}

func (resolver *adaptiveIndexTestResolver) ResolveSQLIndexedSource(name, key, field string, value interface{}) ([]SQLRow, bool, error) {
	resolver.indexCalls++
	return resolver.rows, true, nil
}

func (resolver *adaptiveIndexTestResolver) SQLJSONIndexValueEstimate(key, field string, value interface{}) (int, bool, bool, error) {
	return 1, true, true, nil
}

func TestSQLAdaptivePlannerSkipsPersistentlyMisleadingIndex(t *testing.T) {
	resolver := &adaptiveIndexTestResolver{rows: []SQLRow{{"kind": "common"}, {"kind": "common"}, {"kind": "common"}, {"kind": "common"}}}
	planner := NewSQLAdaptivePlanner(SQLAdaptivePlannerOptions{MinSamples: 1, UnderestimateFactor: 2})
	query := "FROM CACHE('events') AS event WHERE event.kind = 'common' SELECT event.kind"
	first, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, SQLQueryOptions{AdaptivePlanner: planner})
	if err != nil || len(first.Rows) != 4 || resolver.indexCalls != 1 {
		t.Fatalf("first query = %#v, %v; index calls %d", first, err, resolver.indexCalls)
	}
	second, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, SQLQueryOptions{AdaptivePlanner: planner})
	if err != nil || len(second.Rows) != 4 || resolver.indexCalls != 1 {
		t.Fatalf("second query = %#v, %v; index calls %d", second, err, resolver.indexCalls)
	}
}
