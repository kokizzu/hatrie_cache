package hatCache

import (
	"context"
	"reflect"
	"testing"
)

type tr031IndexSelectionBenchmarkResolver struct {
	rows      []SQLRow
	withStats bool
}

func (resolver *tr031IndexSelectionBenchmarkResolver) ResolveSQLSource(_, _ string) ([]SQLRow, error) {
	return resolver.rows, nil
}

func (resolver *tr031IndexSelectionBenchmarkResolver) ResolveSQLIndexedSource(_, _, field string, value interface{}) ([]SQLRow, bool, error) {
	rows := make([]SQLRow, 0)
	for _, row := range resolver.rows {
		if reflect.DeepEqual(row[field], value) {
			rows = append(rows, row)
		}
	}
	return rows, true, nil
}

func (resolver *tr031IndexSelectionBenchmarkResolver) SQLJSONIndexStats(_ string, fields ...string) (SQLJSONIndexStats, bool, error) {
	if !resolver.withStats || len(fields) != 1 {
		return SQLJSONIndexStats{}, false, nil
	}
	switch fields[0] {
	case "kind":
		return SQLJSONIndexStats{Rows: len(resolver.rows), DistinctKeys: 2}, true, nil
	case "id":
		return SQLJSONIndexStats{Rows: len(resolver.rows), DistinctKeys: len(resolver.rows)}, true, nil
	default:
		return SQLJSONIndexStats{}, false, nil
	}
}

func BenchmarkTR031AutomaticIndexChoice(b *testing.B) {
	rows := make([]SQLRow, 20_000)
	for index := range rows {
		kind := "rare"
		if index%2 == 0 {
			kind = "common"
		}
		rows[index] = SQLRow{"id": int64(index), "kind": kind}
	}
	query := "FROM CACHE('events') AS event WHERE event.kind = 'common' AND event.id = 19998 SELECT event.id"

	bench := func(b *testing.B, withStats bool) {
		resolver := &tr031IndexSelectionBenchmarkResolver{rows: rows, withStats: withStats}
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			result, err := ExecuteSQLQuery(query, resolver)
			if err != nil || len(result.Rows) != 1 {
				b.Fatalf("query result = %#v, error = %v", result.Rows, err)
			}
		}
	}

	b.Run("legacy_left_to_right", func(b *testing.B) { bench(b, false) })
	b.Run("estimated_selectivity", func(b *testing.B) { bench(b, true) })
}

type tr031AdaptiveBenchmarkResolver struct {
	rows []SQLRow
}

func (resolver *tr031AdaptiveBenchmarkResolver) ResolveSQLSource(_, _ string) ([]SQLRow, error) {
	return resolver.rows, nil
}

func (resolver *tr031AdaptiveBenchmarkResolver) ResolveSQLIndexedSource(_, _, _ string, _ interface{}) ([]SQLRow, bool, error) {
	return append([]SQLRow(nil), resolver.rows...), true, nil
}

func (resolver *tr031AdaptiveBenchmarkResolver) SQLJSONIndexValueEstimate(_ string, _ string, _ interface{}) (int, bool, bool, error) {
	return 1, true, true, nil
}

func BenchmarkTR031AdaptiveFeedback(b *testing.B) {
	rows := make([]SQLRow, 20_000)
	for index := range rows {
		kind := "rare"
		if index%2 == 0 {
			kind = "common"
		}
		rows[index] = SQLRow{"id": int64(index), "kind": kind}
	}
	query := "FROM CACHE('events') AS event WHERE event.kind = 'common' SELECT event.id"

	b.Run("plain_index", func(b *testing.B) {
		resolver := &tr031AdaptiveBenchmarkResolver{rows: rows}
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			result, err := ExecuteSQLQuery(query, resolver)
			if err != nil || len(result.Rows) != len(rows)/2 {
				b.Fatalf("query result rows = %d, error = %v", len(result.Rows), err)
			}
		}
	})

	b.Run("adaptive_feedback", func(b *testing.B) {
		resolver := &tr031AdaptiveBenchmarkResolver{rows: rows}
		planner := NewSQLAdaptivePlanner(SQLAdaptivePlannerOptions{MinSamples: 1, UnderestimateFactor: 2})
		if _, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, SQLQueryOptions{AdaptivePlanner: planner}); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			result, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, SQLQueryOptions{AdaptivePlanner: planner})
			if err != nil || len(result.Rows) != len(rows)/2 {
				b.Fatalf("query result rows = %d, error = %v", len(result.Rows), err)
			}
		}
	})
}
