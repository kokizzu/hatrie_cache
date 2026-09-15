package hatSql

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func ch036AggregateBenchmarkQuery(selectExpr string) string {
	var query strings.Builder
	query.WriteString("FROM VALUES ")
	for index := 0; index < 1024; index++ {
		if index > 0 {
			query.WriteString(", ")
		}
		fmt.Fprintf(&query, "(%d)", index%256)
	}
	query.WriteString(" AS events(value) SELECT ")
	query.WriteString(selectExpr)
	return query.String()
}

func BenchmarkCH036RowAggregateBaseline(b *testing.B) {
	query, err := CompileSQLQuery(ch036AggregateBenchmarkQuery("SUM(events.value)"))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := query.Execute(context.Background(), nil, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 1 {
			b.Fatalf("baseline rows = %d, want 1", len(result.Rows))
		}
	}
}

func BenchmarkCH036StateAggregate(b *testing.B) {
	query, err := CompileSQLQuery(ch036AggregateBenchmarkQuery("SUM_STATE(events.value)"))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	var stateBytes int
	for range b.N {
		result, err := query.Execute(context.Background(), nil, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 1 {
			b.Fatalf("state rows = %d, want 1", len(result.Rows))
		}
		state, ok := result.Rows[0]["sum_state"].([]byte)
		if !ok || len(state) == 0 {
			b.Fatalf("state = %#v, want non-empty []byte", result.Rows[0]["sum_state"])
		}
		stateBytes = len(state)
	}
	b.StopTimer()
	b.ReportMetric(float64(stateBytes), "state-bytes/op")
	b.ReportMetric(float64(1024*8), "scalar-input-bytes/op")
}

func BenchmarkCH036AggregateStateMerge(b *testing.B) {
	serialized := encodeSQLAggregateState(sqlAggregateStateAccumulator{
		kind:  sqlAggregateStateSum,
		count: 1024,
		sum:   130560,
		seen:  true,
	})
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		state := sqlAggregateStateAccumulator{kind: sqlAggregateStateSum}
		if err := state.mergeValue(serialized); err != nil {
			b.Fatal(err)
		}
		if state.result(false) != float64(130560) {
			b.Fatalf("merged state sum = %#v, want 130560", state.result(false))
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(len(serialized)), "state-bytes/op")
}
