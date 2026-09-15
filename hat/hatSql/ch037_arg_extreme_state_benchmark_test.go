package hatSql

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func ch037ArgExtremeBenchmarkQuery(selectExpr string) string {
	var query strings.Builder
	query.WriteString("FROM VALUES ")
	for index := 0; index < 1024; index++ {
		if index > 0 {
			query.WriteString(", ")
		}
		fmt.Fprintf(&query, "('payload-%04d', %d)", index, index%256)
	}
	query.WriteString(" AS events(payload, score) SELECT ")
	query.WriteString(selectExpr)
	return query.String()
}

func TestCH037ArgExtremeStateUsesDirectSourcePlan(t *testing.T) {
	query, err := CompileSQLQuery(ch037ArgExtremeBenchmarkQuery("ARGMAX_STATE(events.payload, events.score)"))
	if err != nil {
		t.Fatal(err)
	}
	aggregates, ok := sqlGlobalStreamAggregates(query.template)
	if !ok {
		t.Fatal("arg-extreme state query did not select the global stream plan")
	}
	if !sqlArgExtremeDirectSourcePlan(query.template, aggregates) {
		t.Fatal("arg-extreme state query did not select the direct source plan")
	}
}

func BenchmarkCH037ArgExtremeBaseline(b *testing.B) {
	query, err := CompileSQLQuery(ch037ArgExtremeBenchmarkQuery("ARGMAX(events.payload, events.score)"))
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
		if len(result.Rows) != 1 || result.Rows[0]["argmax"] != "payload-0255" {
			b.Fatalf("baseline result = %#v, want payload-0255", result.Rows)
		}
	}
	b.StopTimer()
	b.ReportMetric(1024, "rows/op")
}

func BenchmarkCH037ArgExtremeState(b *testing.B) {
	query, err := CompileSQLQuery(ch037ArgExtremeBenchmarkQuery("ARGMAX_STATE(events.payload, events.score)"))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	stateBytes := 0
	for range b.N {
		result, err := query.Execute(context.Background(), nil, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 1 {
			b.Fatalf("state rows = %d, want 1", len(result.Rows))
		}
		state, ok := result.Rows[0]["argmax_state"].([]byte)
		if !ok || len(state) == 0 {
			b.Fatalf("state = %#v, want non-empty []byte", result.Rows[0]["argmax_state"])
		}
		stateBytes = len(state)
	}
	b.StopTimer()
	b.ReportMetric(float64(stateBytes), "state-bytes/op")
	b.ReportMetric(1024, "rows/op")
}

func BenchmarkCH037ArgExtremeStateMerge(b *testing.B) {
	serialized, err := sqlEncodeArgExtremeState(sqlArgExtremeStateMax, SQLCollationBinary, "payload-0255", int64(255), true)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		state := sqlArgExtremeStateAccumulator{kind: sqlArgExtremeStateMax}
		if err := state.mergeValue(serialized); err != nil {
			b.Fatal(err)
		}
		if state.result(false) != "payload-0255" {
			b.Fatalf("merged state result = %#v, want payload-0255", state.result(false))
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(len(serialized)), "state-bytes/op")
}
