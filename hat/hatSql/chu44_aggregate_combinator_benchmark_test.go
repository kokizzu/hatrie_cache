package hatSql

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func chu44AggregateStateQuery(function string) string {
	var values strings.Builder
	for index := 0; index < 128; index++ {
		if index != 0 {
			values.WriteString(", ")
		}
		fmt.Fprintf(&values, "(%d, %t)", index, index%2 == 0)
	}
	return fmt.Sprintf("FROM VALUES %s AS events(amount, keep) SELECT %s AS state", values.String(), function)
}

func BenchmarkCHU44AggregateStateFilter(b *testing.B) {
	query := chu44AggregateStateQuery("SUM_STATE(events.amount) FILTER (WHERE events.keep)")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLQueryContext(context.Background(), query, nil, SQLQueryOptions{})
		if err != nil || len(result.Rows) != 1 {
			b.Fatalf("filtered state query result=%#v err=%v", result.Rows, err)
		}
	}
}

func BenchmarkCHU44AggregateStateIf(b *testing.B) {
	query := chu44AggregateStateQuery("SUM_STATE_IF(events.amount, events.keep)")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLQueryContext(context.Background(), query, nil, SQLQueryOptions{})
		if err != nil || len(result.Rows) != 1 {
			b.Fatalf("composed state query result=%#v err=%v", result.Rows, err)
		}
	}
}
