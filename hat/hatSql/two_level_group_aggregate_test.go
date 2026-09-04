package hatSql

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

type twoLevelColumnarResolver struct {
	batch ColumnarBatch
}

func (resolver twoLevelColumnarResolver) ResolveSQLColumnarSource(string, string, []string) (ColumnarBatch, bool, error) {
	return resolver.batch, true, nil
}

func (resolver twoLevelColumnarResolver) ResolveSQLSource(string, string) ([]Row, error) {
	rows := make([]Row, resolver.batch.Rows)
	for index := range rows {
		group, _ := resolver.batch.Value("group", index)
		value, _ := resolver.batch.Value("value", index)
		rows[index] = Row{"group": group, "value": value}
	}
	return rows, nil
}

func newTwoLevelColumnarResolver(rows int) twoLevelColumnarResolver {
	return newTwoLevelColumnarResolverWithGroups(rows, 257)
}

func newTwoLevelColumnarResolverWithGroups(rows, groupCount int) twoLevelColumnarResolver {
	groups := make([]interface{}, rows)
	values := make([]interface{}, rows)
	for index := 0; index < rows; index++ {
		groups[index] = fmt.Sprintf("group-%03d", index%groupCount)
		values[index] = int64(index%1000 - 500)
	}
	return twoLevelColumnarResolver{batch: ColumnarBatch{
		Columns: map[string][]interface{}{"group": groups, "value": values},
		Rows:    rows,
	}}
}

func TestSQLColumnarTwoLevelGroupAggregateMatchesSequentialResults(t *testing.T) {
	resolver := newTwoLevelColumnarResolver(32 * 1024)
	query := "SELECT group, COUNT(*) AS n, MIN(value) AS low, MAX(value) AS high FROM CACHE('items') GROUP BY group"
	sequential, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("sequential query error = %v", err)
	}
	parallel, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{Workers: 2})
	if err != nil {
		t.Fatalf("two-level query error = %v", err)
	}
	if !reflect.DeepEqual(parallel.Columns, sequential.Columns) || !reflect.DeepEqual(parallel.Rows, sequential.Rows) {
		t.Fatalf("two-level result differs from sequential result: parallel=%#v sequential=%#v", parallel, sequential)
	}

	var event SQLQueryEvent
	_, err = ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{
		Workers: 2,
		Observer: SQLQueryObserverFunc(func(value SQLQueryEvent) {
			event = value
		}),
	})
	if err != nil {
		t.Fatalf("two-level observed query error = %v", err)
	}
	found := false
	for _, operator := range event.Operators {
		if strings.Contains(operator.Node, "TWO-LEVEL") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("two-level operators = %#v, want a TWO-LEVEL aggregate step", event.Operators)
	}
}

func TestSQLColumnarTwoLevelGroupAggregateKeepsSmallInputsOnSingleLevelPath(t *testing.T) {
	resolver := newTwoLevelColumnarResolver(1024)
	query := "SELECT group, COUNT(*) AS n FROM CACHE('items') GROUP BY group"
	var event SQLQueryEvent
	result, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{
		Workers: 2,
		Observer: SQLQueryObserverFunc(func(value SQLQueryEvent) {
			event = value
		}),
	})
	if err != nil {
		t.Fatalf("small grouped query error = %v", err)
	}
	if len(result.Rows) != 257 {
		t.Fatalf("small grouped rows = %d, want 257", len(result.Rows))
	}
	for _, operator := range event.Operators {
		if strings.Contains(operator.Node, "TWO-LEVEL") {
			t.Fatalf("small grouped operators = %#v, want the single-level path", event.Operators)
		}
	}
}

func TestSQLColumnarTwoLevelGroupAggregatePreservesGroupSkewLimit(t *testing.T) {
	resolver := newTwoLevelColumnarResolverWithGroups(32*1024, 1)
	_, err := ExecuteSQLQueryContext(context.Background(), "SELECT group, COUNT(*) AS n FROM CACHE('items') GROUP BY group", resolver, SQLQueryOptions{
		Workers:            2,
		MaxGroupRowsPerKey: 100,
	})
	if err == nil || !strings.Contains(err.Error(), "SQL group skew limit exceeded") {
		t.Fatalf("skew-limited grouped query error = %v, want skew-limit error", err)
	}
}

func TestSQLColumnarTwoLevelGroupAggregateKeepsSumOnEstablishedPath(t *testing.T) {
	resolver := newTwoLevelColumnarResolver(32 * 1024)
	query := "SELECT group, SUM(value) AS total FROM CACHE('items') GROUP BY group"
	var event SQLQueryEvent
	result, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{
		Workers: 2,
		Observer: SQLQueryObserverFunc(func(value SQLQueryEvent) {
			event = value
		}),
	})
	if err != nil {
		t.Fatalf("sum query error = %v", err)
	}
	if len(result.Rows) != 257 {
		t.Fatalf("sum query rows = %d, want 257", len(result.Rows))
	}
	for _, operator := range event.Operators {
		if strings.Contains(operator.Node, "TWO-LEVEL") {
			t.Fatalf("sum query operators = %#v, want established aggregate path", event.Operators)
		}
	}
}

func TestSQLColumnarTwoLevelGroupAggregateKeepsSingleAggregateOnEstablishedPath(t *testing.T) {
	resolver := newTwoLevelColumnarResolver(32 * 1024)
	query := "SELECT group, COUNT(*) AS n FROM CACHE('items') GROUP BY group"
	var event SQLQueryEvent
	_, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{
		Workers: 2,
		Observer: SQLQueryObserverFunc(func(value SQLQueryEvent) {
			event = value
		}),
	})
	if err != nil {
		t.Fatalf("single aggregate query error = %v", err)
	}
	for _, operator := range event.Operators {
		if strings.Contains(operator.Node, "TWO-LEVEL") {
			t.Fatalf("single aggregate operators = %#v, want established aggregate path", event.Operators)
		}
	}
}
