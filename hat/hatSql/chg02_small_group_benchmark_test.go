package hatSql_test

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestCHG02SmallGroupAggregatePreservesRows(t *testing.T) {
	query := chg02GroupQuery(4, 8)
	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, nil, hatSql.SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	want := []hatSql.Row{
		{"group_id": int64(0), "total": int64(8)},
		{"group_id": int64(1), "total": int64(8)},
		{"group_id": int64(2), "total": int64(8)},
		{"group_id": int64(3), "total": int64(8)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

var chg02BenchmarkResult hatSql.SQLQueryResult

func BenchmarkCHG02GroupedAggregate(b *testing.B) {
	for _, groups := range []int{1, 4, 16, 64, 256} {
		b.Run("groups="+strconv.Itoa(groups), func(b *testing.B) {
			query := chg02GroupQuery(groups, 2048/groups)
			cache := hatSql.NewSQLPreparedQueryCache(1)
			if _, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, nil, hatSql.SQLQueryOptions{PreparedCache: cache}); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, nil, hatSql.SQLQueryOptions{PreparedCache: cache})
				if err != nil {
					b.Fatal(err)
				}
				chg02BenchmarkResult = result
			}
		})
	}
}

func chg02GroupQuery(groups, rowsPerGroup int) string {
	var values strings.Builder
	values.WriteString("FROM VALUES ")
	for group := 0; group < groups; group++ {
		for row := 0; row < rowsPerGroup; row++ {
			if group != 0 || row != 0 {
				values.WriteString(", ")
			}
			values.WriteString("(")
			values.WriteString(fmt.Sprintf("%d", group))
			values.WriteString(")")
		}
	}
	values.WriteString(" AS src(group_id) GROUP BY src.group_id SELECT src.group_id, COUNT(*) AS total")
	return values.String()
}
