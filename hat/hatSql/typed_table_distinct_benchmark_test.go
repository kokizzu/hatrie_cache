package hatSql_test

import (
	"strconv"
	"testing"

	"hatrie_cache/hat/hatSql"
)

const typedTableDistinctBenchmarkRows = 10_000

func BenchmarkTypedTableAggregateCountDistinct(b *testing.B) {
	b.Run("incremental_non_final_value_update", func(b *testing.B) {
		table, aggregate := newTypedTableDistinctBenchmarkTable(b)
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			change, err := table.Upsert("event-5000", []hatSql.TypedTableValue{hatSql.TypedString("blue"), hatSql.TypedInt64(int64(1 + index%998))})
			if err != nil {
				b.Fatal(err)
			}
			if err := aggregate.Apply([]hatSql.TypedTableChange{change}); err != nil {
				b.Fatal(err)
			}
			rows := aggregate.Rows()
			if len(rows) != 1 || rows[0]["count"] != int64(typedTableDistinctBenchmarkRows) || rows[0]["count_distinct"] != int64(1_000) {
				b.Fatalf("aggregate rows = %#v", rows)
			}
		}
	})
	b.Run("full_rescan_10000_rows", func(b *testing.B) {
		table, _ := newTypedTableDistinctBenchmarkTable(b)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			values := make(map[int64]struct{}, 1_000)
			for _, row := range table.Rows() {
				values[row["points"].(int64)] = struct{}{}
			}
			if len(values) != 1_000 {
				b.Fatalf("full rescan count distinct = %d", len(values))
			}
		}
	})
}

func newTypedTableDistinctBenchmarkTable(b *testing.B) (*hatSql.TypedTable, *hatSql.TypedTableAggregate) {
	b.Helper()
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{Name: "events", Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}, {Name: "points", Kind: hatSql.TypedTableInt64}}})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < typedTableDistinctBenchmarkRows; index++ {
		key := "event-" + strconv.Itoa(index)
		if _, err := table.Upsert(key, []hatSql.TypedTableValue{hatSql.TypedString("blue"), hatSql.TypedInt64(int64(index % 1_000))}); err != nil {
			b.Fatal(err)
		}
	}
	changes, _, err := table.ChangesAfter(0, typedTableDistinctBenchmarkRows)
	if err != nil {
		b.Fatal(err)
	}
	aggregate, err := hatSql.NewTypedTableAggregate(table, hatSql.TypedTableAggregateDefinition{GroupBy: []string{"team"}, DistinctField: "points"})
	if err != nil {
		b.Fatal(err)
	}
	if err := aggregate.Apply(changes); err != nil {
		b.Fatal(err)
	}
	return table, aggregate
}
