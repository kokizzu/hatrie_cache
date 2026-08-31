package hatSql_test

import (
	"strconv"
	"testing"

	"hatrie_cache/hat/hatSql"
)

const typedTableBenchmarkRows = 10_000

func BenchmarkTypedTableAggregate(b *testing.B) {
	b.Run("incremental_one_change", func(b *testing.B) {
		table, aggregate := newTypedTableBenchmarkTable(b)
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			change, err := table.Upsert("event-00000", []hatSql.TypedTableValue{hatSql.TypedString("blue"), hatSql.TypedInt64(int64(index))})
			if err != nil {
				b.Fatal(err)
			}
			if err := aggregate.Apply([]hatSql.TypedTableChange{change}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("full_rescan_10000_rows", func(b *testing.B) {
		table, _ := newTypedTableBenchmarkTable(b)
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			rows := table.Rows()
			var count int64
			var sum int64
			for _, row := range rows {
				count++
				sum += row["points"].(int64)
			}
			if count != typedTableBenchmarkRows || sum < 0 {
				b.Fatal("unexpected full rescan aggregate")
			}
		}
	})
}

func newTypedTableBenchmarkTable(b *testing.B) (*hatSql.TypedTable, *hatSql.TypedTableAggregate) {
	b.Helper()
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{Name: "events", Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}, {Name: "points", Kind: hatSql.TypedTableInt64}}})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < typedTableBenchmarkRows; index++ {
		key := "event-" + strconv.Itoa(index)
		if _, err := table.Upsert(key, []hatSql.TypedTableValue{hatSql.TypedString("blue"), hatSql.TypedInt64(int64(index))}); err != nil {
			b.Fatal(err)
		}
	}
	changes, _, err := table.ChangesAfter(0, typedTableBenchmarkRows)
	if err != nil {
		b.Fatal(err)
	}
	aggregate, err := hatSql.NewTypedTableAggregate(table, hatSql.TypedTableAggregateDefinition{GroupBy: []string{"team"}, SumField: "points"})
	if err != nil {
		b.Fatal(err)
	}
	if err := aggregate.Apply(changes); err != nil {
		b.Fatal(err)
	}
	return table, aggregate
}
