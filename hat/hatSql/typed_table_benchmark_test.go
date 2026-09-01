package hatSql_test

import (
	"context"
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

func BenchmarkTypedTableAggregateMinMax(b *testing.B) {
	b.Run("incremental_non_extreme_update", func(b *testing.B) {
		table, aggregate := newTypedTableMinMaxBenchmarkTable(b)
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			change, err := table.Upsert("event-5000", []hatSql.TypedTableValue{hatSql.TypedString("blue"), hatSql.TypedInt64(int64(5_000 + index%10))})
			if err != nil {
				b.Fatal(err)
			}
			if err := aggregate.Apply([]hatSql.TypedTableChange{change}); err != nil {
				b.Fatal(err)
			}
			rows := aggregate.Rows()
			if len(rows) != 1 || rows[0]["min"] != int64(0) || rows[0]["max"] != int64(typedTableBenchmarkRows-1) {
				b.Fatalf("aggregate rows = %#v", rows)
			}
		}
	})
	b.Run("full_rescan_10000_rows", func(b *testing.B) {
		table, _ := newTypedTableMinMaxBenchmarkTable(b)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			rows := table.Rows()
			minimum, maximum := rows[0]["points"].(int64), rows[0]["points"].(int64)
			for _, row := range rows[1:] {
				points := row["points"].(int64)
				if points < minimum {
					minimum = points
				}
				if points > maximum {
					maximum = points
				}
			}
			if minimum != 0 || maximum != typedTableBenchmarkRows-1 {
				b.Fatalf("full rescan min/max = %d/%d", minimum, maximum)
			}
		}
	})
}

func BenchmarkTypedTableColumnarLayoutCache(b *testing.B) {
	const query = "FROM CACHE('events') SELECT team WHERE points = 9999"
	b.Run("rebuild_each_query", func(b *testing.B) {
		table := newTypedTableColumnarBenchmarkTable(b, hatSql.TypedTableColumnarCacheOptions{})
		b.ResetTimer()
		for b.Loop() {
			result, err := hatSql.ExecuteQueryParameters(context.Background(), query, table, nil, hatSql.QueryOptions{})
			if err != nil || len(result.Rows) != 1 || result.Rows[0]["team"] != "blue" {
				b.Fatalf("ExecuteQueryParameters() = %#v, %v", result, err)
			}
		}
	})
	b.Run("warmed_immutable_layout", func(b *testing.B) {
		table := newTypedTableColumnarBenchmarkTable(b, hatSql.TypedTableColumnarCacheOptions{Enabled: true, MaxBytes: 4 << 20, MinReads: 1})
		if _, err := hatSql.ExecuteQueryParameters(context.Background(), query, table, nil, hatSql.QueryOptions{}); err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		for b.Loop() {
			result, err := hatSql.ExecuteQueryParameters(context.Background(), query, table, nil, hatSql.QueryOptions{})
			if err != nil || len(result.Rows) != 1 || result.Rows[0]["team"] != "blue" {
				b.Fatalf("ExecuteQueryParameters() = %#v, %v", result, err)
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

func newTypedTableMinMaxBenchmarkTable(b *testing.B) (*hatSql.TypedTable, *hatSql.TypedTableAggregate) {
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
	aggregate, err := hatSql.NewTypedTableAggregate(table, hatSql.TypedTableAggregateDefinition{GroupBy: []string{"team"}, MinField: "points", MaxField: "points"})
	if err != nil {
		b.Fatal(err)
	}
	if err := aggregate.Apply(changes); err != nil {
		b.Fatal(err)
	}
	return table, aggregate
}

func newTypedTableColumnarBenchmarkTable(b *testing.B, options hatSql.TypedTableColumnarCacheOptions) *hatSql.TypedTable {
	b.Helper()
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: "events",
		Columns: []hatSql.TypedTableColumn{
			{Name: "team", Kind: hatSql.TypedTableString},
			{Name: "points", Kind: hatSql.TypedTableInt64},
		},
		ColumnarCache: options,
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < typedTableBenchmarkRows; index++ {
		key := "event-" + strconv.Itoa(index)
		if _, err := table.Upsert(key, []hatSql.TypedTableValue{hatSql.TypedString("blue"), hatSql.TypedInt64(int64(index))}); err != nil {
			b.Fatal(err)
		}
	}
	return table
}
