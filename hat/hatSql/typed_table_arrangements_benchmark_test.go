package hatSql_test

import (
	"strconv"
	"testing"

	"hatrie_cache/hat/hatSql"
)

const typedTableArrangementBenchmarkRows = 10_000

var typedTableArrangementBenchmarkRowsResult []hatSql.Row

func BenchmarkTypedTableAggregateArrangements(b *testing.B) {
	table, changes := newTypedTableArrangementBenchmarkInput(b)
	definition := hatSql.TypedTableAggregateDefinition{GroupBy: []string{"team"}, SumField: "points"}
	b.Run("independent_two_consumers", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			first, err := hatSql.NewTypedTableAggregate(table, definition)
			if err != nil {
				b.Fatal(err)
			}
			second, err := hatSql.NewTypedTableAggregate(table, definition)
			if err != nil {
				b.Fatal(err)
			}
			if err := first.Apply(changes); err != nil {
				b.Fatal(err)
			}
			if err := second.Apply(changes); err != nil {
				b.Fatal(err)
			}
			typedTableArrangementBenchmarkRowsResult = second.Rows()
		}
	})
	b.Run("shared_two_consumers", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			arrangements, err := hatSql.NewTypedTableAggregateArrangements(table)
			if err != nil {
				b.Fatal(err)
			}
			first, err := arrangements.Acquire(definition)
			if err != nil {
				b.Fatal(err)
			}
			second, err := arrangements.Acquire(definition)
			if err != nil {
				b.Fatal(err)
			}
			if err := first.Apply(changes); err != nil {
				b.Fatal(err)
			}
			typedTableArrangementBenchmarkRowsResult = second.Rows()
			first.Release()
			second.Release()
		}
	})
}

func newTypedTableArrangementBenchmarkInput(b *testing.B) (*hatSql.TypedTable, []hatSql.TypedTableChange) {
	b.Helper()
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: "scores",
		Columns: []hatSql.TypedTableColumn{
			{Name: "team", Kind: hatSql.TypedTableString},
			{Name: "points", Kind: hatSql.TypedTableInt64},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < typedTableArrangementBenchmarkRows; index++ {
		if _, err := table.Upsert("row-"+strconv.Itoa(index), []hatSql.TypedTableValue{hatSql.TypedString("team-" + strconv.Itoa(index%32)), hatSql.TypedInt64(int64(index % 100))}); err != nil {
			b.Fatal(err)
		}
	}
	changes, _, err := table.ChangesAfter(0, typedTableArrangementBenchmarkRows)
	if err != nil {
		b.Fatal(err)
	}
	return table, changes
}
