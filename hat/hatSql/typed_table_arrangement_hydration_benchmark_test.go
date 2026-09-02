package hatSql_test

import (
	"strconv"
	"testing"

	"hatrie_cache/hat/hatSql"
)

const typedTableArrangementHydrationBenchmarkRows = 10000

var typedTableArrangementHydrationBenchmarkSink uint64

func BenchmarkTypedTableArrangementHydration(b *testing.B) {
	b.Run("hydrate_one_change", func(b *testing.B) {
		table, _, definition := newTypedTableArrangementBenchmarkFixture(b)
		arrangements, err := hatSql.NewTypedTableAggregateArrangements(table)
		if err != nil {
			b.Fatal(err)
		}
		arrangement, err := arrangements.Acquire(definition)
		if err != nil {
			b.Fatal(err)
		}
		defer arrangement.Release()
		for {
			report, err := arrangement.Hydrate(1024)
			if err != nil {
				b.Fatal(err)
			}
			if report.Complete {
				break
			}
		}
		if err := table.CompactChangesThrough(uint64(typedTableArrangementHydrationBenchmarkRows)); err != nil {
			b.Fatal(err)
		}

		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			b.StopTimer()
			change, err := table.Upsert("tail", []hatSql.TypedTableValue{
				{Kind: hatSql.TypedTableString, String: "red", Valid: true},
				{Kind: hatSql.TypedTableInt64, Int64: int64(index), Valid: true},
			})
			if err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			report, err := arrangement.Hydrate(1)
			if err != nil {
				b.Fatal(err)
			}
			typedTableArrangementHydrationBenchmarkSink = report.After
			b.StopTimer()
			if report.Applied != 1 || report.After != change.Sequence {
				b.Fatalf("Hydrate() report = %#v for change %#v", report, change)
			}
			if err := table.CompactChangesThrough(change.Sequence); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
		}
	})

	b.Run("rebuild_10000_changes", func(b *testing.B) {
		table, initialChanges, definition := newTypedTableArrangementBenchmarkFixture(b)
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			change := hatSql.TypedTableChange{
				Sequence:  typedTableArrangementHydrationBenchmarkRows + 1,
				Key:       "tail",
				Operation: "UPDATE",
				Before:    typedTableArrangementBenchmarkValues(typedTableArrangementHydrationBenchmarkRows - 1),
				After:     typedTableArrangementBenchmarkValues(index),
			}
			b.StartTimer()
			aggregate, err := hatSql.NewTypedTableAggregate(table, definition)
			if err != nil {
				b.Fatal(err)
			}
			if err := aggregate.Apply(initialChanges); err != nil {
				b.Fatal(err)
			}
			if err := aggregate.Apply([]hatSql.TypedTableChange{change}); err != nil {
				b.Fatal(err)
			}
			typedTableArrangementHydrationBenchmarkSink = aggregate.Checkpoint()
			b.StopTimer()
			b.StartTimer()
		}
	})
}

func newTypedTableArrangementBenchmarkFixture(b *testing.B) (*hatSql.TypedTable, []hatSql.TypedTableChange, hatSql.TypedTableAggregateDefinition) {
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
	for index := 0; index < typedTableArrangementHydrationBenchmarkRows; index++ {
		key := "row-" + strconv.Itoa(index)
		if index == typedTableArrangementHydrationBenchmarkRows-1 {
			key = "tail"
		}
		if _, err := table.Upsert(key, typedTableArrangementBenchmarkValues(index)); err != nil {
			b.Fatal(err)
		}
	}
	changes, _, err := table.ChangesAfter(0, typedTableArrangementHydrationBenchmarkRows)
	if err != nil {
		b.Fatal(err)
	}
	return table, changes, hatSql.TypedTableAggregateDefinition{GroupBy: []string{"team"}, SumField: "points"}
}

func typedTableArrangementBenchmarkValues(points int) []hatSql.TypedTableValue {
	return []hatSql.TypedTableValue{
		{Kind: hatSql.TypedTableString, String: "red", Valid: true},
		{Kind: hatSql.TypedTableInt64, Int64: int64(points), Valid: true},
	}
}
