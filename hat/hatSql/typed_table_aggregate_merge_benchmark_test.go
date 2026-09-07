package hatSql_test

import (
	"strconv"
	"testing"

	"hatrie_cache/hat/hatSql"
)

const typedTableAggregateMergeBenchmarkRowsPerPartition = 4096

var typedTableAggregateMergeBenchmarkSink *hatSql.TypedTableAggregate

type typedTableAggregateMergeBenchmarkFixture struct {
	definition     hatSql.TypedTableAggregateDefinition
	first          *hatSql.TypedTableAggregate
	second         *hatSql.TypedTableAggregate
	targetTable    *hatSql.TypedTable
	combinedChange []hatSql.TypedTableChange
}

func BenchmarkTypedTableAggregateMergePartials(b *testing.B) {
	fixture := newTypedTableAggregateMergeBenchmarkFixture(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		target, err := hatSql.NewTypedTableAggregate(fixture.targetTable, fixture.definition)
		if err != nil {
			b.Fatal(err)
		}
		if err := target.MergePartials(fixture.first, fixture.second); err != nil {
			b.Fatal(err)
		}
		typedTableAggregateMergeBenchmarkSink = target
	}
}

func BenchmarkTypedTableAggregateReplayCombined(b *testing.B) {
	fixture := newTypedTableAggregateMergeBenchmarkFixture(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		target, err := hatSql.NewTypedTableAggregate(fixture.targetTable, fixture.definition)
		if err != nil {
			b.Fatal(err)
		}
		if err := target.Apply(fixture.combinedChange); err != nil {
			b.Fatal(err)
		}
		typedTableAggregateMergeBenchmarkSink = target
	}
}

func newTypedTableAggregateMergeBenchmarkFixture(b *testing.B) typedTableAggregateMergeBenchmarkFixture {
	b.Helper()
	definition := hatSql.TypedTableAggregateDefinition{
		GroupBy:       []string{"team"},
		SumField:      "points",
		MinField:      "points",
		MaxField:      "points",
		DistinctField: "label",
	}
	_, firstAggregate, _ := newTypedTableAggregateMergeBenchmarkPartition(b, "first", 0, typedTableAggregateMergeBenchmarkRowsPerPartition)
	_, secondAggregate, _ := newTypedTableAggregateMergeBenchmarkPartition(b, "second", typedTableAggregateMergeBenchmarkRowsPerPartition, typedTableAggregateMergeBenchmarkRowsPerPartition)
	_, _, combinedChanges := newTypedTableAggregateMergeBenchmarkPartition(b, "combined", 0, typedTableAggregateMergeBenchmarkRowsPerPartition*2)
	targetTable := newTypedTableAggregateMergeBenchmarkTable(b, "target")
	return typedTableAggregateMergeBenchmarkFixture{
		definition:     definition,
		first:          firstAggregate,
		second:         secondAggregate,
		targetTable:    targetTable,
		combinedChange: combinedChanges,
	}
}

func newTypedTableAggregateMergeBenchmarkPartition(b *testing.B, name string, offset, count int) (*hatSql.TypedTable, *hatSql.TypedTableAggregate, []hatSql.TypedTableChange) {
	b.Helper()
	table := newTypedTableAggregateMergeBenchmarkTable(b, name)
	definition := hatSql.TypedTableAggregateDefinition{
		GroupBy:       []string{"team"},
		SumField:      "points",
		MinField:      "points",
		MaxField:      "points",
		DistinctField: "label",
	}
	changes := make([]hatSql.TypedTableChange, 0, count)
	for index := 0; index < count; index++ {
		rowIndex := offset + index
		change, err := table.Upsert(
			"row-"+strconv.Itoa(rowIndex),
			[]hatSql.TypedTableValue{
				hatSql.TypedString("team-" + strconv.Itoa(rowIndex%256)),
				hatSql.TypedInt64(int64(rowIndex)),
				hatSql.TypedString("label-" + strconv.Itoa(rowIndex%512)),
			},
		)
		if err != nil {
			b.Fatal(err)
		}
		changes = append(changes, change)
	}
	aggregate, err := hatSql.NewTypedTableAggregate(table, definition)
	if err != nil {
		b.Fatal(err)
	}
	if err := aggregate.Apply(changes); err != nil {
		b.Fatal(err)
	}
	return table, aggregate, changes
}

func newTypedTableAggregateMergeBenchmarkTable(b *testing.B, name string) *hatSql.TypedTable {
	b.Helper()
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: name,
		Columns: []hatSql.TypedTableColumn{
			{Name: "team", Kind: hatSql.TypedTableString},
			{Name: "points", Kind: hatSql.TypedTableInt64},
			{Name: "label", Kind: hatSql.TypedTableString},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	return table
}
