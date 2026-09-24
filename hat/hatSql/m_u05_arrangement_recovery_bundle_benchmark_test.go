package hatSql_test

import (
	"encoding/json"
	"strconv"
	"testing"

	"hatrie_cache/hat/hatSql"
)

var typedTableArrangementRecoveryBundleBenchmarkSink int

func BenchmarkTypedTableArrangementRecoveryBundle(b *testing.B) {
	fixture := newTypedTableArrangementRecoveryBundleBenchmarkFixture(b)
	encoded, err := json.Marshal(fixture.checkpoint)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("capture_manual_catalogs", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.ReportMetric(float64(len(encoded)), "checkpoint-bytes")
		for range b.N {
			aggregates, err := fixture.aggregateCatalog.CaptureCheckpoints()
			if err != nil {
				b.Fatal(err)
			}
			joins, err := fixture.joinCatalog.CaptureCheckpoints()
			if err != nil {
				b.Fatal(err)
			}
			typedTableArrangementRecoveryBundleBenchmarkSink = len(aggregates) + len(joins)
		}
	})

	b.Run("capture_bundle", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.ReportMetric(float64(len(encoded)), "checkpoint-bytes")
		for range b.N {
			checkpoint, err := hatSql.CaptureTypedTableArrangementRecovery(
				[]*hatSql.TypedTableAggregateArrangements{fixture.aggregateCatalog},
				[]*hatSql.TypedTableJoinArrangements{fixture.joinCatalog},
			)
			if err != nil {
				b.Fatal(err)
			}
			typedTableArrangementRecoveryBundleBenchmarkSink = len(checkpoint.Aggregates) + len(checkpoint.Joins)
		}
	})

	b.Run("restore_manual_catalogs", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.ReportMetric(float64(len(encoded)), "checkpoint-bytes")
		for range b.N {
			aggregates, err := hatSql.NewTypedTableAggregateArrangements(fixture.aggregateTable)
			if err != nil {
				b.Fatal(err)
			}
			joins, err := hatSql.NewTypedTableJoinArrangements(fixture.leftTable, fixture.rightTable)
			if err != nil {
				b.Fatal(err)
			}
			aggregateLeases, err := aggregates.RestoreCheckpoints(fixture.checkpoint.Aggregates)
			if err != nil {
				b.Fatal(err)
			}
			joinLeases, err := joins.RestoreCheckpoints(fixture.checkpoint.Joins)
			if err != nil {
				b.Fatal(err)
			}
			for _, lease := range aggregateLeases {
				lease.Release()
			}
			for _, lease := range joinLeases {
				lease.Release()
			}
			typedTableArrangementRecoveryBundleBenchmarkSink = len(aggregateLeases) + len(joinLeases)
		}
	})

	b.Run("restore_bundle", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.ReportMetric(float64(len(encoded)), "checkpoint-bytes")
		for range b.N {
			aggregates, err := hatSql.NewTypedTableAggregateArrangements(fixture.aggregateTable)
			if err != nil {
				b.Fatal(err)
			}
			joins, err := hatSql.NewTypedTableJoinArrangements(fixture.leftTable, fixture.rightTable)
			if err != nil {
				b.Fatal(err)
			}
			lease, err := hatSql.RestoreTypedTableArrangementRecovery(
				[]*hatSql.TypedTableAggregateArrangements{aggregates},
				[]*hatSql.TypedTableJoinArrangements{joins},
				fixture.checkpoint,
			)
			if err != nil {
				b.Fatal(err)
			}
			lease.Release()
			typedTableArrangementRecoveryBundleBenchmarkSink = len(lease.AggregateArrangements()) + len(lease.JoinArrangements())
		}
	})
}

type typedTableArrangementRecoveryBundleBenchmarkFixture struct {
	aggregateTable   *hatSql.TypedTable
	leftTable        *hatSql.TypedTable
	rightTable       *hatSql.TypedTable
	aggregateCatalog *hatSql.TypedTableAggregateArrangements
	joinCatalog      *hatSql.TypedTableJoinArrangements
	checkpoint       hatSql.TypedTableArrangementRecoveryCheckpoint
}

func newTypedTableArrangementRecoveryBundleBenchmarkFixture(b *testing.B) typedTableArrangementRecoveryBundleBenchmarkFixture {
	b.Helper()
	aggregateTable, aggregateChanges := newTypedTableArrangementRecoveryAggregateBenchmarkTable(b)
	aggregateCatalog, err := hatSql.NewTypedTableAggregateArrangements(aggregateTable)
	if err != nil {
		b.Fatal(err)
	}
	aggregate, err := aggregateCatalog.Acquire(hatSql.TypedTableAggregateDefinition{
		GroupBy:       []string{"team"},
		SumField:      "points",
		MinField:      "points",
		MaxField:      "points",
		DistinctField: "name",
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := aggregate.Apply(aggregateChanges); err != nil {
		b.Fatal(err)
	}

	leftTable, leftChanges := newTypedTableArrangementRecoveryJoinBenchmarkTable(b, "benchmark-left", "left")
	rightTable, rightChanges := newTypedTableArrangementRecoveryJoinBenchmarkTable(b, "benchmark-right", "right")
	joinCatalog, err := hatSql.NewTypedTableJoinArrangements(leftTable, rightTable)
	if err != nil {
		b.Fatal(err)
	}
	join, err := joinCatalog.Acquire(hatSql.TypedTableJoinDefinition{LeftField: "team", RightField: "team"})
	if err != nil {
		b.Fatal(err)
	}
	if err := join.ApplyLeft(leftChanges); err != nil {
		b.Fatal(err)
	}
	if err := join.ApplyRight(rightChanges); err != nil {
		b.Fatal(err)
	}

	checkpoint, err := hatSql.CaptureTypedTableArrangementRecovery(
		[]*hatSql.TypedTableAggregateArrangements{aggregateCatalog},
		[]*hatSql.TypedTableJoinArrangements{joinCatalog},
	)
	if err != nil {
		b.Fatal(err)
	}
	return typedTableArrangementRecoveryBundleBenchmarkFixture{
		aggregateTable:   aggregateTable,
		leftTable:        leftTable,
		rightTable:       rightTable,
		aggregateCatalog: aggregateCatalog,
		joinCatalog:      joinCatalog,
		checkpoint:       checkpoint,
	}
}

func newTypedTableArrangementRecoveryAggregateBenchmarkTable(b *testing.B) (*hatSql.TypedTable, []hatSql.TypedTableChange) {
	b.Helper()
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: "benchmark-recovery-scores",
		Columns: []hatSql.TypedTableColumn{
			{Name: "team", Kind: hatSql.TypedTableString},
			{Name: "points", Kind: hatSql.TypedTableInt64},
			{Name: "name", Kind: hatSql.TypedTableString},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	changes := make([]hatSql.TypedTableChange, 0, 4096)
	for index := 0; index < 4096; index++ {
		change, err := table.Upsert(
			"row-"+strconv.Itoa(index),
			[]hatSql.TypedTableValue{
				hatSql.TypedString("team-" + strconv.Itoa(index%64)),
				hatSql.TypedInt64(int64(index % 997)),
				hatSql.TypedString("name-" + strconv.Itoa(index%1024)),
			},
		)
		if err != nil {
			b.Fatal(err)
		}
		changes = append(changes, change)
	}
	return table, changes
}

func newTypedTableArrangementRecoveryJoinBenchmarkTable(b *testing.B, name, prefix string) (*hatSql.TypedTable, []hatSql.TypedTableChange) {
	b.Helper()
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: name,
		Columns: []hatSql.TypedTableColumn{
			{Name: "team", Kind: hatSql.TypedTableString},
			{Name: "label", Kind: hatSql.TypedTableString},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	changes := make([]hatSql.TypedTableChange, 0, 1024)
	for index := 0; index < 1024; index++ {
		change, err := table.Upsert(
			prefix+"-"+strconv.Itoa(index),
			[]hatSql.TypedTableValue{
				hatSql.TypedString("team-" + strconv.Itoa(index%64)),
				hatSql.TypedString(prefix + "-label-" + strconv.Itoa(index)),
			},
		)
		if err != nil {
			b.Fatal(err)
		}
		changes = append(changes, change)
	}
	return table, changes
}
