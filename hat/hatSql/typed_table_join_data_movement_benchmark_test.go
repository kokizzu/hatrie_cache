package hatSql_test

import (
	"strconv"
	"testing"

	"hatrie_cache/hat/hatSql"
)

const typedTableJoinDataMovementBenchmarkChanges = 4096

var typedTableJoinDataMovementBenchmarkSink hatSql.TypedTableJoinDataMovement

func BenchmarkTypedTableJoinDataMovement(b *testing.B) {
	fixture := newTypedTableJoinDataMovementBenchmarkFixture(b)
	for _, benchmark := range []struct {
		name    string
		options hatSql.TypedTableJoinOptions
	}{
		{name: "disabled"},
		{name: "enabled", options: hatSql.TypedTableJoinOptions{TrackDataMovement: true}},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				join, err := hatSql.NewTypedTableJoinWithOptions(
					fixture.left,
					fixture.right,
					hatSql.TypedTableJoinDefinition{LeftField: "team", RightField: "team"},
					benchmark.options,
				)
				if err != nil {
					b.Fatal(err)
				}
				if err := join.ApplyLeft(fixture.changes); err != nil {
					b.Fatal(err)
				}
				typedTableJoinDataMovementBenchmarkSink = join.DataMovement()
			}
		})
	}
}

type typedTableJoinDataMovementBenchmarkFixture struct {
	left, right *hatSql.TypedTable
	changes     []hatSql.TypedTableChange
}

func newTypedTableJoinDataMovementBenchmarkFixture(b *testing.B) typedTableJoinDataMovementBenchmarkFixture {
	b.Helper()
	left := newDataMovementJoinTable(b, "benchmark-left")
	right := newDataMovementJoinTable(b, "benchmark-right")
	changeTable := newDataMovementJoinTable(b, "benchmark-changes")
	changes := make([]hatSql.TypedTableChange, 0, typedTableJoinDataMovementBenchmarkChanges)
	for index := 0; index < typedTableJoinDataMovementBenchmarkChanges; index++ {
		change, err := changeTable.Upsert(
			"row-"+strconv.Itoa(index),
			[]hatSql.TypedTableValue{
				hatSql.TypedString("team-" + strconv.Itoa(index%256)),
				hatSql.TypedInt64(int64(index)),
			},
		)
		if err != nil {
			b.Fatal(err)
		}
		changes = append(changes, change)
	}
	return typedTableJoinDataMovementBenchmarkFixture{left: left, right: right, changes: changes}
}
