package hatSql_test

import (
	"fmt"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTypedTableJoinSemijoinReductionRehydratesAndDemotesRows(t *testing.T) {
	left, right := newSemijoinTestTables(t)
	if _, err := left.Upsert("l-red", []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedString("left-red")}); err != nil {
		t.Fatal(err)
	}
	if _, err := left.Upsert("l-blue", []hatSql.TypedTableValue{hatSql.TypedString("blue"), hatSql.TypedString("left-blue")}); err != nil {
		t.Fatal(err)
	}
	if _, err := right.Upsert("r-red", []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedString("right-red")}); err != nil {
		t.Fatal(err)
	}
	if _, err := right.Upsert("r-green", []hatSql.TypedTableValue{hatSql.TypedString("green"), hatSql.TypedString("right-green")}); err != nil {
		t.Fatal(err)
	}

	join, err := hatSql.NewTypedTableJoinWithOptions(left, right, semijoinDefinition(), hatSql.TypedTableJoinOptions{SemijoinReduction: true})
	if err != nil {
		t.Fatal(err)
	}
	assertTypedTableJoinPairs(t, join.Rows(), "l-red/r-red")
	assertSemijoinStats(t, join.Stats(), 1, 1, 1, 1)

	change, err := right.Upsert("r-blue", []hatSql.TypedTableValue{hatSql.TypedString("blue"), hatSql.TypedString("right-blue")})
	if err != nil {
		t.Fatal(err)
	}
	if err := join.ApplyRight([]hatSql.TypedTableChange{change}); err != nil {
		t.Fatal(err)
	}
	assertTypedTableJoinPairs(t, join.Rows(), "l-blue/r-blue", "l-red/r-red")
	assertSemijoinStats(t, join.Stats(), 2, 2, 0, 1)

	change, err = right.Delete("r-blue")
	if err != nil {
		t.Fatal(err)
	}
	if err := join.ApplyRight([]hatSql.TypedTableChange{change}); err != nil {
		t.Fatal(err)
	}
	assertTypedTableJoinPairs(t, join.Rows(), "l-red/r-red")
	assertSemijoinStats(t, join.Stats(), 1, 1, 1, 1)

	change, err = left.Upsert("l-blue", []hatSql.TypedTableValue{hatSql.TypedString("green"), hatSql.TypedString("left-green")})
	if err != nil {
		t.Fatal(err)
	}
	if err := join.ApplyLeft([]hatSql.TypedTableChange{change}); err != nil {
		t.Fatal(err)
	}
	assertTypedTableJoinPairs(t, join.Rows(), "l-blue/r-green", "l-red/r-red")
	assertSemijoinStats(t, join.Stats(), 2, 2, 0, 0)
}

func TestTypedTableJoinSemijoinReductionDoesNotReadUnappliedSourceUpdates(t *testing.T) {
	left, right := newSemijoinTestTables(t)
	if _, err := left.Upsert("l-blue", []hatSql.TypedTableValue{hatSql.TypedString("blue"), hatSql.TypedString("left-blue")}); err != nil {
		t.Fatal(err)
	}
	join, err := hatSql.NewTypedTableJoinWithOptions(left, right, semijoinDefinition(), hatSql.TypedTableJoinOptions{SemijoinReduction: true})
	if err != nil {
		t.Fatal(err)
	}

	leftChange, err := left.Upsert("l-blue", []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedString("left-red")})
	if err != nil {
		t.Fatal(err)
	}
	rightChange, err := right.Upsert("r-red", []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedString("right-red")})
	if err != nil {
		t.Fatal(err)
	}
	if err := join.ApplyRight([]hatSql.TypedTableChange{rightChange}); err != nil {
		t.Fatal(err)
	}
	assertTypedTableJoinPairs(t, join.Rows())

	if err := join.ApplyLeft([]hatSql.TypedTableChange{leftChange}); err != nil {
		t.Fatal(err)
	}
	assertTypedTableJoinPairs(t, join.Rows(), "l-blue/r-red")
}

func TestTypedTableJoinSemijoinReductionRetriesPendingRowsAfterCheckpointAdvance(t *testing.T) {
	left, right := newSemijoinTestTables(t)
	for _, key := range []string{"l-one", "l-two"} {
		if _, err := left.Upsert(key, []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedString(key)}); err != nil {
			t.Fatal(err)
		}
	}
	join, err := hatSql.NewTypedTableJoinWithOptions(left, right, semijoinDefinition(), hatSql.TypedTableJoinOptions{SemijoinReduction: true})
	if err != nil {
		t.Fatal(err)
	}

	leftChange, err := left.Upsert("l-one", []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedString("updated")})
	if err != nil {
		t.Fatal(err)
	}
	rightChange, err := right.Upsert("r-one", []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedString("right")})
	if err != nil {
		t.Fatal(err)
	}
	if err := join.ApplyRight([]hatSql.TypedTableChange{rightChange}); err != nil {
		t.Fatal(err)
	}
	assertTypedTableJoinPairs(t, join.Rows())

	if err := join.ApplyLeft([]hatSql.TypedTableChange{leftChange}); err != nil {
		t.Fatal(err)
	}
	assertTypedTableJoinPairs(t, join.Rows(), "l-one/r-one", "l-two/r-one")
	assertSemijoinStats(t, join.Stats(), 2, 1, 0, 0)
}

func TestTypedTableJoinArrangementsPropagateSemijoinOptions(t *testing.T) {
	left, right := newSemijoinTestTables(t)
	if _, err := left.Upsert("l-red", []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedString("left")}); err != nil {
		t.Fatal(err)
	}
	arrangements, err := hatSql.NewTypedTableJoinArrangementsWithOptions(left, right, hatSql.TypedTableJoinOptions{SemijoinReduction: true})
	if err != nil {
		t.Fatal(err)
	}
	arrangement, err := arrangements.Acquire(semijoinDefinition())
	if err != nil {
		t.Fatal(err)
	}
	defer arrangement.Release()
	stats, err := arrangement.Stats()
	if err != nil {
		t.Fatal(err)
	}
	assertSemijoinStats(t, stats, 0, 0, 1, 0)
}

func TestTypedTableJoinDefaultKeepsFullRowRetention(t *testing.T) {
	left, right := newSemijoinTestTables(t)
	if _, err := left.Upsert("l-blue", []hatSql.TypedTableValue{hatSql.TypedString("blue"), hatSql.TypedString("left")}); err != nil {
		t.Fatal(err)
	}
	join, err := hatSql.NewTypedTableJoin(left, right, semijoinDefinition())
	if err != nil {
		t.Fatal(err)
	}
	stats := join.Stats()
	if stats.SemijoinReduction || stats.LeftRows != 1 || stats.RightRows != 0 || stats.LeftPending != 0 || stats.RightPending != 0 {
		t.Fatalf("default join stats = %#v, want one retained left row and no pending rows", stats)
	}
}

func newSemijoinTestTables(t testing.TB) (*hatSql.TypedTable, *hatSql.TypedTable) {
	t.Helper()
	schema := func(name string) hatSql.TypedTableSchema {
		return hatSql.TypedTableSchema{
			Name: name,
			Columns: []hatSql.TypedTableColumn{
				{Name: "team", Kind: hatSql.TypedTableString},
				{Name: "payload", Kind: hatSql.TypedTableString},
			},
		}
	}
	left, err := hatSql.NewTypedTable(schema("semijoin-left"))
	if err != nil {
		t.Fatal(err)
	}
	right, err := hatSql.NewTypedTable(schema("semijoin-right"))
	if err != nil {
		t.Fatal(err)
	}
	return left, right
}

func semijoinDefinition() hatSql.TypedTableJoinDefinition {
	return hatSql.TypedTableJoinDefinition{LeftField: "team", RightField: "team"}
}

func assertSemijoinStats(t *testing.T, stats hatSql.TypedTableJoinStats, leftRows, rightRows, leftPending, rightPending int) {
	t.Helper()
	if !stats.SemijoinReduction {
		t.Fatal("semijoin reduction is not enabled")
	}
	if stats.LeftRows != leftRows || stats.RightRows != rightRows || stats.LeftPending != leftPending || stats.RightPending != rightPending {
		t.Fatalf("semijoin stats = %#v, want rows=%d/%d pending=%d/%d", stats, leftRows, rightRows, leftPending, rightPending)
	}
}

func BenchmarkTypedTableJoinSemijoinConstruction(b *testing.B) {
	left, right := newSemijoinBenchmarkTables(b)
	b.Run("default", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			join, err := hatSql.NewTypedTableJoin(left, right, semijoinDefinition())
			if err != nil {
				b.Fatal(err)
			}
			if stats := join.Stats(); stats.LeftRows != 4096 || stats.RightRows != 4096 {
				b.Fatalf("default stats = %#v", stats)
			}
		}
	})
	b.Run("semijoin", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			join, err := hatSql.NewTypedTableJoinWithOptions(left, right, semijoinDefinition(), hatSql.TypedTableJoinOptions{SemijoinReduction: true})
			if err != nil {
				b.Fatal(err)
			}
			if stats := join.Stats(); stats.LeftRows != 0 || stats.RightRows != 0 || stats.LeftPending != 4096 || stats.RightPending != 4096 {
				b.Fatalf("semijoin stats = %#v", stats)
			}
		}
	})
}

func newSemijoinBenchmarkTables(b *testing.B) (*hatSql.TypedTable, *hatSql.TypedTable) {
	b.Helper()
	left, right := newSemijoinTestTables(b)
	for index := range 4096 {
		leftKey := "left-team-" + benchmarkNumber(index)
		rightKey := "right-team-" + benchmarkNumber(index)
		if _, err := left.Upsert(leftKey, []hatSql.TypedTableValue{hatSql.TypedString(leftKey), hatSql.TypedString("left-payload-" + benchmarkNumber(index))}); err != nil {
			b.Fatal(err)
		}
		if _, err := right.Upsert(rightKey, []hatSql.TypedTableValue{hatSql.TypedString(rightKey), hatSql.TypedString("right-payload-" + benchmarkNumber(index))}); err != nil {
			b.Fatal(err)
		}
	}
	return left, right
}

func benchmarkNumber(value int) string {
	return fmt.Sprintf("%05d", value)
}
