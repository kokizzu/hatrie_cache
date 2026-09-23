package hatSql_test

import (
	"fmt"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkM215HighChurnJoinRowsBaseline(b *testing.B) {
	join := newM215HighChurnJoin(b)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		change := m215HighChurnChange(index + 1)
		if err := join.ApplyLeft([]hatSql.TypedTableChange{change}); err != nil {
			b.Fatal(err)
		}
		if got := join.Rows(); len(got) != m215HighChurnExpectedPairs {
			b.Fatalf("join rows = %d, want %d", len(got), m215HighChurnExpectedPairs)
		}
	}
}

func BenchmarkM215HighChurnJoinApplyBaseline(b *testing.B) {
	join := newM215HighChurnJoin(b)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := join.ApplyLeft([]hatSql.TypedTableChange{m215HighChurnChange(index + 1)}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkM215HighChurnJoinDeltas(b *testing.B) {
	join := newM215HighChurnJoin(b)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		deltas, err := join.ApplyLeftDeltas([]hatSql.TypedTableChange{m215HighChurnChange(index + 1)})
		if err != nil {
			b.Fatal(err)
		}
		if len(deltas) != 2*256 {
			b.Fatalf("join deltas = %d, want %d", len(deltas), 2*256)
		}
	}
}

const m215HighChurnExpectedPairs = 256 * 256
const m215HighChurnInitialSequence = 256

func newM215HighChurnJoin(tb testing.TB) *hatSql.TypedTableJoin {
	tb.Helper()
	left, right := newM215HighChurnTable(tb, "left"), newM215HighChurnTable(tb, "right")
	join, err := hatSql.NewTypedTableJoin(left, right, hatSql.TypedTableJoinDefinition{LeftField: "team", RightField: "team"})
	if err != nil {
		tb.Fatal(err)
	}
	return join
}

func newM215HighChurnTable(tb testing.TB, side string) *hatSql.TypedTable {
	tb.Helper()
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: "m215_high_churn_" + side,
		Columns: []hatSql.TypedTableColumn{
			{Name: "team", Kind: hatSql.TypedTableString},
			{Name: "value", Kind: hatSql.TypedTableInt64},
		},
	})
	if err != nil {
		tb.Fatal(err)
	}
	for index := 0; index < 256; index++ {
		if _, err := table.Upsert(fmt.Sprintf("%s-%03d", side, index), []hatSql.TypedTableValue{
			hatSql.TypedString("hot"),
			hatSql.TypedInt64(int64(index)),
		}); err != nil {
			tb.Fatal(err)
		}
	}
	return table
}

func m215HighChurnChange(sequence int) hatSql.TypedTableChange {
	return hatSql.TypedTableChange{
		Sequence:  uint64(m215HighChurnInitialSequence + sequence),
		Operation: "UPDATE",
		Key:       "left-000",
		Before:    []hatSql.TypedTableValue{hatSql.TypedString("hot"), hatSql.TypedInt64(int64(sequence - 1))},
		After:     []hatSql.TypedTableValue{hatSql.TypedString("hot"), hatSql.TypedInt64(int64(sequence))},
	}
}
