package hatSql_test

import (
	"fmt"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkTypedTableJoinCoalescing(b *testing.B) {
	for _, benchmark := range []struct {
		name      string
		coalesced bool
	}{
		{name: "two_separate_updates"},
		{name: "one_coalesced_batch", coalesced: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				join, changes := newTypedTableJoinCoalescingBenchmark(b)
				b.StartTimer()
				var err error
				if benchmark.coalesced {
					err = join.ApplyLeft(changes)
				} else {
					err = join.ApplyLeft(changes[:1])
					if err == nil {
						err = join.ApplyLeft(changes[1:])
					}
				}
				if err != nil || len(join.Rows()) != 0 {
					b.Fatalf("ApplyLeft() = %v, rows = %d", err, len(join.Rows()))
				}
			}
		})
	}
}

func newTypedTableJoinCoalescingBenchmark(b *testing.B) (*hatSql.TypedTableJoin, []hatSql.TypedTableChange) {
	b.Helper()
	left, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{Name: "left", Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}}})
	if err != nil {
		b.Fatal(err)
	}
	right, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{Name: "right", Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}}})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := left.Upsert("a", []hatSql.TypedTableValue{hatSql.TypedString("blue")}); err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 1024; index++ {
		if _, err := right.Upsert(fmt.Sprintf("right-%d", index), []hatSql.TypedTableValue{hatSql.TypedString("red")}); err != nil {
			b.Fatal(err)
		}
	}
	join, err := hatSql.NewTypedTableJoin(left, right, hatSql.TypedTableJoinDefinition{LeftField: "team", RightField: "team"})
	if err != nil {
		b.Fatal(err)
	}
	first, err := left.Upsert("a", []hatSql.TypedTableValue{hatSql.TypedString("red")})
	if err != nil {
		b.Fatal(err)
	}
	last, err := left.Upsert("a", []hatSql.TypedTableValue{hatSql.TypedString("blue")})
	if err != nil {
		b.Fatal(err)
	}
	return join, []hatSql.TypedTableChange{first, last}
}
