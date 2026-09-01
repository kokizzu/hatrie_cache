package hatSql_test

import (
	"strconv"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkTypedTableJoin(b *testing.B) {
	left, right := newTypedTableJoinBenchmarkTables(b)
	definition := hatSql.TypedTableJoinDefinition{LeftField: "team", RightField: "team"}
	b.Run("incremental_one_left_update", func(b *testing.B) {
		join, err := hatSql.NewTypedTableJoin(left, right, definition)
		if err != nil { b.Fatal(err) }
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			change, err := left.Upsert("left-0", []hatSql.TypedTableValue{hatSql.TypedString("team-0"), hatSql.TypedInt64(int64(index))})
			if err != nil { b.Fatal(err) }
			if err := join.ApplyLeft([]hatSql.TypedTableChange{change}); err != nil { b.Fatal(err) }
		}
	})
	b.Run("full_rebuild_after_one_left_update", func(b *testing.B) {
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if _, err := left.Upsert("left-0", []hatSql.TypedTableValue{hatSql.TypedString("team-0"), hatSql.TypedInt64(int64(index))}); err != nil { b.Fatal(err) }
			join, err := hatSql.NewTypedTableJoin(left, right, definition)
			if err != nil || len(join.Rows()) == 0 { b.Fatalf("NewTypedTableJoin() = %#v, %v", join, err) }
		}
	})
}

func newTypedTableJoinBenchmarkTables(b *testing.B) (*hatSql.TypedTable, *hatSql.TypedTable) {
	b.Helper()
	left, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{Name: "left", Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}, {Name: "score", Kind: hatSql.TypedTableInt64}}})
	if err != nil { b.Fatal(err) }
	right, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{Name: "right", Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}, {Name: "name", Kind: hatSql.TypedTableString}}})
	if err != nil { b.Fatal(err) }
	for index := 0; index < typedTableBenchmarkRows; index++ {
		team := "team-" + strconv.Itoa(index%64)
		if _, err := left.Upsert("left-"+strconv.Itoa(index), []hatSql.TypedTableValue{hatSql.TypedString(team), hatSql.TypedInt64(int64(index))}); err != nil { b.Fatal(err) }
		if _, err := right.Upsert("right-"+strconv.Itoa(index), []hatSql.TypedTableValue{hatSql.TypedString(team), hatSql.TypedString("name-" + strconv.Itoa(index))}); err != nil { b.Fatal(err) }
	}
	return left, right
}
