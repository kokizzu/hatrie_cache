package hatSql_test

import (
	"fmt"
	"testing"

	"hatrie_cache/hat/hatSql"
)

var typedTableMergeJoinBenchmarkMatches int
var typedTableHashJoinBenchmarkMatches int

func BenchmarkMergeSortedTypedTableJoin(b *testing.B) {
	left := make([]hatSql.TypedTableMergeJoinInput, 4096)
	right := make([]hatSql.TypedTableMergeJoinInput, 4096)
	for index := range left {
		key := fmt.Sprintf("%05d", index)
		left[index] = hatSql.TypedTableMergeJoinInput{Key: "l" + key, Values: []hatSql.TypedTableValue{hatSql.TypedString(key)}}
		right[index] = hatSql.TypedTableMergeJoinInput{Key: "r" + key, Values: []hatSql.TypedTableValue{hatSql.TypedString(key)}}
	}

	b.ResetTimer()
	for range b.N {
		matches := 0
		err := hatSql.MergeSortedTypedTableJoin(left, right, 0, 0, func(hatSql.TypedTableJoinRow) error {
			matches++
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
		typedTableMergeJoinBenchmarkMatches += matches
	}
}

func BenchmarkTypedTableJoinRows(b *testing.B) {
	leftTable, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name:    "merge_benchmark_left",
		Columns: []hatSql.TypedTableColumn{{Name: "join_key", Kind: hatSql.TypedTableString}},
	})
	if err != nil {
		b.Fatal(err)
	}
	rightTable, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name:    "merge_benchmark_right",
		Columns: []hatSql.TypedTableColumn{{Name: "join_key", Kind: hatSql.TypedTableString}},
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := range 4096 {
		key := fmt.Sprintf("%05d", index)
		if _, err := leftTable.Upsert("l"+key, []hatSql.TypedTableValue{hatSql.TypedString(key)}); err != nil {
			b.Fatal(err)
		}
		if _, err := rightTable.Upsert("r"+key, []hatSql.TypedTableValue{hatSql.TypedString(key)}); err != nil {
			b.Fatal(err)
		}
	}
	join, err := hatSql.NewTypedTableJoin(leftTable, rightTable, hatSql.TypedTableJoinDefinition{LeftField: "join_key", RightField: "join_key"})
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for range b.N {
		rows := join.Rows()
		if len(rows) != 4096 {
			b.Fatalf("join rows = %d, want 4096", len(rows))
		}
		typedTableHashJoinBenchmarkMatches += len(rows)
	}
}
