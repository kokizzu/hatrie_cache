package hatSql_test

import (
	"fmt"
	"sort"
	"testing"

	"hatrie_cache/hat/hatSql"
)

var typedTableSortedArrangementBenchmarkSink int

func BenchmarkTypedTableSortedArrangementApply(b *testing.B) {
	table := newSortedBenchmarkTable(b)
	arrangement, err := hatSql.NewTypedTableSortedArrangement(table, hatSql.TypedTableSortedArrangementDefinition{Field: "team"})
	if err != nil {
		b.Fatal(err)
	}
	changes := sortedArrangementBenchmarkChanges(256)
	b.ResetTimer()
	for iteration := range b.N {
		for index := range changes {
			changes[index].Sequence = uint64(4097 + iteration*len(changes) + index)
		}
		if err := arrangement.Apply(changes); err != nil {
			b.Fatal(err)
		}
		typedTableSortedArrangementBenchmarkSink += int(arrangement.Checkpoint())
	}
}

func BenchmarkTypedTableSortedArrangementRebuild(b *testing.B) {
	rows := makeSortedBenchmarkRows()
	changes := sortedArrangementBenchmarkChanges(256)
	positions := make(map[string]int, len(rows))
	for index, row := range rows {
		positions[row.Key] = index
	}
	b.ResetTimer()
	for range b.N {
		for _, change := range changes {
			rows[positions[change.Key]] = hatSql.TypedTableMergeJoinInput{Key: change.Key, Values: change.After}
		}
		sort.Slice(rows, func(left, right int) bool {
			leftTeam := rows[left].Values[0].String
			rightTeam := rows[right].Values[0].String
			if leftTeam != rightTeam {
				return leftTeam < rightTeam
			}
			return rows[left].Key < rows[right].Key
		})
		for index, row := range rows {
			positions[row.Key] = index
		}
		typedTableSortedArrangementBenchmarkSink += len(rows)
	}
}

func BenchmarkTypedTableSortedArrangementSingleApply(b *testing.B) {
	table := newSortedBenchmarkTable(b)
	arrangement, err := hatSql.NewTypedTableSortedArrangement(table, hatSql.TypedTableSortedArrangementDefinition{Field: "team"})
	if err != nil {
		b.Fatal(err)
	}
	changes := sortedArrangementBenchmarkChanges(1)
	b.ResetTimer()
	for iteration := range b.N {
		changes[0].Sequence = uint64(4097 + iteration)
		if err := arrangement.Apply(changes); err != nil {
			b.Fatal(err)
		}
		typedTableSortedArrangementBenchmarkSink += int(arrangement.Checkpoint())
	}
}

func BenchmarkTypedTableSortedArrangementSingleRebuild(b *testing.B) {
	rows := makeSortedBenchmarkRows()
	changes := sortedArrangementBenchmarkChanges(1)
	positions := make(map[string]int, len(rows))
	for index, row := range rows {
		positions[row.Key] = index
	}
	b.ResetTimer()
	for range b.N {
		for _, change := range changes {
			rows[positions[change.Key]] = hatSql.TypedTableMergeJoinInput{Key: change.Key, Values: change.After}
		}
		sort.Slice(rows, func(left, right int) bool {
			leftTeam := rows[left].Values[0].String
			rightTeam := rows[right].Values[0].String
			if leftTeam != rightTeam {
				return leftTeam < rightTeam
			}
			return rows[left].Key < rows[right].Key
		})
		for index, row := range rows {
			positions[row.Key] = index
		}
		typedTableSortedArrangementBenchmarkSink += len(rows)
	}
}

func BenchmarkTypedTableSortedArrangementRows(b *testing.B) {
	table := newSortedBenchmarkTable(b)
	arrangement, err := hatSql.NewTypedTableSortedArrangement(table, hatSql.TypedTableSortedArrangementDefinition{Field: "team"})
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for range b.N {
		typedTableSortedArrangementBenchmarkSink += len(arrangement.Rows())
	}
}

func BenchmarkTypedTableSortedArrangementRowsPage(b *testing.B) {
	table := newSortedBenchmarkTable(b)
	arrangement, err := hatSql.NewTypedTableSortedArrangement(table, hatSql.TypedTableSortedArrangementDefinition{Field: "team"})
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for range b.N {
		typedTableSortedArrangementBenchmarkSink += len(arrangement.RowsPage(0, 10))
	}
}

func makeSortedBenchmarkRows() []hatSql.TypedTableMergeJoinInput {
	rows := make([]hatSql.TypedTableMergeJoinInput, 4096)
	for index := range rows {
		rows[index] = hatSql.TypedTableMergeJoinInput{
			Key:    fmt.Sprintf("key-%05d", index),
			Values: []hatSql.TypedTableValue{hatSql.TypedString(fmt.Sprintf("team-%05d", index)), hatSql.TypedInt64(int64(index))},
		}
	}
	return rows
}

func sortedArrangementBenchmarkChanges(count int) []hatSql.TypedTableChange {
	changes := make([]hatSql.TypedTableChange, count)
	for index := range changes {
		changes[index] = hatSql.TypedTableChange{
			Sequence:  uint64(4097 + index),
			Operation: "UPDATE",
			Key:       fmt.Sprintf("key-%05d", index),
			After:     []hatSql.TypedTableValue{hatSql.TypedString(fmt.Sprintf("team-%05d", 8192-index)), hatSql.TypedInt64(int64(index))},
		}
	}
	return changes
}

func newSortedBenchmarkTable(b *testing.B) *hatSql.TypedTable {
	b.Helper()
	table := newSortedArrangementTable(b, "sorted_benchmark")
	for index := range 4096 {
		key := fmt.Sprintf("key-%05d", index)
		if _, err := table.Upsert(key, []hatSql.TypedTableValue{hatSql.TypedString(fmt.Sprintf("team-%05d", index)), hatSql.TypedInt64(int64(index))}); err != nil {
			b.Fatal(err)
		}
	}
	return table
}
