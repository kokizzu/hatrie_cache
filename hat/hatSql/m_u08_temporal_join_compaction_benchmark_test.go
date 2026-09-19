package hatSql

import (
	"strconv"
	"testing"
)

func BenchmarkDifferentialTemporalJoinLoad(b *testing.B) {
	left, right := mU08TemporalJoinBenchmarkRows(1024)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		join, err := NewDifferentialTemporalJoin(DifferentialTemporalJoinDefinition{
			MaxTimeDistance: 4,
			LeftKey:         func(row SQLRow) string { return row["group"].(string) },
			RightKey:        func(row SQLRow) string { return row["group"].(string) },
		})
		if err != nil {
			b.Fatal(err)
		}
		if _, err := join.ApplyLeft(left); err != nil {
			b.Fatal(err)
		}
		if _, err := join.ApplyRight(right); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDifferentialTemporalJoinCompact(b *testing.B) {
	left, right := mU08TemporalJoinBenchmarkRows(1024)
	b.ReportAllocs()
	for range b.N {
		b.StopTimer()
		join, err := NewDifferentialTemporalJoin(DifferentialTemporalJoinDefinition{
			MaxTimeDistance: 4,
			LeftKey:         func(row SQLRow) string { return row["group"].(string) },
			RightKey:        func(row SQLRow) string { return row["group"].(string) },
		})
		if err != nil {
			b.Fatal(err)
		}
		if _, err := join.ApplyLeft(left); err != nil {
			b.Fatal(err)
		}
		if _, err := join.ApplyRight(right); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		stats, err := join.Compact(1024, 1024)
		if err != nil {
			b.Fatal(err)
		}
		if stats.RemovedLeft == 0 || stats.RemovedRight == 0 {
			b.Fatal("Compact() removed no rows")
		}
	}
}

func mU08TemporalJoinBenchmarkRows(count int) ([]DifferentialRow, []DifferentialRow) {
	left := make([]DifferentialRow, count)
	right := make([]DifferentialRow, count)
	for index := 0; index < count; index++ {
		group := strconv.Itoa(index)
		left[index] = DifferentialRow{
			Key:  "left-" + group,
			Time: uint64(index),
			Diff: 1,
			Row:  Row{"group": group, "value": index},
		}
		right[index] = DifferentialRow{
			Key:  "right-" + group,
			Time: uint64(index),
			Diff: 1,
			Row:  Row{"group": group, "value": index},
		}
	}
	return left, right
}
