package hatSql

import (
	"fmt"
	"sort"
	"testing"
)

func BenchmarkM065DifferentialRowNumberLagTail(b *testing.B) {
	seed := m065BenchmarkSeed(1024)
	window, err := NewDifferentialRowNumberLagWindow(DifferentialRowNumberLagWindowOptions{Lag: 1, MaxRows: len(seed) + 1})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := window.Apply(seed); err != nil {
		b.Fatal(err)
	}
	insert := DifferentialRow{Key: "tail", Time: uint64(len(seed) + 1), Diff: 1, Row: Row{"value": "tail"}}
	retract := insert
	retract.Diff = -1
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := window.Apply([]DifferentialRow{insert}); err != nil {
			b.Fatal(err)
		}
		if _, err := window.Apply([]DifferentialRow{retract}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkM065FullRebuildCorrectionTail(b *testing.B) {
	seed := m065BenchmarkSeed(1024)
	insert := DifferentialRow{Key: "tail", Time: uint64(len(seed) + 1), Diff: 1, Row: Row{"value": "tail"}}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		withTail := append(append([]DifferentialRow(nil), seed...), insert)
		m065FullRebuildCorrections(seed, withTail, 1)
		m065FullRebuildCorrections(withTail, seed, 1)
	}
}

func BenchmarkM065DifferentialRowNumberLagFront(b *testing.B) {
	seed := m065BenchmarkSeed(1024)
	window, err := NewDifferentialRowNumberLagWindow(DifferentialRowNumberLagWindowOptions{Lag: 1, MaxRows: len(seed) + 1})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := window.Apply(seed); err != nil {
		b.Fatal(err)
	}
	insert := DifferentialRow{Key: "front", Time: 0, Diff: 1, Row: Row{"value": "front"}}
	retract := insert
	retract.Diff = -1
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := window.Apply([]DifferentialRow{insert}); err != nil {
			b.Fatal(err)
		}
		if _, err := window.Apply([]DifferentialRow{retract}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkM065FullRebuildCorrectionFront(b *testing.B) {
	seed := m065BenchmarkSeed(1024)
	insert := DifferentialRow{Key: "front", Time: 0, Diff: 1, Row: Row{"value": "front"}}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		withFront := append([]DifferentialRow{insert}, seed...)
		m065FullRebuildCorrections(seed, withFront, 1)
		m065FullRebuildCorrections(withFront, seed, 1)
	}
}

func m065BenchmarkSeed(size int) []DifferentialRow {
	rows := make([]DifferentialRow, size)
	for index := range rows {
		rows[index] = DifferentialRow{
			Key:  fmt.Sprintf("key-%04d", index),
			Time: uint64(index + 1),
			Diff: 1,
			Row:  Row{"value": index},
		}
	}
	return rows
}

func m065FullRebuild(updates []DifferentialRow, lag int) []DifferentialRowNumberLagRow {
	rows := append([]DifferentialRow(nil), updates...)
	sort.Slice(rows, func(left, right int) bool {
		if rows[left].Time != rows[right].Time {
			return rows[left].Time < rows[right].Time
		}
		return rows[left].Key < rows[right].Key
	})
	output := make([]DifferentialRowNumberLagRow, 0, len(rows))
	for index, row := range rows {
		var lagRow SQLRow
		hasLag := lag > 0 && index >= lag
		if hasLag {
			lagRow = cloneDifferentialRowNumberLagSQLRow(rows[index-lag].Row)
		}
		output = append(output, DifferentialRowNumberLagRow{
			Key:       row.Key,
			Time:      row.Time,
			Diff:      1,
			RowNumber: uint64(index + 1),
			Row:       cloneDifferentialRowNumberLagSQLRow(row.Row),
			HasLag:    hasLag,
			LagRow:    lagRow,
		})
	}
	return output
}

func m065FullRebuildCorrections(before, after []DifferentialRow, lag int) []DifferentialRowNumberLagRow {
	return differentialRowNumberLagCorrections(
		map[string][]DifferentialRowNumberLagRow{"": m065FullRebuild(before, lag)},
		map[string][]DifferentialRowNumberLagRow{"": m065FullRebuild(after, lag)},
	)
}
