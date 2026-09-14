package hatSql

import (
	"fmt"
	"testing"
)

type m029BenchmarkIntervalRow struct {
	key   string
	group string
	start int64
	end   int64
}

func BenchmarkMZ029IncrementalIntervalJoinRebuildBaseline(b *testing.B) {
	left, right := m029BenchmarkIntervalRows()
	b.ReportAllocs()
	b.ResetTimer()
	var checksum int64
	for operation := 0; operation < b.N; operation++ {
		rightByGroup := make(map[string][]m029BenchmarkIntervalRow, 100)
		for _, row := range right {
			rightByGroup[row.group] = append(rightByGroup[row.group], row)
		}
		joined := 0
		for _, leftRow := range left {
			for _, rightRow := range rightByGroup[leftRow.group] {
				if intervalsOverlap(leftRow.start, leftRow.end, rightRow.start, rightRow.end) {
					joined++
					checksum += int64(len(leftRow.key) + len(rightRow.key))
				}
			}
		}
		checksum += int64(joined)
	}
	b.StopTimer()
	if checksum == 0 {
		b.Fatal("unexpected empty benchmark checksum")
	}
}

func BenchmarkMZ029IncrementalIntervalJoinIncremental(b *testing.B) {
	join, err := NewIncrementalIntervalJoin(m029BenchmarkIntervalJoinDefinition())
	if err != nil {
		b.Fatal(err)
	}
	left, right := m029BenchmarkIntervalRows()
	seed := make([]IncrementalIntervalJoinUpdate, 0, len(left)+len(right))
	for _, row := range left {
		seed = append(seed, IncrementalIntervalJoinUpdate{
			Side: IncrementalIntervalJoinLeft,
			Row:  DifferentialRow{Key: row.key, Diff: 1, Row: m029BenchmarkRowToSQLRow(row)},
		})
	}
	for _, row := range right {
		seed = append(seed, IncrementalIntervalJoinUpdate{
			Side: IncrementalIntervalJoinRight,
			Row:  DifferentialRow{Key: row.key, Diff: 1, Row: m029BenchmarkRowToSQLRow(row)},
		})
	}
	if _, err = join.Apply(seed); err != nil {
		b.Fatal(err)
	}
	oldRow := m029BenchmarkRowToSQLRow(left[0])
	newRow := cloneIncrementalIntervalJoinRow(oldRow)
	newRow["id"] = "replacement"
	replacement := []IncrementalIntervalJoinUpdate{
		{Side: IncrementalIntervalJoinLeft, Row: DifferentialRow{Key: left[0].key, Diff: -1}},
		{Side: IncrementalIntervalJoinLeft, Row: DifferentialRow{Key: left[0].key, Diff: 1, Row: newRow}},
	}
	if _, err = join.Apply(replacement); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	var checksum int64
	for operation := 0; operation < b.N; operation++ {
		deltas, err := join.Apply(replacement)
		if err != nil {
			b.Fatal(err)
		}
		for _, delta := range deltas {
			checksum += int64(len(delta.Key)) + delta.Diff
		}
	}
	b.StopTimer()
	if checksum == 0 {
		b.Fatal("unexpected empty benchmark checksum")
	}
}

func m029BenchmarkIntervalRows() ([]m029BenchmarkIntervalRow, []m029BenchmarkIntervalRow) {
	left := make([]m029BenchmarkIntervalRow, 0, 10000)
	right := make([]m029BenchmarkIntervalRow, 0, 10000)
	for group := 0; group < 100; group++ {
		for index := 0; index < 100; index++ {
			start := int64(index * 2)
			left = append(left, m029BenchmarkIntervalRow{
				key: fmt.Sprintf("l-%05d", group*100+index), group: fmt.Sprintf("g-%02d", group), start: start, end: start + 3,
			})
			right = append(right, m029BenchmarkIntervalRow{
				key: fmt.Sprintf("r-%05d", group*100+index), group: fmt.Sprintf("g-%02d", group), start: start + 1, end: start + 4,
			})
		}
	}
	return left, right
}

func m029BenchmarkIntervalJoinDefinition() IncrementalIntervalJoinDefinition {
	return IncrementalIntervalJoinDefinition{
		LeftKey:       m029BenchmarkIntervalJoinKey,
		RightKey:      m029BenchmarkIntervalJoinKey,
		LeftInterval:  m029BenchmarkIntervalJoinBounds,
		RightInterval: m029BenchmarkIntervalJoinBounds,
		Merge: func(left, right Row) (Row, error) {
			return Row{"left": left["id"], "right": right["id"]}, nil
		},
	}
}

func m029BenchmarkIntervalJoinKey(row Row) (string, error) {
	return row["group"].(string), nil
}

func m029BenchmarkIntervalJoinBounds(row Row) (int64, int64, error) {
	return row["start"].(int64), row["end"].(int64), nil
}

func m029BenchmarkRowToSQLRow(row m029BenchmarkIntervalRow) Row {
	return Row{"id": row.key, "group": row.group, "start": row.start, "end": row.end}
}
