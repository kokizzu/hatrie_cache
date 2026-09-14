package hatSql

import (
	"fmt"
	"testing"
)

type m030JoinBenchmarkRow struct {
	key   string
	group string
	value int64
}

func BenchmarkMZ030DifferentialJoinRebuildBaseline(b *testing.B) {
	const rows = 10000
	left := make([]m030JoinBenchmarkRow, rows)
	right := make([]m030JoinBenchmarkRow, rows)
	for index := 0; index < rows; index++ {
		group := fmt.Sprintf("group-%05d", index)
		left[index] = m030JoinBenchmarkRow{key: fmt.Sprintf("left-%05d", index), group: group, value: int64(index)}
		right[index] = m030JoinBenchmarkRow{key: fmt.Sprintf("right-%05d", index), group: group, value: int64(index * 2)}
	}
	b.ReportAllocs()
	b.ResetTimer()
	var checksum int64
	for iteration := 0; iteration < b.N; iteration++ {
		index := (iteration*7919 + 17) % rows
		left[index].value = int64((iteration*104729 + 23) % (rows * 4))
		rightByGroup := make(map[string]m030JoinBenchmarkRow, rows)
		for _, row := range right {
			rightByGroup[row.group] = row
		}
		joined := make([]DifferentialRow, 0, rows)
		for _, row := range left {
			matched, ok := rightByGroup[row.group]
			if !ok {
				continue
			}
			joined = append(joined, DifferentialRow{
				Key:  row.key + "\x00" + matched.key,
				Diff: 1,
				Row:  Row{"left_id": row.key, "right_id": matched.key, "group": row.group},
			})
		}
		checksum += int64(len(joined)) + left[index].value + right[index].value
	}
	b.StopTimer()
	b.ReportMetric(float64(checksum), "checksum")
}

func BenchmarkMZ030DifferentialJoinIncremental(b *testing.B) {
	const rows = 10000
	seed := make([]IncrementalJoinUpdate, 0, rows*2)
	for index := 0; index < rows; index++ {
		group := fmt.Sprintf("group-%05d", index)
		leftKey := fmt.Sprintf("left-%05d", index)
		rightKey := fmt.Sprintf("right-%05d", index)
		seed = append(seed,
			IncrementalJoinUpdate{Side: IncrementalJoinLeft, Row: DifferentialRow{Key: leftKey, Diff: 1, Row: Row{"id": leftKey, "group": group, "value": int64(index)}}},
			IncrementalJoinUpdate{Side: IncrementalJoinRight, Row: DifferentialRow{Key: rightKey, Diff: 1, Row: Row{"id": rightKey, "group": group, "value": int64(index * 2)}}},
		)
	}
	join, err := NewIncrementalJoin(IncrementalJoinDefinition{
		LeftKey:  m030JoinKey,
		RightKey: m030JoinKey,
		Merge:    m030JoinMerge,
	})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := join.Apply(seed); err != nil {
		b.Fatal(err)
	}
	updates := []IncrementalJoinUpdate{
		{Side: IncrementalJoinLeft, Row: DifferentialRow{Key: "left-00000", Diff: -1}},
		{Side: IncrementalJoinLeft, Row: DifferentialRow{Key: "left-00000", Diff: 1, Row: Row{"id": "left-00000", "group": "group-00000", "value": int64(0)}}},
	}
	b.ReportAllocs()
	b.ResetTimer()
	var checksum int64
	for iteration := 0; iteration < b.N; iteration++ {
		updates[1].Row.Row["value"] = int64((iteration*104729 + 23) % (rows * 4))
		output, err := join.Apply(updates)
		if err != nil {
			b.Fatal(err)
		}
		checksum += int64(len(output))
	}
	b.StopTimer()
	b.ReportMetric(float64(checksum), "checksum")
}
